#!/usr/bin/env python3
"""本地 solana-test-validator + Yellowstone gRPC 插件：HTTP 轮询 vs WebSocket vs gRPC 三路同窗对比。
只用本地链与本地生成的一次性测试密钥，不涉及主网资金。
用法: bench_solana_local_3way.py <commitment processed|confirmed> <n_tx> <out.json>
"""
import asyncio, json, sys, time, base58, statistics, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'grpc-local', 'gen'))
import grpc, aiohttp, websockets
import geyser_pb2 as g, geyser_pb2_grpc as gg
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.system_program import transfer, TransferParams
from solders.transaction import Transaction
from solders.message import Message
from solders.hash import Hash

RPC = "http://127.0.0.1:8899"; WS = "ws://127.0.0.1:8900"; GRPC = "127.0.0.1:10000"
COMMIT = sys.argv[1]; N = int(sys.argv[2]); OUT = sys.argv[3]
now = lambda: time.time() * 1000
seen = {"grpc": {}, "ws": {}, "http": {}}
slots = {"grpc": {}, "ws": {}, "http": {}}
sent = {}
stop = asyncio.Event()

async def rpc(s, method, params):
    async with s.post(RPC, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params}) as r:
        j = await r.json()
        if "error" in j: raise RuntimeError(j["error"])
        return j["result"]

async def grpc_task(dest, ready):
    lvl = {"processed": g.PROCESSED, "confirmed": g.CONFIRMED}[COMMIT]
    async with grpc.aio.insecure_channel(GRPC) as ch:
        stub = gg.GeyserStub(ch)
        q = asyncio.Queue()
        req = g.SubscribeRequest(commitment=lvl)
        req.transactions["dest"].CopyFrom(g.SubscribeRequestFilterTransactions(vote=False, failed=False, account_include=[dest]))
        req.slots["s"].CopyFrom(g.SubscribeRequestFilterSlots(filter_by_commitment=False, interslot_updates=True))
        await q.put(req)
        async def reqs():
            while True:
                yield await q.get()
        call = stub.Subscribe(reqs())
        ready.set()
        async for u in call:
            t = now()
            k = u.WhichOneof("update_oneof")
            if k == "transaction":
                sig = base58.b58encode(u.transaction.transaction.signature).decode()
                seen["grpc"].setdefault(sig, t)
            elif k == "slot":
                st = g.SlotStatus.Name(u.slot.status)
                slots.setdefault("grpc_" + st, {}).setdefault(u.slot.slot, t)
            if stop.is_set(): call.cancel(); return

async def ws_task(dest, ready):
    async with websockets.connect(WS, max_size=None) as w:
        await w.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "logsSubscribe", "params": [{"mentions": [dest]}, {"commitment": COMMIT}]}))
        await w.send(json.dumps({"jsonrpc": "2.0", "id": 2, "method": "slotSubscribe"}))
        ready.set()
        while not stop.is_set():
            try: m = json.loads(await asyncio.wait_for(w.recv(), 1))
            except asyncio.TimeoutError: continue
            t = now()
            if m.get("method") == "logsNotification":
                seen["ws"].setdefault(m["params"]["result"]["value"]["signature"], t)
            elif m.get("method") == "slotNotification":
                # slotSubscribe 只推 processed 级别的新 slot
                slots["ws"].setdefault(m["params"]["result"]["slot"], t)

async def http_task(s, dest, ready, interval=0.1):
    # getSignaturesForAddress 最低只支持 confirmed；processed 档也只能用 confirmed 查
    c = "confirmed"
    ready.set(); i = 0
    while not stop.is_set():
        t0 = now()
        try:
            if i % 2 == 0:
                for r in await rpc(s, "getSignaturesForAddress", [dest, {"limit": 50, "commitment": c}]):
                    seen["http"].setdefault(r["signature"], now())
            else:
                sl = await rpc(s, "getSlot", [{"commitment": COMMIT}])
                slots["http"].setdefault(sl, now())
        except Exception as e:
            print("http err", e)
        i += 1
        await asyncio.sleep(max(0, interval - (now() - t0) / 1000))

def stats(xs):
    xs = sorted(xs)
    if not xs: return {"n": 0}
    p = lambda q: xs[min(len(xs) - 1, int(round(q * (len(xs) - 1))))]
    return {"n": len(xs), "min": round(xs[0], 1), "median": round(statistics.median(xs), 1), "p90": round(p(0.9), 1), "p99": round(p(0.99), 1), "max": round(xs[-1], 1), "mean": round(statistics.mean(xs), 1)}

async def main():
    payer = Keypair(); dest = Keypair().pubkey()
    async with aiohttp.ClientSession() as s:
        ver = await rpc(s, "getVersion", [])
        sig = await rpc(s, "requestAirdrop", [str(payer.pubkey()), 100_000_000_000])
        for _ in range(60):
            st = (await rpc(s, "getSignatureStatuses", [[sig]]))["value"][0]
            if st and st.get("confirmationStatus") in ("confirmed", "finalized"): break
            await asyncio.sleep(0.5)
        readies = [asyncio.Event() for _ in range(3)]
        tasks = [asyncio.create_task(grpc_task(str(dest), readies[0])),
                 asyncio.create_task(ws_task(str(dest), readies[1])),
                 asyncio.create_task(http_task(s, str(dest), readies[2]))]
        for r in readies: await r.wait()
        await asyncio.sleep(3)
        win_start = now()
        for i in range(N):
            bh = Hash.from_string((await rpc(s, "getLatestBlockhash", [{"commitment": "processed"}]))["value"]["blockhash"])
            ix = transfer(TransferParams(from_pubkey=payer.pubkey(), to_pubkey=dest, lamports=1_000_000 + i))
            tx = Transaction([payer], Message([ix], payer.pubkey()), bh)
            raw = base58.b58encode(bytes(tx)).decode()
            t = now()
            sg = await rpc(s, "sendTransaction", [raw, {"skipPreflight": True, "preflightCommitment": "processed"}])
            sent[sg] = t
            await asyncio.sleep(0.4)
        await asyncio.sleep(25 if COMMIT == "confirmed" else 15)
        win_end = now()
        stop.set()
        await asyncio.sleep(2)
        for t in tasks: t.cancel()
    res = {"commitment": COMMIT, "n_sent": N, "validator_version": ver, "window_ms": [win_start, win_end],
           "dest_note": "一次性本地测试地址，随机生成", "methods": {}}
    for m in ("grpc", "ws", "http"):
        lat = [seen[m][k] - sent[k] for k in sent if k in seen[m]]
        res["methods"][m] = {"detected": sum(1 for k in sent if k in seen[m]), "missed": [k for k in sent if k not in seen[m]],
                             "latency_ms_from_send": stats(lat)}
    # 同一 slot 三路首次看到的时间差（以 gRPC 为基准）
    def d(a, b):
        A, B = slots.get(a, {}), slots.get(b, {}); c = set(A) & set(B)
        return stats([A[k] - B[k] for k in c])
    ref = "grpc_SLOT_" + COMMIT.upper()
    res["slot_delta_ms"] = {
        "ws_slotSubscribe_minus_grpc_CREATED_BANK": d("ws", "grpc_SLOT_CREATED_BANK"),
        "ws_slotSubscribe_minus_grpc_FIRST_SHRED": d("ws", "grpc_SLOT_FIRST_SHRED_RECEIVED"),
        "http_getSlot_minus_grpc_" + COMMIT: d("http", ref),
        "grpc_PROCESSED_minus_grpc_CREATED_BANK": d("grpc_SLOT_PROCESSED", "grpc_SLOT_CREATED_BANK"),
        "grpc_CONFIRMED_minus_grpc_PROCESSED": d("grpc_SLOT_CONFIRMED", "grpc_SLOT_PROCESSED"),
        "grpc_FINALIZED_minus_grpc_CONFIRMED": d("grpc_SLOT_FINALIZED", "grpc_SLOT_CONFIRMED"),
    }
    res["slot_counts"] = {m: len(v) for m, v in slots.items()}
    # 对每笔交易：哪一路先到
    first = {"grpc": 0, "ws": 0, "http": 0}
    for k in sent:
        c = {m: seen[m][k] for m in seen if k in seen[m]}
        if c: first[min(c, key=c.get)] += 1
    res["first_to_see"] = first
    res["raw"] = {"sent": sent, "seen": seen, "slots": {k: {str(a): b for a, b in v.items()} for k, v in slots.items()}}
    json.dump(res, open(OUT, "w"), indent=1)
    print(json.dumps({k: v for k, v in res.items() if k != "raw"}, ensure_ascii=False, indent=1))

asyncio.run(main())
