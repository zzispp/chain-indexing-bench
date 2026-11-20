#!/usr/bin/env python3
"""Re-run Base same-window compare for full duration (no early WS stop)."""
from __future__ import annotations
import asyncio, json, statistics, time
from pathlib import Path
import aiohttp, websockets

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "bench-raw"
WSS = "wss://base-rpc.publicnode.com"
HTTP = "https://base-rpc.publicnode.com"
DURATION = 120.0
POLL_INTERVAL = 0.15

def now_ms(): return time.time() * 1000
def hex_to_int(h): return int(h, 16)
def summarize(xs):
    if not xs: return {"n": 0}
    return {"n": len(xs), "min_ms": round(min(xs),2), "max_ms": round(max(xs),2),
            "mean_ms": round(statistics.mean(xs),2), "median_ms": round(statistics.median(xs),2),
            "p90_ms": round(sorted(xs)[max(0,int(len(xs)*0.9)-1)],2)}

async def http_rpc(session, method, params):
    async with session.post(HTTP, json={"jsonrpc":"2.0","id":1,"method":method,"params":params},
                            timeout=aiohttp.ClientTimeout(total=15)) as resp:
        data = await resp.json()
        if "error" in data: raise RuntimeError(data["error"])
        return data["result"]

async def http_poll(duration_s):
    events, seen = [], {}
    t0 = time.time()
    async with aiohttp.ClientSession() as session:
        while time.time() - t0 < duration_s:
            t_req = now_ms()
            try:
                bn = await http_rpc(session, "eth_blockNumber", [])
                h = hex_to_int(bn); t_resp = now_ms()
                if h not in seen:
                    seen[h] = t_resp
                    events.append({"height": h, "first_seen_ms": t_resp, "rtt_ms": t_resp - t_req})
            except Exception as e:
                events.append({"error": str(e), "t_ms": now_ms()})
            await asyncio.sleep(POLL_INTERVAL)
    return {"unique_heights": len(seen), "events": events, "heights_sorted": sorted(seen.keys()),
            "endpoint": HTTP, "poll_interval_s": POLL_INTERVAL, "duration_s": duration_s}

async def ws_heads(duration_s):
    """Receive heads; getBlock in background tasks so recv loop never blocks."""
    heads, latencies = [], []
    t0 = time.time()
    async with aiohttp.ClientSession() as session:
        async with websockets.connect(WSS, open_timeout=15, max_size=8_000_000) as ws:
            await ws.send(json.dumps({"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}))
            sub = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
            sub_id = sub.get("result")
            pending = set()

            async def do_getblock(height, hhash, number_hex, t_recv):
                t0b = now_ms(); ok=False; err=None; txc=None
                try:
                    block = await http_rpc(session, "eth_getBlockByNumber", [number_hex, False])
                    ok = block is not None and block.get("hash") == hhash
                    txc = len(block.get("transactions") or []) if block else None
                except Exception as e:
                    err = str(e)
                t1b = now_ms(); lat = t1b - t_recv
                latencies.append(lat)
                heads.append({"height": height, "hash": hhash, "recv_ms": t_recv,
                              "getblock_ok": ok, "getblock_latency_ms": lat, "getblock_error": err,
                              "block_tx_count": txc})

            while time.time() - t0 < duration_s:
                timeout = max(0.1, duration_s - (time.time() - t0))
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                except asyncio.TimeoutError:
                    break
                t_recv = now_ms()
                msg = json.loads(raw)
                if msg.get("method") != "eth_subscription": continue
                params = msg.get("params") or {}
                if params.get("subscription") != sub_id: continue
                header = params.get("result") or {}
                height = hex_to_int(header["number"])
                task = asyncio.create_task(do_getblock(height, header.get("hash"), header["number"], t_recv))
                pending.add(task)
                task.add_done_callback(pending.discard)
            if pending:
                await asyncio.wait(pending, timeout=20)
    heads.sort(key=lambda x: (x["recv_ms"], x["height"]))
    return {"heads_count": len(heads), "heads": heads, "getblock_latencies_ms": latencies,
            "wss": WSS, "http": HTTP, "duration_s": duration_s}

async def main():
    print(f"Base full-window compare duration={DURATION}s", flush=True)
    t_start = time.time()
    http_task = asyncio.create_task(http_poll(DURATION))
    ws_task = asyncio.create_task(ws_heads(DURATION))
    http_res, ws_res = await asyncio.gather(http_task, ws_task)
    t_end = time.time()
    http_heights = set(http_res["heights_sorted"])
    ws_heights = {h["height"] for h in ws_res["heads"]}
    both = http_heights & ws_heights
    http_first = {e["height"]: e["first_seen_ms"] for e in http_res["events"] if "height" in e}
    ws_first = {}
    for h in ws_res["heads"]:
        ws_first.setdefault(h["height"], h["recv_ms"])
    deltas = [{"height": ht, "ws_first_ms": ws_first[ht], "http_first_ms": http_first[ht],
               "http_minus_ws_ms": http_first[ht]-ws_first[ht]} for ht in sorted(both)]
    intervals = []
    heads = ws_res["heads"]
    for i in range(1, len(heads)):
        intervals.append(heads[i]["recv_ms"] - heads[i-1]["recv_ms"])
    # gap detection on WS stream (naked)
    ws_sorted = sorted(ws_heights)
    gaps = []
    for i in range(1, len(ws_sorted)):
        if ws_sorted[i] != ws_sorted[i-1] + 1:
            gaps.append({"from": ws_sorted[i-1]+1, "to": ws_sorted[i]-1, "count": ws_sorted[i]-ws_sorted[i-1]-1})
    out = {
        "chain": "base",
        "note": "full-duration re-run; WS getBlock non-blocking; no early stop",
        "wall_start_unix": t_start, "wall_end_unix": t_end, "duration_s": t_end-t_start,
        "http": http_res, "ws": ws_res,
        "overlap_heights": len(both),
        "only_http_heights": sorted(http_heights - ws_heights),
        "only_ws_heights": sorted(ws_heights - http_heights),
        "first_seen_deltas": deltas,
        "http_minus_ws_latency_summary_ms": summarize([d["http_minus_ws_ms"] for d in deltas]),
        "getblock_after_head_summary_ms": summarize(ws_res["getblock_latencies_ms"]),
        "http_poll_rtt_summary_ms": summarize([e["rtt_ms"] for e in http_res["events"] if "rtt_ms" in e]),
        "ws_head_interval_ms": summarize(intervals) if intervals else {"n":0},
        "ws_internal_gaps": gaps,
        "ws_internal_gap_blocks": sum(g["count"] for g in gaps),
        "getblock_ok_rate": (sum(1 for h in heads if h["getblock_ok"]) / len(heads)) if heads else 0,
    }
    (RAW / "base_http_vs_ws_full.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps({
        "duration_s": round(out["duration_s"],2),
        "ws_heads": out["ws"]["heads_count"],
        "http_unique": out["http"]["unique_heights"],
        "overlap": out["overlap_heights"],
        "only_http": len(out["only_http_heights"]),
        "only_ws": len(out["only_ws_heights"]),
        "ws_gap_blocks": out["ws_internal_gap_blocks"],
        "getblock": out["getblock_after_head_summary_ms"],
        "http_minus_ws": out["http_minus_ws_latency_summary_ms"],
        "head_interval": out["ws_head_interval_ms"],
        "getblock_ok_rate": out["getblock_ok_rate"],
    }, indent=2), flush=True)

asyncio.run(main())
