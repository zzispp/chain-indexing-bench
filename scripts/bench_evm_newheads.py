#!/usr/bin/env python3
"""EVM 断连回补 + ETH 同窗对照（只读）。

Base 正式 120s 同窗对比请用 bench_evm_base_rerun.py；本脚本不再跑已作废的 45s 口径，
也不再写入 base_http_vs_ws.json。
"""
from __future__ import annotations

import asyncio
import json
import statistics
import time
from pathlib import Path

import aiohttp
import websockets

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "bench-raw"
RAW.mkdir(parents=True, exist_ok=True)

# Prefer Base: ~200ms blocks → more samples in short window
CHAINS = {
    "base": {
        "wss": "wss://base-rpc.publicnode.com",
        "http": "https://base-rpc.publicnode.com",
        "explorer": "https://basescan.org/block/",
        "target_heads": 40,
        "poll_interval": 0.15,
        "disconnect_sleep": 25,
    },
    "ethereum": {
        "wss": "wss://ethereum-rpc.publicnode.com",
        "http": "https://ethereum-rpc.publicnode.com",
        "explorer": "https://etherscan.io/block/",
        "target_heads": 8,
        "poll_interval": 1.0,
        "disconnect_sleep": 35,
    },
}


def now_ms() -> float:
    return time.time() * 1000


def hex_to_int(h: str) -> int:
    return int(h, 16)


async def http_rpc(session: aiohttp.ClientSession, url: str, method: str, params: list):
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as resp:
        data = await resp.json()
        if "error" in data:
            raise RuntimeError(data["error"])
        return data["result"]


async def run_http_poll(chain: str, cfg: dict, duration_s: float) -> dict:
    """Poll eth_blockNumber periodically; record first-seen time per height."""
    events = []
    seen = {}
    t0 = time.time()
    async with aiohttp.ClientSession() as session:
        while time.time() - t0 < duration_s:
            t_req = now_ms()
            try:
                bn = await http_rpc(session, cfg["http"], "eth_blockNumber", [])
                height = hex_to_int(bn)
                t_resp = now_ms()
                if height not in seen:
                    seen[height] = t_resp
                    events.append(
                        {
                            "height": height,
                            "first_seen_ms": t_resp,
                            "rtt_ms": t_resp - t_req,
                            "method": "http_poll",
                        }
                    )
            except Exception as e:
                events.append({"error": str(e), "t_ms": now_ms(), "method": "http_poll"})
            await asyncio.sleep(cfg["poll_interval"])
    return {
        "chain": chain,
        "method": "http_poll",
        "endpoint": cfg["http"],
        "duration_s": duration_s,
        "poll_interval_s": cfg["poll_interval"],
        "unique_heights": len(seen),
        "events": events,
        "heights_sorted": sorted(seen.keys()),
    }


async def run_ws_newheads(chain: str, cfg: dict, duration_s: float) -> dict:
    """Subscribe newHeads; on each head, immediately eth_getBlockByNumber via HTTP; measure latency."""
    heads = []
    getblock_latencies = []
    t0 = time.time()
    async with aiohttp.ClientSession() as session:
        async with websockets.connect(cfg["wss"], open_timeout=15, max_size=8_000_000) as ws:
            await ws.send(
                json.dumps(
                    {"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": ["newHeads"]}
                )
            )
            sub = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
            sub_id = sub.get("result")
            while time.time() - t0 < duration_s and len(heads) < cfg["target_heads"] * 2:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=30)
                except asyncio.TimeoutError:
                    break
                t_recv = now_ms()
                msg = json.loads(raw)
                if msg.get("method") != "eth_subscription":
                    continue
                params = msg.get("params") or {}
                if params.get("subscription") != sub_id:
                    continue
                header = params.get("result") or {}
                height = hex_to_int(header["number"])
                hhash = header.get("hash")
                # Measure: head received → getBlock success
                t_gb0 = now_ms()
                ok = False
                block = None
                err = None
                try:
                    block = await http_rpc(
                        session, cfg["http"], "eth_getBlockByNumber", [header["number"], False]
                    )
                    ok = block is not None and block.get("hash") == hhash
                except Exception as e:
                    err = str(e)
                t_gb1 = now_ms()
                lat = t_gb1 - t_recv
                getblock_latencies.append(lat)
                heads.append(
                    {
                        "height": height,
                        "hash": hhash,
                        "recv_ms": t_recv,
                        "getblock_ok": ok,
                        "getblock_latency_ms": lat,
                        "getblock_error": err,
                        "block_tx_count": len(block.get("transactions") or []) if block else None,
                        "timestamp_hex": header.get("timestamp"),
                    }
                )
    return {
        "chain": chain,
        "method": "ws_newHeads",
        "wss": cfg["wss"],
        "http": cfg["http"],
        "duration_s": duration_s,
        "heads_count": len(heads),
        "getblock_latencies_ms": getblock_latencies,
        "heads": heads,
    }


async def run_disconnect_backfill(chain: str, cfg: dict) -> dict:
    """Subscribe a few heads → disconnect → sleep → reconnect tip → backfill gap with getBlock."""
    sleep_s = cfg["disconnect_sleep"]
    collected = []
    async with aiohttp.ClientSession() as session:
        # Phase 1: subscribe until we have >= 3 heads
        async with websockets.connect(cfg["wss"], open_timeout=15, max_size=8_000_000) as ws:
            await ws.send(
                json.dumps(
                    {"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": ["newHeads"]}
                )
            )
            sub = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
            sub_id = sub.get("result")
            while len(collected) < 5:
                raw = await asyncio.wait_for(ws.recv(), timeout=60)
                msg = json.loads(raw)
                if msg.get("method") != "eth_subscription":
                    continue
                if (msg.get("params") or {}).get("subscription") != sub_id:
                    continue
                header = msg["params"]["result"]
                collected.append(
                    {
                        "height": hex_to_int(header["number"]),
                        "hash": header.get("hash"),
                        "recv_ms": now_ms(),
                    }
                )
            last_height = collected[-1]["height"]
            disconnect_at = now_ms()
            # intentional disconnect by exiting context

        # Phase 2: sleep (miss blocks)
        await asyncio.sleep(sleep_s)
        tip_hex = await http_rpc(session, cfg["http"], "eth_blockNumber", [])
        tip = hex_to_int(tip_hex)
        gap_from = last_height + 1
        gap_to = tip
        gap_heights = list(range(gap_from, gap_to + 1)) if tip >= gap_from else []

        # Naked: no backfill — would miss gap_heights
        naked_missed = len(gap_heights)

        # Optimized: backfill each height via eth_getBlockByNumber
        backfilled = []
        failures = []
        t_bf0 = now_ms()
        for h in gap_heights:
            try:
                blk = await http_rpc(
                    session, cfg["http"], "eth_getBlockByNumber", [hex(h), False]
                )
                if blk and blk.get("hash"):
                    backfilled.append(
                        {
                            "height": h,
                            "hash": blk["hash"],
                            "parentHash": blk.get("parentHash"),
                            "tx_count": len(blk.get("transactions") or []),
                        }
                    )
                else:
                    failures.append({"height": h, "reason": "null_block"})
            except Exception as e:
                failures.append({"height": h, "reason": str(e)})
            await asyncio.sleep(0.05)  # be gentle on public RPC
        t_bf1 = now_ms()

        # Continuity check via parentHash chain
        continuity_ok = True
        continuity_breaks = []
        if backfilled:
            # also fetch last known block for parent link
            try:
                prev = await http_rpc(
                    session, cfg["http"], "eth_getBlockByNumber", [hex(last_height), False]
                )
                prev_hash = prev.get("hash") if prev else None
            except Exception:
                prev_hash = collected[-1].get("hash")
            for b in backfilled:
                if prev_hash and b.get("parentHash") and b["parentHash"].lower() != prev_hash.lower():
                    continuity_ok = False
                    continuity_breaks.append(
                        {"height": b["height"], "expected_parent": prev_hash, "got": b["parentHash"]}
                    )
                prev_hash = b["hash"]

        return {
            "chain": chain,
            "method": "disconnect_backfill",
            "wss": cfg["wss"],
            "http": cfg["http"],
            "pre_disconnect_heads": collected,
            "last_height_before_disconnect": last_height,
            "disconnect_at_ms": disconnect_at,
            "sleep_s": sleep_s,
            "tip_after_reconnect": tip,
            "gap_from": gap_from,
            "gap_to": gap_to,
            "gap_block_count": len(gap_heights),
            "naked_would_miss": naked_missed,
            "backfilled_count": len(backfilled),
            "backfill_failures": failures,
            "backfill_success_rate": (len(backfilled) / len(gap_heights)) if gap_heights else 1.0,
            "backfill_duration_ms": t_bf1 - t_bf0,
            "parentHash_continuity_ok": continuity_ok,
            "continuity_breaks": continuity_breaks,
            "backfilled_sample": backfilled[:5] + (backfilled[-2:] if len(backfilled) > 5 else []),
        }


def summarize_latencies(xs: list[float]) -> dict:
    if not xs:
        return {"n": 0}
    return {
        "n": len(xs),
        "min_ms": round(min(xs), 2),
        "max_ms": round(max(xs), 2),
        "mean_ms": round(statistics.mean(xs), 2),
        "median_ms": round(statistics.median(xs), 2),
        "p90_ms": round(sorted(xs)[max(0, int(len(xs) * 0.9) - 1)], 2),
    }


async def compare_same_window(chain: str, cfg: dict, duration_s: float) -> dict:
    """Run HTTP poll and WS subscribe concurrently for same wall-clock window."""
    t_start = time.time()
    http_task = asyncio.create_task(run_http_poll(chain, cfg, duration_s))
    ws_task = asyncio.create_task(run_ws_newheads(chain, cfg, duration_s))
    http_res, ws_res = await asyncio.gather(http_task, ws_task)
    t_end = time.time()

    http_heights = set(http_res["heights_sorted"])
    ws_heights = {h["height"] for h in ws_res["heads"]}
    both = http_heights & ws_heights
    only_http = sorted(http_heights - ws_heights)
    only_ws = sorted(ws_heights - http_heights)

    # First-seen delta for overlapping heights: WS recv vs HTTP first poll see
    http_first = {e["height"]: e["first_seen_ms"] for e in http_res["events"] if "height" in e}
    ws_first = {}
    for h in ws_res["heads"]:
        ws_first.setdefault(h["height"], h["recv_ms"])

    deltas = []
    for ht in sorted(both):
        # positive => HTTP saw later than WS
        deltas.append(
            {
                "height": ht,
                "ws_first_ms": ws_first[ht],
                "http_first_ms": http_first[ht],
                "http_minus_ws_ms": http_first[ht] - ws_first[ht],
            }
        )

    return {
        "chain": chain,
        "wall_start_unix": t_start,
        "wall_end_unix": t_end,
        "duration_s": t_end - t_start,
        "http": http_res,
        "ws": ws_res,
        "overlap_heights": len(both),
        "only_http_heights": only_http,
        "only_ws_heights": only_ws,
        "first_seen_deltas": deltas,
        "http_minus_ws_latency_summary_ms": summarize_latencies(
            [d["http_minus_ws_ms"] for d in deltas]
        ),
        "getblock_after_head_summary_ms": summarize_latencies(ws_res["getblock_latencies_ms"]),
        "http_poll_rtt_summary_ms": summarize_latencies(
            [e["rtt_ms"] for e in http_res["events"] if "rtt_ms" in e]
        ),
    }


async def main():
    started = time.strftime("%Y-%m-%d %H:%M:%S %z")
    out = {"started_beijing": started, "chains": {}}

    # 不要在本脚本里跑 45s Base 同窗对比：旧口径会因 WS 路径阻塞 getBlock 而早停，数据已作废。
    # Base 正式 120s 同窗请用 scripts/bench_evm_base_rerun.py（写入 base_http_vs_ws_full.json）。
    print(
        "=== 跳过 Base 同窗对比 ===\n"
        "旧 45s 口径已作废。请改跑: python scripts/bench_evm_base_rerun.py  （120s，写入 bench-raw/base_http_vs_ws_full.json）",
        flush=True,
    )

    print("=== Base disconnect backfill ===", flush=True)
    base_bf = await run_disconnect_backfill("base", CHAINS["base"])
    out["chains"]["base_backfill"] = base_bf
    (RAW / "base_backfill.json").write_text(json.dumps(base_bf, indent=2), encoding="utf-8")
    print(
        f"Base backfill: gap={base_bf['gap_block_count']} success={base_bf['backfill_success_rate']} "
        f"naked_miss={base_bf['naked_would_miss']}",
        flush=True,
    )

    # Ethereum: shorter window (slower blocks)
    print("=== Ethereum same-window HTTP vs WS ===", flush=True)
    eth_cmp = await compare_same_window("ethereum", CHAINS["ethereum"], duration_s=72)
    out["chains"]["ethereum_compare"] = eth_cmp
    (RAW / "ethereum_http_vs_ws.json").write_text(json.dumps(eth_cmp, indent=2), encoding="utf-8")
    print(
        f"ETH: WS heads={eth_cmp['ws']['heads_count']} HTTP unique={eth_cmp['http']['unique_heights']} "
        f"getBlock_med={eth_cmp['getblock_after_head_summary_ms']}",
        flush=True,
    )

    print("=== Ethereum disconnect backfill ===", flush=True)
    eth_bf = await run_disconnect_backfill("ethereum", CHAINS["ethereum"])
    out["chains"]["ethereum_backfill"] = eth_bf
    (RAW / "ethereum_backfill.json").write_text(json.dumps(eth_bf, indent=2), encoding="utf-8")
    print(
        f"ETH backfill: gap={eth_bf['gap_block_count']} success={eth_bf['backfill_success_rate']}",
        flush=True,
    )

    out["finished_beijing"] = time.strftime("%Y-%m-%d %H:%M:%S %z")
    (RAW / "evm_bench_summary.json").write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
    print("DONE", out["finished_beijing"], flush=True)


if __name__ == "__main__":
    asyncio.run(main())
