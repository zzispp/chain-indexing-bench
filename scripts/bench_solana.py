#!/usr/bin/env python3
"""Solana commitment slot diffs + logsSubscribe vs HTTP getSlot. Read-only. No Yellowstone (no free endpoint)."""
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

ENDPOINTS = [
    {
        "name": "solana_official",
        "http": "https://api.mainnet-beta.solana.com",
        "wss": "wss://api.mainnet-beta.solana.com",
    },
    {
        "name": "solana_publicnode",
        "http": "https://solana-rpc.publicnode.com",
        "wss": "wss://solana-rpc.publicnode.com",
    },
]


def now_ms() -> float:
    return time.time() * 1000


def summarize(xs: list[float]) -> dict:
    if not xs:
        return {"n": 0}
    return {
        "n": len(xs),
        "min": round(min(xs), 3),
        "max": round(max(xs), 3),
        "mean": round(statistics.mean(xs), 3),
        "median": round(statistics.median(xs), 3),
    }


async def rpc(session, url, method, params):
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=20)) as resp:
        data = await resp.json()
        if "error" in data:
            raise RuntimeError(json.dumps(data["error"]))
        return data["result"]


async def sample_commitments(session, http_url: str, samples: int = 30, interval: float = 0.8) -> dict:
    rows = []
    for i in range(samples):
        t0 = now_ms()
        try:
            # parallel-ish sequential to avoid hammering
            processed = await rpc(session, http_url, "getSlot", [{"commitment": "processed"}])
            confirmed = await rpc(session, http_url, "getSlot", [{"commitment": "confirmed"}])
            finalized = await rpc(session, http_url, "getSlot", [{"commitment": "finalized"}])
            t1 = now_ms()
            rows.append(
                {
                    "i": i,
                    "t_ms": t1,
                    "rtt_ms": t1 - t0,
                    "processed": processed,
                    "confirmed": confirmed,
                    "finalized": finalized,
                    "proc_minus_conf": processed - confirmed,
                    "conf_minus_fin": confirmed - finalized,
                    "proc_minus_fin": processed - finalized,
                }
            )
        except Exception as e:
            rows.append({"i": i, "error": str(e), "t_ms": now_ms()})
        await asyncio.sleep(interval)
    ok = [r for r in rows if "processed" in r]
    return {
        "samples_ok": len(ok),
        "samples_total": samples,
        "rows": rows,
        "proc_minus_fin_summary": summarize([r["proc_minus_fin"] for r in ok]),
        "conf_minus_fin_summary": summarize([r["conf_minus_fin"] for r in ok]),
        "proc_minus_conf_summary": summarize([r["proc_minus_conf"] for r in ok]),
        "rtt_summary_ms": summarize([r["rtt_ms"] for r in ok]),
    }


async def http_poll_slots(session, http_url: str, duration_s: float, interval: float = 0.4) -> dict:
    seen = {}
    events = []
    t0 = time.time()
    while time.time() - t0 < duration_s:
        t_req = now_ms()
        try:
            slot = await rpc(session, http_url, "getSlot", [{"commitment": "processed"}])
            t_resp = now_ms()
            if slot not in seen:
                seen[slot] = t_resp
                events.append({"slot": slot, "first_seen_ms": t_resp, "rtt_ms": t_resp - t_req})
        except Exception as e:
            events.append({"error": str(e), "t_ms": now_ms()})
        await asyncio.sleep(interval)
    return {"unique_slots": len(seen), "events": events, "slots_sorted": sorted(seen.keys())}


async def ws_slot_subscribe(wss_url: str, duration_s: float) -> dict:
    slots = []
    t0 = time.time()
    async with websockets.connect(wss_url, open_timeout=15, max_size=4_000_000) as ws:
        await ws.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "slotSubscribe"}))
        sub = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
        sub_id = sub.get("result")
        while time.time() - t0 < duration_s:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=15)
            except asyncio.TimeoutError:
                break
            t_recv = now_ms()
            msg = json.loads(raw)
            if msg.get("method") != "slotNotification":
                continue
            result = (msg.get("params") or {}).get("result") or {}
            slots.append(
                {
                    "slot": result.get("slot"),
                    "parent": result.get("parent"),
                    "root": result.get("root"),
                    "recv_ms": t_recv,
                }
            )
    unique = {}
    for s in slots:
        if s.get("slot") is not None:
            unique.setdefault(s["slot"], s["recv_ms"])
    return {
        "notifications": len(slots),
        "unique_slots": len(unique),
        "slots": slots,
        "first_seen": unique,
        "sub_id": sub_id,
    }


async def ws_logs_subscribe(wss_url: str, duration_s: float) -> dict:
    """Subscribe logs for all mentions — may be rate-limited on public RPC; short window."""
    notes = []
    t0 = time.time()
    err = None
    try:
        async with websockets.connect(wss_url, open_timeout=15, max_size=8_000_000) as ws:
            # Use mentions of System Program as a high-traffic but valid filter,
            # or "all" if allowed. Prefer "all" first; fall back.
            req = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "logsSubscribe",
                "params": ["all", {"commitment": "processed"}],
            }
            await ws.send(json.dumps(req))
            sub = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
            if "error" in sub:
                # fallback: mentions System Program
                req = {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "logsSubscribe",
                    "params": [
                        {"mentions": ["11111111111111111111111111111111"]},
                        {"commitment": "processed"},
                    ],
                }
                await ws.send(json.dumps(req))
                sub = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
                if "error" in sub:
                    return {"ok": False, "error": sub["error"], "notes": []}
            sub_id = sub.get("result")
            while time.time() - t0 < duration_s:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=10)
                except asyncio.TimeoutError:
                    break
                t_recv = now_ms()
                msg = json.loads(raw)
                if msg.get("method") != "logsNotification":
                    continue
                result = (msg.get("params") or {}).get("result") or {}
                value = result.get("value") or {}
                ctx = result.get("context") or {}
                notes.append(
                    {
                        "slot": ctx.get("slot"),
                        "signature": value.get("signature"),
                        "err": value.get("err"),
                        "recv_ms": t_recv,
                    }
                )
                if len(notes) >= 200:
                    break
            return {
                "ok": True,
                "sub_id": sub_id,
                "notifications": len(notes),
                "unique_signatures": len({n["signature"] for n in notes if n.get("signature")}),
                "unique_slots": len({n["slot"] for n in notes if n.get("slot") is not None}),
                "notes_sample": notes[:20],
                "notes": notes,
            }
    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        return {"ok": False, "error": err, "notes": notes}


async def same_window_compare(ep: dict, duration_s: float = 35) -> dict:
    async with aiohttp.ClientSession() as session:
        http_task = asyncio.create_task(http_poll_slots(session, ep["http"], duration_s, 0.35))
        ws_task = asyncio.create_task(ws_slot_subscribe(ep["wss"], duration_s))
        http_res, ws_res = await asyncio.gather(http_task, ws_task)

    http_slots = set(http_res["slots_sorted"])
    ws_slots = set(ws_res["first_seen"].keys())
    both = http_slots & ws_slots
    deltas = []
    http_first = {e["slot"]: e["first_seen_ms"] for e in http_res["events"] if "slot" in e}
    for sl in sorted(both):
        deltas.append(
            {
                "slot": sl,
                "ws_first_ms": ws_res["first_seen"][sl],
                "http_first_ms": http_first[sl],
                "http_minus_ws_ms": http_first[sl] - ws_res["first_seen"][sl],
            }
        )
    return {
        "endpoint": ep["name"],
        "http": ep["http"],
        "wss": ep["wss"],
        "duration_s": duration_s,
        "http_poll": {k: v for k, v in http_res.items() if k != "events"}
        | {"events_count": len(http_res["events"]), "events_sample": http_res["events"][:10]},
        "ws_slot": {
            "notifications": ws_res["notifications"],
            "unique_slots": ws_res["unique_slots"],
            "slots_sample": ws_res["slots"][:15],
        },
        "overlap_slots": len(both),
        "only_http": sorted(http_slots - ws_slots)[:50],
        "only_ws": sorted(ws_slots - http_slots)[:50],
        "only_http_count": len(http_slots - ws_slots),
        "only_ws_count": len(ws_slots - http_slots),
        "first_seen_deltas_sample": deltas[:30],
        "http_minus_ws_summary_ms": summarize([d["http_minus_ws_ms"] for d in deltas]),
        "_http_events_full": http_res["events"],
        "_ws_full": ws_res,
    }


async def main():
    started = time.strftime("%Y-%m-%d %H:%M:%S %z")
    out = {
        "started_beijing": started,
        "yellowstone": {
            "status": "未测",
            "reason": "无公开免费 Yellowstone gRPC 端点；Triton/Helius 等需付费或 API key。本轮只读公开 RPC/WS，不注册付费流。",
        },
        "endpoints": {},
    }

    async with aiohttp.ClientSession() as session:
        for ep in ENDPOINTS:
            print(f"=== commitment samples {ep['name']} ===", flush=True)
            try:
                cm = await sample_commitments(session, ep["http"], samples=25, interval=0.7)
                out["endpoints"].setdefault(ep["name"], {})["commitment"] = cm
                (RAW / f"{ep['name']}_commitment.json").write_text(
                    json.dumps(cm, indent=2), encoding="utf-8"
                )
                print(f"  proc-fin: {cm['proc_minus_fin_summary']}", flush=True)
            except Exception as e:
                out["endpoints"].setdefault(ep["name"], {})["commitment"] = {
                    "error": str(e)
                }
                print(f"  FAIL commitment: {e}", flush=True)

    # Prefer publicnode for same-window if official rate-limits
    for ep in ENDPOINTS:
        print(f"=== same-window HTTP vs WS slot {ep['name']} ===", flush=True)
        try:
            cmp = await same_window_compare(ep, duration_s=30)
            # strip bulky internals for summary file but keep raw
            full_path = RAW / f"{ep['name']}_http_vs_ws_slot.json"
            full_path.write_text(json.dumps(cmp, indent=2, default=str), encoding="utf-8")
            slim = {k: v for k, v in cmp.items() if not k.startswith("_")}
            out["endpoints"].setdefault(ep["name"], {})["http_vs_ws_slot"] = slim
            print(
                f"  overlap={slim['overlap_slots']} only_http={slim['only_http_count']} "
                f"only_ws={slim['only_ws_count']} delta={slim['http_minus_ws_summary_ms']}",
                flush=True,
            )
        except Exception as e:
            out["endpoints"].setdefault(ep["name"], {})["http_vs_ws_slot"] = {"error": str(e)}
            print(f"  FAIL compare: {e}", flush=True)

        print(f"=== logsSubscribe {ep['name']} ===", flush=True)
        try:
            logs = await ws_logs_subscribe(ep["wss"], duration_s=20)
            (RAW / f"{ep['name']}_logsSubscribe.json").write_text(
                json.dumps(logs, indent=2, default=str), encoding="utf-8"
            )
            out["endpoints"].setdefault(ep["name"], {})["logsSubscribe"] = {
                k: v for k, v in logs.items() if k != "notes"
            }
            print(f"  logs: ok={logs.get('ok')} n={logs.get('notifications')}", flush=True)
        except Exception as e:
            out["endpoints"].setdefault(ep["name"], {})["logsSubscribe"] = {"error": str(e)}
            print(f"  FAIL logs: {e}", flush=True)

    out["finished_beijing"] = time.strftime("%Y-%m-%d %H:%M:%S %z")
    (RAW / "solana_bench_summary.json").write_text(
        json.dumps(out, indent=2, default=str), encoding="utf-8"
    )
    print("DONE", out["finished_beijing"], flush=True)


if __name__ == "__main__":
    asyncio.run(main())
