#!/usr/bin/env python3
"""Aggregate solana-local-3way run JSON into summary.json."""
from __future__ import annotations
import json, statistics, glob, os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DIR = ROOT / "bench-raw" / "solana-local-3way"
OUT = DIR / "summary.json"

def load_runs():
    runs = []
    for p in sorted(DIR.glob("*_run*.json")):
        if p.name.endswith("summary.json"):
            continue
        d = json.load(open(p))
        d["_file"] = p.name
        runs.append(d)
    return runs

def pool_latencies(runs, method):
    xs = []
    for r in runs:
        # reconstruct from raw if present, else use per-run stats only
        sent = r.get("raw", {}).get("sent", {})
        seen = r.get("raw", {}).get("seen", {}).get(method, {})
        if sent and seen:
            for k, t0 in sent.items():
                if k in seen:
                    xs.append(seen[k] - t0)
        else:
            # fallback: can't pool individuals
            pass
    return xs

def stats(xs):
    xs = sorted(xs)
    if not xs:
        return {"n": 0}
    p = lambda q: xs[min(len(xs) - 1, int(round(q * (len(xs) - 1))))]
    return {
        "n": len(xs),
        "min": round(xs[0], 1),
        "median": round(statistics.median(xs), 1),
        "p90": round(p(0.9), 1),
        "p99": round(p(0.99), 1),
        "max": round(xs[-1], 1),
        "mean": round(statistics.mean(xs), 1),
    }

def aggregate(group_name, runs):
    if not runs:
        return None
    out = {
        "commitment": group_name,
        "n_runs": len(runs),
        "files": [r["_file"] for r in runs],
        "n_sent_total": sum(r["n_sent"] for r in runs),
        "validator_version": runs[0].get("validator_version"),
        "per_run": [],
        "pooled_latency_ms": {},
        "pooled_first_to_see": {"grpc": 0, "ws": 0, "http": 0},
        "pooled_detected": {},
        "pooled_missed_n": {},
    }
    for r in runs:
        pr = {
            "file": r["_file"],
            "n_sent": r["n_sent"],
            "methods": {
                m: {
                    "detected": r["methods"][m]["detected"],
                    "missed_n": len(r["methods"][m].get("missed", [])),
                    "latency_ms_from_send": r["methods"][m]["latency_ms_from_send"],
                }
                for m in ("grpc", "ws", "http")
            },
            "first_to_see": r.get("first_to_see"),
            "slot_delta_ms": r.get("slot_delta_ms"),
            "slot_counts": r.get("slot_counts"),
        }
        out["per_run"].append(pr)
        for m, c in (r.get("first_to_see") or {}).items():
            out["pooled_first_to_see"][m] = out["pooled_first_to_see"].get(m, 0) + c
    for m in ("grpc", "ws", "http"):
        xs = pool_latencies(runs, m)
        out["pooled_latency_ms"][m] = stats(xs)
        out["pooled_detected"][m] = sum(r["methods"][m]["detected"] for r in runs)
        out["pooled_missed_n"][m] = sum(len(r["methods"][m].get("missed", [])) for r in runs)
    # average of per-run medians (robust if raw missing)
    out["mean_of_run_medians_ms"] = {}
    for m in ("grpc", "ws", "http"):
        meds = [r["methods"][m]["latency_ms_from_send"].get("median") for r in runs
                if r["methods"][m]["latency_ms_from_send"].get("n", 0)]
        out["mean_of_run_medians_ms"][m] = round(statistics.mean(meds), 1) if meds else None
    return out

def main():
    runs = load_runs()
    by = {"processed": [], "confirmed": []}
    for r in runs:
        c = r.get("commitment")
        if c in by:
            by[c].append(r)
    summary = {
        "generated_beijing": __import__("time").strftime("%Y-%m-%d %H:%M:%S %z"),
        "env": {
            "solana_core": "4.4.1",
            "yellowstone_grpc_geyser": "17.0.2+solana.4.4.1",
            "rpc": "http://127.0.0.1:8899",
            "ws": "ws://127.0.0.1:8900",
            "grpc": "127.0.0.1:10000",
            "plugin_config": "grpc-local/config.json",
            "ledger": "grpc-local/ledger",
            "note": "本地 solana-test-validator + Yellowstone 插件；一次性测试密钥；非主网",
        },
        "runs_found": [r["_file"] for r in runs],
        "processed": aggregate("processed", by["processed"]),
        "confirmed": aggregate("confirmed", by["confirmed"]),
    }
    OUT.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps({
        "runs": summary["runs_found"],
        "processed": None if not summary["processed"] else {
            "n_runs": summary["processed"]["n_runs"],
            "pooled_latency_ms": summary["processed"]["pooled_latency_ms"],
            "first_to_see": summary["processed"]["pooled_first_to_see"],
            "missed": summary["processed"]["pooled_missed_n"],
        },
        "confirmed": None if not summary["confirmed"] else {
            "n_runs": summary["confirmed"]["n_runs"],
            "pooled_latency_ms": summary["confirmed"]["pooled_latency_ms"],
            "first_to_see": summary["confirmed"]["pooled_first_to_see"],
            "missed": summary["confirmed"]["pooled_missed_n"],
        },
    }, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
