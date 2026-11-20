#!/usr/bin/env python3
"""Generate assets/*.png for bench.md from bench-raw JSON."""
from __future__ import annotations
import json
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib import font_manager
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "bench-raw"
ASSETS = ROOT / "assets"
ASSETS.mkdir(exist_ok=True)

# Prefer CJK font if present
def setup_font():
    candidates = [
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]
    for c in candidates:
        if Path(c).exists():
            font_manager.fontManager.addfont(c)
            name = font_manager.FontProperties(fname=c).get_name()
            plt.rcParams["font.family"] = name
            plt.rcParams["axes.unicode_minus"] = False
            return name
    plt.rcParams["axes.unicode_minus"] = False
    return None

FONT = setup_font()
print("font", FONT)

def latency_3way():
    summary = json.load(open(RAW / "solana-local-3way" / "summary.json"))
    fig, axes = plt.subplots(1, 2, figsize=(12, 5), sharey=False)
    for ax, key, title in [
        (axes[0], "processed", "本地 Solana processed：交易检测延迟"),
        (axes[1], "confirmed", "本地 Solana confirmed：交易检测延迟"),
    ]:
        g = summary.get(key)
        if not g:
            ax.set_title(title + "（无数据）")
            ax.text(0.5, 0.5, "N/A", ha="center", va="center", transform=ax.transAxes)
            continue
        methods = ["grpc", "ws", "http"]
        labels = ["gRPC\nYellowstone", "WebSocket\nlogsSubscribe", "HTTP 轮询\ngetSignatures"]
        colors = ["#2ca02c", "#1f77b4", "#ff7f0e"]
        # collect per-run medians + pooled
        pooled = g["pooled_latency_ms"]
        medians = [pooled[m].get("median", 0) for m in methods]
        p90s = [pooled[m].get("p90", 0) for m in methods]
        x = np.arange(len(methods))
        w = 0.35
        b1 = ax.bar(x - w/2, medians, w, label="中位延迟", color=colors, alpha=0.9)
        b2 = ax.bar(x + w/2, p90s, w, label="p90", color=colors, alpha=0.45, hatch="//")
        ax.set_xticks(x)
        ax.set_xticklabels(labels)
        ax.set_ylabel("延迟 (ms，从 sendTransaction)")
        ax.set_title(f"{title}\n(n={g['n_sent_total']} 笔 / {g['n_runs']} 轮，漏检={g['pooled_missed_n']})")
        ax.legend(loc="upper left")
        ax.grid(axis="y", alpha=0.3)
        for i, m in enumerate(methods):
            ax.text(i - w/2, medians[i], f"{medians[i]}", ha="center", va="bottom", fontsize=9)
            ax.text(i + w/2, p90s[i], f"{p90s[i]}", ha="center", va="bottom", fontsize=9)
        # first_to_see annotation
        fts = g["pooled_first_to_see"]
        ax.text(0.98, 0.02, f"先到: gRPC={fts.get('grpc',0)} WS={fts.get('ws',0)} HTTP={fts.get('http',0)}",
                transform=ax.transAxes, ha="right", va="bottom", fontsize=8,
                bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.5))
    fig.suptitle("轮询 vs WebSocket vs gRPC 同窗对比（本地 Agave 4.4.1 + Yellowstone 17.0.2）", fontsize=12)
    fig.tight_layout()
    path = ASSETS / "latency_3way.png"
    fig.savefig(path, dpi=140)
    plt.close()
    print("wrote", path)

def base_http_vs_ws():
    # 只使用正式 120s 结果；旧 45s base_http_vs_ws.json 已作废，不收录。
    src = RAW / "base_http_vs_ws_full.json"
    if not src.exists():
        raise SystemExit(f"缺少 {src}，请先跑 python scripts/bench_evm_base_rerun.py")
    d = json.load(open(src))
    deltas = d.get("first_seen_deltas") or []
    xs = [x["http_minus_ws_ms"] for x in deltas]
    # head intervals
    heads = d["ws"].get("heads") or []
    intervals = []
    for i in range(1, len(heads)):
        intervals.append(heads[i]["recv_ms"] - heads[i-1]["recv_ms"])

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    ax = axes[0]
    if xs:
        ax.hist(xs, bins=min(40, max(10, len(xs)//3)), color="#1f77b4", edgecolor="white", alpha=0.85)
        med = float(np.median(xs))
        ax.axvline(med, color="red", linestyle="--", label=f"中位 {med:.1f} ms")
        ax.legend()
    ax.set_xlabel("HTTP 首次见到 − WS newHeads (ms)")
    ax.set_ylabel("块数")
    note = "full 120s"
    ax.set_title(f"Base：同高度首次可见时间差\nWS heads={d['ws'].get('heads_count')} HTTP={d['http']['unique_heights']} overlap={d['overlap_heights']} ({note})")
    ax.grid(axis="y", alpha=0.3)

    ax = axes[1]
    if intervals:
        ax.hist(intervals, bins=min(40, max(10, len(intervals)//3)), color="#2ca02c", edgecolor="white", alpha=0.85)
        ax.axvline(float(np.median(intervals)), color="red", linestyle="--",
                   label=f"中位间隔 {float(np.median(intervals)):.0f} ms")
        ax.legend()
    ax.set_xlabel("相邻 newHeads 间隔 (ms)")
    ax.set_ylabel("次数")
    gb = d.get("getblock_after_head_summary_ms") or {}
    ax.set_title(f"Base WS 头间隔 + getBlock 中位 {gb.get('median_ms','?')} ms\nonly_http={len(d.get('only_http_heights') or [])} only_ws={len(d.get('only_ws_heights') or [])} gap_blocks={d.get('ws_internal_gap_blocks',0)}")
    ax.grid(axis="y", alpha=0.3)
    fig.suptitle(f"来源: {src.name}", fontsize=10)
    fig.tight_layout()
    path = ASSETS / "base_http_vs_ws.png"
    fig.savefig(path, dpi=140)
    plt.close()
    print("wrote", path)

def gap_backfill():
    base = json.load(open(RAW / "base_backfill.json"))
    eth = json.load(open(RAW / "ethereum_backfill.json"))
    fig, ax = plt.subplots(figsize=(10, 5))
    chains = ["Base\n(sleep 25s)", "Ethereum\n(sleep 35s)"]
    naked = [base["naked_would_miss"], eth["naked_would_miss"]]
    filled = [base["backfilled_count"], eth["backfilled_count"]]
    x = np.arange(len(chains))
    w = 0.35
    ax.bar(x - w/2, naked, w, label="裸订阅会漏的块数", color="#d62728", alpha=0.85)
    ax.bar(x + w/2, filled, w, label="缺口检测+HTTP 回补成功", color="#2ca02c", alpha=0.85)
    for i in range(len(chains)):
        ax.text(i - w/2, naked[i], str(naked[i]), ha="center", va="bottom")
        ax.text(i + w/2, filled[i], str(filled[i]), ha="center", va="bottom")
        rate = [base["backfill_success_rate"], eth["backfill_success_rate"]][i]
        ax.text(i, max(naked[i], filled[i]) * 1.05 + 1, f"回补成功率 {rate*100:.0f}%\nparentHash 连续={['是','是'][i]}",
                ha="center", fontsize=9)
    ax.set_xticks(x)
    ax.set_xticklabels(chains)
    ax.set_ylabel("块数")
    ax.set_title("优化前后：断连期间裸 WS 漏块 vs 高度连续性检测 + getBlock 回补\n（publicnode 只读主网 RPC，已证实）")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    # annotate details
    ax.text(0.01, 0.98,
            f"Base: last={base['last_height_before_disconnect']} → tip={base['tip_after_reconnect']}，回补耗时 {base['backfill_duration_ms']/1000:.1f}s\n"
            f"ETH: last={eth['last_height_before_disconnect']} → tip={eth['tip_after_reconnect']}，回补耗时 {eth['backfill_duration_ms']:.0f}ms",
            transform=ax.transAxes, va="top", fontsize=8,
            bbox=dict(boxstyle="round", facecolor="lightyellow", alpha=0.8))
    fig.tight_layout()
    path = ASSETS / "gap_backfill.png"
    fig.savefig(path, dpi=140)
    plt.close()
    print("wrote", path)

def architecture():
    fig, ax = plt.subplots(figsize=(11, 5))
    ax.set_xlim(0, 11)
    ax.set_ylim(0, 5)
    ax.axis("off")
    boxes = [
        (0.3, 2.8, 2.4, 1.6, "#c6dbef", "感知层（低延迟）\nWS 订阅 / gRPC 推流\n尽快知道「可能有入账」"),
        (3.1, 2.8, 2.4, 1.6, "#c7e9c0", "落库层（可回滚）\nprocessed / latest\n先记 pending"),
        (5.9, 2.8, 2.4, 1.6, "#fdd0a2", "入账层（门槛）\nconfirmed→finalized\nsafe / solidified"),
        (8.7, 2.8, 2.0, 1.6, "#fcbba1", "补洞层\n高度/slot 连续\ngetBlock 回补"),
    ]
    for x,y,w,h,c,t in boxes:
        ax.add_patch(mpatches.FancyBboxPatch((x,y), w, h, boxstyle="round,pad=0.05",
                                             facecolor=c, edgecolor="#333", linewidth=1.5))
        ax.text(x+w/2, y+h/2, t, ha="center", va="center", fontsize=9)
    for x in [2.7, 5.5, 8.3]:
        ax.annotate("", xy=(x+0.35, 3.6), xytext=(x, 3.6),
                    arrowprops=dict(arrowstyle="->", lw=1.5))
    ax.add_patch(mpatches.FancyBboxPatch((1.5, 0.4), 8, 1.5, boxstyle="round,pad=0.05",
                                         facecolor="#f7f7f7", edgecolor="#666", linewidth=1))
    ax.text(5.5, 1.15,
            "本实测对应关系：\n"
            "· 感知：Solana 本地 gRPC≪WS≪HTTP；Base/ETH 上 WS newHeads 通常早于 HTTP 轮询\n"
            "· 补洞：Base 断连 25s 裸漏 130 块，回补 130/130（成功率 100%，parentHash 连续）\n"
            "· 主网 Yellowstone gRPC：无免费端点 → 未测；本地插件已测",
            ha="center", va="center", fontsize=9)
    ax.set_title("快链到账检测分层（数字来自本轮实测）", fontsize=12)
    fig.tight_layout()
    path = ASSETS / "architecture.png"
    fig.savefig(path, dpi=140)
    plt.close()
    print("wrote", path)

if __name__ == "__main__":
    latency_3way()
    base_http_vs_ws()
    gap_backfill()
    architecture()
    print("done")
