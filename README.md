# 各链扫块对比 bench

对比「怎么更快、更稳地看到新块 / 新交易」的三种接法：

- HTTP 轮询
- WebSocket 订阅
- 本地 Solana 上的 Yellowstone gRPC（geyser 插件）

本仓库提供可复现脚本，以及一轮已跑完的精选结果（`bench-raw/`）和配图（`assets/`）。  
**主网 Yellowstone gRPC 未测**（没有公开免费端点；不注册付费流、不使用 API key）。本地插件结论不能直接外推到付费主网 SLA。

旧的 Base 45 秒同窗数据（`base_http_vs_ws.json`）因 WS 路径里同步 `getBlock` 导致早停，**已作废，本仓库不收录**。Base 正式口径是 120 秒的 `bench-raw/base_http_vs_ws_full.json`。

## 依赖

Python 3.11+（实测为 3.13.5）：

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

本地三路对比另外需要 Agave `solana-test-validator` 和 Yellowstone geyser 插件，见 [`grpc-local/README.md`](grpc-local/README.md)。脚本默认把结果写到仓库根目录下的 `bench-raw/`、`assets/`（相对脚本定位，不依赖 `/workspace/...` 绝对路径）。

公开 RPC 对照只读公共端点（publicnode、Solana 官方），不需要密钥。

## 怎么跑

以下命令都在**仓库根目录**执行。

### 1. 本地 Solana：gRPC / WS / HTTP 三路同窗

先按 `grpc-local/README.md` 启动带插件的 `solana-test-validator`，并生成 `grpc-local/gen/` 下的 proto stub。节点起来后：

```bash
mkdir -p bench-raw/solana-local-3way
python scripts/bench_solana_local_3way.py processed 100 bench-raw/solana-local-3way/processed_run1.json
python scripts/bench_solana_local_3way.py confirmed 100 bench-raw/solana-local-3way/confirmed_run1.json
# 可重复跑 run2 / run3，再汇总
python scripts/summarize_local_3way.py
```

脚本会在本地链上用**一次性随机测试密钥**做 airdrop + `SystemProgram.transfer`，不涉及主网资金。

### 2. Base：HTTP `eth_blockNumber` vs WS `newHeads`（推荐 120s）

```bash
python scripts/bench_evm_base_rerun.py
# 写入 bench-raw/base_http_vs_ws_full.json
```

请用这个脚本，不要用 `bench_evm_newheads.py` 去跑 Base 同窗：后者旧默认是 45 秒且存在早停问题，公开包里已改为跳过该步骤，并提示改用本脚本。

### 3. 断连回补（Base / Ethereum）

```bash
python scripts/bench_evm_newheads.py
# 写入 bench-raw/base_backfill.json、ethereum_backfill.json
# 仍会跑一段 Ethereum 同窗（出块慢、样本少，只作方向印证）
```

### 4. Solana 主网公开 RPC 对照（非 gRPC）

```bash
python scripts/bench_solana.py
```

### 5. 重新出图（可选）

```bash
python scripts/make_assets.py
```

## 本轮已证实的要点

核验时间：2026-11-20 约 03:53–04:11（Asia/Singapore，UTC+8）。环境：Agave 4.4.1 + Yellowstone 17.0.2（本地插件）。原始摘要见 `bench-raw/`。

1. **本地 processed**（300 笔 / 3 轮）：gRPC 中位 **2.1 ms**，WS `logsSubscribe` **200.6 ms**，HTTP `getSignaturesForAddress` **313.0 ms**；先到 gRPC **297/300**；漏检 0。  
   注意：`getSignaturesForAddress` 最低 commitment 是 `confirmed`，processed 档的 HTTP 数字是「用 confirmed 轮询去追 processed 发送」的上界，不能当成 processed 级 HTTP API。
2. **本地 confirmed**：gRPC 中位 **256.5 ms** ≈ WS **250.0 ms**（门槛档优势消失）；漏检 0。
3. **Base 120s**：WS 599 / HTTP 589 / overlap 589；HTTP−WS 中位 **+80.4 ms**；WS 内部缺口 0。
4. **断连回补**：Base 裸漏 130 → 回补 130/130；Ethereum 裸漏 2 → 回补 2/2。
5. **Solana 主网公开 RPC 对照**：官方 HTTP−WS slot 中位 **+185.9 ms**；publicnode **+455.0 ms**。
6. **主网 Yellowstone gRPC：未测**（无免费端点）。

配图：`assets/latency_3way.png`、`assets/base_http_vs_ws.png`、`assets/gap_backfill.png`、`assets/architecture.png`。

本仓库只收录 `solana-local-3way/summary.json`（含各 run 汇总，不含逐笔 raw / console），以及正式 120s Base 结果和回补 / 主网 slot 对照摘要。未打包超大 console、逐 run 原始事件，也未收录作废的 45s Base 文件。
