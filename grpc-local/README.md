# 本地 Agave + Yellowstone gRPC 插件

本目录只提供 **proto 源文件**和一份面向本地 `solana-test-validator` 的 `config.example.json`。  
**请自行下载** Yellowstone geyser 插件与 Agave，不要把下面这些文件提交进仓库：

- `libyellowstone_grpc_geyser.so`（或其它平台的插件二进制）
- `client-linux-amd64` 等官方 CLI
- `ledger/`（账本）
- validator keypair / 身份密钥
- 填好真实 `libpath` 的 `config.json`（可从 example 复制后本地使用，不要提交）

本轮实测用过的组合：**Agave 4.4.1** + **Yellowstone gRPC geyser 17.0.2+solana.4.4.1**（proto 14.0.1）。插件版本需要与 validator 的 Solana 版本匹配。

## 1. 安装 Agave（`solana-test-validator`）

按 [Agave](https://github.com/anza-xyz/agave) / Solana 官方安装说明，装好 4.4.1（或你要对齐的版本），确认：

```bash
solana-test-validator --version
```

## 2. 下载 Yellowstone geyser 插件

到 [rpcpool/yellowstone-grpc](https://github.com/rpcpool/yellowstone-grpc) 的 Releases，下载与 Agave/Solana 版本匹配的 `libyellowstone_grpc_geyser.so`（或对应平台产物），放到本目录，并保证 `config.json` 里的 `libpath` 指向它（example 里是 `./libyellowstone_grpc_geyser.so`）。

## 3. 生成 Python stub

三路对比脚本会从 `grpc-local/gen/` 导入生成代码，需要先：

```bash
# 在仓库根目录
python -m pip install grpcio grpcio-tools
mkdir -p grpc-local/gen
python -m grpc_tools.protoc \
  -I grpc-local \
  --python_out=grpc-local/gen \
  --grpc_python_out=grpc-local/gen \
  grpc-local/geyser.proto grpc-local/solana-storage.proto
# 让 gen 成为包（可选）
touch grpc-local/gen/__init__.py
```

`scripts/bench_solana_local_3way.py` 已经把 `grpc-local/gen` 加进 `sys.path`。

## 4. 启动本地验证节点

```bash
cd grpc-local
cp config.example.json config.json
# 按需确认 libpath 指向你下载的 .so
solana-test-validator --ledger ./ledger \
  --geyser-plugin-config ./config.json --rpc-port 8899
```

默认端口：HTTP RPC `8899`，WebSocket `8900`，gRPC `10000`（见 example 的 `grpc.listen`）。

`config.example.json` 是按本地实测改过的可运行模板（监听 `127.0.0.1:10000`，`transactions.any = true`，没有 TLS / billing / 本机绝对路径）。它不是上游仓库的原版 example。`x_token` 保持为 `null`，本地不需要鉴权。
