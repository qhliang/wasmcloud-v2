# CLAUDE.md

本文件为 Claude Code (claude.ai/code) 在本仓库中工作时提供指导。

## 代码仓库背景

本项目引用upstream仓库wasmcloud，修改后提交至original。可通过git remote -v查看远程仓库信息。

## 构建与开发命令

```bash
cargo build                              # 构建 wash CLI（默认 workspace 成员）
cargo build --workspace                  # 构建所有 workspace crate
cargo build --release                    # Release 构建（启用 LTO，opt-level="s"，stripped）
cargo test                               # 运行 wash 的测试
cargo test --workspace                   # 运行所有 crate 的测试
cargo test <test_name>                   # 按名称运行单个测试
cargo test --test integration_oci        # 运行指定的集成测试文件
cargo test -p wash-runtime --features wasip3  # 启用 wasip3 特性测试（CI 必跑）
cargo clippy --workspace                 # 对所有 crate 执行 lint 检查
cargo clippy --workspace --features wasip3   # CI 实际使用的 lint 命令
cargo +nightly fmt -- --check            # 检查代码格式（需要 nightly 工具链）
cargo machete                            # 检查未使用的依赖
cargo zigbuild --release --target <target>   # 交叉编译 release 二进制（Linux musl 用）
make proto                               # 重新生成 protobuf 代码（需要 Docker 和 buf）
gh run list                              # 检查Github Workflow运行情况
```

集成测试需要设置环境变量：`OCI_INTEGRATION_TESTS=1`、`NATS_INTEGRATION_TESTS=1`。

### 构建依赖

- **Rust toolchain**：通过 `rust-toolchain.toml` 锁定 stable，含 clippy/rustfmt，目标平台 `wasm32-unknown-unknown`、`wasm32-wasip1`、`wasm32-wasip2`
- **protoc** 29.x（编译依赖 `pbjson-build`/`tonic-prost-build` 的 crate 时需要）
- **Docker + buf**（仅 `make proto` 需要）
- **nightly 工具链**（仅 `cargo +nightly fmt` 需要）

## 架构

**wash** 是一个用于开发、构建和管理 WebAssembly 组件的 CLI 工具，基于 Component Model 和 WASI Preview 2。

### Workspace Crate（`crates/`）

> **注意**：`Cargo.toml` 的 `workspace.members` 只列出了 `wash`、`wash-runtime` 以及 `custom_plugin_*`。`llm-gateway-*`（http/messaging/messaging-proxy/types）目前**不在 workspace 成员中**，不会被 `cargo build --workspace` 编译。

- **`wash`** — CLI 二进制 + 库。子命令位于 `src/cli/`（每个子命令一个文件：build, dev, host, new, oci, wit, config, inspect, update, completion）。
- **`wash-runtime`** — 基于 Wasmtime 的运行时，用于执行 Wasm 组件：
  - `engine/` — Wasmtime 引擎配置和组件实例化（`ctx.rs`/`mod.rs`/`value.rs`/`workload.rs`）
  - `host/` — 宿主进程（`http.rs`/`http_p3.rs` HTTP 服务器、`sysinfo.rs` 系统信息）
  - `plugin/` — WASI 接口实现（wasi_blobstore, wasi_config, wasi_keyvalue, wasi_logging, wasi_otel, wasi_webgpu, wasmcloud_messaging, wasmcloud_postgres）
  - `washlet/` — 基于 OCI 的组件拉取与执行运行时
  - `sockets/` — 网络套接字支持
  - 顶层文件：`lib.rs`、`observability.rs`、`oci.rs`、`types.rs`、`wit.rs`
  - 可选特性 `wasip3`：启用 Wasmtime WASI P3 支持（CI 单独跑测试与 lint）
- **`custom_plugin_*`** — 宿主端插件，编译进宿主二进制文件中。当前包括：
  - 存储/基础设施：`custom_plugin_kv`、`custom_plugin_blobstore`、`custom_plugin_cf_d1`（Cloudflare D1）、`custom_plugin_nats_utils`
  - LLM/通信：`custom_plugin_llm_gateway_provider`、`custom_plugin_mail`、`custom_plugin_codex`
  - 调度：`custom_plugin_crontab`
  - IM 平台：`custom_plugin_dingtalk_stream`（钉钉）、`custom_plugin_feishu`（飞书）、`custom_plugin_wechat`（微信）、`custom_plugin_telegram`

### 非 Rust 组件

- **`runtime-operator/`** — Kubernetes operator（Go）
- **`runtime-gateway/`** — 网络网关（Go）
- **`charts/`** — Kubernetes 部署的 Helm charts
- **`proto/`** — Protocol Buffer 定义（通过 `buf` 生成）

### 关键设计要点

- WebAssembly Component Model 与 WASI Preview 2；目标平台 `wasm32-wasip2`、`wasm32-wasip1`、`wasm32-unknown-unknown`
- Rust edition 2024，MSRV 1.91.0，Cargo resolver v3
- Release 配置：启用 LTO，`opt-level = "s"`，stripped

## 代码规范

- **禁止使用 `unwrap()`、`expect()`、`panic!()`** — clippy `deny` 级别。使用 `anyhow::Result` 配合 `.context()`
- **禁止使用 `println!`/`eprintln!`** — 所有 CLI 输出使用 `CommandOutput`
- **禁止使用 `dbg!`** — 使用 `tracing` crate 进行日志记录
- **禁止直接索引 `arr[i]`/`slice[i]`** — clippy `indexing_slicing = 'deny'`，必须用 `.get(i)?` / `.get(i).context(...)?` 处理越界
- 超过 100ms 的操作使用 `#[instrument]`
- 环境变量以 `WASH_` 为前缀
- 字符串插值：使用 `format!("{value}")` 而非 `format!("{}", value)`
- 错误消息：小写开头，无末尾句号
- 完整 lint 规则见根 `Cargo.toml` 的 `[workspace.lints.rust]` 和 `[workspace.lints.clippy]`，全部 `warnings = 'deny'`（任何告警即编译失败）

## 提交格式

```
<type>: <description>
```

类型：`feat`、`fix`、`docs`、`style`、`refactor`、`test`、`chore`

## 提交命令

git push -u origin main

## commit前必须要做的步骤

**关键约束**：以下命令必须**全 workspace 执行**，禁止用 `-p <crate>` 缩小范围。理由：本地 nightly rustfmt 与 CI 的 nightly 版本可能不同，导致未触碰的 crate 中存在格式漂移；只在改动的 crate 上跑 fmt/clippy 会漏掉这些漂移，push 后 CI 才报错。即使本次改动只涉及单个 crate，也要跑全 workspace 检查。

1. 执行test测试：`cargo test --workspace` 与 `cargo test -p wash-runtime --features wasip3`
2. 使用fmt格式化：`cargo +nightly fmt -- --check`（**禁止 `-p` 过滤**）
3. 使用clippy检查：`cargo clippy --workspace --features wasip3`（**禁止 `-p` 过滤**）
4. 使用machete检查未使用的依赖：`cargo machete`

> 注：`cargo-machete` 在根 `Cargo.toml` 的 `[workspace.metadata.cargo-machete]` 中忽略了一些在 `build.rs` 中使用的依赖（如 `pbjson`/`tonic`/`tonic-prost`），这是预期的。

> 历史教训：commit `7af9ad04a` 引入 `find_interface` 后只跑了局部 fmt，遗留了 8 个 custom_plugin_* 文件的 `let-else` 漂移，导致 main 分支 CI 连续 5 次 wash workflow 失败。事后只能用 `cargo +nightly fmt --` 一次性修复。

## http-api-distributed示例编译方法

在examples/http-api-distributed目录下执行../../target/debug/wash build --skip-fetch构建
