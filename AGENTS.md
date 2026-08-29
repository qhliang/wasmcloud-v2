# AGENTS.md

本文件为 Codex agent 在本仓库中工作时提供指导和约束。

## 项目背景

本项目基于 upstream 仓库 wasmcloud，修改后推送到 origin。远程仓库信息通过 `git remote -v` 查看。

## 构建命令

```bash
cargo build                                # 构建 wash CLI（默认 workspace 成员）
cargo build --workspace                    # 构建所有 workspace crate
cargo build --release                      # Release 构建
cargo test --workspace                     # 运行所有 crate 测试
cargo test -p wash-runtime --features wasi-tls,host-component-plugins  # CI 必跑
cargo clippy --workspace --features wasi-tls,host-component-plugins    # CI lint 命令
cargo +nightly fmt -- --check              # 代码格式检查
cargo machete                              # 未使用依赖检查
cargo xtask build-fixtures                 # 生成 wash-runtime 集成测试所需 wasm fixtures（tests/wasm/ 下，被 gitignore）
```

## 仓库结构

- `crates/wash/` — CLI 二进制 + 库，子命令在 `src/cli/` 下
- `crates/wash-runtime/` — 基于 Wasmtime 的运行时
  - `host/` — HTTP 服务器 (`http.rs`, `http_p3.rs`)
  - `plugin/` — WASI 接口实现（blobstore, config, keyvalue, logging, messaging, otel, postgres, webgpu）
  - `engine/` — 引擎和组件实例化 (`ctx.rs`, `workload.rs`)
- `crates/custom_plugin_*` — 宿主端插件，编译进宿主二进制：
  - 存储/基础设施：`kv`, `blobstore`, `cf_d1`, `nats_utils`, `event_monitor`
  - LLM/通信：`llm_gateway_provider`, `mail`, `codex`
  - 调度：`crontab`, `workflow`, `task_queue`
  - IM：`dingtalk_stream`, `feishu`, `wechat`, `telegram`
- `crates/task_queue_core/` — task queue 共享核心库：协议类型、`QueueConfig`、JetStream 资源命名、META KV 读写、任务提交/取消、结果 schema 与重试退避；宿主插件与 native worker 共用，避免协议漂移
- `crates/task_queue_worker/` — Rust native worker 运行时：实现 `task_queue_core::worker::Worker` 后用 `WorkerRunner` 拉取 `<queue>-worker` durable consumer
- `crates/wash-runtime/src/plugin/mod.rs` — `HostPlugin` trait 定义
- `crates/wash/src/cli/host.rs` — host 模式插件注册
- `crates/wash/src/cli/dev.rs` — dev 模式插件注册
- `examples/http-api-distributed/` — 集成测试 example

## 代码规范

- **禁止 `unwrap()`、`expect()`、`panic!()`** — 使用 `anyhow::Result` + `.context()`
- **禁止 `println!`/`eprintln!`** — 使用 `tracing` crate
- **禁止 `dbg!`** — 使用 `tracing`
- **禁止 `arr[i]` 直接索引** — 用 `.get(i)?` / `.get(i).context(...)?`
- 字符串插值：`format!("{value}")` 而不是 `format!("{}", value)`
- 错误消息：小写开头，无末尾句号
- `warnings = 'deny'` — 任何警告都会导致编译失败

## 版本规范

- **代码变更后无需改动 wasmcloud 项目版本**：修改任何代码（bug 修复 / 功能新增 / 重构 / 配置调整）后，不需要修改 workspace 根 `Cargo.toml` 的 `[workspace.package].version`。
- **custom host plugin 子 crate 可单独更新版本**：若改动落在某个 custom host plugin 子 crate（`crates/custom_plugin_*`），可以更新该子 crate 自己的版本号（在子 crate 的 `Cargo.toml` 中声明独立 `version`，不再继承 `workspace.package`）。其余子 crate 与 workspace 根版本保持不变。
- 仅当子 crate 即将对外发布时才需要 bump 版本；内部未发布的功能改动可暂不更新。

## 新增/修改 custom plugin 清单

**新增 custom plugin：**

1. 创建 `crates/custom_plugin_<name>/` 目录，包含 `Cargo.toml`、`src/lib.rs`、`wit/deps/<name>.wit`、`wit/world.wit`
2. 在 workspace 根 `Cargo.toml` 的 `members` 中添加
3. 在 `crates/wash/Cargo.toml` 中添加依赖
4. 在 `crates/wash/src/cli/host.rs` 中 import 并 `with_plugin()`
5. 在 `crates/wash/src/cli/dev.rs` 中同样注册
6. 参考已有 plugin（如 `custom_plugin_crontab`、`custom_plugin_event_monitor`）的实现模式

**修改 custom plugin 逻辑时：**

7. **必须同步更新对应的 `README.md`**，包括接口变更、新增/删除 export/import、配置参数变化等

## 提交规范

- 格式：`<type>: <description>`，类型：`feat` / `fix` / `docs` / `style` / `refactor` / `test` / `chore`
- **禁止自动 push** — push 前必须征得用户明确同意
- **更新代码后需同步更新 `AGENTS.md`** — 若改动影响构建命令、feature 标志、仓库结构、插件清单或代码规范，应同步更新本文件
- commit 前必须执行（全 workspace 范围，不可用 `-p` 缩小）：
  1. `cargo test --workspace` + `cargo test -p wash-runtime --features wasi-tls,host-component-plugins`
  2. `cargo +nightly fmt -- --check`
  3. `cargo clippy --workspace --features wasi-tls,host-component-plugins`
  4. `cargo machete`
- **fixtures 导致的失败可忽略，不用处理** — `cargo test --workspace` 编译集成测试需要 `crates/wash-runtime/tests/wasm/*.wasm`（构建产物，被 gitignore，需 `cargo xtask build-fixtures` 生成；部分 fixture 如 `http-egress-plugin` 需从 ghcr.io 拉取 WASI 依赖，网络受限时会失败）。此类失败属于环境问题，与代码改动无关，可直接提交

## http-api-distributed example 编译

在 `examples/http-api-distributed/` 下执行 `../../target/debug/wash build --skip-fetch`

## 用户偏好

- 使用中文回复
- "继续" 表示直接推进不必确认
- "提交到本地git" 表示 commit 但不 push
- 偏好提供多个优先级的方案选项（P0/P1/P2），只做单一高优先级改动
- 精确值（如 5MB、500 chars）应作为硬约束常量实现
- 交互式功能（如 onclick）必须验证导出到了公共命名空间
