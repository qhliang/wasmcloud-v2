# custom_plugin_d1 设计文档

## 1. 目标与边界

新增 custom host plugin `custom_plugin_d1`，对外只提供**操作特定 Cloudflare D1 库中表数据**的接口，
**不提供任何操作表结构（DDL）的能力**。

### 明确支持的场景

- 单表 CRUD：`insert` / `update` / `delete` / `select`
- 跨表 JOIN：`inner` / `left`
- 聚合：`COUNT(*)`、`COUNT(col)`、`SUM` / `AVG` / `MIN` / `MAX`
- 分组与排序：`GROUP BY`、`ORDER BY`、分页
- 计数：`count()`
- UPSERT：`ON CONFLICT ... DO UPDATE` / `DO NOTHING`

### 明确不支持（刻意的设计取舍）

| 不支持 | 原因 |
| --- | --- |
| DDL（`CREATE` / `ALTER` / `DROP` / `TRUNCATE`） | 核心约束。guest 无 SQL 入口 ⇒ 类型层面不可达 |
| `PRAGMA` / `ATTACH` / `DETACH` / `VACUUM` / `REINDEX` / `ANALYZE` | 结构与文件级操作 |
| `HAVING` | 需要 condition 目标支持聚合表达式，显著抬高接口复杂度；当前需求不需要 |
| 子查询 / `CASE WHEN` / 窗口函数 / `UNION` | 需要通用表达式 AST。当前需求不需要 |
| `INSERT OR REPLACE` | 见 §6.2 |
| 表名/列名黑名单（`sqlite_*` / `_cf_*`） | 决策为"所有表开放"，写入系统表由 D1 平台侧拒绝 |

---

## 2. 核心安全模型

### 2.1 为什么不能复用 `custom_plugin_cf_d1`

`custom_plugin_cf_d1` 的 `query` / `query_batch` 是**裸 SQL 透传**（`custom_plugin_cf_d1/src/lib.rs:398-436`），
`sql` 参数未经任何校验直接发往 D1。经全仓检索确认：

- 无任何 SQL parser 依赖（`Cargo.toml` 中无 `sqlparser` / `rusqlite` / `libsqlite` / `diesel` / `sea-query`）
- 无任何 SQL 关键字过滤、DDL guard 或只读校验原语

**D1 服务端也不拦 DDL**。官方文档确认 `CREATE TABLE` / `ALTER TABLE` / `DROP TABLE` / `CREATE INDEX` /
`CREATE VIEW` 都是正常支持的语句，`PRAGMA optimize` 甚至会写入 `sqlite_stat1`。

因此 **宿主侧是唯一防线**，不可依赖服务端兜底。

### 2.2 防线设计：结构化而非过滤

如果选择"暴露受限 SQL + 关键字过滤"，防线是**运行时**的，且 SQLite 的逃逸面比表面看起来大
（`SELECT ... INTO newtable` 会建表；关键字可被注释切断；`INTO` 在 `INSERT INTO` 中合法但在
`SELECT INTO` 中是 DDL）。手写 tokenizer 本身即安全负债。

本插件改为**结构化查询构建器**：guest 提交的是 AST 字段，不是 SQL 字符串。
防线从"运行时校验"变为"**结构上不可达**"—— 这是最强的保证形态，且无需引入并维护 SQL 解析器。

### 2.3 唯一的注入面：标识符

值一律走 `?` 参数化（SQLite 预编译语句原生支持）。**表名、列名、别名、表限定符无法参数化**，
是本插件唯一的注入面。对策是两道：

1. 正则白名单 `^[A-Za-z_][A-Za-z0-9_]{0,62}$` —— 挡掉引号、分号、空格、非 ASCII、关键字前缀
2. 通过后统一用 `"..."` 包裹 —— 挡掉残余

两道叠加后，标识符不可能逃逸出字符串字面量。

---

## 3. WIT 接口

package `custom:d1-table@0.1.0`，world 中的接口走 `imports`。

> **注意**：仓库中 5 个插件（`cf-d1` / `mail` / `feishu` / `telegram` / `wechat`）的 `world()`
> 把宿主提供的接口写进了 `exports`，与 `crontab` / `event_monitor` / `task_queue` 的正确写法相反。
> 权威定义见 `wash-runtime/src/wit.rs:119-139`：`imports` = 插件**提供**给 workload 的。
> **本插件不照抄这一历史笔误。**

### 3.1 `types` interface

```
variant table-error {
    invalid-request(string),
    invalid-identifier(string),
    database-error(string),
    connection-error(string),
    unexpected(string),
}

record d1-config { account-id: string, api-token: string, database-id: string }
variant column-value { null, integer(s64), real(f64), text(string), blob(list<u8>) }
record result-row { values: list<column-value> }
record column-meta { name: string, column-type: option<string> }
record query-result { columns, rows, rows-affected, last-insert-rowid }

enum aggregate-fn { count, sum, avg, min, max }
enum comparison-op { eq, ne, lt, le, gt, ge, like, is-null, not-null }
record condition { column: string, op: comparison-op, value: option<column-value> }

variant projection {
    column(table: option<string>, name: string),
    count-all,
    aggregate(aggregate-fn, table: option<string>, name: string),
}
record select-item { expr: projection, alias: option<string> }

enum join-kind { inner, left }
record join-clause { kind: join-kind, table: string, on: list<condition> }
enum sort-direction { asc, desc }
record order-by { column: string, direction: sort-direction }
record page { limit: option<u32>, offset: option<u32> }
record field { name: string, value: column-value }

enum upsert-mode { update, ignore }
record upsert-spec { mode, conflict-columns: list<string>, update-columns: list<string> }
```

### 3.2 `table` interface

```wit
resource d1-table-client {
    constructor();                                   // 无参
    select(table, joins, projection, conditions, group-by, order-by, page) -> query-result
    count(table, joins, conditions) -> u64
    insert(table, fields) -> query-result
    upsert(table, fields, spec) -> query-result
    update(table, fields, conditions) -> query-result
    delete(table, conditions) -> query-result
    stop() -> _
}
```

### 3.3 `d1-config` 的"复用"

`d1-config` 定义在 `custom:cf-d1` 包的 `interface types` **内部**（`custom_plugin_cf_d1/wit/deps/custom-cf-d1.wit:20-27`），
不是包顶层类型。若在新包中 `use custom:cf-d1/types.{d1-config}`，新插件的 world 就必须同时
import `custom:cf-d1/types`，导致两个插件同时提供同一 host interface，依赖 wasmtime linker 对重复定义的容忍度。

事实基础：

- 全仓 8 个插件的 `wit/deps/` **各只有自己那一个包**，零跨包 `use` 先例
- 每个插件的 `bindgen!` 独立调用 `add_to_linker`，重复定义同一 interface 的行为无先例、无测试覆盖

且这种耦合对 guest **没有任何收益**：guest 无论如何都要为新接口 `custom:d1-table/table` 生成新绑定，
constructor 参数的类型来自哪个包不影响调用点。

**结论**：在本包 `types` interface 中**逐字段复制** `d1-config` 定义。WIT record 是结构化的，
字段名与类型一致即 wire 兼容，零耦合、零新模式。

### 3.4 配置来源：仅 interface config

WIT `constructor()` **无参**，guest 在类型层面就没有传入配置的可能。
宿主侧统一使用 `resolve_field(None, &interface_config, key)`
（`wash-runtime/src/plugin/config.rs`），已验证 `wasm_value = None` 时直接落到 `interface_config`。

与 `custom_plugin_cf_d1` 的双配置源（constructor 优先 + interface config 兜底）不同：
本插件**不允许覆盖**，配置权完全归宿主运维。

---

## 4. SQL 生成

| 方法 | 形态 |
| --- | --- |
| `select` | `SELECT <items> FROM "t" <joins> [WHERE ..] [GROUP BY ..] [ORDER BY ..] [LIMIT ?] [OFFSET ?]` |
| `count` | `SELECT COUNT(*) FROM "t" <joins> [WHERE ..]` |
| `insert` | `INSERT INTO "t" ("a","b") VALUES (?,?)` |
| `upsert` | `INSERT INTO "t" ("a","b","c") VALUES (?,?,?) ON CONFLICT ("a") DO UPDATE SET "b"=excluded."b", "c"=excluded."c"`<br>`INSERT INTO "t" ("a","b") VALUES (?,?) ON CONFLICT ("a") DO NOTHING` |
| `update` | `UPDATE "t" SET "a"=? WHERE ..` |
| `delete` | `DELETE FROM "t" WHERE ..` |

要点：

- 投影项：`"t"."c" AS "x"` / `COUNT(*)` / `SUM("t"."c") AS "x"`
- JOIN：`LEFT JOIN "u" ON ..`
- 条件：`"c" = ?`；`"c" IS NULL` / `"c" IS NOT NULL` **不占位符**
- upsert 更新值用 `excluded."col"`（而非重复 `?`）—— 少一组参数，且语义明确为"本次尝试插入的值"
- `offset` 有值而 `limit` 为 `none` 时生成 `LIMIT -1 OFFSET ?`（SQLite 要求 OFFSET 必须配 LIMIT）

---

## 5. 护栏清单

### 5.1 标识符

- 不匹配 `^[A-Za-z_][A-Za-z0-9_]{0,62}$` → `invalid-identifier`
- 错误信息中给出可复制正则（依 `task_queue_core/src/config.rs:102-117` 的风格）
- **不加** `sqlite_` / `_cf_` 前缀黑名单（决策：所有表开放）

### 5.2 结构

| 条件 | 理由 |
| --- | --- |
| `insert` / `upsert`：`fields` 为空 → 拒绝 | 无意义的语句 |
| `update` / `delete`：`conditions` 为空 → 拒绝 | 防一条调用清全表 |
| `select`：`projection` 为空 → 拒绝 | 无意义的语句 |
| `join-clause.on` 为空 → 拒绝 | 笛卡尔积，通常是 bug |
| `page.limit > 1000` → **报错**（非钳制） | 静默截断会让调用方拿到不完整数据而不知情 |
| `group-by` 列未出现在 `projection` 中 → 不报错，仅文档说明 | SQLite 允许，结果由实现定义 |

### 5.3 条件

- `is-null` / `not-null` 带 `value` → 拒绝
- 其余 op 缺 `value` → 拒绝

### 5.4 upsert

| 条件 | 理由 |
| --- | --- |
| `conflict-columns` 为空 → 拒绝 | 见 §6.1 |
| `mode=ignore` 却带 `update-columns` → 拒绝 | 语义矛盾 |
| `mode=update` 且解析后 `update-columns` 为空 → 拒绝 | 否则 `DO UPDATE SET` 空列表是语法错误 |
| `update-columns` 含有不在 `fields` 中的列 → 拒绝 | `excluded."x"` 会引用不存在的列 |

---

## 6. upsert 设计依据

### 6.1 为什么强制显式冲突目标

SQLite 3.35.0 起允许 `DO UPDATE` 省略冲突目标，此时它会匹配**任何**未被前序子句捕获的唯一性约束。
行为不可预测。接口**强制要求**显式 `conflict-columns`。

`ON CONFLICT` 语法自 SQLite 3.24.0（2018）引入；D1 是现代 SQLite，两者均可用。
D1 仅在参数绑定上有限制（只支持 `?` 与 `?NNN`，不支持命名参数），与本设计一致。

### 6.2 为什么不提供 `INSERT OR REPLACE`

它名字像 upsert，语义**完全不同**：冲突时先 `DELETE` 旧行再插入新行。因此：

- 触发外键 `ON DELETE CASCADE`
- 未列出的列被重置为默认值（真 upsert 则保留）
- `last_insert_rowid` 变化
- DELETE / INSERT 触发器都会执行

混入接口几乎必然误用，而 `mode=update` 已覆盖真实需求。

### 6.3 使用者必须知道的 SQLite 语义

| 事实 | 影响 |
| --- | --- |
| UPSERT **仅对唯一性约束生效**（PK / UNIQUE / unique index），不干预 NOT NULL / CHECK / 外键 / 触发器 | 不能理解为"吞掉所有冲突" |
| `DO UPDATE` 的冲突算法恒为 `ABORT` | 部分失败即整条语句回滚 |
| **UPSERT 不支持虚拟表（vtab）** | ⚠️ D1 支持 FTS5，向 FTS 表 upsert 会直接报错 |

以上需在 README 中明示。

---

## 7. 从 `custom_plugin_cf_d1` 复用的部分

均为**已过生产验证**的模式：

| 模式 | 依据 |
| --- | --- |
| `bindgen!` + `with!` 挂 resource handle | `custom_plugin_cf_d1/src/lib.rs:27-37` |
| HTTP 客户端按 `account_id\|database_id\|token哈希` 缓存 | `:49-53, 105-117` |
| 超时：连接 5s / 整体 30s | `:352-358` |
| OTel Counter | `:76-98` |
| `D1QueryRequest/Response` + `/query` 端点 | `:148-180, 429-436` |
| `ColumnValue` ↔ JSON 双向转换 | `:230-269` |

**缓存的必要性**（`custom_plugin_cf_d1/src/lib.rs:106-115` 的注释记录）：
2026-09-11 生产排查实测，宿主每次导出调用可能重新实例化 guest，guest 侧 `OnceLock` 随之失效，
于是每个构造函数都跑一遍；不缓存就是"每次查询新建一个 reqwest Client → 重新 TLS 握手"，
网络抖动时每次查询各自挂到超时，平白放大延迟。

**超时的必要性**（`:349-352` 的注释记录）：
此前无任何超时配置，网络抖动时请求会挂到 TCP/TLS 层自行放弃（实测 40~52s 才报错），
期间堵住宿主**串行**的回调分发器。5s 连接 / 30s 整体超时把失败变成快速失败，交给上层重试。

---

## 8. 错误二分

依 `custom_plugin_mail` 的清晰范例：

| 类别 | 映射 | 效果 |
| --- | --- | --- |
| 业务拒绝（标识符非法、参数缺失、条件为空） | `Err(TableError::InvalidRequest / InvalidIdentifier)` | guest 收到可读的 `result::Err`，可自行处理 |
| 基础设施失败（HTTP / 鉴权 / 超时 / 反序列化） | `Err(wasmtime::Error::msg(...))` | trap，宿主记录 |

区分二者的意义：guest 能区分"我的请求写错了"和"服务端出问题了"，从而决定是修 bug 还是重试。

---

## 9. 测试策略

`src/sql.rs` 内 `#[cfg(test)] mod tests`，与 HTTP 完全解耦，可独立跑绿。

### 9.1 标识符负向

`a"b`、`a;DROP TABLE t`、`a b`、`1a`、空串、63 字符、unicode —— 全部拒绝。

### 9.2 正向

各方法 SQL 精确输出断言，含 JOIN 组合、聚合投影、`excluded.` 前缀、
`update-columns` 空值展开为全部 `fields`、`LIMIT -1 OFFSET`、`limit=1001` 报错。

### 9.3 结构负向

§5 中每条护栏各一条用例。

### 9.4 反向断言：DDL 不可达的形式化回归

这是本插件最重要的一组测试。

- `select` / `count` / `delete` 生成的 SQL 不含
  `create` / `drop` / `alter` / `pragma` / `attach` / `detach` / `vacuum` / `reindex` /
  `analyze` / `into` / `replace` / `begin` / `commit` / `rollback`
- `insert` 生成的 SQL 不含上述词（除 `INSERT INTO` 的 `into`）、
  且不含 `ON CONFLICT`、不含 `OR REPLACE`
- `upsert` 生成的 SQL 含 `ON CONFLICT` 且不含 `OR REPLACE`

### 9.5 其他

- 占位符数量与 `params.len()` 一致
- `ColumnValue` ↔ JSON 往返
- `lib.rs` 参照 `custom_plugin_cf_d1/src/lib.rs:797` 的 mocked-HTTP 测试验证配置缺失路径

---

## 10. 注册点

| # | 文件 | 改动 |
| --- | --- | --- |
| 1 | `wasmCloud/Cargo.toml` | `members` 加 `"crates/custom_plugin_d1"` |
| 2 | `wasmCloud/crates/wash/Cargo.toml` | 加 `custom_plugin_d1 = { path = "../custom_plugin_d1" }`（参照 `:77`） |
| 3 | `wasmCloud/crates/wash/src/cli/host.rs` | `use` + `.with_plugin(Arc::new(...))`（参照 `:856`） |
| 4 | `wasmCloud/crates/wash/src/cli/dev.rs` | 同上（参照 `:314`） |
| 5 | `crates/custom_plugin_d1/README.md` | 接口语义 + 配置示例 + §6.3 提示 |
| 6 | `wasmCloud/AGENTS.md` | 插件清单同步 |

**无需改** `KNOWN_PLUGIN_IDS`：已确认该 roster 只含内置插件，8 个 custom 插件均不在其中。