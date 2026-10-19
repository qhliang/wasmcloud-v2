# custom_plugin_d1

Cloudflare D1 的**表数据**访问宿主插件,提供结构化查询接口,**无法操作表结构**。

设计取舍与依据见 [`DESIGN.md`](./DESIGN.md)。

## 与 `custom_plugin_cf_d1` 的区别

仓库里已有的 `custom_plugin_cf_d1` 通过 `query` / `query_batch` 透传任意 SQL 文本。本插件是另一条路线:

| | `custom_plugin_cf_d1` | `custom_plugin_d1`(本插件) |
| --- | --- | --- |
| WIT 包 | `custom:cf-d1/query@0.1.0` | `custom:d1-table/table@0.1.0` |
| 插件 ID | `cf-d1` | `d1-table` |
| SQL 入口 | 任意 SQL 文本 | **无**,只有结构化字段 |
| DDL | 可达(`CREATE` / `ALTER` / `DROP` 都直通) | **不可达** |
| 配置来源 | constructor 参数 + interface config | **仅** interface config |

两者可同时加载,互不影响。

> **为什么需要新插件**:D1 服务端不拦 DDL(官方文档确认 `CREATE TABLE`、`ALTER TABLE`、`DROP TABLE`、`CREATE INDEX` 都是正常语句,`PRAGMA optimize` 甚至会写 `sqlite_stat1`)。若靠宿主侧过滤 SQL 关键字来防守,过滤器的正确性就成了唯一防线,而 SQLite 的逃逸面比表面大得多(`SELECT ... INTO t` 会建表、关键字可被注释切断)。本插件改为不提供 SQL 入口,让 DDL **结构上不可达**。

## 配置

在 `workloaddeployment.yaml` 的 host interface 中设置:

```yaml
components:
  - name: my-component
    type: component
    traits:
      wasmcloud:custom/d1-table:interface:
        account-id: <Cloudflare account ID>
        api-token: <具备 D1 权限的 API token>
        database-id: <D1 database ID>
```

`constructor()` **不接受任何参数** —— 组件在类型层面就无法覆盖配置,凭据的所有权完全在宿主运维方。

## 接口

```wit
resource d1-table-client {
    constructor();

    select(table, joins, projection, conditions, group-by, order-by, page)
        -> result<query-result, table-error>;
    count(table, joins, conditions) -> result<u64, table-error>;
    insert(table, fields) -> result<query-result, table-error>;
    upsert(table, fields, spec) -> result<query-result, table-error>;
    update(table, fields, conditions) -> result<query-result, table-error>;
    delete(table, conditions) -> result<query-result, table-error>;
    stop() -> result<_, table-error>;
}
```

### 查询示例

JOIN + 分组 + 聚合:

```rust
let rows = client.select(
    "orders",
    vec![JoinClause {
        kind: JoinKind::Inner,
        table: "users".to_string(),
        on: vec![condition("user_id", ComparisonOp::Eq, Some(json!(user_id)))],
    }],
    vec![
        SelectItem { expr: Projection::Column(ColumnRef {
            table: Some("users".to_string()),
            name: "name".to_string(),
        }), alias: None },
        SelectItem { expr: Projection::Aggregate(AggregateProjection {
            aggregate: AggregateFn::Sum,
            column: ColumnRef { table: None, name: "amount".to_string() },
        }), alias: Some("total".to_string()) },
    ],
    vec![],
    vec!["users.name".to_string()],       // group-by
    vec![OrderBy {
        column: "total".to_string(),
        direction: SortDirection::Desc,
    }],
    Page { limit: Some(100), offset: None },
).await??;
```

### 不支持

`HAVING`、子查询、`CASE WHEN`、窗口函数、`UNION`、`INSERT OR REPLACE`。需要时再显式添加 —— 与其开放一个 SQL 入口,不如让每一项能力都留在结构化的可控范围内。

## 标识符规则

表名、列名、别名是唯一无法参数化的位置,因此必须校验:

```
^[A-Za-z_][A-Za-z0-9_]{0,62}$
```

通过后统一以 `"..."` 包裹。所以名字恰好是 `select`、`from` 这类关键字是**允许**的 —— 引号使其成为字面标识符。所有**值**走 `?` 参数绑定。

## 护栏

| 条件 | 原因 |
| --- | --- |
| `insert` / `upsert` 的 `fields` 为空 | 无意义语句 |
| `update` / `delete` 的 `conditions` 为空 | 防一条调用改写/清空全表 |
| `select` 的 `projection` 为空 | 无意义语句 |
| `join-clause.on` 为空 | 笛卡尔积,通常是用错 |
| `page.limit > 1000` | **报错而非钳制** —— 静默少返回数据最难被发现 |
| `is-null` / `not-null` 带值;其余算子缺值 | 语义矛盾 |
| `upsert` 的 `conflict-columns` 为空 | 省略冲突目标会匹配任意唯一约束,不可预测 |
| `mode=ignore` 却带 `update-columns` | 语义矛盾 |
| `update-columns` 含有不在 `fields` 中的列 | `excluded."x"` 会引用不存在的列 |

## upsert 语义(务必阅读)

`upsert` 映射到 SQLite 的 `INSERT ... ON CONFLICT`。以下行为来自 SQLite,本插件不改变它们:

- **仅对唯一性约束生效**(PRIMARY KEY / UNIQUE / unique index)。NOT NULL、CHECK、外键约束失败仍会报错 —— 不要理解为"吞掉所有冲突"。
- `DO UPDATE` 的冲突算法恒为 `ABORT`:部分失败即整条语句回滚。
- **UPSERT 不支持虚拟表**。D1 支持 FTS5,向 FTS 表 upsert 会直接报错。

### 为什么不提供 `INSERT OR REPLACE`

`OR REPLACE` 名字像 upsert,语义完全不同:冲突时先 `DELETE` 旧行再插入新行。因此会触发外键 `ON DELETE CASCADE`、把未列出的列重置为默认值、`last_insert_rowid` 变化、DELETE 与 INSERT 触发器都执行。混入接口几乎必然误用,而 `mode=update` 已覆盖真实需求。

## 错误语义

| 类别 | 返回 | 组件侧效果 |
| --- | --- | --- |
| 标识符非法、结构不可用 | `result::Err(invalid-identifier / invalid-request)` | 可恢复,请求写错了 |
| D1 拒绝语句 | `result::Err(database-error)` | 业务错误(如唯一约束冲突) |
| HTTP / 鉴权 / 超时 | trap(`connection-error` 经由 trap 路径) | 服务不可用,可重试 |

区分前两者是为了让组件能判断"该改代码"还是"该重试"。

## 实现要点

- **HTTP 客户端按 `account_id|database_id|token哈希` 缓存**。宿主每次导出调用可能重新实例化 guest,导致每次都新建 `reqwest::Client`、重新 TLS 握手,网络抖动时每个查询各自挂到超时(2026-09-11 生产排查)。
- **超时 5s 连接 / 30s 整体**。无超时时请求会挂 40~52s,期间堵住宿主串行的回调分发器。
- 指标:`d1_table_queries_total`,带 `operation` 属性。

## 开发

```bash
cargo test  -p custom_plugin_d1
cargo clippy -p custom_plugin_d1
```