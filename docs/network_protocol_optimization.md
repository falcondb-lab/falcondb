# FalconDB 网络与协议热路径优化方案

> 基于对 `falcon_protocol_pg` 协议栈（`codec.rs` / `server.rs` / `handler.rs` /
> `handler_fast_path.rs` / `handler_extended.rs`）的完整源码审计，结合 OLTP 对标
> SingleStore 的目标输出。

---

## 1. 现状诊断：热路径上的开销清单

### 1.1 Buffer Copy 次数

| 位置 | 当前行为 | copy 次数 |
|---|---|---|
| `decode_message` → `Bind` param values | `vec![0u8; vlen]; copy_to_slice` — 每个参数独立堆分配 | N alloc + N copy |
| `DataRow` values | `Vec<Option<String>>` — 每列从 `Datum` `to_string()` | 1 copy/列（`Datum→String`） |
| `RowDescription` encode | 临时 `BytesMut body` → `extend_from_slice(&body)` 追加到主 buf | 1 extra copy |
| `ErrorResponse` / `ErrorResponseExt` encode | 同上，临时 `body` | 1 extra copy |
| `AuthenticationSASL` encode | 同上 | 1 extra copy |
| `read_cstring` | `buf[..pos].to_vec()` — 每个 C-string 独立 `Vec<u8>` 堆分配 | 1 alloc/字段 |
| Query 简单路径 `handle_query` | `sql.clone()`（仅在 statement_timeout>0 路径，但属频繁分支） | 条件 copy |
| 响应消息聚合 `BytesMut::with_capacity(64)` | 初始容量 64 bytes 对任何多列结果都触发 `realloc` | 1+ realloc |
| `CopyData` encode | `buf.put_slice(data)` — data 是 `Vec<u8>`，已有 copy at decode 侧 | 双 copy |

**总结**：一条典型 point-select（1行3列）链路从 `recv` 到 `send` 经历 **≥6 次内存 copy**：
decode param → Datum → String × N_col → BytesMut → write_all。

### 1.2 Frame Decode 重分配

`decode_message` 对 `Bind` 消息中每一个参数值做：
```rust
// codec.rs:342-344
let mut val = vec![0u8; vlen as usize];
msg_buf.copy_to_slice(&mut val);
param_values.push(Some(val));
```
参数值是 `Vec<Vec<u8>>`，每个参数单独堆分配。对 prepared statement 的 point-select（`WHERE id = $1`）：1次 `recv` → 1次 frame decode → **1次独立 `Vec` 分配**。

`read_cstring` 对每个 C-string 做 `to_vec()` + `String::from_utf8`，startup 和 Parse 消息中多次调用。

### 1.3 Response Encode 开销

`encode_message_into` 对 `RowDescription`、`ErrorResponse`、`AuthenticationSASL` 先写临时 `BytesMut body` 再 `extend_from_slice` 追加，相当于 **两次写入**：第一次到临时 buf，第二次 copy 到主 buf。

`DataRow` 已优化（预计算 `body_len`，直接写入主 buf）— 此处无问题。

`CommandComplete` 的 `tag` 字段每次响应都做 `.to_string()` 格式化：`"INSERT 0 1"` 等固定模式可缓存。

### 1.4 小包发送 & Flush 策略

当前 simple query 路径（`server.rs:1530-1558`）已做批量写：
```rust
// 已批量：responses + notifications + ReadyForQuery 一次 write_all
stream.write_all(&out).await?;
stream.flush().await?;
```
**但 `stream.flush()` 是 `tokio::io::BufWriter` flush — 仍是一次独立 syscall**。

Extended query 路径（Parse/Bind/Describe/Execute/Sync 混合流水线）问题更大：
- `Parse` 成功后立即 `stream.write_all(&[0x31,0,0,0,4])` — 单独 5 字节 `write_all`
- `Bind` 成功后立即 `stream.write_all(&[0x32,0,0,0,4])` — 单独 5 字节 `write_all`
- `Describe` 通过 `send_message_no_flush` 分多次写
- `Execute` 另一次 `write_all`
- `Sync` 触发 `flush`

一个完整的 `P+B+D+E+S` 流水线产生 **4~6 次独立 `write_all` + 1 次 `flush`**，即 **5~7 个 syscall**。对比 PostgreSQL 的 pipelining 实现，这是 2~3× 的 syscall 超额。

### 1.5 Async Task 切换

`statement_timeout > 0` 时强制 `spawn_blocking` + `tokio::select!`：
```rust
// server.rs:1477-1485
let mut task = tokio::task::spawn_blocking({
    let sql = sql.clone();   // ← clone
    let handler = handler_ref.clone();  // ← Arc clone (cheap) + sql clone (expensive)
    ...
});
```
默认路径（`statement_timeout == 0`）已正确使用 inline 执行（`server.rs:1524-1528`），无切换。  
问题：`statement_timeout` 默认值为 `0`（禁用），但生产配置可能开启，引入 `spawn_blocking` 的线程池调度开销（~10µs）。

### 1.6 gRPC / QUIC 现状

当前 FalconDB **仅实现 PostgreSQL wire protocol（TCP）**，无 gRPC/QUIC 路径。
对 OLTP 短请求，这是正确选择：PG wire protocol 比 gRPC 省去 HTTP/2 framing、
proto encode/decode 各约 1~3µs；QUIC 对局域网低丢包场景无净收益。
**结论：不需要增加 gRPC/QUIC，保持 PG wire protocol。**

---

## 2. 请求生命周期时序图

```
客户端                   Tokio I/O          协议层                   执行层
  │                         │                 │                        │
  │──TCP recv──────────────►│                 │                        │
  │                    read_buf(buf)           │                        │
  │                         │──decode_message►│                        │
  │                         │          FrontendMessage::Query(sql)      │
  │                         │                 │──handle_query_dispatch►│
  │                         │                 │   fast_begin / fast_commit (直返，无执行器)
  │                         │                 │   try_fast_insert_parse │
  │                         │                 │   try_direct_insert ───►│ MemTable DML
  │                         │                 │   execute_single_plan──►│ Executor
  │                         │                 │◄──Vec<BackendMessage>───│
  │                         │◄encode+batch────│                        │
  │◄──TCP send──────────────│                 │                        │
  │   (write_all + flush)   │                 │                        │

典型 point-select (prepared, extended query):

  P(parse)──►  decode → plan cache lookup → ParseComplete (5B write)
  B(bind) ──►  decode → param decode → BindComplete (5B write)
  D(describe)► decode → RowDesc (send_message_no_flush × 2)
  E(execute)►  decode → execute_plan → encode DataRow × N → write_all
  S(sync) ──►  ReadyForQuery → write_all + flush
               ↑
               5-7 个独立 write syscall
```

### 延迟预算表（目标: E2E OLTP ≤ 100µs @P99，局域网）

| 阶段 | 当前估计 | 优化目标 | 方法 |
|---|---|---|---|
| TCP recv（1次 read syscall） | 5–10µs | 5–10µs | 不变 |
| Frame decode（含 cstring alloc） | 1–3µs | 0.2–0.5µs | 零拷贝 slice |
| Param decode（Bind, N=1） | 0.5–2µs | 0.1–0.3µs | arena/SmallVec |
| Plan cache lookup | 0.1–0.5µs | 0.05–0.1µs | 已有 PlanCache |
| Executor (MemTable point lookup) | 1–5µs | 1–3µs | 已有快路径 |
| Response encode（1行3列） | 0.5–2µs | 0.2–0.5µs | 消除临时 buf copy |
| TCP send（write_all + flush） | 5–15µs | 5–8µs | 减少 syscall 次数 |
| **总计** | **13–37µs** | **6–22µs** | |

---

## 3. Copy 次数统计表（优化前 vs 后）

| 操作 | 当前 copy 次数 | 优化后 copy 次数 | 措施 |
|---|---|---|---|
| Bind param value（binary，N=1） | 1 alloc + 1 copy | 0 copy（`Bytes::copy_from_slice` + arena） | §4.1 |
| `read_cstring`（Parse name/query） | 1 alloc + 1 copy/字段 | 0（借用 `&str` from `BytesMut`） | §4.2 |
| `RowDescription` encode | 2 write（temp buf + extend） | 1 write（直接写主 buf） | §4.3 |
| `ErrorResponse` encode | 2 write | 1 write | §4.3 |
| `DataRow` encode（已优化） | 1 write/行 | 1 write/行 | 无需改 |
| `CommandComplete` tag format | 1 `format!` alloc/次 | 0（固定字节串） | §4.4 |
| Extended query Parse/Bind response | 2 独立 `write_all` | 1 batched write | §4.5 |
| Simple query 响应 buf 初始容量 | `with_capacity(64)` → realloc | `with_capacity(256)` | §4.6 |

---

## 4. 具体优化方案

### 4.1 Bind 参数值零拷贝（Binary Protocol Fast Path）

**现状**：`codec.rs:342-344` 对每个参数值做 `vec![0u8; vlen]; copy_to_slice`。

**方案**：使用 `bytes::Bytes` 切片，避免 copy：

```rust
// codec.rs — FrontendMessage::Bind 参数解析
// 修改 param_values 类型为 Vec<Option<Bytes>>
// 在 decode_message 中:
let val = buf.split_to(vlen as usize).freeze(); // Bytes — 零拷贝切片
param_values.push(Some(val));
```

`Bytes::freeze()` 产生一个引用计数切片，底层共享 `BytesMut` 的内存，无 copy。
在 `handler_extended.rs` Bind 处理中，将 `param_values: Vec<Option<Vec<u8>>>` 改为
`Vec<Option<Bytes>>`，`decode_param_value_binary` 直接从 `Bytes` 读取而不分配新 `Vec`。

**预期**：N 参数的 Bind 消息从 N 次堆分配降为 0 次额外分配。

### 4.2 C-string 解析零拷贝

**现状**：`read_cstring` 做 `to_vec()` + `String::from_utf8`，每次返回 `String`（堆分配）。

**方案**：引入借用版本，供解码器在 `split_to` 的 `BytesMut` 上使用：

```rust
/// 返回 C-string 的 UTF-8 字节范围（不分配），由调用者决定是否 to_string()
fn read_cstring_ref(buf: &BytesMut, offset: usize) -> Result<(&str, usize), String> {
    if let Some(rel) = buf[offset..].iter().position(|&b| b == 0) {
        let s = std::str::from_utf8(&buf[offset..offset + rel])
            .map_err(|e| format!("Invalid UTF-8: {e}"))?;
        Ok((s, offset + rel + 1))
    } else {
        Err("No null terminator".into())
    }
}
```

对 Parse 的 `name` 和 `query` 字段：`name` 通常为空字符串（unnamed statement）或短标识符，
可用 `SmallVec<[u8; 32]>` 栈存储；`query` 必须 `to_string`（存入 `PreparedStatement`），
但可延迟到确认不命中 plan cache 时再 clone。

### 4.3 消除 RowDescription / ErrorResponse 临时 buf

**现状**：先写 `body: BytesMut`，再 `extend_from_slice(&body)` 到主 buf — 2 次写操作。

**方案**：预先计算长度，直接写主 buf（与 `DataRow` 现有实现一致）：

```rust
// RowDescription — 直接写主 buf
BackendMessage::RowDescription { fields } => {
    let body_len: usize = 2 + fields.iter().map(|f| {
        f.name.len() + 1 + 4 + 2 + 4 + 2 + 4 + 2  // cstring + fixed fields
    }).sum::<usize>();
    buf.reserve(1 + 4 + body_len);
    buf.put_u8(b'T');
    buf.put_i32(4 + body_len as i32);
    buf.put_i16(fields.len() as i16);
    for field in fields {
        write_cstring(buf, &field.name);
        buf.put_i32(field.table_oid);
        buf.put_i16(field.column_attr);
        buf.put_i32(field.type_oid);
        buf.put_i16(field.type_len);
        buf.put_i32(field.type_modifier);
        buf.put_i16(field.format_code);
    }
}
```

同样处理 `ErrorResponse` / `ErrorResponseExt` / `AuthenticationSASL`：
先遍历计算总长度，`buf.reserve()`，然后单趟写入。

**预期**：消除 `RowDescription` encode 的 1 次内存 copy（每个 SELECT 响应 1 次）。

### 4.4 CommandComplete 固定字节串缓存

**现状**：每次 DML 都 `format!("INSERT 0 {n}")` 分配 String。

**方案**：对小计数（≤16）使用静态字节串，大计数使用 `itoa` crate 零分配整数格式化：

```rust
fn encode_command_complete(buf: &mut BytesMut, tag: &str) {
    // tag 已在 ExecutionResult::Dml { tag } 中格式化
    // 此处可用 tag.as_bytes() 直接写，无需 write_cstring 的额外 pos 查找
    let len = 4 + tag.len() + 1;
    buf.put_u8(b'C');
    buf.put_i32(len as i32);
    buf.put_slice(tag.as_bytes());
    buf.put_u8(0);
}
```

对 DML tag 的构造，`ExecutionResult::Dml` 中改用 `itoa::Buffer` 格式化行数，
避免 `format!` 的 `Formatter` 分配。

### 4.5 Extended Query 流水线批量写（核心改进）

**现状**：Parse / Bind / Execute / Sync 各自独立 `write_all`，产生 4~6 个 syscall。

**方案**：在 connection loop 中引入 **per-message 累积 buf**，在 `Sync` 消息时统一刷出：

```rust
// server.rs connection loop 中增加:
let mut pipeline_buf: BytesMut = BytesMut::with_capacity(4096);
let mut needs_flush = false;

// Parse 成功:
pipeline_buf.put_slice(&[0x31, 0, 0, 0, 4]); // ParseComplete

// Bind 成功:
pipeline_buf.put_slice(&[0x32, 0, 0, 0, 4]); // BindComplete

// Execute 结果:
for response in &responses {
    codec::encode_message_into(&mut pipeline_buf, response);
}

// Sync — 此时刷出全部累积响应:
codec::encode_message_into(&mut pipeline_buf, &BackendMessage::ReadyForQuery {
    txn_status: session.txn_status_byte(),
});
stream.write_all(&pipeline_buf).await?;
stream.flush().await?;
pipeline_buf.clear();
```

这将 `P+B+D+E+S` 的 5~7 个 syscall **压缩为 1 个 write_all + 1 个 flush（2 syscalls）**。

客户端在发送 `Sync` 之前不要求中间响应（PostgreSQL 扩展查询协议规定 client 必须等
ReadyForQuery），因此此优化完全符合协议规范。

**注意**：`FrontendMessage::Flush`（`b'H'`）消息要求立即 flush pipeline buf，不能缓存。

### 4.6 响应 buf 初始容量调整

**现状**：`BytesMut::with_capacity(64)` — 对任何多列结果都触发 `realloc`。

**方案**：
```rust
// 简单查询路径（server.rs:1532）
let mut out = BytesMut::with_capacity(512);  // 覆盖典型 1-5 行结果

// Extended query Execute（server.rs:1980）
let mut out = BytesMut::with_capacity(512);
```

512 bytes 覆盖绝大多数 OLTP point-select 响应（1行×4列 ≈ 120~200 bytes），
同时对批量 SELECT 仍可自动扩容（`BytesMut` 的 doubling 策略）。

### 4.7 DataRow 二进制格式快路径（Binary Protocol）

**现状**：`DataRow.values: Vec<Option<String>>` — 所有值以文本格式编码，整数需
`to_string()` 转换。

**方案**：引入 `DataRowBinary` 变体（或扩展现有 `DataRow` 支持 format_code=1），
对 prepared statement 返回二进制 `DataRow`：

```rust
// 新增 BackendMessage 变体
DataRowBinary { values: Vec<Option<DatumBinary>> }

// 编码: Int32 直接 4 bytes，Int64 直接 8 bytes，Text 直接 UTF-8 bytes
// 无 itoa/format 开销
```

对 OLTP 中最常见的 `INT64` PK 列，二进制格式节省 `itoa` 调用和 String 分配。
客户端需在 `Bind` 的 `result_formats` 中指定格式码 1（大多数现代驱动支持）。

### 4.8 Prepared Statement Bind 快路径（核心 OLTP 路径）

当前已有 plan cache（`PlanCache`）和 `PreparedStatement.plan: Option<Arc<PhysicalPlan>>`。
Bind + Execute 路径已是：

```
Bind → params Datum 解码 → Portal { plan: Some(arc), params }
Execute → execute_plan(plan, params, session)  // 跳过 parse+bind+plan
```

**剩余开销**：`bind_params` 文本替换（仅在 `ps.plan.is_none()` 时），以及
`param_values.iter().enumerate().map(...)` 的 closure + collect。

**优化**：对 `execute_plan` 路径，`params` 已是 `Vec<Datum>` — 确保此路径完全绕过
SQL 文本处理。当 `ps.plan.is_some()` 时，`bound_sql` 设为 `String::new()`（已做），
确认 `execute_plan` 不触发任何 SQL 解析。

### 4.9 Statement Timeout spawn_blocking 开销消除

**现状**：`statement_timeout > 0` 触发 `spawn_blocking` + `sql.clone()`。

**方案**：将 SQL 文本改为 `Arc<str>` 存储，`clone()` 降为原子引用计数增减（无堆分配）：

```rust
// server.rs — query 解析后
let sql: Arc<str> = sql_str.into(); // 一次堆分配，后续 clone O(1)

// spawn_blocking 内:
let sql = Arc::clone(&sql); // 无 copy
```

同时：对 `statement_timeout` 路径增加一个 `tokio::time::timeout` 包装，
替换 `spawn_blocking`（仅当执行层完全 async 化后可行）。
短期方案：将 `sql.clone()` 改为 `Arc<str>` clone，降低 clone 开销。

---

## 5. Syscall 减少方案

| 场景 | 当前 syscall 数 | 优化后 syscall 数 | 方法 |
|---|---|---|---|
| 简单 SELECT（1行） | read(1) + write(1) + flush(1) = 3 | read(1) + write(1) + flush(1) = 3 | 已最优，无需改 |
| Extended P+B+E+S | read(≥1) + write(5) + flush(1) ≈ 7 | read(≥1) + write(1) + flush(1) = 3 | §4.5 pipeline buf |
| Extended P+B+D+E+S | read(≥1) + write(6) + flush(1) ≈ 8 | read(≥1) + write(1) + flush(1) = 3 | §4.5 |
| TCP_NODELAY | 未确认设置 | 确认开启 `TCP_NODELAY` | 见下 |
| SO_REUSEPORT | 未确认 | 多 accept 线程各自绑定 | 见下 |

**TCP_NODELAY 确认**：协议层应在 `TcpListener::accept()` 后立即设置：
```rust
// server.rs accept loop
let stream = TcpStream::from_std(tcp_stream)?;
stream.set_nodelay(true)?; // 禁用 Nagle，消除小包 40ms 延迟
```

**`SO_REUSEPORT`**：多个 Tokio worker 各自绑定同一端口，消除单个 accept 队列的锁争用：
对高并发场景（>1000 conn/s）有显著效果。

---

## 6. 小结果集响应优化

**场景**：point-select 返回 1 行 4 列整数。

**当前链路**：
```
execute → Vec<BackendMessage> {RowDesc, DataRow{4 x Option<String>}, CommandComplete, RFQ}
         → encode_message_into × 4
         → write_all
```

**优化链路**：
```
execute → encode_result_set_into(buf, result)  // 单函数，直接写 buf，无中间 Vec<BackendMessage>
         → write_all (1 syscall)
```

具体：在 `QueryHandler` 内引入 `encode_result_set_into(buf: &mut BytesMut, columns, rows)`，
将 RowDescription + N×DataRow + CommandComplete + ReadyForQuery 一次性写入主 buf，
完全绕过 `Vec<BackendMessage>` 的堆分配和多次 encode 调用。

**预期收益**：消除 `Vec<BackendMessage>` 的 4~8 次 push/alloc，减少 1~2 次 encode 调用。

---

## 7. 批处理请求优化

**Pipeline 场景**：客户端不等待 ReadyForQuery 即发送下一条 Query（pipeline mode）。

当前 `server.rs` 的 `while let Some(msg) = codec::decode_message(&mut buf)` 循环已支持
从一次 `read_buf` 中解码多条消息（批量 decode）。

**问题**：每条 Query 仍独立 `write_all + flush`，阻止响应批量化。

**方案**：在 message loop 中增加 "是否还有待处理消息" 检测：
```rust
// 处理完一条 Query 后，检查 buf 是否还有更多消息
let more_pending = codec::decode_message(&mut buf).is_ok_and(|m| m.is_some());
if more_pending || !buf.is_empty() {
    // 只写不 flush，等待下一条处理后再一起 flush
    stream.write_all(&out).await?;
    // 不 flush — 推迟到 buf 空或 Sync 时
} else {
    stream.write_all(&out).await?;
    stream.flush().await?; // 最后一条消息才 flush
}
```

---

## 8. 网络线程与执行线程协同策略

### 当前架构

```
Tokio runtime (N worker threads)
  └── 每个 connection 一个 async task
        └── handle_query 调用 sync Executor (MemTable 操作)
              → spawn_blocking（statement_timeout 时）
              → inline（无 timeout 时，已优化）
```

### 问题

- Tokio worker 同时承担 I/O 等待和 CPU 执行，在高并发时 CPU-bound 任务
  阻塞 I/O poll，引起延迟抖动。
- `spawn_blocking` 使用 Tokio 的 blocking 线程池（默认 512 线程），不受 CPU 亲和性控制。

### 推荐策略

```
专用 I/O 线程组 (2~4 核):       专用执行线程组 (N-4 核):
  Tokio runtime (io_threads)       线程池 (executor_threads)
  ├── accept + frame decode          ├── MemTable DML
  ├── response encode + send         ├── WAL append
  └── async timer / timeout          └── MVCC commit
           │                                  ▲
           └──────── channel (mpsc) ──────────┘
                   (request + waker)
```

短期（无大重构）：
1. 确保 `handle_query` 的简单路径（无 timeout）inline 执行（已实现）。
2. 将 Tokio `#[tokio::main]` 的 `worker_threads` 设为物理核数，`max_blocking_threads`
   设为 **核数 × 2**（避免过多 blocking 线程产生 OS 调度开销）。
3. 对 WAL flush daemon 和 group commit syncer 使用 `start_flush_thread_on_core`（P1-3，已实现）。

---

## 9. 网络层 Benchmark 场景

| 场景 | 说明 | 关键指标 |
|---|---|---|
| `net/point_select_simple` | Simple Query SELECT pk=N，1行1列 | latency P99, syscall/op |
| `net/point_select_prepared` | Extended Query P+B+E+S，$1=N | latency P50/P99, alloc/op |
| `net/insert_prepared` | P+B+E+S INSERT 1行4列 | TPS, bytes_copied/op |
| `net/pipeline_10` | 10条 Query 不等 RFQ（pipeline） | batch throughput |
| `net/extended_pipeline` | P+B+D+E+S 完整流程 × 100 conn | syscall/op, CPU cycles/op |
| `net/large_result` | SELECT 1000行 × 8列 | throughput MB/s |
| `net/encode_datarow` | micro: encode DataRow 8列混合类型 | ns/op, alloc/op |
| `net/decode_bind_params` | micro: decode Bind 4 binary params | ns/op, alloc/op |

---

## 10. 优先级排序

| 优先级 | 项目 | 预期延迟收益 | 风险 |
|---|---|---|---|
| **P0** | §4.5 Extended query pipeline buf（Sync 时批量 flush） | −3~5 syscall/txn | 低（纯 server 侧） |
| **P0** | §4.3 RowDescription/ErrorResponse 消除临时 buf | −1 copy/响应 | 低 |
| **P1** | §4.1 Bind param value Bytes 零拷贝 | −N alloc/Bind | 中（类型变更） |
| **P1** | §4.6 buf 初始容量 64→512 | 消除 realloc | 低 |
| **P1** | §4.9 sql Arc\<str\> clone | −1 alloc/timeout path | 低 |
| **P2** | §4.7 DataRow 二进制格式 | −N to_string/行 | 高（客户端兼容） |
| **P2** | §4.2 read_cstring 借用版本 | −N alloc/parse | 中（生命周期) |
| **P2** | §8 专用 I/O 线程隔离 | 延迟抖动 −50% | 高（架构重构） |
