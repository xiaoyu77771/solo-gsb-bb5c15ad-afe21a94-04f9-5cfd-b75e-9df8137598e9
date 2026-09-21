# 工厂校准时间线服务（Calibration Timeline API）

接收同一传感器在一个校准批次中**乱序、重叠且可能撤回**的测量片段修订，按
**版本因果关系**与**半开时间区间切分规则**生成**确定性**的有效标定时间线；
发现缺失父版本时返回**可补传的缺口清单**；重复事件**幂等**。

- 框架：FastAPI + Pydantic v2 + Uvicorn
- 存储：进程内存（零外部依赖；`asyncio.Lock` 保证并发安全与批量写入原子性）
- 交付：`Dockerfile` + `docker-compose.yml`，镜像构建时安装依赖、容器启动即完成初始化

---

## 一条命令启动

```bash
docker compose up --build
```

服务启动后监听 **8080** 端口：

| 用途 | 地址 |
| --- | --- |
| 健康检查 | http://localhost:8080/healthz |
| 交互式 API 文档（Swagger UI） | http://localhost:8080/docs |
| OpenAPI 描述 | http://localhost:8080/openapi.json |

停止：`Ctrl+C`；后台运行：`docker compose up --build -d`；查看日志：
`docker compose logs -f`。

> 状态保存在进程内存中，**必须以单 Uvicorn worker 运行**（Compose 已固定）。
> 扩成多 worker / 多副本会导致状态各自独立。`docker compose restart` 即清空状态。

---

## 必要配置

全部通过环境变量配置，均有默认值，开箱即用：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PORT` | `8080` | 容器内监听端口 |
| `HOST` | `0.0.0.0` | 监听地址 |

宿主机访问端口在 `docker-compose.yml` 的 `ports: "8080:8080"` 中调整，
例如改成 `18080:8080` 后访问 http://localhost:18080 。

本地（无 Docker）运行：

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8080
```

---

## 数据模型与规则

### 修订事件

```json
{
  "event_id": "evt-2",
  "batch_id": "CAL-42",
  "sensor_id": "T-01",
  "parent_event_id": "evt-1",
  "op": "put",
  "start": 5.0,
  "end": 15.0,
  "value": {"offset": 0.02}
}
```

- `event_id`：**全局唯一**幂等键。
- `parent_event_id`：因果父版本；根事件为 `null`。事件沿 parent 指针构成一条
  从根出发的链。同一 `(batch, sensor)` 只允许**一个根**、链必须**线性**（不允许分叉）。
- `op`：
  - `put`：发布/修订一段测量片段，必须携带 `start`、`end`、`value`；
    时间区间为半开 `[start, end)`，要求 `start < end`。
  - `retract`：撤回一个已发布的 put，必须携带 `target_event_id`，不带时间字段。
    撤回本身也是链上的一个版本节点（其 parent 决定因果位置）。

### 时间线切分规则

1. 从根可达的事件构成**已连通（connected）**集合；父版本缺失的事件为
   **断链（detached）**，不参与时间线，直到父版本补传后自动挂接。
2. 已连通的 retract 使其目标 put 失效：该 put 不再覆盖任何时间点。
3. 把所有生效 put 的端点投影到时间轴，扫描线按半开区间切分（端点处先结束、
   后开始）；任一时点由**因果深度最大**（链上最新）的覆盖片段胜出。
4. 相邻且来自同一 put 的片段会合并（典型场景：中间修订被撤回，老版本恢复为整段）。
5. 结果只取决于事件内容与因果关系，**与上传顺序无关**——同一组事件任意顺序
   上传得到完全相同的时间线。

### 缺口（gaps）

- `missing_parent`：事件引用了尚未上传的父版本。`required_event_id` 为最近的
  缺失祖先（补传它即可让整段断链挂接），`blocked_by_event_id` 为被阻塞的事件。
- `missing_target`：已连通的 retract 撤回了一个完全未知的事件。

缺口补传后重查（或在补传请求的响应中）即可看到缺口消失、时间线延展。

### 幂等与冲突

- 相同 `event_id` + **完全一致的负载**重复提交：HTTP 200，计入 `duplicate`，状态不变。
- 相同 `event_id` 但内容不同：`409 event_conflict`。
- 多根、分叉、环、跨 batch/sensor 引用、retract 非 put、非法区间等：`422 invalid_events`。
- 一次请求中的事件**原子写入**：任一事件非法则整批不落库。

---

## 接口

### 1. 上传修订事件

```
POST /v1/batches/{batch_id}/sensors/{sensor_id}/events
```

```bash
curl -s -X POST http://localhost:8080/v1/batches/CAL-42/sensors/T-01/events \
  -H 'Content-Type: application/json' \
  -d '{"events": [
    {"event_id":"evt-1","batch_id":"CAL-42","sensor_id":"T-01",
     "parent_event_id":null,"op":"put","start":0,"end":10,"value":{"offset":0.01}},
    {"event_id":"evt-2","batch_id":"CAL-42","sensor_id":"T-01",
     "parent_event_id":"evt-1","op":"put","start":5,"end":15,"value":{"offset":0.02}}
  ]}'
```

响应（上传后立即返回该传感器的当前时间线与缺口）：

```json
{
  "accepted": 2,
  "duplicate": 0,
  "total_events": 2,
  "connected_events": 2,
  "detached_events": 0,
  "gaps": [],
  "timeline": [
    {"start": 0.0, "end": 5.0,  "value": {"offset": 0.01}, "event_id": "evt-1", "depth": 0},
    {"start": 5.0, "end": 15.0, "value": {"offset": 0.02}, "event_id": "evt-2", "depth": 1}
  ]
}
```

乱序上传示例（先到子版本）：响应中出现缺口，补传父版本后自动闭合：

```bash
# 只有 evt-2，缺 evt-1
curl -s -X POST http://localhost:8080/v1/batches/CAL-42/sensors/T-01/events \
  -H 'Content-Type: application/json' \
  -d '{"events":[{"event_id":"evt-2","batch_id":"CAL-42","sensor_id":"T-01",
      "parent_event_id":"evt-1","op":"put","start":5,"end":15,"value":{"offset":0.02}}]}'
# -> "timeline": [], "gaps": [{"type":"missing_parent",
#     "required_event_id":"evt-1","blocked_by_event_id":"evt-2"}]
```

撤回：

```bash
curl -s -X POST http://localhost:8080/v1/batches/CAL-42/sensors/T-01/events \
  -H 'Content-Type: application/json' \
  -d '{"events":[{"event_id":"evt-3","batch_id":"CAL-42","sensor_id":"T-01",
      "parent_event_id":"evt-2","op":"retract","target_event_id":"evt-2"}]}'
```

### 2. 查询单个传感器结果

```
GET /v1/batches/{batch_id}/sensors/{sensor_id}/timeline
```

返回 `total_events / connected_events / detached_events / gaps / timeline`。
未知 batch/sensor 返回 `404`。

### 3. 查询整个批次

```
GET /v1/batches/{batch_id}        # 批次内全部传感器结果（sensor_id 排序）
GET /v1/batches                   # 批次列表
```

### 4. 重置内存状态

```
POST /admin/reset                 # 清空全部状态
POST /admin/reset?batch_id=CAL-42 # 只清空指定批次
```

```json
{"reset": true, "dropped_events": 3, "remaining_batches": 0}
```

### 5. 健康检查

```
GET /healthz -> {"status": "ok"}
```

所有错误响应统一形如：

```json
{"error": {"code": "invalid_events", "message": "..."}}
```

---

## 测试

```bash
pip install -r requirements-dev.txt
pytest
```

共 22 个用例，覆盖：重叠切分、乱序到达与缺口补传、撤回与同源段合并、
未知撤回目标、幂等重复、内容冲突、批量原子性、多根/分叉/跨域引用拒绝、
批次隔离、批次查询与重置等。
