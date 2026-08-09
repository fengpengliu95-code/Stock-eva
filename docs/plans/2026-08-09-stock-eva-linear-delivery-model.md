# Stock EVA 线性交付工作模式

状态：自 2026-08-09 起生效，覆盖此前的多版本并行执行方式，但不改变产品 Roadmap 的功能范围。

## 1. 目标

Stock EVA 改为单版本、单切片线性交付。优先保证 point-in-time 数据真实性、版本间契约稳定和可复现验收，不再以同时启动多个 Roadmap 阶段换取局部开发速度。

## 2. WIP 与并行上限

- 同一时间只能有一个 Roadmap 切片处于开发状态，WIP limit = 1。
- 子智能体硬上限为 3；正常状态只启动 1 个开发子智能体。
- 同一时间只能有一个能修改代码的子智能体。
- 测试/质量审核必须等待开发提交形成稳定 commit 后才能开始。
- 产品验收必须等待独立审核通过并集成主线后才能开始。
- 不允许提前开发后续 Release；允许只读记录 parking-lot 风险，但不得写代码或运行重复测试。
- 后端测试、前端测试和生产验收不得在多个任务中重复并发运行。

## 3. 角色与顺序

### 阶段 A：开发

- 一个开发子智能体、一个隔离 worktree、一个明确切片。
- 先 RED 后 GREEN，只运行该切片的聚焦测试。
- 完成后提交并停止，不自行宣布 Release GO。

### 阶段 B：独立测试与质量审核

- 一个只读审核子智能体审查开发 commit。
- 默认合并规格、代码质量、安全和测试审核，不再为同一 commit 同时启动多个 reviewer。
- 如发现问题，审核停止；原开发子智能体修复并追加 commit，然后由同一 reviewer 复审。
- 审核通过前不得启动下一切片。

### 阶段 C：集成与产品验收

- Root 负责 cherry-pick、串行全量测试、API/数据回读和浏览器验收。
- 只有静态审核、自动化测试、真实数据状态和产品路径均通过，才能记录 GO。
- GO 后立即移除已完成 worktree 并归档任务，再启动下一切片。

## 4. 版本门禁

每个切片必须依次满足：

1. 范围和数据契约冻结；
2. 开发 commit 与聚焦测试通过；
3. 独立审核 APPROVED；
4. 主线集成与全量测试通过；
5. 真实 API/数据 readback 通过；
6. 浏览器产品验收通过；
7. acceptance 文档记录证据；
8. 才能解锁下一切片。

任何一步为 REQUEST CHANGES、degraded、unavailable 或证据不足，都停留在当前切片，不向后并行扩散。

## 5. 当前执行队列

### Completed：R1-A GO

- R1-A 已完成独立复审、main 集成、串行全量测试、真实生产分类发布、API
  不可变性回读和浏览器决策路径验收。
- `r1a-observability` clean worktree 在记录最终证据后移除，分支与 commit 保留审计。
- 用户已确认进入 R1-B；R1-A 的分支与 commit 保留审计。

### Completed：R1-B GO

- R1-B 已完成缓存完整性修复、独立复审、main 集成、768 项全量测试、真实生产
  API 冷/热性能、只读数据哈希与浏览器总览验收。
- 生产 API 的 launchd 分类已从后台批处理修正为用户交互服务；批处理 agent 保持后台。
- 用户已确认进入 R1-C。
- R1 未整体达到 GO 前，不继续任何 R2/R3 开发。

### Completed：R1-C GO

- R1-C 已修复范围外成员污染价格覆盖分母的问题，完成独立复审、主线集成、全量测试、
  真实 API/数据回读和浏览器验收。
- 用户已确认进入 R1-D。

### Paused for approval：R1-D GO

- R1-D 已完成独立板块工作区、严格路由、上下文往返、迟到响应隔离和桌面排名控件
  边界修复。
- 已完成独立复审、主线全量测试、真实 API 回读、正式安装、浏览器验收和受保护数据
  指纹比对。
- 当前不启动开发子智能体，等待用户确认后才进入 R1-E。

### Next after approval：R1-E Release 1 最终收口

- 只在用户确认 R1-D 后启动；先冻结 R1-E 的产品验收范围，再进入单开发者切片。
- R1-E 关闭前不恢复 R2 worktree，也不启动任何 R2/R3 测试或修改。

### Parked：R2-C1 市场发布 provenance

- 保留 worktree：`r2c1`，保留全部未提交修改。
- 不分配子智能体，不运行测试，不继续修改。
- 仅在 R1-A/R1-E 完成 GO 后恢复，从现有 dirty worktree 继续，而不是重新实现。

### Locked

- R2-C2 EvidencePack/decision journal
- R2-D integrated review workspace
- R2-E Release 2 acceptance
- R3-A 至 R3-E
- 最终 completion audit 与部署验收

这些阶段按上述顺序逐个解锁，不允许跨阶段开发。

## 6. Token 与重复工作控制

- 不再创建独立的长期 Codex task/thread；子任务只使用当前主任务内的临时子智能体。
- 每个 commit 默认只有一个 reviewer；除非首个 reviewer 明确要求专项安全复审。
- reviewer 复用既有测试证据，只补缺失的 adversarial cases，不重复完整开发探索。
- Root 维护唯一的 acceptance 状态和问题清单，避免多个任务分别诊断同一问题。
- 运行测试前检查是否已有测试进程；全量测试只由 Root 在集成后运行一次。
- 后续任务只读取当前切片所需文档和文件，禁止提前加载未来 Release 的大范围上下文。

## 7. 恢复与归档规则

- archived 任务只是从活动列表隐藏，历史仍可读取；仅当前置版本 GO 后才恢复对应任务。
- parked worktree 不代表任务运行，也不得被新智能体并行修改。
- dirty worktree 永不强制删除；先提交、审核或明确放弃。
- 完成并进入 main 的 clean worktree 当日移除，分支按审计需要保留。
