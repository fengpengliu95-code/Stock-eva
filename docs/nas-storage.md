# NAS 市场数据集准备

Stock EVA 将三类数据明确分开：

- 本机可变状态：用户 SQLite、行情控制 DuckDB、刷新/回填审计、锁、staging、
  DuckDB spill 和临时文件。
- 本机行情镜像：从已验证 manifest 原子同步的不可变 Parquet，是 macOS 后台服务的
  实际读取和后续日增量发布位置。
- NAS 市场归档：一次完整回填与校验后的历史分区，保留为可重建、可再次同步的归档源。

当前实现包含配置边界、只读 preflight、失败降级、不可变分区发布和可恢复的全主板
历史回填。NAS 数据集的首次初始化必须由显式命令确认；不会保存 SMB 凭据。

交互式只读检查已确认 `/Volumes/Stock` 是可读的 SMB 挂载，但实际 LaunchAgent
验证显示 macOS TCC 拒绝后台进程直接访问 `/Volumes/Stock`。因此 NAS readiness
用于交互式归档与镜像维护；后台 API、刷新、策略和预警使用
`STOCK_EVA_LOCAL_MARKET_DATASET_ROOT` 指定的本机镜像。

## 配置

开发模式默认不设置任何 dataset root，应用使用本地 DuckDB。交互式归档工具配置
NAS 路径；macOS 后台运行时同时配置优先级更高的本机镜像：

```dotenv
STOCK_EVA_LOCAL_CONTROL_DIR=var/control
STOCK_EVA_LOCAL_STAGING_DIR=var/staging
STOCK_EVA_LOCAL_LOCK_DIR=var/locks
STOCK_EVA_LOCAL_TEMP_DIR=var/tmp
STOCK_EVA_NAS_MARKET_DATASET_ROOT=/Volumes/Stock/stock-eva-market
STOCK_EVA_LOCAL_MARKET_DATASET_ROOT=/Users/finlay/Library/Application Support/Stock EVA/data/market-dataset
```

不要在 `.env` 中放 SMB URL、用户名、密码或令牌。SMB3 连接和凭据由操作系统负责。
NAS 根和本机镜像均不能与可变数据库、用户数据、staging、锁或临时目录重叠。两个
dataset root 同时存在时，本机镜像优先，后台不会探测或回退读取 NAS。

`GET /api/v1/storage/readiness` 只读检查：

1. 路径是绝对路径、已经存在且为目录；
2. NAS 模式下，最深匹配挂载点的类型为 macOS `smbfs` 或 Linux `cifs`；本机镜像
   模式不要求网络挂载；
3. `.stock-eva-dataset.json` 可读、少于 1 MiB 且内容为：

   ```json
   {"dataset":"stock-eva-market","schema_version":2}
   ```

4. `manifest.json` 可读、少于 1 MiB，且至少满足：

   ```json
   {
     "dataset": "stock-eva-market",
     "schema_version": 2,
     "generation": "generation-YYYYMMDD-HHMMSS",
     "files": []
   }
   ```

readiness 检查不列举数据文件、不计算全量 hash、不创建缺失目录，也不测试写权限。
镜像创建和 `NasMarketStore.validate_readiness()` 会另行核对 manifest 引用的每个
文件、SHA-256、schema 和行数。macOS
`mount` 输出通常只能确认 `smbfs`，不能证明实际协商版本；SMB3 仍需在系统挂载配置
或 NAS 管理界面确认。

## 首次初始化

先把 NAS 共享盘通过 SMB3 挂载到操作系统，并把根配置为共享盘内一个**尚不存在的
子目录**，不能使用共享盘根目录。首次创建需要显式确认：

```bash
STOCK_EVA_NAS_MARKET_DATASET_ROOT=/Volumes/Stock/stock-eva-market \
  uv run python -m backend.app.cli storage-init --initialize-empty-nas-dataset
```

市场数据 schema v2 将 canonical `adjust_factor` 定义为累计后复权因子，并由查询
截止日动态归一化前复权序列。旧 schema v1 保存的是会被后续事件重标的前复权值，
新代码必须拒绝读取；升级时应归档旧数据集并重新初始化、回填，不能原地混合分区。

命令拒绝已存在的路径、非 SMB/CIFS 挂载和共享盘根目录。它只创建空数据集的哨兵、
空 manifest 与 `bars`、`manifests/history`、`checksums`、`_staging`、`quarantine`
目录；不会请求行情。随后可调用 `GET /api/v1/storage/readiness` 核验。

## 失败语义

- 未配置 dataset root：`mode=local`、`status=ready`，继续读取本地开发 DuckDB。
- 本机镜像就绪：`mode=local_dataset`、`status=ready`、
  `serving_source=local`，后台不访问 NAS。
- 本机镜像缺失或无效：fail closed，不回退到 NAS；私有 API 仍可用。
- NAS 路径、挂载类型、哨兵或 manifest 无效：`market_data_available=false`。
- NAS 元数据检查通过：`status=ready`、`serving_source=nas`；应用只从当前
  manifest 明确列出的已发布 Parquet 分区读取。空 manifest 返回安全空态，不会补读
  staging 或 quarantine 文件。

当前配置的数据集（本机镜像或交互式 NAS）不可用时，summary、history、组合估值、
策略运行和预警评估返回明确的 `503 market_storage_unavailable`。健康检查、持仓、
自选、策略定义、预警规则/历史和手动状态转换继续使用本机 SQLite。

## NAS 归档到本机镜像

镜像命令默认 dry-run；显式执行时只复制 manifest 引用的已发布对象：

```bash
uv run python -m backend.app.storage.mirror \
  --source /Volumes/Stock/stock-eva-market \
  --destination "$HOME/Library/Application Support/Stock EVA/data/market-dataset"

uv run python -m backend.app.storage.mirror \
  --source /Volumes/Stock/stock-eva-market \
  --destination "$HOME/Library/Application Support/Stock EVA/data/market-dataset" \
  --execute
```

执行顺序是：完整验证源 manifest/hash/schema/行数，在目标同卷 partial 目录复制，
再次验证本机 readback，再逐个发布不可变分区，并把 manifest 作为最后一个原子
rename 的发布指针。源不完整、复制中断或目标复核失败时，旧 manifest 仍然有效；
较旧 NAS 归档也不会覆盖包含更多交易日的本机数据。相同 generation 再次执行返回
`reused`，不重复复制；历史发生冲突则 fail closed。

2026-07-28 已验证的 NAS schema v2 归档包含 260 个交易日、829,494 行，最新日期
2026-07-24，占用约 38 MB。本机镜像应保持相同 generation、文件数、行数和 hash；
这些数字描述行情归档，不包含用户持仓、自选、策略或预警数据库。

## 两阶段发布接口

`backend.app.storage.publication.DatasetPublication` 固定实现以下五个阶段：

1. `stage_and_validate`：在本机 staging 生成分区、canonical 校验结果、manifest 和
   SHA-256；不得在 NAS 上直接生成可见数据。
2. `upload_partial`：上传至目标共享内不可发布的 partial 名称。
3. `readback_and_verify`：从 NAS 重新打开并读取，核对大小、行数、schema 和 hash。
4. `publish_by_atomic_rename`：只允许在同一 SMB 共享、同一文件系统内 rename；
   不接受跨共享 copy+delete 冒充原子发布。
5. `publish_manifest`：数据分区发布后，最后以临时 manifest 加同共享原子 rename
   移动 published pointer。

任一验证、上传或 readback 失败，旧 manifest 保持不变；partial 只保留审计/清理，
不能被 summary、history、估值、策略或预警读取。NAS 历史归档和本机镜像内的每日
增量均使用同一 manifest 发布语义，不另开弱一致性读取路径。

首次全量回填使用 `full-market-backfill`。它默认 dry-run，只有显式增加
`--execute-all-main-board-history` 才抓取并发布；每个交易日都是独立的严格校验和
manifest 检查点。已发布日期不会被覆盖，中断后可安全重启。具体命令、硬上限和请求
边界见 [第一版真实数据就绪](real-data-readiness.md#nas-全主板历史回填)。

## 实际接入前的最少输入

用户只需提供一个信息：操作系统已通过 SMB3 挂载后的 NAS 数据集绝对路径。交互式
工具核对挂载、哨兵、manifest 和文件完整性后创建本机镜像。应用不需要 NAS 主机名、
账户或密码，LaunchAgent 也不需要 NAS 的 TCC 授权。若共享是空目录，初始化
哨兵/manifest 属于后续显式写入步骤，不能由只读探测自动完成。
