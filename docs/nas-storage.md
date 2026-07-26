# NAS 市场数据集准备

Stock EVA 将两类数据明确分开：

- 本机可变状态：用户 SQLite、行情控制 DuckDB、刷新/回填审计、锁、staging、
  DuckDB spill 和临时文件。
- 可选 NAS 市场数据集：一次完整回填与校验后的历史分区，以及之后的每日增量分区。

当前实现包含配置边界、只读 preflight、失败降级、不可变分区发布和可恢复的全主板
历史回填。NAS 数据集的首次
初始化必须由显式命令确认；不会保存 SMB 凭据、读取真实行情或执行全市场抓取。

## 配置

默认不设置 `STOCK_EVA_NAS_MARKET_DATASET_ROOT`，应用继续使用现有本地 DuckDB
开发模式。启用准备检查时，只配置操作系统已经挂载好的绝对路径：

```dotenv
STOCK_EVA_LOCAL_CONTROL_DIR=var/control
STOCK_EVA_LOCAL_STAGING_DIR=var/staging
STOCK_EVA_LOCAL_LOCK_DIR=var/locks
STOCK_EVA_LOCAL_TEMP_DIR=var/tmp
STOCK_EVA_NAS_MARKET_DATASET_ROOT=/Volumes/StockEva/market
```

不要在 `.env` 中放 SMB URL、用户名、密码或令牌。SMB3 连接和凭据由操作系统负责。
NAS 根不能与 `market_data_dir`、`user_data_dir` 或任何本地 runtime 目录重叠。

`GET /api/v1/storage/readiness` 只读检查：

1. 路径是绝对路径、已经存在且为目录；
2. 最深匹配挂载点的类型为 macOS `smbfs` 或 Linux `cifs`；
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

检查不列举数据文件、不计算全量 hash、不创建缺失目录，也不测试写权限。macOS
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

- 未配置 NAS：`mode=local`、`status=ready`，继续读取本地开发 DuckDB。
- NAS 路径、挂载类型、哨兵或 manifest 无效：`market_data_available=false`。
- NAS 元数据检查通过：`status=ready`、`serving_source=nas`；应用只从当前
  manifest 明确列出的已发布 Parquet 分区读取。空 manifest 返回安全空态，不会补读
  staging 或 quarantine 文件。

NAS 挂载、哨兵或 manifest 不可用时，summary、history、组合估值、策略运行和预警
评估返回明确的 `503 market_storage_unavailable`。健康检查、持仓、自选、策略定义、
预警规则/历史和手动状态转换继续使用本机 SQLite，不受 NAS 中断影响。

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
不能被 summary、history、估值、策略或预警读取。每日增量与首次全量回填使用同一
发布协议，不另开弱一致性路径。

首次全量回填使用 `full-market-backfill`。它默认 dry-run，只有显式增加
`--execute-all-main-board-history` 才抓取并发布；每个交易日都是独立的严格校验和
manifest 检查点。已发布日期不会被覆盖，中断后可安全重启。具体命令、硬上限和请求
边界见 [第一版真实数据就绪](real-data-readiness.md#nas-全主板历史回填)。

## 实际接入前的最少输入

用户只需提供一个信息：操作系统已通过 SMB3 挂载后的 NAS 数据集绝对路径。随后授权
执行一次只读 preflight，核对挂载类型、哨兵和 manifest。应用不需要 NAS 主机名、
账户或密码。若共享是空目录，初始化哨兵/manifest 属于后续显式写入步骤，必须单独
确认，不能由只读探测自动完成。
