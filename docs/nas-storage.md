# NAS 市场数据集准备

Stock EVA 将两类数据明确分开：

- 本机可变状态：用户 SQLite、行情控制 DuckDB、刷新/回填审计、锁、staging、
  DuckDB spill 和临时文件。
- 可选 NAS 市场数据集：一次完整回填与校验后的历史分区，以及之后的每日增量分区。

本轮只实现配置边界、只读 preflight、失败降级和未来发布接口；不会连接 NAS、创建
共享目录、保存 SMB 凭据、读取真实数据集或执行全市场抓取。

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
   {"dataset":"stock-eva-market","schema_version":1}
   ```

4. `manifest.json` 可读、少于 1 MiB，且至少满足：

   ```json
   {
     "dataset": "stock-eva-market",
     "schema_version": 1,
     "generation": "generation-YYYYMMDD-HHMMSS",
     "files": []
   }
   ```

检查不列举数据文件、不计算全量 hash、不创建缺失目录，也不测试写权限。macOS
`mount` 输出通常只能确认 `smbfs`，不能证明实际协商版本；SMB3 仍需在系统挂载配置
或 NAS 管理界面确认。

## 失败语义

- 未配置 NAS：`mode=local`、`status=ready`，继续读取本地开发 DuckDB。
- NAS 路径、挂载类型、哨兵或 manifest 无效：`market_data_available=false`。
- NAS 元数据检查通过：`status=ready`，但本轮仍返回
  `reason_code=nas_dataset_reader_not_implemented`，不会假装已经读取 NAS。

NAS 模式下，只要行情读取能力未就绪，summary、history、组合估值、策略运行和预警
评估返回明确的 `503 market_storage_unavailable`。健康检查、持仓、自选、策略定义、
预警规则/历史和手动状态转换继续使用本机 SQLite，不受 NAS 中断影响。

## 两阶段发布接口

`backend.app.storage.publication.DatasetPublication` 固定未来实现的五个阶段：

1. `stage_and_validate`：在本机 staging 生成分区、canonical 校验结果、manifest 和
   SHA-256；不得在 NAS 上直接生成可见数据。
2. `upload_partial`：上传至目标共享内不可发布的 partial 名称。
3. `readback_and_verify`：从 NAS 重新打开并读取，核对大小、行数、schema 和 hash。
4. `publish_by_atomic_rename`：只允许在同一 SMB 共享、同一文件系统内 rename；
   不接受跨共享 copy+delete 冒充原子发布。
5. `publish_manifest`：数据分区发布后，最后以临时 manifest 加同共享原子 rename
   移动 published pointer。

任一验证、上传或 readback 失败，旧 manifest 保持不变；partial 只保留审计/清理，
不能被 summary、history、估值、策略或预警读取。未来每日增量与首次全量回填使用同一
发布协议，不另开弱一致性路径。

## 实际接入前的最少输入

用户只需提供一个信息：操作系统已通过 SMB3 挂载后的 NAS 数据集绝对路径。随后授权
执行一次只读 preflight，核对挂载类型、哨兵和 manifest。应用不需要 NAS 主机名、
账户或密码。若共享是空目录，初始化哨兵/manifest 属于后续显式写入步骤，必须单独
确认，不能由只读探测自动完成。
