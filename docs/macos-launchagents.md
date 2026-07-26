# macOS 本地自动运行

Stock EVA 使用 5 个当前用户级 LaunchAgent，不需要管理员权限，不保存 NAS
账号、密码、token 或 SMB URL。

| Label | 作用 | 运行方式 |
| --- | --- | --- |
| `com.finlay.stock-eva.api` | FastAPI | 登录后启动，异常退出自动恢复 |
| `com.finlay.stock-eva.web` | 静态工作台 | 登录后启动，异常退出自动恢复 |
| `com.finlay.stock-eva.refresh` | 日终刷新、策略与预警流水线 | 18:10、18:40、19:20、20:10、21:00、次晨 07:15 |
| `com.finlay.stock-eva.calendar` | 官方日历机器对账 | 登录启动、每日 16:30、每月 1 日 04:05 |
| `com.finlay.stock-eva.backup` | 私有 SQLite 一致性备份 | 每日 02:30 |

所有任务使用绝对项目路径和项目 `.venv/bin/python`。`.env` 仍位于项目根目录、
被 Git 忽略；LaunchAgent plist 不复制任何 `.env` 内容。

## 安装前置

1. Finder 已将 Stock SMB 共享挂载到 `/Volumes/Stock`，凭据只由 macOS
   Keychain 保存。
2. 项目根目录存在 `.env`，至少包含：

   ```dotenv
   STOCK_EVA_NAS_MARKET_DATASET_ROOT=/Volumes/Stock/stock-eva-market
   STOCK_EVA_AUTO_REFRESH_ENABLED=false
   ```

   自动刷新必须为 `false`，因为 LaunchAgent 的幂等 one-shot 命令是唯一调度
   所有者，避免 API 内置循环重复运行。
   私有备份当前固定读取 `var/user/stock_eva_user.sqlite3`；如果 `.env` 显式
   改写 `STOCK_EVA_USER_DATA_DIR` 或 `STOCK_EVA_USER_DATABASE_NAME`，安装器
   会 fail closed，而不会静默备份错误文件。
3. `.venv/bin/python` 已存在且依赖完整。
4. 停止目前手动启动、占用 8000/8080 端口的验收进程。

NAS 未挂载、挂载类型错误、哨兵或 manifest 无效时，现有 storage preflight 会
fail closed：日终刷新不写本地替代行情；API 仍可提供健康状态及本地私有数据。
LaunchAgent 不会主动挂载 SMB，也不包含网络凭据。

## 只读预检

安装脚本默认为 `--check`，只渲染到系统临时目录并执行 `plutil -lint`：

```bash
"/Users/finlay/Documents/Stock- evaluation/scripts/stock_eva_launchagents_install.sh" --check
```

## 安装

```bash
"/Users/finlay/Documents/Stock- evaluation/scripts/stock_eva_launchagents_install.sh" --install
```

它只写入：

- `~/Library/LaunchAgents/com.finlay.stock-eva.*.plist`
- `~/Library/Logs/Stock EVA/`
- `~/Library/Application Support/Stock EVA/backups/`

安装器会先检查 8000/8080 端口。非 Stock EVA LaunchAgent 占用端口时会在写入
plist 前退出；升级期间如果任一 plist 安装或 bootstrap 失败，会恢复原 plist
及原加载状态。日志和私有备份目录权限为 `0700`，日志文件权限为 `0600`。

`RunAtLoad` 会在安装时启动 API、工作台和一次日历检查，因此应在真实历史回填
结束、手动服务停止后执行。

## 状态

```bash
"/Users/finlay/Documents/Stock- evaluation/scripts/stock_eva_launchagents_status.sh"
```

状态脚本只读取 launchd、API、工作台和 storage readiness。它不连接或写入 NAS。
某个 agent 未加载时返回非零，便于诊断。

日志位于 `~/Library/Logs/Stock EVA/`。任务错过执行时间（关机或休眠）后，
下一次 07:15、启动检查或幂等 catch-up 会继续补齐；读取接口本身不会触发抓取。

## 卸载

卸载脚本默认也是只读计划：

```bash
"/Users/finlay/Documents/Stock- evaluation/scripts/stock_eva_launchagents_uninstall.sh" --check
```

显式卸载：

```bash
"/Users/finlay/Documents/Stock- evaluation/scripts/stock_eva_launchagents_uninstall.sh" --uninstall
```

卸载只 bootout 并删除上述 5 个 plist。日志、备份、`.env`、本地数据库和 NAS
市场数据均保留。
