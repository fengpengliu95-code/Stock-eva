# macOS 本地自动运行

Stock EVA 使用 5 个当前用户级 LaunchAgent，不需要管理员权限，不保存 NAS
账号、密码、token 或 SMB URL。

| Label | 作用 | 运行方式 |
| --- | --- | --- |
| `com.finlay.stock-eva.api` | FastAPI | 登录后启动，异常退出自动恢复 |
| `com.finlay.stock-eva.web` | 静态工作台 | 登录后启动，异常退出自动恢复 |
| `com.finlay.stock-eva.refresh` | 日终刷新、策略与预警流水线 | 16:00、18:10、18:40、19:20、20:10、21:00、次晨 07:15 |
| `com.finlay.stock-eva.calendar` | 官方日历机器对账 | 登录启动、每日 16:30、每月 1 日 04:05 |
| `com.finlay.stock-eva.backup` | 私有 SQLite 一致性备份 | 每日 02:30 |

API 是用户发起的本机请求服务，因此模板使用 launchd `ProcessType=Interactive`。日终刷新、
日历和备份仍使用 `Background`（并保留低优先级 I/O）；静态 Web 服务不因这一 API 调度
修复而变更。这样不会把后台批处理提升为交互优先级。

## 运行时与行情镜像

真实安装验证发现，交互式用户可以读取 `/Volumes/Stock`，但 macOS TCC 会拒绝
LaunchAgent 后台进程直接打开该卷；把项目 Python 或工作目录放在 `Documents` 下也会
在 Python 初始化前被阻塞。因此正式后台任务不再从开发仓库或 NAS 直接启动和读取。

安装器从当前 Git 提交构建独立的、不可编辑的生产运行时，并使用以下边界：

```text
~/Library/Application Support/Stock EVA/
├── runtime/
│   ├── current -> releases/<git-sha>
│   └── releases/<git-sha>/       # 代码、生产 .venv、public 静态文件
├── config/.env                   # 0600；不进入 release 或 plist
├── data/
│   ├── market-dataset/           # 已验证的本机行情镜像
│   ├── market/
│   ├── user/
│   ├── control/
│   ├── staging/
│   ├── locks/
│   └── tmp/
└── backups/
```

LaunchAgent 只引用 `Application Support` 下的绝对路径。Web 只发布
`runtime/current/public`，不能访问 `.env`、后端源码或用户数据库。开发仓库继续用于
开发和构建，不是后台运行目录。Web agent 使用项目的标准库静态服务并只绑定
`127.0.0.1:8080`；所有响应统一设置 `Cache-Control: no-store`，确保 release 切换后
固定 URL 不会混用不同版本的 HTML、JavaScript 或 CSS。

NAS `/Volumes/Stock/stock-eva-market` 是经 manifest、SHA-256、Parquet schema 和行数
校验的归档源。交互式安装阶段先把 manifest 引用的文件复制到本机临时区，完整
readback 后逐个发布不可变分区，并最后原子切换 manifest；失败会保留上一份有效
manifest，较旧归档不会覆盖本机新增交易日。
API、日历、日终刷新、策略和预警随后只读写本机镜像，不依赖 launchd 对 NAS 的访问。

## 安装前置

1. Finder 已将 Stock SMB 共享挂载到 `/Volumes/Stock`，凭据只由 macOS
   Keychain 保存。挂载只供交互式安装/归档维护使用。
2. 项目根目录存在 `.env`，至少包含：

   ```dotenv
   STOCK_EVA_NAS_MARKET_DATASET_ROOT=/Volumes/Stock/stock-eva-market
   STOCK_EVA_AUTO_REFRESH_ENABLED=false
   STOCK_EVA_SCHEDULED_REFRESH_ENABLED=true
   ```

   自动刷新必须为 `false`，因为 LaunchAgent 的幂等 one-shot 命令是唯一调度
   所有者，避免 API 内置循环重复运行。
   安装器会把可变目录改写为 `Application Support/Stock EVA/data` 下的绝对路径，
   并设置 `STOCK_EVA_LOCAL_MARKET_DATASET_ROOT` 指向本机镜像。私有备份的源固定为
   `data/user/stock_eva_user.sqlite3` 与
   `data/user/stock_eva_portfolio.sqlite3` 的同一原子 bundle；如果项目 `.env` 显式
   改写 `STOCK_EVA_USER_DATA_DIR`、`STOCK_EVA_USER_DATABASE_NAME` 或
   `STOCK_EVA_PORTFOLIO_DATABASE_NAME`，安装器
   会 fail closed，而不会静默备份错误文件。
3. `.venv/bin/python` 已存在且依赖完整。
4. `npm` 可执行，且 `workspace/package-lock.json` 已跟踪；安装器会用 lockfile 在
   release staging 内构建前端，不使用项目根目录或未跟踪 `node_modules`。
5. 停止目前手动启动、占用 8000/8080 端口的验收进程。

安装时 NAS 未挂载、挂载类型错误、哨兵、manifest、hash、schema 或行数无效时，
镜像步骤 fail closed，既不会替换上一镜像，也不会安装一套依赖不完整行情的服务。
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

如果 NAS 当前未挂载，但本机 `data/market-dataset` 已经是完整的不可变镜像，
可显式请求只读复用模式：

```bash
"/Users/finlay/Documents/Stock- evaluation/scripts/stock_eva_launchagents_install.sh" \
  --install --reuse-local-dataset
```

该模式由新 release runtime 重新核验哨兵、manifest 及每个 Parquet 的 SHA-256、schema
和行数，成功时输出 `verified local reuse; copied_bytes=0`。它不访问或伪装 NAS，
也不改写本机 manifest、Parquet 或 pointer；任一缺失、损坏、空目录或路径错误都会在
`runtime/current` 切换前触发既有 rollback。`--reuse-local-dataset` 不能与 `--check`
一起使用。

它只写入：

- `~/Library/LaunchAgents/com.finlay.stock-eva.*.plist`
- `~/Library/Logs/Stock EVA/`
- `~/Library/Application Support/Stock EVA/runtime/`
- `~/Library/Application Support/Stock EVA/config/`
- `~/Library/Application Support/Stock EVA/data/`
- `~/Library/Application Support/Stock EVA/backups/`

release 只从当前 Git 提交提取 tracked 文件；不会复制项目 `.env`、`var/`、未跟踪
PDF 或 `node_modules`。生产环境使用 `uv sync --frozen --no-dev --no-editable` 重新
构建 Python 环境，避免 `.pth` 继续引用 `Documents` 开发路径。工作台在隔离
staging 中执行 `npm ci --ignore-scripts` 与 `npm run build`，随后删除
`node_modules`、TypeScript 源码和构建配置，只把生成的静态文件移入 `public/workspace`。
`current` 仅在 release、配置和
本机行情镜像全部校验后以 manifest 最后发布；安装或 bootstrap 失败会恢复旧 plist、旧 release
指针和旧配置。

安装器会先检查 8000/8080 端口。非 Stock EVA LaunchAgent 占用端口时会在写入
plist 前退出。日志、配置、数据和备份目录权限为 `0700`，日志及 `.env` 权限为
`0600`。

## 状态

```bash
"/Users/finlay/Documents/Stock- evaluation/scripts/stock_eva_launchagents_status.sh"
```

状态脚本只读取 launchd、API、工作台和 storage readiness。正式后台状态应显示
`mode=local_dataset`、`serving_source=local`，而不是要求 launchd 读取 NAS。某个
agent 未加载时返回非零，便于诊断。

日志位于 `~/Library/Logs/Stock EVA/`。任务错过执行时间（关机或休眠）后，
下一次 07:15、启动检查或幂等 catch-up 会继续补齐；读取接口本身不会触发抓取。

截至 2026-07-28，隔离运行时和 5 个用户级 LaunchAgent 已正式安装。本机镜像为
`local_dataset/ready/local`，已发布至 2026-07-27；API 8000 与工作台 8080 均正常。
日历同步、日终刷新和私有 SQLite 备份最近执行均为 exit 0；桌面/移动浏览器验收通过。
NAS 继续作为已验证历史归档，不是 launchd 的运行依赖。

## 卸载

卸载脚本默认也是只读计划：

```bash
"/Users/finlay/Documents/Stock- evaluation/scripts/stock_eva_launchagents_uninstall.sh" --check
```

显式卸载：

```bash
"/Users/finlay/Documents/Stock- evaluation/scripts/stock_eva_launchagents_uninstall.sh" --uninstall
```

卸载只 bootout 并删除上述 5 个 plist。日志、`Application Support` 中的 release、
配置、本机镜像、私有数据库、备份和 NAS 归档均保留。
