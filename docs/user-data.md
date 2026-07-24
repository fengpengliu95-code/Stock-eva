# 阶段 2 用户数据与估值边界

## 本地数据范围

阶段 2 只保存用户主动录入的手动持仓和自选列表，数据库默认为
`var/user/stock_eva_user.sqlite3`。系统不连接券商、不读取证券账户、不保存登录凭据，
也没有下单、调仓或自动交易接口。

阶段 3 的策略定义、版本、运行输入和信号解释也保存在同一数据库，因此备份与隐私
要求同样适用于策略记录。

持仓字段包括：

- `symbol`
- `quantity`
- `avg_cost`
- `as_of_date`
- `today_buy_qty`
- `version`

数量和成本以十进制字符串写入 SQLite，避免二进制浮点改变用户输入。每个证券只能有
一条持仓。更新和删除必须提交当前 `expected_version`，SQL 只修改版本匹配的行；
过期客户端得到 HTTP 409，不会覆盖较新的编辑。

自选列表名称唯一，列表内证券使用 `(watchlist_id, symbol)` 唯一约束。重复添加是
幂等操作，返回现有项目；重复删除也是安全空操作。

## 估值语义

组合估值只读取阶段 1 DuckDB 最近一次刷新日期的单一 BaoStock canonical 收盘数据。
不会回退到其他来源，也不会用之前交易日价格替代当日缺失或停牌记录。

- 正常覆盖：`quantity × close` 计算估算市值。
- 成本基准：`quantity × avg_cost`。
- 未实现盈亏：估算市值减成本基准。
- 停牌、缺失行情或持仓日期晚于行情日期：市值和盈亏为 `null`。
- 部分覆盖时，只返回明确命名的 `covered_*` 汇总及 N/M 覆盖，不把缺失项当作零。

所有估值响应固定声明：`按用户成本与当日收盘估算、未计交易费用`。它是记录和复盘
工具，不是账户对账、税务计算、投资建议或可交易报价。估值只读取最新 published
完整快照，并由后端交易日历判断新鲜度；调用方不能输入或覆盖期望交易日。

## API

### 手动持仓

- `POST /api/v1/portfolio/positions`
- `GET /api/v1/portfolio/positions`
- `GET /api/v1/portfolio/positions/{id}`
- `PUT /api/v1/portfolio/positions/{id}`
- `DELETE /api/v1/portfolio/positions/{id}?expected_version=N`
- `GET /api/v1/portfolio/valuation`

### 自选

- `POST /api/v1/watchlists`
- `GET /api/v1/watchlists`
- `DELETE /api/v1/watchlists/{id}?expected_version=N`
- `GET /api/v1/watchlists/{id}/items`
- `POST /api/v1/watchlists/{id}/items`
- `DELETE /api/v1/watchlists/{id}/items/{symbol}`

交互式请求/响应模型同时发布在 `/api/docs`。

## 隐私边界

- `var/user/`、SQLite 主文件、WAL/SHM、备份文件均不得进入 Git。
- 不要在测试夹具、截图、日志、Issue 或提交信息中放入真实持仓。
- 备份可能包含完整持仓，应只存放在用户控制的加密磁盘或加密备份中。
- 不要把用户数据库复制到公开云盘、项目目录中的示例文件或远程仓库。
- 当前 SQLite 文件没有应用层加密，应启用 FileVault 或等价的磁盘加密。
- 当前 API 没有用户认证，只允许绑定 `127.0.0.1`；不得直接暴露到局域网或公网。

## 备份与恢复

备份前先停止 API 写入，然后使用 SQLite 在线备份命令：

```bash
mkdir -p "$HOME/StockEVA-private-backups"
sqlite3 var/user/stock_eva_user.sqlite3 \
  ".backup '$HOME/StockEVA-private-backups/stock_eva_user-backup.sqlite3'"
sqlite3 "$HOME/StockEVA-private-backups/stock_eva_user-backup.sqlite3" \
  "PRAGMA integrity_check;"
```

只有 `integrity_check` 返回 `ok` 才视为可用备份。恢复时停止 API，先备份当前数据库，
再把已验证备份复制回配置的 `STOCK_EVA_USER_DATA_DIR`，启动后先调用只读列表接口检查。
恢复会替换当前用户状态，因此不要在 API 运行或仍有写请求时操作。
