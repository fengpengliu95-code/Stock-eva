# 🐍 Python 量化交易工具链

> **"Python 是量化交易者的瑞士军刀 — 一门语言，覆盖从数据获取到实盘交易的完整流程。"**

---

## 目录

1. [为什么选择 Python？](#为什么选择-python)
2. [环境搭建](#环境搭建)
3. [核心库详解](#核心库详解)
4. [数据获取工具](#数据获取工具)
5. [回测框架](#回测框架)
6. [实战示例](#实战示例)
7. [进阶工具推荐](#进阶工具推荐)

---

## 为什么选择 Python？

在众多编程语言中，Python 已经成为量化交易领域的**事实标准**。以下是选择 Python 的核心理由：

### 🏆 Python 的优势

| 优势 | 说明 |
|------|------|
| **生态丰富** | 拥有最完善的金融/科学计算库生态系统 |
| **入门简单** | 语法简洁，非计算机专业也能快速上手 |
| **社区活跃** | 遇到问题很容易找到解决方案 |
| **免费开源** | 语言本身和绑大部分库都完全免费 |
| **全流程覆盖** | 从数据获取→分析→回测→机器学习→实盘，一站式解决 |
| **快速原型** | 适合快速验证策略想法 |

### 📊 语言对比

| 特性 | Python | C++ | Java | R | MATLAB |
|------|--------|-----|------|---|--------|
| 学习难度 | ⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐ | ⭐⭐ |
| 执行速度 | 中等 | 极快 | 快 | 慢 | 中等 |
| 金融库生态 | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ |
| 社区支持 | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐ |
| 实盘适用 | ✅ | ✅ | ✅ | ❌ | ❌ |
| 费用 | 免费 | 免费 | 免费 | 免费 | 💰付费 |

> 💡 **建议**：先用 Python 做策略研究和回测，如果对执行速度有极高要求，再考虑用 C++ 重写核心模块。

---

## 环境搭建

### 方案一：Anaconda / Miniconda（推荐）

Anaconda 是一个包含了大量科学计算库的 Python 发行版，专为数据科学和量化分析设计。

#### 安装步骤

```bash
# 方法1：安装完整版 Anaconda（约 3GB，包含大量预装库）
# 从 https://www.anaconda.com/download 下载安装包

# 方法2：安装精简版 Miniconda（推荐，约 50MB）
# 从 https://docs.conda.io/en/latest/miniconda.html 下载

# 安装完成后，创建量化交易专用环境
conda create -n quant python=3.11 -y

# 激活环境
conda activate quant

# 安装核心库
pip install numpy pandas matplotlib plotly jupyter
pip install akshare tushare yfinance
pip install backtrader ta-lib
```

#### 为什么要用虚拟环境？

```
系统 Python     ←── 不要动这个！
    │
    ├── quant 环境     ←── 量化交易专用
    │   ├── pandas 2.x
    │   ├── numpy 1.x
    │   └── backtrader
    │
    └── ml 环境        ←── 机器学习专用
        ├── tensorflow
        └── pytorch
```

- **隔离依赖**：不同项目可能需要不同版本的库
- **避免冲突**：安装新库不会影响其他项目
- **便于管理**：可以随时删除或重建环境

### 方案二：Jupyter Notebook / JupyterLab

Jupyter 是量化研究的最佳交互式环境，支持**代码 + 文本 + 图表**混合编写。

```bash
# 安装 JupyterLab
pip install jupyterlab

# 启动 JupyterLab
jupyter lab
```

#### Jupyter 的优势

- 📝 **交互式编程**：逐段运行代码，实时查看结果
- 📊 **图表内嵌**：Matplotlib、Plotly 图表直接显示在 Notebook 中
- 📋 **文档化**：可以在代码旁边写 Markdown 笔记
- 🔄 **快速迭代**：适合策略研究和数据探索

### 推荐的项目目录结构

```
quant_project/
├── data/               # 原始数据和处理后的数据
│   ├── raw/            # 原始下载的数据
│   └── processed/      # 清洗后的数据
├── notebooks/          # Jupyter Notebook 研究文件
│   ├── 01_data_exploration.ipynb
│   ├── 02_strategy_research.ipynb
│   └── 03_backtest_analysis.ipynb
├── strategies/         # 策略代码
│   ├── ma_cross.py
│   └── momentum.py
├── utils/              # 工具函数
│   ├── data_loader.py
│   └── indicators.py
├── backtest/           # 回测脚本和结果
├── config/             # 配置文件
└── requirements.txt    # 依赖列表
```

---

## 核心库详解

### 📦 NumPy — 科学计算的基石

NumPy（Numerical Python）是 Python 科学计算的基础库，提供**高效的多维数组操作**和**向量化计算**。

#### 为什么需要 NumPy？

```python
# ❌ 原生 Python 列表运算 — 很慢
prices = [100, 102, 98, 105, 103]
returns = []
for i in range(1, len(prices)):
    returns.append((prices[i] - prices[i-1]) / prices[i-1])

# ✅ NumPy 向量化运算 — 快100倍
import numpy as np
prices = np.array([100, 102, 98, 105, 103])
returns = np.diff(prices) / prices[:-1]
# 结果: array([ 0.02, -0.0392,  0.0714, -0.019 ])
```

#### 常用操作示例

```python
import numpy as np

# === 创建数组 ===
prices = np.array([100, 102, 98, 105, 103, 107, 110])

# === 基本统计 ===
print(f"均值: {np.mean(prices):.2f}")          # 103.57
print(f"标准差: {np.std(prices):.2f}")          # 3.87
print(f"最大值: {np.max(prices)}")              # 110
print(f"最小值: {np.min(prices)}")              # 98

# === 收益率计算 ===
# 简单收益率
simple_returns = np.diff(prices) / prices[:-1]
# 对数收益率（更常用，具有可加性）
log_returns = np.log(prices[1:] / prices[:-1])

# === 累计收益率 ===
cumulative_return = np.cumprod(1 + simple_returns) - 1

# === 移动窗口计算 ===
# 5日移动平均（手动实现）
window = 3
ma = np.convolve(prices, np.ones(window)/window, mode='valid')

# === 随机模拟（蒙特卡洛） ===
np.random.seed(42)
# 模拟1000条价格路径，每条252个交易日
daily_returns = np.random.normal(0.0005, 0.02, (1000, 252))
price_paths = 100 * np.cumprod(1 + daily_returns, axis=1)

# === 相关系数矩阵 ===
stock_a = np.random.normal(0.001, 0.02, 100)
stock_b = np.random.normal(0.0008, 0.015, 100)
correlation = np.corrcoef(stock_a, stock_b)[0, 1]
```

#### 重要概念：向量化 vs 循环

```python
# 向量化的威力：比较计算100万个数据点的速度
import time

n = 1_000_000
data = np.random.random(n)

# 循环方式
start = time.time()
result_loop = [x ** 2 + 2 * x + 1 for x in data]
loop_time = time.time() - start

# 向量化方式
start = time.time()
result_vec = data ** 2 + 2 * data + 1
vec_time = time.time() - start

# 向量化通常快50-100倍！
```

---

### 📦 Pandas — 数据分析的利器

Pandas 是 Python 最重要的数据分析库，专为**表格数据**和**时间序列**设计。在量化交易中，几乎所有的数据操作都离不开 Pandas。

#### 核心数据结构

```python
import pandas as pd

# === Series：一维标签数组 ===
prices = pd.Series(
    [100, 102, 98, 105, 103],
    index=pd.date_range('2024-01-01', periods=5, freq='D'),
    name='close'
)

# === DataFrame：二维表格（最常用） ===
df = pd.DataFrame({
    'open':   [99, 101, 97, 104, 102],
    'high':   [103, 104, 100, 106, 105],
    'low':    [98, 100, 96, 103, 101],
    'close':  [100, 102, 98, 105, 103],
    'volume': [1000, 1200, 800, 1500, 1100]
}, index=pd.date_range('2024-01-01', periods=5, freq='D'))
```

#### 时间序列操作

```python
# === 读取CSV数据 ===
df = pd.read_csv('stock_data.csv', parse_dates=['date'], index_col='date')

# === 重采样（日线→周线） ===
weekly = df.resample('W').agg({
    'open': 'first',    # 周开盘价 = 第一天的开盘价
    'high': 'max',      # 周最高价 = 一周内最高价
    'low': 'min',       # 周最低价 = 一周内最低价
    'close': 'last',    # 周收盘价 = 最后一天的收盘价
    'volume': 'sum'     # 周成交量 = 一周成交量之和
})

# === 滚动窗口计算 ===
df['MA5'] = df['close'].rolling(window=5).mean()     # 5日均线
df['MA20'] = df['close'].rolling(window=20).mean()    # 20日均线
df['MA60'] = df['close'].rolling(window=60).mean()    # 60日均线
df['std20'] = df['close'].rolling(window=20).std()    # 20日波动率

# === 计算收益率 ===
df['daily_return'] = df['close'].pct_change()         # 日收益率
df['log_return'] = np.log(df['close'] / df['close'].shift(1))  # 对数收益率
df['cum_return'] = (1 + df['daily_return']).cumprod() - 1       # 累计收益率
```

#### 数据清洗

```python
# === 处理缺失值 ===
df.dropna()                          # 删除包含NaN的行
df.fillna(method='ffill')            # 前向填充
df.fillna(method='bfill')            # 后向填充
df['close'].interpolate()            # 线性插值

# === 数据筛选 ===
# 筛选收益率大于5%的交易日
big_moves = df[df['daily_return'] > 0.05]

# 筛选成交量前10名
top_volume = df.nlargest(10, 'volume')

# 多条件筛选
signals = df[(df['MA5'] > df['MA20']) & (df['volume'] > df['volume'].rolling(20).mean())]

# === 数据合并 ===
# 合并多只股票数据
stock1 = pd.read_csv('stock1.csv', index_col='date', parse_dates=True)
stock2 = pd.read_csv('stock2.csv', index_col='date', parse_dates=True)
combined = pd.merge(stock1, stock2, left_index=True, right_index=True, suffixes=('_1', '_2'))
```

---

### 📦 Matplotlib / Plotly — 数据可视化

#### Matplotlib：经典静态图表

```python
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# === 基础K线图（简化版） ===
fig, axes = plt.subplots(2, 1, figsize=(14, 8), height_ratios=[3, 1],
                          gridspec_kw={'hspace': 0.1})

# 价格图
ax1 = axes[0]
ax1.plot(df.index, df['close'], label='收盘价', color='#2196F3', linewidth=1.5)
ax1.plot(df.index, df['MA5'], label='MA5', color='#FF9800', linewidth=1)
ax1.plot(df.index, df['MA20'], label='MA20', color='#4CAF50', linewidth=1)
ax1.set_title('股票价格走势图', fontsize=14)
ax1.legend(loc='upper left')
ax1.grid(True, alpha=0.3)

# 成交量图
ax2 = axes[1]
colors = ['#EF5350' if c >= o else '#26A69A'
          for c, o in zip(df['close'], df['open'])]
ax2.bar(df.index, df['volume'], color=colors, alpha=0.7)
ax2.set_title('成交量', fontsize=12)
ax2.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig('stock_chart.png', dpi=150, bbox_inches='tight')
plt.show()
```

#### Plotly：交互式图表

```python
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# === 专业K线图（带成交量） ===
fig = make_subplots(
    rows=2, cols=1,
    shared_xaxes=True,
    vertical_spacing=0.03,
    row_heights=[0.7, 0.3]
)

# 添加K线
fig.add_trace(
    go.Candlestick(
        x=df.index,
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close'],
        name='K线',
        increasing_line_color='#EF5350',   # 阳线红色（中国习惯）
        decreasing_line_color='#26A69A'    # 阴线绿色
    ),
    row=1, col=1
)

# 添加均线
fig.add_trace(
    go.Scatter(x=df.index, y=df['MA5'], name='MA5',
               line=dict(color='#FF9800', width=1)),
    row=1, col=1
)

fig.add_trace(
    go.Scatter(x=df.index, y=df['MA20'], name='MA20',
               line=dict(color='#2196F3', width=1)),
    row=1, col=1
)

# 添加成交量
colors = ['#EF5350' if c >= o else '#26A69A'
          for c, o in zip(df['close'], df['open'])]
fig.add_trace(
    go.Bar(x=df.index, y=df['volume'], name='成交量',
           marker_color=colors),
    row=2, col=1
)

fig.update_layout(
    title='股票K线图',
    xaxis_rangeslider_visible=False,
    template='plotly_dark',
    height=600
)
fig.show()
```

---

## 数据获取工具

### 📡 数据源对比

| 数据源 | 覆盖市场 | 费用 | 数据质量 | 实时数据 | 适合场景 |
|-------|---------|------|---------|---------|---------|
| **AKShare** | A股/港股/美股/期货 | 免费 | ⭐⭐⭐ | 支持 | 个人研究首选 |
| **Tushare** | A股为主 | 需积分 | ⭐⭐⭐⭐ | 支持 | 专业研究 |
| **yfinance** | 美股/全球 | 免费 | ⭐⭐⭐ | 延迟 | 海外市场研究 |
| **Wind** | 全市场 | 💰昂贵 | ⭐⭐⭐⭐⭐ | 支持 | 机构级别 |
| **通达信/同花顺** | A股 | 免费/付费 | ⭐⭐⭐ | 支持 | 看盘+导出数据 |

### AKShare（推荐入门使用）

AKShare 是一个**完全免费**的开源金融数据接口库，支持 A 股、港股、美股、期货等多种数据。

```python
import akshare as ak

# === 获取A股日线数据 ===
df = ak.stock_zh_a_hist(
    symbol="000001",           # 平安银行
    period="daily",            # 日线
    start_date="20230101",
    end_date="20241231",
    adjust="qfq"               # 前复权
)
print(df.head())
"""
         日期     开盘     收盘     最高     最低       成交量     成交额
0  2023-01-03  13.05  13.17  13.22  12.98   780345  1.02e+09
1  2023-01-04  13.20  13.30  13.35  13.15   654321  8.65e+08
...
"""

# === 获取实时行情 ===
realtime = ak.stock_zh_a_spot_em()
print(realtime[['代码', '名称', '最新价', '涨跌幅', '成交量']].head(10))

# === 获取指数数据 ===
sh_index = ak.stock_zh_index_daily(symbol="sh000001")  # 上证指数

# === 获取财务数据 ===
finance = ak.stock_financial_report_sina(
    stock="000001",
    symbol="资产负债表"
)

# === 获取板块数据 ===
sectors = ak.stock_board_industry_name_em()
```

### Tushare

Tushare 提供更专业的数据接口，但需要**注册获取积分**（积分可通过分享等方式获取）。

```python
import tushare as ts

# 设置 token（注册后获取）
ts.set_token('your_token_here')
pro = ts.pro_api()

# === 获取日线数据 ===
df = pro.daily(
    ts_code='000001.SZ',
    start_date='20230101',
    end_date='20241231'
)

# === 获取基本面数据 ===
# 每日指标
daily_basic = pro.daily_basic(
    ts_code='000001.SZ',
    trade_date='20241231',
    fields='ts_code,trade_date,pe_ttm,pb,ps_ttm,total_mv'
)

# === 获取财务数据 ===
income = pro.income(
    ts_code='000001.SZ',
    start_date='20230101',
    end_date='20241231'
)

# === 获取股票列表 ===
stock_list = pro.stock_basic(
    exchange='',
    list_status='L',
    fields='ts_code,symbol,name,area,industry,list_date'
)
```

### yfinance（港股与美股数据）

`yfinance` 是获取雅虎财经数据的开源工具，对于获取**港股**历史行情非常方便。在 A 股实战中由于网络和代码格式原因，我们主要使用 AKShare 和 Tushare，但在分析港股或进行多市场对比时，`yfinance` 是很好的补充。

*注：雅虎财经中的港股代码格式为“四位数字.HK”，如腾讯控股为 `0700.HK`。*

```python
import yfinance as yf

# === 获取港股单只股票数据（以腾讯控股为例） ===
tencent = yf.Ticker("0700.HK")

# 历史数据
hist = tencent.history(period="1y")  # 最近1年
print(hist.head())

# 公司基本信息
info = tencent.info
print(f"公司名称: {info['longName']}")
print(f"行业类别: {info['industry']}")

# === 批量下载港股数据 ===
tickers = ["0700.HK", "9988.HK", "3690.HK", "1810.HK"]  # 腾讯、阿里、美团、小米
data = yf.download(tickers, start="2023-01-01", end="2024-12-31")

# 获取所有股票的收盘价
close_prices = data['Close']
```

### API 调用基础

很多数据源提供 RESTful API，了解基本的 API 调用方法很有帮助。以下以获取股票日线行情为例的通用 API 请求示例：

```python
import requests
import json

# === 基础 API 调用示例 ===
url = "https://api.example.com/v1/stock/daily"
params = {
    "symbol": "600519",  # 以贵州茅台为例
    "start_date": "2024-01-01",
    "end_date": "2024-12-31",
    "api_key": "your_api_key"
}

response = requests.get(url, params=params)

if response.status_code == 200:
    data = response.json()
    df = pd.DataFrame(data['results'])
    print(df.head())
else:
    print(f"请求失败: {response.status_code}")

# === 处理频率限制 ===
import time

symbols = ["AAPL", "GOOGL", "MSFT"]
all_data = []

for symbol in symbols:
    response = requests.get(url, params={"symbol": symbol})
    all_data.append(response.json())
    time.sleep(0.5)  # 每次请求间隔0.5秒，避免触发频率限制
```

---

## 回测框架

### Backtrader 基础使用

Backtrader 是 Python 最流行的回测框架之一，功能强大且灵活。

#### 核心概念

```
┌─────────────────────────────────────────┐
│            Backtrader 架构               │
│                                         │
│  ┌─────────┐    ┌──────────┐            │
│  │ Data Feed│───→│ Strategy │            │
│  │ (数据源) │    │ (策略)   │            │
│  └─────────┘    └────┬─────┘            │
│                      │                   │
│                 ┌────▼─────┐            │
│                 │  Broker  │            │
│                 │ (经纪人)  │            │
│                 └────┬─────┘            │
│                      │                   │
│                 ┌────▼─────┐            │
│                 │ Analyzer │            │
│                 │ (分析器)  │            │
│                 └──────────┘            │
└─────────────────────────────────────────┘
```

#### 基础回测示例

```python
import backtrader as bt
import datetime

# === 定义策略 ===
class SmaCross(bt.Strategy):
    """均线交叉策略"""
    params = (
        ('fast_period', 5),     # 快速均线周期
        ('slow_period', 20),    # 慢速均线周期
    )

    def __init__(self):
        # 计算均线
        self.fast_ma = bt.indicators.SMA(
            self.data.close, period=self.params.fast_period
        )
        self.slow_ma = bt.indicators.SMA(
            self.data.close, period=self.params.slow_period
        )
        # 交叉信号
        self.crossover = bt.indicators.CrossOver(self.fast_ma, self.slow_ma)

    def next(self):
        if not self.position:  # 未持仓
            if self.crossover > 0:  # 金叉：快速均线上穿慢速均线
                self.buy()
        elif self.crossover < 0:    # 死叉：快速均线下穿慢速均线
            self.close()

# === 运行回测 ===
cerebro = bt.Cerebro()

# 添加策略
cerebro.addstrategy(SmaCross)

# 添加数据
data = bt.feeds.YahooFinanceCSVData(
    dataname='stock_data.csv',
    fromdate=datetime.datetime(2023, 1, 1),
    todate=datetime.datetime(2024, 12, 31)
)
cerebro.adddata(data)

# 设置初始资金
cerebro.broker.setcash(100000)

# 设置佣金
cerebro.broker.setcommission(commission=0.001)  # 千分之一

# 设置每次交易买入的股数
cerebro.addsizer(bt.sizers.FixedSize, stake=100)

# 添加分析器
cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')

# 运行
print(f'初始资金: {cerebro.broker.getvalue():.2f}')
results = cerebro.run()
print(f'最终资金: {cerebro.broker.getvalue():.2f}')

# 获取分析结果
strat = results[0]
print(f"夏普比率: {strat.analyzers.sharpe.get_analysis()['sharperatio']:.2f}")
print(f"最大回撤: {strat.analyzers.drawdown.get_analysis()['max']['drawdown']:.2f}%")

# 绘图
cerebro.plot(style='candlestick')
```

### vectorbt 简介

vectorbt 是一个**高性能的向量化回测框架**，适合快速批量测试策略参数。

```python
import vectorbt as vbt
import numpy as np

# === 快速回测均线交叉 ===
# 下载港股腾讯控股数据作为示例
price = vbt.YFData.download("0700.HK", start="2020-01-01", end="2024-12-31").get("Close")

# 批量测试不同参数组合
fast_windows = np.arange(5, 30, 5)     # [5, 10, 15, 20, 25]
slow_windows = np.arange(20, 80, 10)   # [20, 30, 40, 50, 60, 70]

# 一行代码完成所有组合的回测！
fast_ma, slow_ma = vbt.MA.run_combs(
    price, fast_windows, slow_windows,
    r=2, short_names=['fast', 'slow']
)

# 生成交易信号
entries = fast_ma.ma_crossed_above(slow_ma)
exits = fast_ma.ma_crossed_below(slow_ma)

# 运行回测
portfolio = vbt.Portfolio.from_signals(price, entries, exits, init_cash=100000)

# 查看所有参数组合的表现
print(portfolio.total_return())
print(portfolio.sharpe_ratio())

# 热力图可视化
portfolio.total_return().vbt.heatmap().show()
```

**vectorbt 的优势**：
- ⚡ **极快**：向量化计算，比循环快数百倍
- 📊 **参数优化**：可一次性测试数千个参数组合
- 📈 **内置可视化**：丰富的图表功能

---

## 实战示例

### 示例一：获取股票数据并绘制K线图

```python
"""
完整示例：获取A股数据并绘制专业K线图
"""
import akshare as ak
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ====== 第1步：获取数据 ======
print("正在获取数据...")
df = ak.stock_zh_a_hist(
    symbol="600519",           # 贵州茅台
    period="daily",
    start_date="20240101",
    end_date="20241231",
    adjust="qfq"
)

# 重命名列并设置索引
df.columns = ['date', 'open', 'close', 'high', 'low', 'volume', 'turnover',
              'amplitude', 'change_pct', 'change', 'turnover_rate']
df['date'] = pd.to_datetime(df['date'])
df.set_index('date', inplace=True)

# ====== 第2步：计算技术指标 ======
df['MA5'] = df['close'].rolling(5).mean()
df['MA20'] = df['close'].rolling(20).mean()
df['MA60'] = df['close'].rolling(60).mean()

# ====== 第3步：绘制K线图 ======
fig = make_subplots(
    rows=2, cols=1,
    shared_xaxes=True,
    vertical_spacing=0.05,
    row_heights=[0.75, 0.25],
    subplot_titles=('贵州茅台(600519) K线图', '成交量')
)

# K线
fig.add_trace(
    go.Candlestick(
        x=df.index, open=df['open'], high=df['high'],
        low=df['low'], close=df['close'], name='K线',
        increasing_line_color='#EF5350',
        decreasing_line_color='#26A69A'
    ), row=1, col=1
)

# 均线
for ma, color, name in [('MA5', '#FF9800', 'MA5'),
                          ('MA20', '#2196F3', 'MA20'),
                          ('MA60', '#9C27B0', 'MA60')]:
    fig.add_trace(
        go.Scatter(x=df.index, y=df[ma], name=name,
                   line=dict(color=color, width=1)),
        row=1, col=1
    )

# 成交量（涨红跌绿）
colors = ['#EF5350' if row['close'] >= row['open'] else '#26A69A'
          for _, row in df.iterrows()]
fig.add_trace(
    go.Bar(x=df.index, y=df['volume'], name='成交量',
           marker_color=colors, opacity=0.7),
    row=2, col=1
)

fig.update_layout(
    height=700, template='plotly_white',
    xaxis_rangeslider_visible=False,
    title_text='贵州茅台(600519) 技术分析图',
    showlegend=True
)
fig.show()
print("图表绘制完成！")
```

### 示例二：计算技术指标

```python
"""
完整示例：计算常用技术指标
"""
import pandas as pd
import numpy as np

def calculate_indicators(df):
    """计算常用技术指标"""

    # ====== 移动平均线 ======
    for period in [5, 10, 20, 60]:
        df[f'MA{period}'] = df['close'].rolling(period).mean()
        df[f'EMA{period}'] = df['close'].ewm(span=period, adjust=False).mean()

    # ====== MACD（指数平滑异同移动平均线） ======
    ema12 = df['close'].ewm(span=12, adjust=False).mean()
    ema26 = df['close'].ewm(span=26, adjust=False).mean()
    df['MACD_DIF'] = ema12 - ema26                          # DIF线
    df['MACD_DEA'] = df['MACD_DIF'].ewm(span=9, adjust=False).mean()  # DEA线
    df['MACD_HIST'] = 2 * (df['MACD_DIF'] - df['MACD_DEA'])           # MACD柱

    # ====== RSI（相对强弱指标） ======
    def calc_rsi(series, period=14):
        delta = series.diff()
        gain = delta.where(delta > 0, 0).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))

    df['RSI6'] = calc_rsi(df['close'], 6)
    df['RSI14'] = calc_rsi(df['close'], 14)

    # ====== 布林带（Bollinger Bands） ======
    period = 20
    df['BB_MID'] = df['close'].rolling(period).mean()
    df['BB_STD'] = df['close'].rolling(period).std()
    df['BB_UPPER'] = df['BB_MID'] + 2 * df['BB_STD']   # 上轨
    df['BB_LOWER'] = df['BB_MID'] - 2 * df['BB_STD']   # 下轨
    # 布林带宽度
    df['BB_WIDTH'] = (df['BB_UPPER'] - df['BB_LOWER']) / df['BB_MID']
    # %B 指标
    df['BB_PCT'] = (df['close'] - df['BB_LOWER']) / (df['BB_UPPER'] - df['BB_LOWER'])

    # ====== ATR（平均真实波幅） ======
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df['ATR14'] = true_range.rolling(window=14).mean()

    # ====== KDJ ======
    low_min = df['low'].rolling(window=9).min()
    high_max = df['high'].rolling(window=9).max()
    rsv = (df['close'] - low_min) / (high_max - low_min) * 100
    df['K'] = rsv.ewm(com=2, adjust=False).mean()
    df['D'] = df['K'].ewm(com=2, adjust=False).mean()
    df['J'] = 3 * df['K'] - 2 * df['D']

    return df

# 使用示例
df = calculate_indicators(df)
print(df[['close', 'MA5', 'MA20', 'MACD_DIF', 'RSI14', 'BB_UPPER', 'BB_LOWER']].tail(10))
```

### 示例三：简单均线交叉策略回测

```python
"""
完整示例：均线交叉策略回测（不使用回测框架，纯Pandas实现）
"""
import pandas as pd
import numpy as np
import akshare as ak
import matplotlib.pyplot as plt

# ====== 第1步：获取数据 ======
df = ak.stock_zh_a_hist(
    symbol="000300",  # 沪深300ETF
    period="daily",
    start_date="20200101",
    end_date="20241231",
    adjust="qfq"
)
df.columns = ['date', 'open', 'close', 'high', 'low', 'volume', 'turnover',
              'amplitude', 'change_pct', 'change', 'turnover_rate']
df['date'] = pd.to_datetime(df['date'])
df.set_index('date', inplace=True)

# ====== 第2步：计算信号 ======
fast_period = 5
slow_period = 20

df['fast_ma'] = df['close'].rolling(fast_period).mean()
df['slow_ma'] = df['close'].rolling(slow_period).mean()

# 生成信号：1=买入，-1=卖出，0=持有
df['signal'] = 0
df.loc[df['fast_ma'] > df['slow_ma'], 'signal'] = 1   # 金叉持有
df.loc[df['fast_ma'] <= df['slow_ma'], 'signal'] = -1  # 死叉空仓

# 只在信号变化时交易
df['position'] = df['signal'].diff()

# ====== 第3步：计算收益 ======
commission = 0.001  # 佣金千分之一（单边）

df['daily_return'] = df['close'].pct_change()
df['strategy_return'] = df['daily_return'] * df['signal'].shift(1)  # 前一天的信号决定今天的仓位

# 扣除交易成本
df.loc[df['position'] != 0, 'strategy_return'] -= commission

# 累计收益
df['cum_benchmark'] = (1 + df['daily_return']).cumprod()
df['cum_strategy'] = (1 + df['strategy_return']).cumprod()

# ====== 第4步：计算绩效指标 ======
trading_days = 252

# 年化收益率
total_days = len(df)
strategy_total_return = df['cum_strategy'].iloc[-1] - 1
annual_return = (1 + strategy_total_return) ** (trading_days / total_days) - 1

# 年化波动率
annual_vol = df['strategy_return'].std() * np.sqrt(trading_days)

# 夏普比率（假设无风险利率3%）
risk_free_rate = 0.03
sharpe_ratio = (annual_return - risk_free_rate) / annual_vol

# 最大回撤
cum_max = df['cum_strategy'].cummax()
drawdown = (df['cum_strategy'] - cum_max) / cum_max
max_drawdown = drawdown.min()

# 胜率
trades = df[df['position'] != 0].copy()
trade_returns = df['strategy_return'][df['signal'].shift(1) == 1]
win_rate = (trade_returns > 0).mean()

print("=" * 50)
print("📊 策略回测报告")
print("=" * 50)
print(f"回测期间: {df.index[0].date()} ~ {df.index[-1].date()}")
print(f"策略总收益率: {strategy_total_return:.2%}")
print(f"年化收益率:   {annual_return:.2%}")
print(f"年化波动率:   {annual_vol:.2%}")
print(f"夏普比率:     {sharpe_ratio:.2f}")
print(f"最大回撤:     {max_drawdown:.2%}")
print(f"胜率:         {win_rate:.2%}")
print("=" * 50)

# ====== 第5步：绘图 ======
fig, axes = plt.subplots(3, 1, figsize=(14, 10), height_ratios=[2, 1, 1])

# 累计收益对比
axes[0].plot(df.index, df['cum_strategy'], label='策略收益', color='#2196F3', linewidth=1.5)
axes[0].plot(df.index, df['cum_benchmark'], label='基准收益', color='#9E9E9E', linewidth=1)
axes[0].set_title('策略 vs 基准 累计收益对比')
axes[0].legend()
axes[0].grid(True, alpha=0.3)

# 回撤曲线
axes[1].fill_between(df.index, drawdown, 0, color='#EF5350', alpha=0.3)
axes[1].plot(df.index, drawdown, color='#EF5350', linewidth=0.5)
axes[1].set_title('回撤曲线')
axes[1].grid(True, alpha=0.3)

# 持仓信号
axes[2].plot(df.index, df['signal'], color='#4CAF50', linewidth=0.5)
axes[2].set_title('持仓信号 (1=多头, -1=空仓)')
axes[2].grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig('backtest_result.png', dpi=150, bbox_inches='tight')
plt.show()
```

---

## 进阶工具推荐

### 🔧 TA-Lib（技术分析库）

TA-Lib 提供了 **150+ 种技术指标**的高效实现。

```python
import talib

# 安装（可能需要额外步骤）
# conda install -c conda-forge ta-lib

# 使用示例
df['RSI'] = talib.RSI(df['close'].values, timeperiod=14)
df['MACD'], df['MACD_SIGNAL'], df['MACD_HIST'] = talib.MACD(
    df['close'].values, fastperiod=12, slowperiod=26, signalperiod=9
)
upper, middle, lower = talib.BBANDS(
    df['close'].values, timeperiod=20, nbdevup=2, nbdevdn=2
)

# 支持的指标类别：
# - 重叠研究：SMA, EMA, BBANDS, SAR 等
# - 动量指标：RSI, MACD, STOCH, CCI 等
# - 成交量指标：AD, ADOSC, OBV 等
# - 波动率指标：ATR, NATR, TRANGE 等
# - 形态识别：CDL系列（吞没形态、十字星等）
```

### 🤖 scikit-learn（机器学习）

将机器学习应用于量化策略：

```python
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

# 构建特征
features = ['MA5', 'MA20', 'RSI14', 'MACD_DIF', 'volume_ratio']
X = df[features].dropna()
y = (df['close'].shift(-1) > df['close']).astype(int)  # 明天涨=1，跌=0
y = y[X.index]

# 分割训练集和测试集
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.3, shuffle=False  # 注意：时间序列不要随机打乱！
)

# 训练模型
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# 预测和评估
predictions = model.predict(X_test)
accuracy = accuracy_score(y_test, predictions)
print(f"预测准确率: {accuracy:.2%}")

# 特征重要性
importance = pd.Series(
    model.feature_importances_, index=features
).sort_values(ascending=False)
print("\n特征重要性:")
print(importance)
```

### 📊 statsmodels（统计建模）

```python
import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller, coint

# === ADF 平稳性检验 ===
result = adfuller(df['close'])
print(f'ADF统计量: {result[0]:.4f}')
print(f'p值: {result[1]:.4f}')
if result[1] < 0.05:
    print("序列是平稳的（拒绝单位根假设）")
else:
    print("序列非平稳（不能拒绝单位根假设）")

# === 协整检验（配对交易基础） ===
score, pvalue, _ = coint(stock_a_prices, stock_b_prices)
print(f'协整检验 p值: {pvalue:.4f}')
if pvalue < 0.05:
    print("两只股票存在协整关系，适合配对交易")

# === 线性回归 ===
X = sm.add_constant(df[['MA5', 'volume']])
y = df['close']
model = sm.OLS(y, X).fit()
print(model.summary())
```

### 📋 工具速查表

| 类别 | 库名 | 用途 | 安装命令 |
|------|------|------|---------|
| 科学计算 | NumPy | 数组运算、线性代数 | `pip install numpy` |
| 数据分析 | Pandas | 表格数据处理 | `pip install pandas` |
| 可视化 | Matplotlib | 静态图表 | `pip install matplotlib` |
| 交互可视化 | Plotly | 交互式图表 | `pip install plotly` |
| A股数据 | AKShare | 免费金融数据 | `pip install akshare` |
| 专业数据 | Tushare | 专业A股数据 | `pip install tushare` |
| 美股数据 | yfinance | 雅虎财经数据 | `pip install yfinance` |
| 回测框架 | Backtrader | 事件驱动回测 | `pip install backtrader` |
| 高效回测 | vectorbt | 向量化回测 | `pip install vectorbt` |
| 技术指标 | TA-Lib | 150+技术指标 | `conda install ta-lib` |
| 机器学习 | scikit-learn | ML模型 | `pip install scikit-learn` |
| 统计建模 | statsmodels | 统计检验/回归 | `pip install statsmodels` |

---

## 本章小结

| 要点 | 内容 |
|------|------|
| 语言选择 | Python 是量化交易的首选语言 |
| 环境搭建 | 用 Conda 管理环境 + Jupyter 做研究 |
| 数据处理 | NumPy 做计算，Pandas 做分析 |
| 数据获取 | AKShare（免费入门）+ Tushare（专业） |
| 回测 | Backtrader（灵活）或 vectorbt（高效） |
| 可视化 | Matplotlib（静态）+ Plotly（交互） |
| 进阶 | TA-Lib + scikit-learn + statsmodels |

> 💡 **建议**：不要试图一次学会所有工具。先从 Pandas + AKShare + Matplotlib 开始，逐步扩展你的工具箱。

---

*← 上一章：[量化交易概述](./01-量化交易概述.md) | 下一章：[经典量化策略详解](./03-经典量化策略.md) →*
