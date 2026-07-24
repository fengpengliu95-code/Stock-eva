import re

md_path = "/Users/finlay/Documents/Stock- evaluation/docs/03-技术分析/01-K线与形态学.md"

with open(md_path, "r", encoding="utf-8") as file:
    content = file.read()

# 1. Replace 2.2 visual structure
visual_pattern = r'```\n\s*┃\s*← 上影线.*?\n```'
content = re.sub(visual_pattern, '![K线基本结构](images/kline_structure.png)', content, flags=re.DOTALL)

# 2. Insert single patterns image under Section 4 header
content = re.sub(
    r'## 4. 经典单根K线形态',
    '## 4. 经典单根K线形态\n\n![经典单根K线形态](images/kline_single_patterns.png)',
    content
)

# Remove all single candle code blocks
single_patterns = [
    # 大阳线
    r'#### 4.1.1 大阳线（长红K线） ☀️\n\n```\n   ┏━━━━━━━━━━┓  ← 收盘价.*?```',
    # 大阴线
    r'#### 4.1.2 大阴线（长绿K线） 🌑\n\n```\n   ┏━━━━━━━━━━┓  ← 开盘价.*?```',
    # 锤子线
    r'#### 4.2.1 锤子线（Hammer）🔨\n\n```\n         ┃\n   ┏━━━━━━━━┓.*?```',
    # 倒锤子线
    r'#### 4.2.3 倒锤子线（Inverted Hammer）\n\n```\n         ┃\n         ┃  ← 上影线.*?```',
    # 普通十字星
    r'#### 4.3.1 普通十字星\n\n```\n         ┃\n   ━━━━━╋━━━━━.*?```',
    # 长十字星
    r'#### 4.3.2 长十字星\n\n```\n         ┃\n         ┃\n         ┃\n   ━━━━━╋━━━━━.*?```',
    # 蜻蜓十字星
    r'#### 4.3.3 蜻蜓十字星（T字线）🪰\n\n```\n   ━━━━━━━━━━━  ← 开盘价.*?```',
    # 墓碑十字星
    r'#### 4.3.4 墓碑十字星 ⚰️\n\n```\n         ┃\n         ┃\n         ┃  ← 很长的上影线.*?```',
    # 光头光脚阳线
    r'#### 4.4.1 光头光脚阳线\n\n```\n   ┏━━━━━━━━━━┓  ← 最高价.*?```',
    # 螺旋桨
    r'#### 4.4.3 螺旋桨（纺锤线）\n\n```\n         ┃\n         ┃\n   ┏━━━━━━━━┓.*?```'
]

for p in single_patterns:
    # We substitute it by removing the code block (keeping the header)
    header = re.search(r'####.*?\n', p).group(0).strip()
    content = re.sub(p, header, content, flags=re.DOTALL)

# 3. Morning / Evening Star
content = re.sub(
    r'#### 5.1.1 早晨之星（Morning Star）🌅\n\n由三根K线组成的底部反转形态：\n\n```\n  第1根.*?```',
    '#### 5.1.1 早晨之星（Morning Star）🌅\n\n![早晨之星与黄昏之星](images/kline_morning_evening.png)\n\n由三根K线组成的底部反转形态：',
    content,
    flags=re.DOTALL
)

# Remove Evening Star ASCII
content = re.sub(
    r'#### 5.2.1 黄昏之星（Evening Star）\n\n**早晨之星的镜像形态，顶部反转信号。**\n\n```\n  第1根.*?```',
    '#### 5.2.1 黄昏之星（Evening Star）\n\n**早晨之星的镜像形态，顶部反转信号。**',
    content,
    flags=re.DOTALL
)

# 4. Engulfing
content = re.sub(
    r'#### 5.1.2 看涨吞没（Bullish Engulfing）\n\n```\n  第1根.*?```',
    '#### 5.1.2 看涨吞没（Bullish Engulfing）\n\n![吞没形态 (看涨吞没与看跌吞没)](images/kline_engulfing.png)\n\n',
    content,
    flags=re.DOTALL
)

# Remove Bearish Engulfing ASCII (since the engulfed image shows both)
content = re.sub(
    r'#### 5.2.2 看跌吞没（Bearish Engulfing）\n\n- 看涨吞没的镜像\n- 出现在上涨趋势末端\n- 大阴线的实体完全包裹前一根阳线的实体',
    '#### 5.2.2 看跌吞没（Bearish Engulfing）\n\n- 看涨吞没的镜像\n- 出现在上涨趋势末端\n- 大阴线的实体完全包裹前一根阳线的实体',
    content
)

# 5. Red Three Soldiers, Crows, Rising Three Methods
content = re.sub(
    r'#### 5.1.3 红三兵（Three White Soldiers）\n\n```\n              ┏━━━┓.*?```',
    '#### 5.1.3 红三兵（Three White Soldiers）\n\n![趋势K线组合 (红三兵、三只乌鸦与上升三法)](images/kline_trend_patterns.png)\n\n',
    content,
    flags=re.DOTALL
)

# Remove Three Crows ASCII
content = re.sub(
    r'#### 5.2.3 三只乌鸦（Three Black Crows）🐦\n\n```\n      ┗━━━┓.*?```',
    '#### 5.2.3 三只乌鸦（Three Black Crows）🐦\n\n',
    content,
    flags=re.DOTALL
)

# Remove Rising Three Methods ASCII
content = re.sub(
    r'#### 5.3.1 上升三法（Rising Three Methods）\n\n```\n      ┏━━━━┓.*?```',
    '#### 5.3.1 上升三法（Rising Three Methods）\n\n',
    content,
    flags=re.DOTALL
)

# Remove Piercing Pattern ASCII
content = re.sub(
    r'#### 5.1.4 底部反转形态（曙光初现 / Piercing Pattern）\n\n```\n  第1根.*?```',
    '#### 5.1.4 底部反转形态（曙光初现 / Piercing Pattern）\n\n',
    content,
    flags=re.DOTALL
)

with open(md_path, "w", encoding="utf-8") as file:
    file.write(content)

print("Markdown K-line images updated successfully.")
