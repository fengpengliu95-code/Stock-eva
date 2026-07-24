/* =============================================
   量化操盘手修炼之路 - Dashboard Application
   ============================================= */

// ===== 立即执行：等待 DOM 加载完毕 =====
document.addEventListener('DOMContentLoaded', () => {
    initParticles();
    initNavigation();
    initScrollAnimations();
    initStatCounters();
    initModules();
    initFlashcards();
    initCalculator();
});

/* =============================================
   1. 粒子动画系统 (Particle Animation System)
   ============================================= */
function initParticles() {
    const canvas = document.getElementById('particleCanvas');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    let particles = [];
    let animationId;

    // 自适应画布尺寸
    function resizeCanvas() {
        canvas.width = canvas.parentElement.offsetWidth;
        canvas.height = canvas.parentElement.offsetHeight;
    }
    resizeCanvas();
    window.addEventListener('resize', resizeCanvas);

    // 粒子类
    class Particle {
        constructor() {
            this.reset();
        }
        reset() {
            this.x = Math.random() * canvas.width;
            this.y = Math.random() * canvas.height;
            this.size = Math.random() * 2 + 0.5;
            this.speedX = (Math.random() - 0.5) * 0.5;
            this.speedY = (Math.random() - 0.5) * 0.5;
            this.opacity = Math.random() * 0.5 + 0.1;
            this.hue = Math.random() > 0.5 ? 160 : 45; // 绿色或金色
        }
        update() {
            this.x += this.speedX;
            this.y += this.speedY;

            // 边界检测：超出则重置
            if (this.x < 0 || this.x > canvas.width ||
                this.y < 0 || this.y > canvas.height) {
                this.reset();
            }
        }
        draw() {
            ctx.beginPath();
            ctx.arc(this.x, this.y, this.size, 0, Math.PI * 2);
            if (this.hue === 160) {
                ctx.fillStyle = `rgba(0, 212, 170, ${this.opacity})`;
            } else {
                ctx.fillStyle = `rgba(255, 215, 0, ${this.opacity * 0.6})`;
            }
            ctx.fill();
        }
    }

    // 创建粒子
    const particleCount = Math.min(80, Math.floor((canvas.width * canvas.height) / 15000));
    for (let i = 0; i < particleCount; i++) {
        particles.push(new Particle());
    }

    // 绘制连线
    function drawConnections() {
        for (let i = 0; i < particles.length; i++) {
            for (let j = i + 1; j < particles.length; j++) {
                const dx = particles[i].x - particles[j].x;
                const dy = particles[i].y - particles[j].y;
                const dist = Math.sqrt(dx * dx + dy * dy);

                if (dist < 150) {
                    const opacity = (1 - dist / 150) * 0.15;
                    ctx.beginPath();
                    ctx.moveTo(particles[i].x, particles[i].y);
                    ctx.lineTo(particles[j].x, particles[j].y);
                    ctx.strokeStyle = `rgba(0, 212, 170, ${opacity})`;
                    ctx.lineWidth = 0.5;
                    ctx.stroke();
                }
            }
        }
    }

    // 主动画循环
    function animate() {
        ctx.clearRect(0, 0, canvas.width, canvas.height);

        particles.forEach(p => {
            p.update();
            p.draw();
        });

        drawConnections();
        animationId = requestAnimationFrame(animate);
    }

    animate();

    // 当页面不可见时暂停动画以节省性能
    document.addEventListener('visibilitychange', () => {
        if (document.hidden) {
            cancelAnimationFrame(animationId);
        } else {
            animate();
        }
    });
}

/* =============================================
   2. 导航栏 (Navigation)
   ============================================= */
function initNavigation() {
    const navbar = document.getElementById('navbar');
    const hamburger = document.getElementById('hamburger');
    const navLinks = document.getElementById('navLinks');
    const allNavLinks = document.querySelectorAll('.nav-link');
    const sections = document.querySelectorAll('.section, .hero-section');

    // 滚动时添加背景
    window.addEventListener('scroll', () => {
        if (window.scrollY > 50) {
            navbar.classList.add('scrolled');
        } else {
            navbar.classList.remove('scrolled');
        }

        // 高亮当前 section 对应的导航项
        let current = '';
        sections.forEach(section => {
            const sectionTop = section.offsetTop - 100;
            if (window.scrollY >= sectionTop) {
                current = section.getAttribute('id');
            }
        });

        allNavLinks.forEach(link => {
            link.classList.remove('active');
            if (link.getAttribute('data-section') === current) {
                link.classList.add('active');
            }
        });
    });

    // 移动端汉堡菜单
    hamburger.addEventListener('click', () => {
        hamburger.classList.toggle('active');
        navLinks.classList.toggle('open');
    });

    // 点击导航链接后关闭移动端菜单
    allNavLinks.forEach(link => {
        link.addEventListener('click', () => {
            hamburger.classList.remove('active');
            navLinks.classList.remove('open');
        });
    });
}

/* =============================================
   3. 滚动动画 (Scroll-based Animations)
   使用 Intersection Observer API
   ============================================= */
function initScrollAnimations() {
    const observerOptions = {
        root: null,
        rootMargin: '0px 0px -60px 0px',
        threshold: 0.1
    };

    const observer = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                entry.target.classList.add('visible');
                // 对模块卡片添加延迟动画
                const cards = entry.target.closest('.modules-grid');
                if (cards) {
                    const allCards = cards.querySelectorAll('.module-card');
                    allCards.forEach((card, i) => {
                        card.style.transitionDelay = `${i * 0.1}s`;
                    });
                }
            }
        });
    }, observerOptions);

    // 观察时间线项目
    document.querySelectorAll('.timeline-item').forEach(el => observer.observe(el));

    // 观察模块卡片
    document.querySelectorAll('.module-card').forEach(el => observer.observe(el));

    // 观察公式卡片
    document.querySelectorAll('.formula-card').forEach(el => observer.observe(el));
}

/* =============================================
   4. 数字计数动画 (Stat Counter Animation)
   ============================================= */
function initStatCounters() {
    const statNumbers = document.querySelectorAll('.stat-number');
    let hasAnimated = false;

    const observer = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting && !hasAnimated) {
                hasAnimated = true;
                statNumbers.forEach(el => {
                    const target = parseInt(el.getAttribute('data-count'));
                    animateCounter(el, target);
                });
            }
        });
    }, { threshold: 0.5 });

    if (statNumbers.length > 0) {
        observer.observe(statNumbers[0].closest('.hero-stats'));
    }

    function animateCounter(el, target) {
        let current = 0;
        const duration = 1500;
        const step = target / (duration / 16);

        function update() {
            current += step;
            if (current >= target) {
                el.textContent = target;
                return;
            }
            el.textContent = Math.floor(current);
            requestAnimationFrame(update);
        }
        update();
    }
}

/* =============================================
   5. 知识模块系统 (Knowledge Modules)
   含展开/收起与 localStorage 进度跟踪
   ============================================= */
function initModules() {
    const moduleCards = document.querySelectorAll('.module-card');
    const savedProgress = JSON.parse(localStorage.getItem('moduleProgress') || '{}');

    // 恢复已保存的进度
    Object.keys(savedProgress).forEach(topicId => {
        const checkbox = document.querySelector(`input[data-topic="${topicId}"]`);
        if (checkbox) {
            checkbox.checked = savedProgress[topicId];
            const item = checkbox.closest('.topic-item');
            if (item) {
                if (savedProgress[topicId]) {
                    item.classList.add('completed');
                } else {
                    item.classList.remove('completed');
                }
            }
        }
    });

    // 链接点击处理 (Markdown Modal)
    const topicLinks = document.querySelectorAll('.topic-link');
    const mdModal = document.getElementById('md-modal');
    const mdOverlay = document.querySelector('.md-modal-overlay');
    const mdClose = document.querySelector('.md-modal-close');
    const mdContent = document.getElementById('md-content');
    const mdTitle = document.querySelector('.md-modal-title');

    function closeMdModal() {
        if(mdModal) {
            mdModal.classList.remove('active');
            document.body.style.overflow = '';
        }
    }

    if (mdClose) mdClose.addEventListener('click', closeMdModal);
    if (mdOverlay) mdOverlay.addEventListener('click', closeMdModal);

    topicLinks.forEach(link => {
        link.addEventListener('click', async (e) => {
            e.stopPropagation();
            
            // 如果在本地 file:// 环境下，直接走原生同页跳转，绕过 fetch 跨域与 popup blocker
            if (window.location.protocol === 'file:') {
                return; // 不执行 preventDefault()，让浏览器原生跳转
            }

            e.preventDefault();

            const url = link.getAttribute('href');
            const title = link.textContent.replace(' 📖', '').trim();
            
            if (!mdModal) {
                window.location.href = url;
                return;
            }

            mdModal.classList.add('active');
            document.body.style.overflow = 'hidden';
            mdTitle.textContent = title;
            mdContent.innerHTML = '<div class="md-loading">正在加载内容...</div>';

            try {
                const response = await fetch(url);
                if (!response.ok) throw new Error(`HTTP ${response.status}`);
                const text = await response.text();
                
                if (typeof marked !== 'undefined') {
                    mdContent.innerHTML = marked.parse(text);
                } else {
                    mdContent.innerHTML = `<pre>${text}</pre>`;
                }
            } catch (err) {
                console.error("加载 Markdown 失败:", err);
                closeMdModal();
                window.location.href = url; // 降级为同页跳转
            }
        });
    });

    // 更新各模块进度条
    moduleCards.forEach(card => {
        updateModuleProgress(card);

        // 展开/收起逻辑
        const expandBtn = card.querySelector('.module-expand-btn');
        expandBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            card.classList.toggle('expanded');
        });

        // 卡片点击也可以展开
        card.addEventListener('click', (e) => {
            if (e.target.closest('.topic-checkbox') || e.target.closest('.module-expand-btn') || e.target.closest('.topic-link')) return;
            card.classList.toggle('expanded');
        });

        // checkbox 变化事件
        const checkboxes = card.querySelectorAll('input[type="checkbox"]');
        checkboxes.forEach(cb => {
            cb.addEventListener('change', () => {
                const topicId = cb.getAttribute('data-topic');
                savedProgress[topicId] = cb.checked;
                localStorage.setItem('moduleProgress', JSON.stringify(savedProgress));
                
                const item = cb.closest('.topic-item');
                if (item) {
                    if (cb.checked) {
                        item.classList.add('completed');
                    } else {
                        item.classList.remove('completed');
                    }
                }
                
                updateModuleProgress(card);
            });
        });
    });

    function updateModuleProgress(card) {
        const checkboxes = card.querySelectorAll('input[type="checkbox"]');
        const total = checkboxes.length;
        const checked = Array.from(checkboxes).filter(cb => cb.checked).length;
        const percentage = total > 0 ? Math.round((checked / total) * 100) : 0;

        const progressFill = card.querySelector('.progress-fill');
        const statusEl = card.querySelector('.module-status');

        progressFill.style.width = percentage + '%';

        if (percentage === 0) {
            statusEl.textContent = '未开始';
            statusEl.style.color = '';
        } else if (percentage === 100) {
            statusEl.textContent = '✅ 已完成';
            statusEl.style.color = '#00d4aa';
        } else {
            statusEl.textContent = `${percentage}% 进行中`;
            statusEl.style.color = '#ffd700';
        }
    }
}

/* =============================================
   6. 概念闪卡系统 (Flashcard System)
   ============================================= */
function initFlashcards() {
    // 闪卡数据：20 个核心概念
    const flashcardData = [
        {
            term: '市盈率',
            en: 'P/E Ratio',
            definition: '股价除以每股收益(EPS)。反映投资者为每单位盈利愿意支付的价格倍数。低PE可能代表低估，但也需结合行业、增长率综合判断。一般A股PE在10-20为合理区间。'
        },
        {
            term: '市净率',
            en: 'P/B Ratio',
            definition: '股价除以每股净资产。衡量股价相对账面价值的溢价程度。PB < 1 称为"破净"，可能是价值陷阱或投资机会。银行股PB通常较低，科技股较高。'
        },
        {
            term: 'K线',
            en: 'Candlestick',
            definition: '也叫蜡烛图，由开盘价、收盘价、最高价、最低价四个价格构成。阳线（红色）表示收盘价高于开盘价，阴线（绿色）反之。是技术分析最基本的工具。'
        },
        {
            term: 'MACD',
            en: 'Moving Average Convergence Divergence',
            definition: '指数平滑异同平均线。由DIF线（12日EMA减26日EMA）、DEA线（DIF的9日EMA）和柱状图组成。金叉（DIF上穿DEA）为买入信号，死叉为卖出信号。'
        },
        {
            term: 'RSI',
            en: 'Relative Strength Index',
            definition: '相对强弱指数。衡量价格变动的速度和幅度，取值0-100。RSI > 70 表示超买（可能回调），RSI < 30 表示超卖（可能反弹）。常用14日周期。'
        },
        {
            term: '均线',
            en: 'Moving Average (MA)',
            definition: '将一定时期内的收盘价取平均值连成的曲线。常用5日（周线）、10日、20日（月线）、60日（季线）、250日（年线）。均线多头排列为牛市信号。'
        },
        {
            term: '涨停板',
            en: 'Daily Limit Up',
            definition: 'A股涨停幅度为10%（ST股为5%，创业板/科创板为20%）。涨停意味着当日买方力量极强，通常伴随重大利好或资金强势介入。连续涨停的股票风险极高。'
        },
        {
            term: '换手率',
            en: 'Turnover Rate',
            definition: '一定时期内成交量与流通股本的比值。高换手率表示交易活跃，可能意味着资金进出频繁。新股上市初期换手率通常较高，一般日换手率3%-7%较为活跃。'
        },
        {
            term: '量比',
            en: 'Volume Ratio',
            definition: '当日成交量与过去5日平均成交量的比值。量比 > 1 表示今日放量，< 1 表示缩量。量比突然放大可能预示股价异动，是盘中选股的重要参考指标。'
        },
        {
            term: '龙头股',
            en: 'Leading Stock',
            definition: '板块或行业中涨幅最大、最先涨停、带领板块上涨的标杆个股。龙头股通常具备题材正宗、市值适中、辨识度高等特征。追龙头是短线交易的核心策略之一。'
        },
        {
            term: '止损',
            en: 'Stop Loss',
            definition: '当亏损达到预设比例时强制卖出，以控制损失。常见止损比例为5%-8%。止损是交易纪律的核心，"截断亏损，让利润奔跑"是交易圣经级格言。'
        },
        {
            term: '仓位',
            en: 'Position Size',
            definition: '投入某只股票或整体市场的资金比例。满仓=100%资金投入，半仓=50%。合理的仓位管理是风险控制的基础，永远不要在单一标的上全仓。'
        },
        {
            term: '回撤',
            en: 'Drawdown',
            definition: '从资产最高点到最低点的跌幅。最大回撤(MDD)是衡量策略风险的核心指标。MDD > 20% 的策略需谨慎使用。控制回撤比追求高收益更重要。'
        },
        {
            term: '夏普比率',
            en: 'Sharpe Ratio',
            definition: '(组合收益率 - 无风险利率) / 组合收益率标准差。衡量每承受一单位风险获得的超额回报。夏普比率 > 1 为良好，> 2 为优秀，> 3 为卓越。'
        },
        {
            term: '阿尔法',
            en: 'Alpha (α)',
            definition: '超额收益率，即策略收益减去市场基准收益后的部分。阿尔法 > 0 表示跑赢市场。量化交易的核心目标就是寻找和捕获阿尔法。'
        },
        {
            term: '贝塔',
            en: 'Beta (β)',
            definition: '衡量个股或组合相对于市场的波动性。贝塔 = 1 表示与市场同步波动；> 1 表示波动更大（更激进）；< 1 表示波动更小（更防御）。'
        },
        {
            term: '布林带',
            en: 'Bollinger Bands',
            definition: '由中轨（N日均线）、上轨（中轨+2倍标准差）、下轨（中轨-2倍标准差）组成。价格触及上轨可能回调，触及下轨可能反弹。带宽收窄预示变盘。'
        },
        {
            term: '均值回归',
            en: 'Mean Reversion',
            definition: '价格偏离均值后倾向于回归均值的统计特性。基于此的策略在价格过高时做空、过低时做多。典型应用包括配对交易和统计套利。适用于震荡市。'
        },
        {
            term: '动量策略',
            en: 'Momentum Strategy',
            definition: '基于"强者恒强"效应，买入近期表现好的资产，卖出表现差的。动量因子是学术界最稳健的因子之一。通常选取3-12个月的回看期，需注意动量反转风险。'
        },
        {
            term: '凯利公式',
            en: 'Kelly Criterion',
            definition: 'f* = (b·p - q) / b。确定每次交易的最优仓位比例。b=盈亏比，p=胜率，q=败率。实战中常用半凯利(f*/2)以降低波动。是仓位管理的理论基石。'
        }
    ];

    let cards = [...flashcardData];
    let currentIndex = 0;

    const stage = document.getElementById('flashcardStage');
    const counter = document.getElementById('fcCounter');
    const dotsContainer = document.getElementById('fcDots');
    const prevBtn = document.getElementById('fcPrev');
    const nextBtn = document.getElementById('fcNext');
    const shuffleBtn = document.getElementById('fcShuffle');

    // 渲染当前闪卡
    function renderCard() {
        const card = cards[currentIndex];
        stage.innerHTML = `
            <div class="flashcard" id="currentFlashcard">
                <div class="flashcard-face flashcard-front">
                    <div class="fc-term">${card.term}</div>
                    <div class="fc-term-en">${card.en}</div>
                    <div class="fc-hint">👆 点击翻转查看定义</div>
                </div>
                <div class="flashcard-face flashcard-back">
                    <div class="fc-def-title">${card.term} (${card.en})</div>
                    <div class="fc-definition">${card.definition}</div>
                </div>
            </div>
        `;

        // 绑定翻转事件
        const flashcard = document.getElementById('currentFlashcard');
        flashcard.addEventListener('click', () => {
            flashcard.classList.toggle('flipped');
        });

        // 更新计数器
        counter.textContent = `${currentIndex + 1} / ${cards.length}`;

        // 更新指示点
        renderDots();
    }

    // 渲染底部指示点
    function renderDots() {
        dotsContainer.innerHTML = '';
        cards.forEach((_, i) => {
            const dot = document.createElement('div');
            dot.className = 'fc-dot' + (i === currentIndex ? ' active' : '');
            dot.addEventListener('click', () => {
                currentIndex = i;
                renderCard();
            });
            dotsContainer.appendChild(dot);
        });
    }

    // 上一张
    prevBtn.addEventListener('click', () => {
        currentIndex = (currentIndex - 1 + cards.length) % cards.length;
        renderCard();
    });

    // 下一张
    nextBtn.addEventListener('click', () => {
        currentIndex = (currentIndex + 1) % cards.length;
        renderCard();
    });

    // 随机排列 (Fisher-Yates 洗牌算法)
    shuffleBtn.addEventListener('click', () => {
        for (let i = cards.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1));
            [cards[i], cards[j]] = [cards[j], cards[i]];
        }
        currentIndex = 0;
        renderCard();

        // 添加按钮动画反馈
        shuffleBtn.style.transform = 'scale(0.95)';
        setTimeout(() => { shuffleBtn.style.transform = ''; }, 150);
    });

    // 键盘导航
    document.addEventListener('keydown', (e) => {
        // 只在闪卡区域可见时响应
        const section = document.getElementById('flashcards');
        const rect = section.getBoundingClientRect();
        const isVisible = rect.top < window.innerHeight && rect.bottom > 0;
        if (!isVisible) return;

        if (e.key === 'ArrowLeft') { prevBtn.click(); }
        if (e.key === 'ArrowRight') { nextBtn.click(); }
        if (e.key === ' ' || e.key === 'Enter') {
            e.preventDefault();
            const fc = document.getElementById('currentFlashcard');
            if (fc) fc.classList.toggle('flipped');
        }
    });

    renderCard();
}

/* =============================================
   7. 技术指标计算器 (Technical Indicator Calculator)
   包含 SMA, EMA, RSI, 布林带 计算与 Canvas 图表
   ============================================= */
function initCalculator() {
    const calcBtn = document.getElementById('calcBtn');
    calcBtn.addEventListener('click', runCalculation);

    function runCalculation() {
        const rawInput = document.getElementById('priceInput').value;
        const prices = rawInput
            .split(',')
            .map(s => parseFloat(s.trim()))
            .filter(n => !isNaN(n));

        if (prices.length < 5) {
            alert('请输入至少5个价格数据');
            return;
        }

        const smaPeriod = parseInt(document.getElementById('smaPeriod').value) || 5;
        const emaPeriod = parseInt(document.getElementById('emaPeriod').value) || 5;
        const rsiPeriod = parseInt(document.getElementById('rsiPeriod').value) || 14;
        const bbPeriod = parseInt(document.getElementById('bbPeriod').value) || 5;

        // 计算各指标
        const smaValues = calcSMA(prices, smaPeriod);
        const emaValues = calcEMA(prices, emaPeriod);
        const rsiValues = calcRSI(prices, rsiPeriod);
        const bbValues = calcBollingerBands(prices, bbPeriod);

        // 获取最新值
        const latestSMA = smaValues.length > 0 ? smaValues[smaValues.length - 1] : null;
        const latestEMA = emaValues.length > 0 ? emaValues[emaValues.length - 1] : null;
        const latestRSI = rsiValues.length > 0 ? rsiValues[rsiValues.length - 1] : null;
        const latestBB = bbValues.length > 0 ? bbValues[bbValues.length - 1] : null;
        const latestPrice = prices[prices.length - 1];

        // 更新结果表格
        updateResultsTable(latestPrice, latestSMA, latestEMA, latestRSI, latestBB, smaPeriod, emaPeriod, rsiPeriod, bbPeriod);

        // 绘制图表
        drawChart(prices, smaValues, emaValues, bbValues, smaPeriod, emaPeriod, bbPeriod);
    }

    // ----- SMA: 简单移动平均 -----
    function calcSMA(prices, period) {
        const result = [];
        for (let i = period - 1; i < prices.length; i++) {
            let sum = 0;
            for (let j = i - period + 1; j <= i; j++) {
                sum += prices[j];
            }
            result.push(sum / period);
        }
        return result;
    }

    // ----- EMA: 指数移动平均 -----
    function calcEMA(prices, period) {
        const result = [];
        const multiplier = 2 / (period + 1);

        // 首个 EMA 值使用 SMA
        let sum = 0;
        for (let i = 0; i < period; i++) {
            sum += prices[i];
        }
        result.push(sum / period);

        // 后续使用 EMA 公式
        for (let i = period; i < prices.length; i++) {
            const ema = (prices[i] - result[result.length - 1]) * multiplier + result[result.length - 1];
            result.push(ema);
        }
        return result;
    }

    // ----- RSI: 相对强弱指数 -----
    function calcRSI(prices, period) {
        if (prices.length < period + 1) return [];

        const gains = [];
        const losses = [];

        // 计算每日涨跌
        for (let i = 1; i < prices.length; i++) {
            const change = prices[i] - prices[i - 1];
            gains.push(change > 0 ? change : 0);
            losses.push(change < 0 ? Math.abs(change) : 0);
        }

        const result = [];

        // 首个平均值
        let avgGain = gains.slice(0, period).reduce((a, b) => a + b, 0) / period;
        let avgLoss = losses.slice(0, period).reduce((a, b) => a + b, 0) / period;

        let rs = avgLoss === 0 ? 100 : avgGain / avgLoss;
        result.push(100 - 100 / (1 + rs));

        // 后续使用平滑公式
        for (let i = period; i < gains.length; i++) {
            avgGain = (avgGain * (period - 1) + gains[i]) / period;
            avgLoss = (avgLoss * (period - 1) + losses[i]) / period;
            rs = avgLoss === 0 ? 100 : avgGain / avgLoss;
            result.push(100 - 100 / (1 + rs));
        }

        return result;
    }

    // ----- 布林带 -----
    function calcBollingerBands(prices, period) {
        const result = [];
        for (let i = period - 1; i < prices.length; i++) {
            const slice = prices.slice(i - period + 1, i + 1);
            const mean = slice.reduce((a, b) => a + b, 0) / period;
            const variance = slice.reduce((sum, p) => sum + Math.pow(p - mean, 2), 0) / period;
            const stdDev = Math.sqrt(variance);
            result.push({
                upper: mean + 2 * stdDev,
                middle: mean,
                lower: mean - 2 * stdDev
            });
        }
        return result;
    }

    // ----- 更新结果表格 -----
    function updateResultsTable(price, sma, ema, rsi, bb, smaPeriod, emaPeriod, rsiPeriod, bbPeriod) {
        const tbody = document.getElementById('calcTableBody');

        let rows = '';

        // 当前价格
        rows += `<tr>
            <td>📍 当前价格</td>
            <td>${price.toFixed(2)}</td>
            <td>—</td>
        </tr>`;

        // SMA
        if (sma !== null) {
            const signal = price > sma ? '看多 ↑' : '看空 ↓';
            const signalClass = price > sma ? 'signal-buy' : 'signal-sell';
            rows += `<tr>
                <td>SMA(${smaPeriod})</td>
                <td>${sma.toFixed(2)}</td>
                <td class="${signalClass}">${signal}</td>
            </tr>`;
        }

        // EMA
        if (ema !== null) {
            const signal = price > ema ? '看多 ↑' : '看空 ↓';
            const signalClass = price > ema ? 'signal-buy' : 'signal-sell';
            rows += `<tr>
                <td>EMA(${emaPeriod})</td>
                <td>${ema.toFixed(2)}</td>
                <td class="${signalClass}">${signal}</td>
            </tr>`;
        }

        // RSI
        if (rsi !== null) {
            let signal, signalClass;
            if (rsi > 70) {
                signal = '超买 ⚠️';
                signalClass = 'signal-sell';
            } else if (rsi < 30) {
                signal = '超卖 🟢';
                signalClass = 'signal-buy';
            } else {
                signal = '中性 ◉';
                signalClass = 'signal-neutral';
            }
            rows += `<tr>
                <td>RSI(${rsiPeriod})</td>
                <td>${rsi.toFixed(2)}</td>
                <td class="${signalClass}">${signal}</td>
            </tr>`;
        }

        // 布林带
        if (bb !== null) {
            let signal, signalClass;
            if (price >= bb.upper) {
                signal = '触及上轨 ⚠️';
                signalClass = 'signal-sell';
            } else if (price <= bb.lower) {
                signal = '触及下轨 🟢';
                signalClass = 'signal-buy';
            } else {
                signal = '区间内 ◉';
                signalClass = 'signal-neutral';
            }
            rows += `<tr>
                <td>布林带 上轨(${bbPeriod})</td>
                <td>${bb.upper.toFixed(2)}</td>
                <td class="${signalClass}">${signal}</td>
            </tr>`;
            rows += `<tr>
                <td>布林带 中轨(${bbPeriod})</td>
                <td>${bb.middle.toFixed(2)}</td>
                <td>—</td>
            </tr>`;
            rows += `<tr>
                <td>布林带 下轨(${bbPeriod})</td>
                <td>${bb.lower.toFixed(2)}</td>
                <td>—</td>
            </tr>`;
        }

        tbody.innerHTML = rows;
    }

    // ----- Canvas 图表绘制 -----
    function drawChart(prices, smaValues, emaValues, bbValues, smaPeriod, emaPeriod, bbPeriod) {
        const canvas = document.getElementById('calcChart');
        const ctx = canvas.getContext('2d');

        // 高DPI适配
        const dpr = window.devicePixelRatio || 1;
        const rect = canvas.parentElement.getBoundingClientRect();
        canvas.width = rect.width * dpr;
        canvas.height = 350 * dpr;
        canvas.style.width = rect.width + 'px';
        canvas.style.height = '350px';
        ctx.scale(dpr, dpr);

        const W = rect.width;
        const H = 350;
        const padding = { top: 30, right: 20, bottom: 40, left: 60 };
        const chartW = W - padding.left - padding.right;
        const chartH = H - padding.top - padding.bottom;

        // 清空画布
        ctx.clearRect(0, 0, W, H);

        // 收集所有数值以确定 Y 轴范围
        let allValues = [...prices];
        bbValues.forEach(bb => {
            allValues.push(bb.upper, bb.lower);
        });
        const minVal = Math.min(...allValues) * 0.995;
        const maxVal = Math.max(...allValues) * 1.005;
        const valRange = maxVal - minVal;

        // 坐标转换函数
        function xPos(index) {
            return padding.left + (index / (prices.length - 1)) * chartW;
        }
        function yPos(value) {
            return padding.top + (1 - (value - minVal) / valRange) * chartH;
        }

        // 绘制网格
        ctx.strokeStyle = 'rgba(255, 255, 255, 0.04)';
        ctx.lineWidth = 1;
        const gridLines = 5;
        for (let i = 0; i <= gridLines; i++) {
            const y = padding.top + (i / gridLines) * chartH;
            ctx.beginPath();
            ctx.moveTo(padding.left, y);
            ctx.lineTo(W - padding.right, y);
            ctx.stroke();

            // Y 轴标签
            const val = maxVal - (i / gridLines) * valRange;
            ctx.fillStyle = '#5a6478';
            ctx.font = '11px Inter, sans-serif';
            ctx.textAlign = 'right';
            ctx.fillText(val.toFixed(1), padding.left - 8, y + 4);
        }

        // X 轴标签（数据点序号）
        const xLabelStep = Math.max(1, Math.floor(prices.length / 6));
        ctx.fillStyle = '#5a6478';
        ctx.textAlign = 'center';
        for (let i = 0; i < prices.length; i += xLabelStep) {
            ctx.fillText(i + 1, xPos(i), H - padding.bottom + 20);
        }

        // 绘制布林带（半透明区域）
        if (bbValues.length > 0) {
            const offset = prices.length - bbValues.length;
            ctx.beginPath();
            for (let i = 0; i < bbValues.length; i++) {
                const x = xPos(i + offset);
                if (i === 0) ctx.moveTo(x, yPos(bbValues[i].upper));
                else ctx.lineTo(x, yPos(bbValues[i].upper));
            }
            for (let i = bbValues.length - 1; i >= 0; i--) {
                ctx.lineTo(xPos(i + offset), yPos(bbValues[i].lower));
            }
            ctx.closePath();
            ctx.fillStyle = 'rgba(168, 85, 247, 0.08)';
            ctx.fill();

            // 上轨线
            drawLine(ctx, bbValues.map((b, i) => ({ x: xPos(i + offset), y: yPos(b.upper) })), 'rgba(168, 85, 247, 0.4)', 1);
            // 下轨线
            drawLine(ctx, bbValues.map((b, i) => ({ x: xPos(i + offset), y: yPos(b.lower) })), 'rgba(168, 85, 247, 0.4)', 1);
            // 中轨线
            drawLine(ctx, bbValues.map((b, i) => ({ x: xPos(i + offset), y: yPos(b.middle) })), 'rgba(168, 85, 247, 0.6)', 1, [4, 4]);
        }

        // 绘制 SMA 线
        if (smaValues.length > 0) {
            const offset = prices.length - smaValues.length;
            drawLine(ctx, smaValues.map((v, i) => ({ x: xPos(i + offset), y: yPos(v) })), '#3b82f6', 1.5);
        }

        // 绘制 EMA 线
        if (emaValues.length > 0) {
            const offset = prices.length - emaValues.length;
            drawLine(ctx, emaValues.map((v, i) => ({ x: xPos(i + offset), y: yPos(v) })), '#ffd700', 1.5);
        }

        // 绘制价格线（渐变区域 + 线条）
        // 面积渐变
        const priceGrad = ctx.createLinearGradient(0, padding.top, 0, H - padding.bottom);
        priceGrad.addColorStop(0, 'rgba(0, 212, 170, 0.15)');
        priceGrad.addColorStop(1, 'rgba(0, 212, 170, 0)');

        ctx.beginPath();
        ctx.moveTo(xPos(0), yPos(prices[0]));
        for (let i = 1; i < prices.length; i++) {
            ctx.lineTo(xPos(i), yPos(prices[i]));
        }
        ctx.lineTo(xPos(prices.length - 1), H - padding.bottom);
        ctx.lineTo(xPos(0), H - padding.bottom);
        ctx.closePath();
        ctx.fillStyle = priceGrad;
        ctx.fill();

        // 价格线条
        drawLine(ctx, prices.map((v, i) => ({ x: xPos(i), y: yPos(v) })), '#00d4aa', 2);

        // 价格数据点（最后一个点高亮）
        const lastX = xPos(prices.length - 1);
        const lastY = yPos(prices[prices.length - 1]);
        ctx.beginPath();
        ctx.arc(lastX, lastY, 4, 0, Math.PI * 2);
        ctx.fillStyle = '#00d4aa';
        ctx.fill();
        ctx.beginPath();
        ctx.arc(lastX, lastY, 8, 0, Math.PI * 2);
        ctx.strokeStyle = 'rgba(0, 212, 170, 0.3)';
        ctx.lineWidth = 2;
        ctx.stroke();

        // 图例
        const legends = [
            { label: '价格', color: '#00d4aa' },
            { label: `SMA(${smaPeriod})`, color: '#3b82f6' },
            { label: `EMA(${emaPeriod})`, color: '#ffd700' },
            { label: `布林带(${bbPeriod})`, color: '#a855f7' }
        ];

        let legendX = padding.left;
        legends.forEach(legend => {
            ctx.beginPath();
            ctx.moveTo(legendX, 14);
            ctx.lineTo(legendX + 20, 14);
            ctx.strokeStyle = legend.color;
            ctx.lineWidth = 2;
            ctx.stroke();

            ctx.fillStyle = '#8b95a8';
            ctx.font = '11px "Noto Sans SC", sans-serif';
            ctx.textAlign = 'left';
            ctx.fillText(legend.label, legendX + 24, 18);
            legendX += ctx.measureText(legend.label).width + 44;
        });
    }

    // 辅助函数：绘制折线
    function drawLine(ctx, points, color, width, dash = []) {
        if (points.length < 2) return;
        ctx.beginPath();
        ctx.setLineDash(dash);
        ctx.moveTo(points[0].x, points[0].y);
        for (let i = 1; i < points.length; i++) {
            ctx.lineTo(points[i].x, points[i].y);
        }
        ctx.strokeStyle = color;
        ctx.lineWidth = width;
        ctx.stroke();
        ctx.setLineDash([]);
    }

    // 页面加载后自动计算一次默认数据
    runCalculation();
}
