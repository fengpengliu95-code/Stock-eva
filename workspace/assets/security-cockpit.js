const oa = "http://127.0.0.1:8000/api/v1", si = /^(sh|sz)\.[0-9]{6}$/, Ge = /^\d{4}-\d{2}-\d{2}$/;
let ir = class extends Error {
  status;
  detail;
  constructor(e, t) {
    super(t), this.name = "ApiError", this.status = e, this.detail = t;
  }
};
class li extends Error {
  constructor() {
    super("no backend-provided trading dates"), this.name = "NoTradingDatesError";
  }
}
function hr(r) {
  return typeof r == "object" && r !== null && !Array.isArray(r);
}
function ye(r, e) {
  const t = r[e];
  if (typeof t != "string") throw new TypeError(`${e} must be a string`);
  return t;
}
function ce(r, e) {
  const t = r[e];
  if (typeof t != "number" || !Number.isFinite(t))
    throw new TypeError(`${e} must be a finite number`);
  return t;
}
function Vt(r, e) {
  return r[e] === null ? null : ce(r, e);
}
function sa(r) {
  if (!hr(r)) throw new TypeError("series point must be an object");
  const e = ye(r, "trade_date");
  if (!Ge.test(e))
    throw new TypeError("trade_date must be an ISO date");
  return {
    trade_date: e,
    open: ce(r, "open"),
    high: ce(r, "high"),
    low: ce(r, "low"),
    close: ce(r, "close"),
    volume: ce(r, "volume"),
    amount: ce(r, "amount"),
    ma5: Vt(r, "ma5"),
    ma10: Vt(r, "ma10"),
    ma20: Vt(r, "ma20"),
    ma60: Vt(r, "ma60"),
    ma120: Vt(r, "ma120"),
    ma250: Vt(r, "ma250"),
    macd: Vt(r, "macd"),
    macd_signal: Vt(r, "macd_signal"),
    macd_hist: Vt(r, "macd_hist"),
    rsi14: Vt(r, "rsi14")
  };
}
function la(r) {
  if (!hr(r)) throw new TypeError("analysis response must be an object");
  const e = ye(r, "symbol"), t = ye(r, "status"), i = ye(r, "source"), a = ye(r, "price_adjustment"), n = ye(r, "formula_version");
  if (!si.test(e)) throw new TypeError("invalid response symbol");
  if (t !== "empty" && t !== "ready")
    throw new TypeError("invalid analysis status");
  if (i !== "baostock") throw new TypeError("invalid analysis source");
  if (a !== "qfq")
    throw new TypeError("invalid price adjustment");
  if (r.as_of !== null && typeof r.as_of != "string")
    throw new TypeError("as_of must be an ISO date or null");
  if (typeof r.as_of == "string" && !Ge.test(r.as_of))
    throw new TypeError("as_of must be an ISO date or null");
  if (!Array.isArray(r.quality_issues) || !r.quality_issues.every((o) => typeof o == "string"))
    throw new TypeError("quality_issues must be a string array");
  if (!Array.isArray(r.series))
    throw new TypeError("series must be an array");
  return {
    symbol: e,
    status: t,
    as_of: r.as_of,
    source: i,
    price_adjustment: a,
    formula_version: n,
    quality_issues: r.quality_issues,
    series: r.series.map(sa)
  };
}
function ui(r) {
  if (!Array.isArray(r) || !r.every(
    (e) => typeof e == "string" && Ge.test(e)
  ))
    throw new TypeError("trading dates must be an ISO date array");
  for (let e = 1; e < r.length; e += 1)
    if (r[e] <= r[e - 1])
      throw new TypeError("trading dates must be strictly ascending");
  return r;
}
function ua(r, e) {
  const t = ui(r);
  if (e !== void 0 && !Ge.test(e))
    throw new TypeError("as_of must be an ISO date");
  const i = e === void 0 ? t : t.filter((n) => n <= e);
  if (i.length === 0) throw new li();
  const a = i.slice(-260);
  return { start: a[0], end: a[a.length - 1] };
}
async function ca(r) {
  try {
    const e = await r.json();
    if (hr(e) && typeof e.detail == "string")
      return e.detail;
  } catch {
  }
  return `HTTP ${r.status}`;
}
async function Dr(r, e, t) {
  const i = await e(`${oa}${r}`, { signal: t });
  if (!i.ok)
    throw new ir(i.status, await ca(i));
  return i.json();
}
async function da(r, e = fetch, t = new AbortController().signal, i) {
  if (!si.test(r)) throw new TypeError("invalid security symbol");
  const a = ui(
    await Dr("/market/history/dates", e, t)
  ), { start: n, end: o } = ua(a, i), s = new URLSearchParams({ start: n, end: o }), l = await Dr(
    `/securities/${encodeURIComponent(r)}/analysis?${s}`,
    e,
    t
  );
  return la(l);
}
function dt(r, e) {
  if (!(!Yt(r) && !Yt(e))) {
    for (var t in e)
      if (Object.prototype.hasOwnProperty.call(e, t)) {
        var i = r[t], a = e[t];
        Yt(a) && Yt(i) ? dt(i, a) : r[t] = Be(a);
      }
  }
}
function Be(r) {
  if (!Yt(r))
    return r;
  var e = null;
  Nt(r) ? e = [] : e = {};
  for (var t in r)
    if (Object.prototype.hasOwnProperty.call(r, t)) {
      var i = r[t];
      Yt(i) ? e[t] = Be(i) : e[t] = i;
    }
  return e;
}
function Nt(r) {
  return Object.prototype.toString.call(r) === "[object Array]";
}
function ct(r) {
  return typeof r == "function";
}
function Yt(r) {
  return typeof r == "object" && C(r);
}
function B(r) {
  return typeof r == "number" && Number.isFinite(r);
}
function C(r) {
  return r != null;
}
function we(r) {
  return typeof r == "boolean";
}
function K(r) {
  return typeof r == "string";
}
var ha = /\\(\\)?/g, va = RegExp(`[^.[\\]]+|\\[(?:([^"'][^[]*)|(["'])((?:(?!\\2)[^\\\\]|\\\\.)*?)\\2)\\]|(?=(?:\\.|\\[\\])(?:\\.|\\[\\]|$))`, "g");
function ht(r, e, t) {
  if (C(r)) {
    var i = [];
    e.replace(va, function(s) {
      for (var l = [], u = 1; u < arguments.length; u++)
        l[u - 1] = arguments[u];
      var c = s;
      return C(l[1]) ? c = l[2].replace(ha, "$1") : C(l[0]) && (c = l[0].trim()), i.push(c), "";
    });
    for (var a = r, n = 0, o = i.length; C(a) && n < o; )
      a = a?.[i[n++]];
    return C(a) ? a : t ?? "--";
  }
  return t ?? "--";
}
function fa(r, e) {
  var t = {};
  return r.formatToParts(new Date(e)).forEach(function(i) {
    var a = i.type, n = i.value;
    switch (a) {
      case "year": {
        t.YYYY = n;
        break;
      }
      case "month": {
        t.MM = n;
        break;
      }
      case "day": {
        t.DD = n;
        break;
      }
      case "hour": {
        t.HH = n === "24" ? "00" : n;
        break;
      }
      case "minute": {
        t.mm = n;
        break;
      }
      case "second": {
        t.ss = n;
        break;
      }
    }
  }), t;
}
function pa(r, e, t) {
  var i = fa(r, e);
  return t.replace(/YYYY|MM|DD|HH|mm|ss/g, function(a) {
    return i[a];
  });
}
function wt(r, e) {
  var t = +r;
  return B(t) ? t.toFixed(e ?? 2) : "".concat(r);
}
function ga(r) {
  var e = +r;
  if (B(e)) {
    if (e > 1e9)
      return "".concat(+(e / 1e9).toFixed(3), "B");
    if (e > 1e6)
      return "".concat(+(e / 1e6).toFixed(3), "M");
    if (e > 1e3)
      return "".concat(+(e / 1e3).toFixed(3), "K");
  }
  return "".concat(r);
}
function ma(r, e) {
  var t = "".concat(r);
  if (e.length === 0)
    return t;
  if (t.includes(".")) {
    var i = t.split(".");
    return "".concat(i[0].replace(/(\d)(?=(\d{3})+$)/g, function(a) {
      return "".concat(a).concat(e);
    }), ".").concat(i[1]);
  }
  return t.replace(/(\d)(?=(\d{3})+$)/g, function(a) {
    return "".concat(a).concat(e);
  });
}
function _a(r, e) {
  var t = "".concat(r), i = new RegExp("\\.0{" + e + ",}[1-9][0-9]*$");
  if (i.test(t)) {
    var a = t.split("."), n = a.length - 1, o = a[n], s = /0*/.exec(o);
    if (C(s)) {
      var l = s[0].length;
      return a[n] = o.replace(/0*/, "0{".concat(l, "}")), a.join(".");
    }
  }
  return t;
}
function kr(r, e) {
  return r.replace(/\{(\w+)\}/g, function(t, i) {
    var a = e[i];
    return C(a) ? a : "{".concat(i, "}");
  });
}
var Te = null;
function te(r) {
  var e, t;
  return (t = (e = r.ownerDocument.defaultView) === null || e === void 0 ? void 0 : e.devicePixelRatio) !== null && t !== void 0 ? t : 1;
}
function he(r, e, t) {
  return "".concat(e ?? "normal", " ").concat(r ?? 12, "px ").concat(t ?? "Helvetica Neue");
}
function Wt(r, e, t, i) {
  if (!C(Te)) {
    var a = document.createElement("canvas"), n = te(a);
    Te = a.getContext("2d"), Te.scale(n, n);
  }
  return Te.font = he(e, t, i), Math.round(Te.measureText(r).width);
}
var ar = function(r, e) {
  return ar = Object.setPrototypeOf || { __proto__: [] } instanceof Array && function(t, i) {
    t.__proto__ = i;
  } || function(t, i) {
    for (var a in i) Object.prototype.hasOwnProperty.call(i, a) && (t[a] = i[a]);
  }, ar(r, e);
};
function q(r, e) {
  if (typeof e != "function" && e !== null)
    throw new TypeError("Class extends value " + String(e) + " is not a constructor or null");
  ar(r, e);
  function t() {
    this.constructor = r;
  }
  r.prototype = e === null ? Object.create(e) : (t.prototype = e.prototype, new t());
}
var M = function() {
  return M = Object.assign || function(e) {
    for (var t, i = 1, a = arguments.length; i < a; i++) {
      t = arguments[i];
      for (var n in t) Object.prototype.hasOwnProperty.call(t, n) && (e[n] = t[n]);
    }
    return e;
  }, M.apply(this, arguments);
};
function Oe(r, e) {
  var t = {};
  for (var i in r) Object.prototype.hasOwnProperty.call(r, i) && e.indexOf(i) < 0 && (t[i] = r[i]);
  if (r != null && typeof Object.getOwnPropertySymbols == "function")
    for (var a = 0, i = Object.getOwnPropertySymbols(r); a < i.length; a++)
      e.indexOf(i[a]) < 0 && Object.prototype.propertyIsEnumerable.call(r, i[a]) && (t[i[a]] = r[i[a]]);
  return t;
}
function vr(r, e, t, i) {
  function a(n) {
    return n instanceof t ? n : new t(function(o) {
      o(n);
    });
  }
  return new (t || (t = Promise))(function(n, o) {
    function s(c) {
      try {
        u(i.next(c));
      } catch (d) {
        o(d);
      }
    }
    function l(c) {
      try {
        u(i.throw(c));
      } catch (d) {
        o(d);
      }
    }
    function u(c) {
      c.done ? n(c.value) : a(c.value).then(s, l);
    }
    u((i = i.apply(r, [])).next());
  });
}
function fr(r, e) {
  var t = { label: 0, sent: function() {
    if (n[0] & 1) throw n[1];
    return n[1];
  }, trys: [], ops: [] }, i, a, n, o = Object.create((typeof Iterator == "function" ? Iterator : Object).prototype);
  return o.next = s(0), o.throw = s(1), o.return = s(2), typeof Symbol == "function" && (o[Symbol.iterator] = function() {
    return this;
  }), o;
  function s(u) {
    return function(c) {
      return l([u, c]);
    };
  }
  function l(u) {
    if (i) throw new TypeError("Generator is already executing.");
    for (; o && (o = 0, u[0] && (t = 0)), t; ) try {
      if (i = 1, a && (n = u[0] & 2 ? a.return : u[0] ? a.throw || ((n = a.return) && n.call(a), 0) : a.next) && !(n = n.call(a, u[1])).done) return n;
      switch (a = 0, n && (u = [u[0] & 2, n.value]), u[0]) {
        case 0:
        case 1:
          n = u;
          break;
        case 4:
          return t.label++, { value: u[1], done: !1 };
        case 5:
          t.label++, a = u[1], u = [0];
          continue;
        case 7:
          u = t.ops.pop(), t.trys.pop();
          continue;
        default:
          if (n = t.trys, !(n = n.length > 0 && n[n.length - 1]) && (u[0] === 6 || u[0] === 2)) {
            t = 0;
            continue;
          }
          if (u[0] === 3 && (!n || u[1] > n[0] && u[1] < n[3])) {
            t.label = u[1];
            break;
          }
          if (u[0] === 6 && t.label < n[1]) {
            t.label = n[1], n = u;
            break;
          }
          if (n && t.label < n[2]) {
            t.label = n[2], t.ops.push(u);
            break;
          }
          n[2] && t.ops.pop(), t.trys.pop();
          continue;
      }
      u = e.call(r, t);
    } catch (c) {
      u = [6, c], a = 0;
    } finally {
      i = n = 0;
    }
    if (u[0] & 5) throw u[1];
    return { value: u[0] ? u[1] : void 0, done: !0 };
  }
}
function bt(r) {
  var e = typeof Symbol == "function" && Symbol.iterator, t = e && r[e], i = 0;
  if (t) return t.call(r);
  if (r && typeof r.length == "number") return {
    next: function() {
      return r && i >= r.length && (r = void 0), { value: r && r[i++], done: !r };
    }
  };
  throw new TypeError(e ? "Object is not iterable." : "Symbol.iterator is not defined.");
}
function Se(r, e) {
  var t = typeof Symbol == "function" && r[Symbol.iterator];
  if (!t) return r;
  var i = t.call(r), a, n = [], o;
  try {
    for (; (e === void 0 || e-- > 0) && !(a = i.next()).done; ) n.push(a.value);
  } catch (s) {
    o = { error: s };
  } finally {
    try {
      a && !a.done && (t = i.return) && t.call(i);
    } finally {
      if (o) throw o.error;
    }
  }
  return n;
}
function be(r, e, t) {
  if (arguments.length === 2) for (var i = 0, a = e.length, n; i < a; i++)
    (n || !(i in e)) && (n || (n = Array.prototype.slice.call(e, 0, i)), n[i] = e[i]);
  return r.concat(n || Array.prototype.slice.call(e));
}
function pr(r) {
  var e = {
    width: 0,
    height: 0,
    left: 0,
    right: 0,
    top: 0,
    bottom: 0
  };
  return C(r) && dt(e, r), e;
}
var Qt = -1;
function Le(r) {
  return ct(window.requestAnimationFrame) ? window.requestAnimationFrame(r) : window.setTimeout(r, 20);
}
function nr(r) {
  ct(window.cancelAnimationFrame) ? window.cancelAnimationFrame(r) : window.clearTimeout(r);
}
var or = (
  /** @class */
  (function() {
    function r(e) {
      this._options = { duration: 500, iterationCount: 1 }, this._currentIterationCount = 0, this._running = !1, this._time = 0, dt(this._options, e);
    }
    return r.prototype._loop = function() {
      var e = this;
      this._running = !0;
      var t = function() {
        var i;
        if (e._running) {
          var a = (/* @__PURE__ */ new Date()).getTime() - e._time;
          a < e._options.duration ? ((i = e._doFrameCallback) === null || i === void 0 || i.call(e, a), Le(t)) : (e.stop(), e._currentIterationCount++, e._currentIterationCount < e._options.iterationCount && e.start());
        }
      };
      Le(t);
    }, r.prototype.doFrame = function(e) {
      return this._doFrameCallback = e, this;
    }, r.prototype.setDuration = function(e) {
      return this._options.duration = e, this;
    }, r.prototype.setIterationCount = function(e) {
      return this._options.iterationCount = e, this;
    }, r.prototype.start = function() {
      this._running || (this._time = (/* @__PURE__ */ new Date()).getTime(), this._loop());
    }, r.prototype.stop = function() {
      var e;
      this._running && ((e = this._doFrameCallback) === null || e === void 0 || e.call(this, this._options.duration)), this._running = !1;
    }, r;
  })()
), Ze = 1, Rr = (/* @__PURE__ */ new Date()).getTime();
function xe(r) {
  var e = (/* @__PURE__ */ new Date()).getTime();
  return e === Rr ? ++Ze : Ze = 1, Rr = e, "".concat(r ?? "").concat(e, "_").concat(Ze);
}
function Gt(r, e) {
  var t, i = document.createElement(r), a = e ?? {};
  for (var n in a)
    i.style[n] = (t = a[n]) !== null && t !== void 0 ? t : "";
  return i;
}
function sr(r, e, t) {
  var i = 0, a = 0;
  for (a = r.length - 1; i !== a; ) {
    var n = Math.floor((a + i) / 2), o = a - i, s = r[n][e];
    if (t === r[i][e])
      return i;
    if (t === r[a][e])
      return a;
    if (t === s)
      return n;
    if (t > s ? i = n : a = n, o <= 2)
      break;
  }
  return i;
}
function ya(r) {
  var e = Math.floor(Ht(r)), t = de(e), i = r / t, a = 0;
  return i < 1.5 ? a = 1 : i < 2.5 ? a = 2 : i < 3.5 ? a = 3 : i < 4.5 ? a = 4 : i < 5.5 ? a = 5 : i < 6.5 ? a = 6 : a = 8, r = a * t, +r.toFixed(Math.abs(e));
}
function Fr(r, e) {
  e = Math.max(0, e ?? 0);
  var t = Math.pow(10, e);
  return Math.round(r * t) / t;
}
function xa(r) {
  var e = r.toString(), t = e.indexOf("e");
  if (t > 0) {
    var i = +e.slice(t + 1);
    return i < 0 ? -i : 0;
  }
  var a = e.indexOf(".");
  return a < 0 ? 0 : e.length - 1 - a;
}
function ci(r, e, t) {
  for (var i, a, n = [Number.MIN_SAFE_INTEGER, Number.MAX_SAFE_INTEGER], o = r.length, s = 0; s < o; ) {
    var l = r[s];
    n[0] = Math.max((i = l[e]) !== null && i !== void 0 ? i : Number.MIN_SAFE_INTEGER, n[0]), n[1] = Math.min((a = l[t]) !== null && a !== void 0 ? a : Number.MAX_SAFE_INTEGER, n[1]), ++s;
  }
  return n;
}
function Ht(r) {
  return r === 0 ? 0 : Math.log10(r);
}
function de(r) {
  return Math.pow(10, r);
}
function Br() {
  return { from: 0, to: 0, realFrom: 0, realTo: 0 };
}
var wa = (
  /** @class */
  (function() {
    function r(e) {
      this._holdingTasks = null, this._running = !1, this._callback = e;
    }
    return r.prototype.add = function(e) {
      this._running ? C(this._holdingTasks) ? this._holdingTasks = M(M({}, this._holdingTasks), e) : this._holdingTasks = e : this._runTask(e);
    }, r.prototype._runTask = function(e) {
      return vr(this, void 0, void 0, function() {
        var t, i;
        return fr(this, function(a) {
          switch (a.label) {
            case 0:
              this._running = !0, a.label = 1;
            case 1:
              return a.trys.push([1, , 3, 4]), [4, Promise.all(Object.values(e))];
            case 2:
              return a.sent(), [3, 4];
            case 3:
              return this._running = !1, (i = this._callback) === null || i === void 0 || i.call(this), C(this._holdingTasks) && (t = this._holdingTasks, this._runTask(t), this._holdingTasks = null), [
                7
                /*endfinally*/
              ];
            case 4:
              return [
                2
                /*return*/
              ];
          }
        });
      });
    }, r.prototype.clear = function() {
      this._holdingTasks = null;
    }, r;
  })()
), pt = {
  PRICE: 2,
  VOLUME: 0
}, ba = (
  /** @class */
  (function() {
    function r() {
      this._callbacks = [];
    }
    return r.prototype.subscribe = function(e) {
      var t = this._callbacks.indexOf(e);
      t < 0 && this._callbacks.push(e);
    }, r.prototype.unsubscribe = function(e) {
      if (ct(e)) {
        var t = this._callbacks.indexOf(e);
        t > -1 && this._callbacks.splice(t, 1);
      } else
        this._callbacks = [];
    }, r.prototype.execute = function(e) {
      this._callbacks.forEach(function(t) {
        t(e);
      });
    }, r.prototype.isEmpty = function() {
      return this._callbacks.length === 0;
    }, r;
  })()
);
function Ce(r) {
  return r === "transparent" || r === "none" || /^[rR][gG][Bb][Aa]\(([\s]*(2[0-4][0-9]|25[0-5]|[01]?[0-9][0-9]?)[\s]*,){3}[\s]*0[\s]*\)$/.test(r) || /^[hH][Ss][Ll][Aa]\(([\s]*(360｜3[0-5][0-9]|[012]?[0-9][0-9]?)[\s]*,)([\s]*((100|[0-9][0-9]?)%|0)[\s]*,){2}([\s]*0[\s]*)\)$/.test(r);
}
function ee(r, e) {
  var t = r.replace(/^#/, ""), i = parseInt(t, 16), a = i >> 16 & 255, n = i >> 8 & 255, o = i & 255;
  return "rgba(".concat(a, ", ").concat(n, ", ").concat(o, ", ").concat(e ?? 1, ")");
}
var N = {
  RED: "#F92855",
  GREEN: "#2DC08E",
  WHITE: "#FFFFFF",
  GREY: "#76808F",
  BLUE: "#1677FF"
};
function Ca() {
  return {
    show: !0,
    horizontal: {
      show: !0,
      size: 1,
      color: "#EDEDED",
      style: "dashed",
      dashedValue: [2, 2]
    },
    vertical: {
      show: !0,
      size: 1,
      color: "#EDEDED",
      style: "dashed",
      dashedValue: [2, 2]
    }
  };
}
function Ea() {
  var r = {
    show: !0,
    color: N.GREY,
    textOffset: 5,
    textSize: 10,
    textFamily: "Helvetica Neue",
    textWeight: "normal"
  };
  return {
    type: "candle_solid",
    bar: {
      compareRule: "current_open",
      upColor: N.GREEN,
      downColor: N.RED,
      noChangeColor: N.GREY,
      upBorderColor: N.GREEN,
      downBorderColor: N.RED,
      noChangeBorderColor: N.GREY,
      upWickColor: N.GREEN,
      downWickColor: N.RED,
      noChangeWickColor: N.GREY
    },
    area: {
      lineSize: 2,
      lineColor: N.BLUE,
      smooth: !1,
      value: "close",
      backgroundColor: [{
        offset: 0,
        color: ee(N.BLUE, 0.01)
      }, {
        offset: 1,
        color: ee(N.BLUE, 0.2)
      }],
      point: {
        show: !0,
        color: N.BLUE,
        radius: 4,
        rippleColor: ee(N.BLUE, 0.3),
        rippleRadius: 8,
        animation: !0,
        animationDuration: 1e3
      }
    },
    priceMark: {
      show: !0,
      high: M({}, r),
      low: M({}, r),
      last: {
        show: !0,
        compareRule: "current_open",
        upColor: N.GREEN,
        downColor: N.RED,
        noChangeColor: N.GREY,
        line: {
          show: !0,
          style: "dashed",
          dashedValue: [4, 4],
          size: 1
        },
        text: {
          show: !0,
          style: "fill",
          size: 12,
          paddingLeft: 4,
          paddingTop: 4,
          paddingRight: 4,
          paddingBottom: 4,
          borderColor: "transparent",
          borderStyle: "solid",
          borderSize: 0,
          borderDashedValue: [2, 2],
          color: N.WHITE,
          family: "Helvetica Neue",
          weight: "normal",
          borderRadius: 2
        },
        extendTexts: []
      }
    },
    tooltip: {
      offsetLeft: 4,
      offsetTop: 6,
      offsetRight: 4,
      offsetBottom: 6,
      showRule: "always",
      showType: "standard",
      rect: {
        position: "fixed",
        paddingLeft: 4,
        paddingRight: 4,
        paddingTop: 4,
        paddingBottom: 4,
        offsetLeft: 4,
        offsetTop: 4,
        offsetRight: 4,
        offsetBottom: 4,
        borderRadius: 4,
        borderSize: 1,
        borderColor: "#F2F3F5",
        color: "#FEFEFE"
      },
      title: {
        show: !0,
        size: 14,
        family: "Helvetica Neue",
        weight: "normal",
        color: N.GREY,
        marginLeft: 8,
        marginTop: 4,
        marginRight: 8,
        marginBottom: 4,
        template: "{ticker} · {period}"
      },
      legend: {
        size: 12,
        family: "Helvetica Neue",
        weight: "normal",
        color: N.GREY,
        marginLeft: 8,
        marginTop: 4,
        marginRight: 8,
        marginBottom: 4,
        defaultValue: "n/a",
        template: [
          { title: "time", value: "{time}" },
          { title: "open", value: "{open}" },
          { title: "high", value: "{high}" },
          { title: "low", value: "{low}" },
          { title: "close", value: "{close}" },
          { title: "volume", value: "{volume}" }
        ]
      },
      features: []
    }
  };
}
function Ia() {
  var r = ee(N.GREEN, 0.7), e = ee(N.RED, 0.7);
  return {
    ohlc: {
      compareRule: "current_open",
      upColor: r,
      downColor: e,
      noChangeColor: N.GREY
    },
    bars: [{
      style: "fill",
      borderStyle: "solid",
      borderSize: 1,
      borderDashedValue: [2, 2],
      upColor: r,
      downColor: e,
      noChangeColor: N.GREY
    }],
    lines: ["#FF9600", "#935EBD", N.BLUE, "#E11D74", "#01C5C4"].map(function(t) {
      return {
        style: "solid",
        smooth: !1,
        size: 1,
        dashedValue: [2, 2],
        color: t
      };
    }),
    circles: [{
      style: "fill",
      borderStyle: "solid",
      borderSize: 1,
      borderDashedValue: [2, 2],
      upColor: r,
      downColor: e,
      noChangeColor: N.GREY
    }],
    texts: [{
      paddingLeft: 0,
      paddingTop: 0,
      paddingRight: 0,
      paddingBottom: 0,
      style: "fill",
      size: 12,
      color: N.BLUE,
      family: "Helvetica Neue",
      weight: "normal",
      borderStyle: "solid",
      borderDashedValue: [2, 2],
      borderSize: 0,
      borderColor: "transparent",
      borderRadius: 0,
      backgroundColor: "transparent"
    }],
    lastValueMark: {
      show: !1,
      text: {
        show: !1,
        style: "fill",
        color: N.WHITE,
        size: 12,
        family: "Helvetica Neue",
        weight: "normal",
        borderStyle: "solid",
        borderColor: "transparent",
        borderSize: 0,
        borderDashedValue: [2, 2],
        paddingLeft: 4,
        paddingTop: 4,
        paddingRight: 4,
        paddingBottom: 4,
        borderRadius: 2
      }
    },
    tooltip: {
      offsetLeft: 4,
      offsetTop: 6,
      offsetRight: 4,
      offsetBottom: 6,
      showRule: "always",
      showType: "standard",
      title: {
        show: !0,
        showName: !0,
        showParams: !0,
        size: 12,
        family: "Helvetica Neue",
        weight: "normal",
        color: N.GREY,
        marginLeft: 8,
        marginTop: 4,
        marginRight: 8,
        marginBottom: 4
      },
      legend: {
        size: 12,
        family: "Helvetica Neue",
        weight: "normal",
        color: N.GREY,
        marginLeft: 8,
        marginTop: 4,
        marginRight: 8,
        marginBottom: 4,
        defaultValue: "n/a"
      },
      features: []
    }
  };
}
function Or() {
  return {
    show: !0,
    size: "auto",
    axisLine: {
      show: !0,
      color: "#DDDDDD",
      size: 1
    },
    tickText: {
      show: !0,
      color: N.GREY,
      size: 12,
      family: "Helvetica Neue",
      weight: "normal",
      marginStart: 4,
      marginEnd: 6
    },
    tickLine: {
      show: !0,
      size: 1,
      length: 3,
      color: "#DDDDDD"
    }
  };
}
function Sa() {
  return {
    show: !0,
    horizontal: {
      show: !0,
      line: {
        show: !0,
        style: "dashed",
        dashedValue: [4, 2],
        size: 1,
        color: N.GREY
      },
      text: {
        show: !0,
        style: "fill",
        color: N.WHITE,
        size: 12,
        family: "Helvetica Neue",
        weight: "normal",
        borderStyle: "solid",
        borderDashedValue: [2, 2],
        borderSize: 1,
        borderColor: N.GREY,
        borderRadius: 2,
        paddingLeft: 4,
        paddingRight: 4,
        paddingTop: 4,
        paddingBottom: 4,
        backgroundColor: N.GREY
      },
      features: []
    },
    vertical: {
      show: !0,
      line: {
        show: !0,
        style: "dashed",
        dashedValue: [4, 2],
        size: 1,
        color: N.GREY
      },
      text: {
        show: !0,
        style: "fill",
        color: N.WHITE,
        size: 12,
        family: "Helvetica Neue",
        weight: "normal",
        borderStyle: "solid",
        borderDashedValue: [2, 2],
        borderSize: 1,
        borderColor: N.GREY,
        borderRadius: 2,
        paddingLeft: 4,
        paddingRight: 4,
        paddingTop: 4,
        paddingBottom: 4,
        backgroundColor: N.GREY
      }
    }
  };
}
function Ta() {
  var r = ee(N.BLUE, 0.35), e = ee(N.BLUE, 0.25);
  function t() {
    return {
      style: "fill",
      color: N.WHITE,
      size: 12,
      family: "Helvetica Neue",
      weight: "normal",
      borderStyle: "solid",
      borderDashedValue: [2, 2],
      borderSize: 1,
      borderRadius: 2,
      borderColor: N.BLUE,
      paddingLeft: 4,
      paddingRight: 4,
      paddingTop: 4,
      paddingBottom: 4,
      backgroundColor: N.BLUE
    };
  }
  return {
    point: {
      color: N.BLUE,
      borderColor: r,
      borderSize: 1,
      radius: 5,
      activeColor: N.BLUE,
      activeBorderColor: r,
      activeBorderSize: 3,
      activeRadius: 5
    },
    line: {
      style: "solid",
      smooth: !1,
      color: N.BLUE,
      size: 1,
      dashedValue: [2, 2]
    },
    rect: {
      style: "fill",
      color: e,
      borderColor: N.BLUE,
      borderSize: 1,
      borderRadius: 0,
      borderStyle: "solid",
      borderDashedValue: [2, 2]
    },
    polygon: {
      style: "fill",
      color: N.BLUE,
      borderColor: N.BLUE,
      borderSize: 1,
      borderStyle: "solid",
      borderDashedValue: [2, 2]
    },
    circle: {
      style: "fill",
      color: e,
      borderColor: N.BLUE,
      borderSize: 1,
      borderStyle: "solid",
      borderDashedValue: [2, 2]
    },
    arc: {
      style: "solid",
      color: N.BLUE,
      size: 1,
      dashedValue: [2, 2]
    },
    text: t()
  };
}
function Aa() {
  return {
    size: 1,
    color: "#DDDDDD",
    fill: !0,
    activeBackgroundColor: ee(N.BLUE, 0.08)
  };
}
function Ma() {
  return {
    grid: Ca(),
    candle: Ea(),
    indicator: Ia(),
    xAxis: Or(),
    yAxis: Or(),
    separator: Aa(),
    crosshair: Sa(),
    overlay: Ta()
  };
}
function gr(r, e, t, i, a) {
  var n = r.result, o = r.figures, s = r.styles, l = ht(s, "texts", i.texts), u = l.length, c = ht(s, "circles", i.circles), d = c.length, h = ht(s, "bars", i.bars), f = h.length, v = ht(s, "lines", i.lines), p = v.length, g = 0, m = 0, x = 0, y = 0, E, _ = 0;
  o.forEach(function(I) {
    var w;
    switch (I.type) {
      case "text": {
        _ = g, E = l[g % u], g++;
        break;
      }
      case "circle": {
        _ = m;
        var b = c[m % d];
        E = M(M({}, b), { color: b.noChangeColor }), m++;
        break;
      }
      case "bar": {
        _ = x;
        var S = h[x % f];
        E = M(M({}, S), { color: S.noChangeColor }), x++;
        break;
      }
      case "line": {
        _ = y, E = v[y % p], y++;
        break;
      }
    }
    if (C(I.type)) {
      var T = (w = I.styles) === null || w === void 0 ? void 0 : w.call(I, {
        data: {
          prev: n[e - 1],
          current: n[e],
          next: n[e + 1]
        },
        indicator: r,
        barSpace: t,
        defaultStyles: i
      });
      a(I, M(M({}, E), T), _);
    }
  });
}
var di = (
  /** @class */
  (function() {
    function r(e) {
      this.precision = 4, this.calcParams = [], this.shouldOhlc = !1, this.shouldFormatBigNumber = !1, this.visible = !0, this.zLevel = 0, this.series = "normal", this.figures = [], this.minValue = null, this.maxValue = null, this.styles = null, this.shouldUpdate = function(t, i) {
        var a = JSON.stringify(t.calcParams) !== JSON.stringify(i.calcParams) || t.figures !== i.figures || t.calc !== i.calc, n = a || t.shortName !== i.shortName || t.paneId !== i.paneId || t.yAxisId !== i.yAxisId || t.series !== i.series || t.minValue !== i.minValue || t.maxValue !== i.maxValue || t.precision !== i.precision || t.shouldOhlc !== i.shouldOhlc || t.shouldFormatBigNumber !== i.shouldFormatBigNumber || t.visible !== i.visible || t.zLevel !== i.zLevel || t.extendData !== i.extendData || t.regenerateFigures !== i.regenerateFigures || t.createTooltipDataSource !== i.createTooltipDataSource || t.draw !== i.draw;
        return { calc: a, draw: n };
      }, this.calc = function() {
        return [];
      }, this.regenerateFigures = null, this.createTooltipDataSource = null, this.draw = null, this.result = [], this._lockSeriesPrecision = !1, this.override(e), this._lockSeriesPrecision = !1;
    }
    return r.prototype.override = function(e) {
      var t, i, a = this, n = a.result;
      a._prevIndicator;
      var o = Oe(a, ["result", "_prevIndicator"]);
      this._prevIndicator = M(M({}, Be(o)), { result: n });
      var s = e.id, l = e.name, u = e.shortName, c = e.precision, d = e.styles, h = e.figures, f = e.calcParams, v = Oe(e, ["id", "name", "shortName", "precision", "styles", "figures", "calcParams"]);
      !K(this.id) && K(s) && (this.id = s), K(this.name) || (this.name = l ?? ""), this.shortName = (t = u ?? this.shortName) !== null && t !== void 0 ? t : this.name, B(c) && (this.precision = c, this._lockSeriesPrecision = !0), C(d) && ((i = this.styles) !== null && i !== void 0 || (this.styles = {}), dt(this.styles, d)), dt(this, v), C(f) && (this.calcParams = f, ct(this.regenerateFigures) && (this.figures = this.regenerateFigures(this.calcParams))), this.figures = h ?? this.figures;
    }, r.prototype.setSeriesPrecision = function(e) {
      this._lockSeriesPrecision || (this.precision = e);
    }, r.prototype.shouldUpdateImp = function() {
      var e = this._prevIndicator.zLevel !== this.zLevel, t = this.shouldUpdate(this._prevIndicator, this);
      return we(t) ? { calc: t, draw: t, sort: e } : M(M({}, t), { sort: e });
    }, r.prototype.calcImp = function(e) {
      return vr(this, void 0, void 0, function() {
        var t;
        return fr(this, function(i) {
          switch (i.label) {
            case 0:
              return i.trys.push([0, 2, , 3]), [4, this.calc(e, this)];
            case 1:
              return t = i.sent(), this.result = t, [2, !0];
            case 2:
              return i.sent(), [2, !1];
            case 3:
              return [
                2
                /*return*/
              ];
          }
        });
      });
    }, r.extend = function(e) {
      var t = (
        /** @class */
        (function(i) {
          q(a, i);
          function a() {
            return i.call(this, e) || this;
          }
          return a;
        })(r)
      );
      return t;
    }, r;
  })()
), Pa = {
  name: "AVP",
  shortName: "AVP",
  series: "price",
  precision: 2,
  figures: [
    { key: "avp", title: "AVP: ", type: "line" }
  ],
  calc: function(r) {
    var e = 0, t = 0;
    return r.map(function(i) {
      var a, n, o = {}, s = (a = i.turnover) !== null && a !== void 0 ? a : 0, l = (n = i.volume) !== null && n !== void 0 ? n : 0;
      return e += s, t += l, t !== 0 && (o.avp = e / t), o;
    });
  }
}, Da = {
  name: "AO",
  shortName: "AO",
  calcParams: [5, 34],
  figures: [{
    key: "ao",
    title: "AO: ",
    type: "bar",
    baseValue: 0,
    styles: function(r) {
      var e, t, i = r.data, a = r.indicator, n = r.defaultStyles, o = i.prev, s = i.current, l = (e = o?.ao) !== null && e !== void 0 ? e : Number.MIN_SAFE_INTEGER, u = (t = s?.ao) !== null && t !== void 0 ? t : Number.MIN_SAFE_INTEGER, c = "";
      u > l ? c = ht(a.styles, "bars[0].upColor", n.bars[0].upColor) : c = ht(a.styles, "bars[0].downColor", n.bars[0].downColor);
      var d = u > l ? "stroke" : "fill";
      return { color: c, style: d, borderColor: c };
    }
  }],
  calc: function(r, e) {
    var t = e.calcParams, i = Math.max(t[0], t[1]), a = 0, n = 0, o = 0, s = 0;
    return r.map(function(l, u) {
      var c = {}, d = (l.low + l.high) / 2;
      if (a += d, n += d, u >= t[0] - 1) {
        o = a / t[0];
        var h = r[u - (t[0] - 1)];
        a -= (h.low + h.high) / 2;
      }
      if (u >= t[1] - 1) {
        s = n / t[1];
        var h = r[u - (t[1] - 1)];
        n -= (h.low + h.high) / 2;
      }
      return u >= i - 1 && (c.ao = o - s), c;
    });
  }
}, ka = {
  name: "BIAS",
  shortName: "BIAS",
  calcParams: [6, 12, 24],
  figures: [
    { key: "bias1", title: "BIAS6: ", type: "line" },
    { key: "bias2", title: "BIAS12: ", type: "line" },
    { key: "bias3", title: "BIAS24: ", type: "line" }
  ],
  regenerateFigures: function(r) {
    return r.map(function(e, t) {
      return { key: "bias".concat(t + 1), title: "BIAS".concat(e, ": "), type: "line" };
    });
  },
  calc: function(r, e) {
    var t = e.calcParams, i = e.figures, a = [];
    return r.map(function(n, o) {
      var s = {}, l = n.close;
      return t.forEach(function(u, c) {
        var d;
        if (a[c] = ((d = a[c]) !== null && d !== void 0 ? d : 0) + l, o >= u - 1) {
          var h = a[c] / t[c];
          s[i[c].key] = (l - h) / h * 100, a[c] -= r[o - (u - 1)].close;
        }
      }), s;
    });
  }
};
function Ra(r, e) {
  var t = r.length, i = 0;
  return r.forEach(function(a) {
    var n = a.close - e;
    i += n * n;
  }), i = Math.abs(i), Math.sqrt(i / t);
}
var Fa = {
  name: "BOLL",
  shortName: "BOLL",
  series: "price",
  calcParams: [20, 2],
  precision: 2,
  shouldOhlc: !0,
  figures: [
    { key: "up", title: "UP: ", type: "line" },
    { key: "mid", title: "MID: ", type: "line" },
    { key: "dn", title: "DN: ", type: "line" }
  ],
  calc: function(r, e) {
    var t = e.calcParams, i = t[0] - 1, a = 0;
    return r.map(function(n, o) {
      var s = n.close, l = {};
      if (a += s, o >= i) {
        l.mid = a / t[0];
        var u = Ra(r.slice(o - i, o + 1), l.mid);
        l.up = l.mid + t[1] * u, l.dn = l.mid - t[1] * u, a -= r[o - i].close;
      }
      return l;
    });
  }
}, Ba = {
  name: "BRAR",
  shortName: "BRAR",
  calcParams: [26],
  figures: [
    { key: "br", title: "BR: ", type: "line" },
    { key: "ar", title: "AR: ", type: "line" }
  ],
  calc: function(r, e) {
    var t = e.calcParams, i = 0, a = 0, n = 0, o = 0;
    return r.map(function(s, l) {
      var u, c, d = {}, h = s.high, f = s.low, v = s.open, p = ((u = r[l - 1]) !== null && u !== void 0 ? u : s).close;
      if (n += h - v, o += v - f, i += h - p, a += p - f, l >= t[0] - 1) {
        o !== 0 ? d.ar = n / o * 100 : d.ar = 0, a !== 0 ? d.br = i / a * 100 : d.br = 0;
        var g = r[l - (t[0] - 1)], m = g.high, x = g.low, y = g.open, E = ((c = r[l - t[0]]) !== null && c !== void 0 ? c : r[l - (t[0] - 1)]).close;
        i -= m - E, a -= E - x, n -= m - y, o -= y - x;
      }
      return d;
    });
  }
}, Oa = {
  name: "BBI",
  shortName: "BBI",
  series: "price",
  precision: 2,
  calcParams: [3, 6, 12, 24],
  shouldOhlc: !0,
  figures: [
    { key: "bbi", title: "BBI: ", type: "line" }
  ],
  calc: function(r, e) {
    var t = e.calcParams, i = Math.max.apply(Math, be([], Se(t), !1)), a = [], n = [];
    return r.map(function(o, s) {
      var l = {}, u = o.close;
      if (t.forEach(function(d, h) {
        var f;
        a[h] = ((f = a[h]) !== null && f !== void 0 ? f : 0) + u, s >= d - 1 && (n[h] = a[h] / d, a[h] -= r[s - (d - 1)].close);
      }), s >= i - 1) {
        var c = 0;
        n.forEach(function(d) {
          c += d;
        }), l.bbi = c / 4;
      }
      return l;
    });
  }
}, La = {
  name: "CCI",
  shortName: "CCI",
  calcParams: [20],
  figures: [
    { key: "cci", title: "CCI: ", type: "line" }
  ],
  calc: function(r, e) {
    var t = e.calcParams, i = t[0] - 1, a = 0, n = [];
    return r.map(function(o, s) {
      var l = {}, u = (o.high + o.low + o.close) / 3;
      if (a += u, n.push(u), s >= i) {
        var c = a / t[0], d = n.slice(s - i, s + 1), h = 0;
        d.forEach(function(p) {
          h += Math.abs(p - c);
        });
        var f = h / t[0];
        l.cci = f !== 0 ? (u - c) / f / 0.015 : 0;
        var v = (r[s - i].high + r[s - i].low + r[s - i].close) / 3;
        a -= v;
      }
      return l;
    });
  }
}, Va = {
  name: "CR",
  shortName: "CR",
  calcParams: [26, 10, 20, 40, 60],
  figures: [
    { key: "cr", title: "CR: ", type: "line" },
    { key: "ma1", title: "MA1: ", type: "line" },
    { key: "ma2", title: "MA2: ", type: "line" },
    { key: "ma3", title: "MA3: ", type: "line" },
    { key: "ma4", title: "MA4: ", type: "line" }
  ],
  calc: function(r, e) {
    var t = e.calcParams, i = Math.ceil(t[1] / 2.5 + 1), a = Math.ceil(t[2] / 2.5 + 1), n = Math.ceil(t[3] / 2.5 + 1), o = Math.ceil(t[4] / 2.5 + 1), s = 0, l = [], u = 0, c = [], d = 0, h = [], f = 0, v = [], p = [];
    return r.forEach(function(g, m) {
      var x, y, E, _, I, w = {}, b = (x = r[m - 1]) !== null && x !== void 0 ? x : g, S = (b.high + b.close + b.low + b.open) / 4, T = Math.max(0, g.high - S), A = Math.max(0, S - g.low);
      m >= t[0] - 1 && (A !== 0 ? w.cr = T / A * 100 : w.cr = 0, s += w.cr, u += w.cr, d += w.cr, f += w.cr, m >= t[0] + t[1] - 2 && (l.push(s / t[1]), m >= t[0] + t[1] + i - 3 && (w.ma1 = l[l.length - 1 - i]), s -= (y = p[m - (t[1] - 1)].cr) !== null && y !== void 0 ? y : 0), m >= t[0] + t[2] - 2 && (c.push(u / t[2]), m >= t[0] + t[2] + a - 3 && (w.ma2 = c[c.length - 1 - a]), u -= (E = p[m - (t[2] - 1)].cr) !== null && E !== void 0 ? E : 0), m >= t[0] + t[3] - 2 && (h.push(d / t[3]), m >= t[0] + t[3] + n - 3 && (w.ma3 = h[h.length - 1 - n]), d -= (_ = p[m - (t[3] - 1)].cr) !== null && _ !== void 0 ? _ : 0), m >= t[0] + t[4] - 2 && (v.push(f / t[4]), m >= t[0] + t[4] + o - 3 && (w.ma4 = v[v.length - 1 - o]), f -= (I = p[m - (t[4] - 1)].cr) !== null && I !== void 0 ? I : 0)), p.push(w);
    }), p;
  }
}, Na = {
  name: "DMA",
  shortName: "DMA",
  calcParams: [10, 50, 10],
  figures: [
    { key: "dma", title: "DMA: ", type: "line" },
    { key: "ama", title: "AMA: ", type: "line" }
  ],
  calc: function(r, e) {
    var t = e.calcParams, i = Math.max(t[0], t[1]), a = 0, n = 0, o = 0, s = [];
    return r.forEach(function(l, u) {
      var c, d = {}, h = l.close;
      a += h, n += h;
      var f = 0, v = 0;
      if (u >= t[0] - 1 && (f = a / t[0], a -= r[u - (t[0] - 1)].close), u >= t[1] - 1 && (v = n / t[1], n -= r[u - (t[1] - 1)].close), u >= i - 1) {
        var p = f - v;
        d.dma = p, o += p, u >= i + t[2] - 2 && (d.ama = o / t[2], o -= (c = s[u - (t[2] - 1)].dma) !== null && c !== void 0 ? c : 0);
      }
      s.push(d);
    }), s;
  }
}, Ya = {
  name: "DMI",
  shortName: "DMI",
  calcParams: [14, 6],
  figures: [
    { key: "pdi", title: "PDI: ", type: "line" },
    { key: "mdi", title: "MDI: ", type: "line" },
    { key: "adx", title: "ADX: ", type: "line" },
    { key: "adxr", title: "ADXR: ", type: "line" }
  ],
  calc: function(r, e) {
    var t = e.calcParams, i = 0, a = 0, n = 0, o = 0, s = 0, l = 0, u = 0, c = 0, d = [];
    return r.forEach(function(h, f) {
      var v, p, g = {}, m = (v = r[f - 1]) !== null && v !== void 0 ? v : h, x = m.close, y = h.high, E = h.low, _ = y - E, I = Math.abs(y - x), w = Math.abs(x - E), b = y - m.high, S = m.low - E, T = Math.max(Math.max(_, I), w), A = b > 0 && b > S ? b : 0, D = S > 0 && S > b ? S : 0;
      if (i += T, a += A, n += D, f >= t[0] - 1) {
        f > t[0] - 1 ? (o = o - o / t[0] + T, s = s - s / t[0] + A, l = l - l / t[0] + D) : (o = i, s = a, l = n);
        var R = 0, P = 0;
        o !== 0 && (R = s * 100 / o, P = l * 100 / o), g.pdi = R, g.mdi = P;
        var k = 0;
        P + R !== 0 && (k = Math.abs(P - R) / (P + R) * 100), u += k, f >= t[0] * 2 - 2 && (f > t[0] * 2 - 2 ? c = (c * (t[0] - 1) + k) / t[0] : c = u / t[0], g.adx = c, f >= t[0] * 2 + t[1] - 3 && (g.adxr = (((p = d[f - (t[1] - 1)].adx) !== null && p !== void 0 ? p : 0) + c) / 2));
      }
      d.push(g);
    }), d;
  }
}, Wa = {
  name: "EMV",
  shortName: "EMV",
  calcParams: [14, 9],
  figures: [
    { key: "emv", title: "EMV: ", type: "line" },
    { key: "maEmv", title: "MAEMV: ", type: "line" }
  ],
  calc: function(r, e) {
    var t = e.calcParams, i = 0, a = [];
    return r.map(function(n, o) {
      var s, l = {};
      if (o > 0) {
        var u = r[o - 1], c = n.high, d = n.low, h = (s = n.volume) !== null && s !== void 0 ? s : 0, f = (c + d) / 2 - (u.high + u.low) / 2;
        if (h === 0 || c - d === 0)
          l.emv = 0;
        else {
          var v = h / 1e8 / (c - d);
          l.emv = f / v;
        }
        i += l.emv, a.push(l.emv), o >= t[0] && (l.maEmv = i / t[0], i -= a[o - t[0]]);
      }
      return l;
    });
  }
}, za = {
  name: "EMA",
  shortName: "EMA",
  series: "price",
  calcParams: [6, 12, 20],
  precision: 2,
  shouldOhlc: !0,
  figures: [
    { key: "ema1", title: "EMA6: ", type: "line" },
    { key: "ema2", title: "EMA12: ", type: "line" },
    { key: "ema3", title: "EMA20: ", type: "line" }
  ],
  regenerateFigures: function(r) {
    return r.map(function(e, t) {
      return { key: "ema".concat(t + 1), title: "EMA".concat(e, ": "), type: "line" };
    });
  },
  calc: function(r, e) {
    var t = e.calcParams, i = e.figures, a = 0, n = [];
    return r.map(function(o, s) {
      var l = {}, u = o.close;
      return a += u, t.forEach(function(c, d) {
        s >= c - 1 && (s > c - 1 ? n[d] = (2 * u + (c - 1) * n[d]) / (c + 1) : n[d] = a / c, l[i[d].key] = n[d]);
      }), l;
    });
  }
}, $a = {
  name: "MTM",
  shortName: "MTM",
  calcParams: [12, 6],
  figures: [
    { key: "mtm", title: "MTM: ", type: "line" },
    { key: "maMtm", title: "MAMTM: ", type: "line" }
  ],
  calc: function(r, e) {
    var t = e.calcParams, i = 0, a = [];
    return r.forEach(function(n, o) {
      var s, l = {};
      if (o >= t[0]) {
        var u = n.close, c = r[o - t[0]].close;
        l.mtm = u - c, i += l.mtm, o >= t[0] + t[1] - 1 && (l.maMtm = i / t[1], i -= (s = a[o - (t[1] - 1)].mtm) !== null && s !== void 0 ? s : 0);
      }
      a.push(l);
    }), a;
  }
}, Xa = {
  name: "MA",
  shortName: "MA",
  series: "price",
  calcParams: [5, 10, 30, 60],
  precision: 2,
  shouldOhlc: !0,
  figures: [
    { key: "ma1", title: "MA5: ", type: "line" },
    { key: "ma2", title: "MA10: ", type: "line" },
    { key: "ma3", title: "MA30: ", type: "line" },
    { key: "ma4", title: "MA60: ", type: "line" }
  ],
  regenerateFigures: function(r) {
    return r.map(function(e, t) {
      return { key: "ma".concat(t + 1), title: "MA".concat(e, ": "), type: "line" };
    });
  },
  calc: function(r, e) {
    var t = e.calcParams, i = e.figures, a = [];
    return r.map(function(n, o) {
      var s = {}, l = n.close;
      return t.forEach(function(u, c) {
        var d;
        a[c] = ((d = a[c]) !== null && d !== void 0 ? d : 0) + l, o >= u - 1 && (s[i[c].key] = a[c] / u, a[c] -= r[o - (u - 1)].close);
      }), s;
    });
  }
}, qa = {
  name: "MACD",
  shortName: "MACD",
  calcParams: [12, 26, 9],
  figures: [
    { key: "dif", title: "DIF: ", type: "line" },
    { key: "dea", title: "DEA: ", type: "line" },
    {
      key: "macd",
      title: "MACD: ",
      type: "bar",
      baseValue: 0,
      styles: function(r) {
        var e, t, i = r.data, a = r.indicator, n = r.defaultStyles, o = i.prev, s = i.current, l = (e = o?.macd) !== null && e !== void 0 ? e : Number.MIN_SAFE_INTEGER, u = (t = s?.macd) !== null && t !== void 0 ? t : Number.MIN_SAFE_INTEGER, c = "";
        u > 0 ? c = ht(a.styles, "bars[0].upColor", n.bars[0].upColor) : u < 0 ? c = ht(a.styles, "bars[0].downColor", n.bars[0].downColor) : c = ht(a.styles, "bars[0].noChangeColor", n.bars[0].noChangeColor);
        var d = l < u ? "stroke" : "fill";
        return { style: d, color: c, borderColor: c };
      }
    }
  ],
  calc: function(r, e) {
    var t = e.calcParams, i = 0, a = 0, n = 0, o = 0, s = 0, l = 0, u = Math.max(t[0], t[1]);
    return r.map(function(c, d) {
      var h = {}, f = c.close;
      return i += f, d >= t[0] - 1 && (d > t[0] - 1 ? a = (2 * f + (t[0] - 1) * a) / (t[0] + 1) : a = i / t[0]), d >= t[1] - 1 && (d > t[1] - 1 ? n = (2 * f + (t[1] - 1) * n) / (t[1] + 1) : n = i / t[1]), d >= u - 1 && (o = a - n, h.dif = o, s += o, d >= u + t[2] - 2 && (d > u + t[2] - 2 ? l = (o * 2 + l * (t[2] - 1)) / (t[2] + 1) : l = s / t[2], h.macd = (o - l) * 2, h.dea = l)), h;
    });
  }
}, Ha = {
  name: "OBV",
  shortName: "OBV",
  calcParams: [30],
  figures: [
    { key: "obv", title: "OBV: ", type: "line" },
    { key: "maObv", title: "MAOBV: ", type: "line" }
  ],
  calc: function(r, e) {
    var t = e.calcParams, i = 0, a = 0, n = [];
    return r.forEach(function(o, s) {
      var l, u, c, d, h = (l = r[s - 1]) !== null && l !== void 0 ? l : o;
      o.close < h.close ? a -= (u = o.volume) !== null && u !== void 0 ? u : 0 : o.close > h.close && (a += (c = o.volume) !== null && c !== void 0 ? c : 0);
      var f = { obv: a };
      i += a, s >= t[0] - 1 && (f.maObv = i / t[0], i -= (d = n[s - (t[0] - 1)].obv) !== null && d !== void 0 ? d : 0), n.push(f);
    }), n;
  }
}, Ua = {
  name: "PVT",
  shortName: "PVT",
  figures: [
    { key: "pvt", title: "PVT: ", type: "line" }
  ],
  calc: function(r) {
    var e = 0;
    return r.map(function(t, i) {
      var a, n, o = {}, s = t.close, l = (a = t.volume) !== null && a !== void 0 ? a : 1, u = ((n = r[i - 1]) !== null && n !== void 0 ? n : t).close, c = 0, d = u * l;
      return d !== 0 && (c = (s - u) / d), e += c, o.pvt = e, o;
    });
  }
}, Ga = {
  name: "PSY",
  shortName: "PSY",
  calcParams: [12, 6],
  figures: [
    { key: "psy", title: "PSY: ", type: "line" },
    { key: "maPsy", title: "MAPSY: ", type: "line" }
  ],
  calc: function(r, e) {
    var t = e.calcParams, i = 0, a = 0, n = [], o = [];
    return r.forEach(function(s, l) {
      var u, c, d = {}, h = ((u = r[l - 1]) !== null && u !== void 0 ? u : s).close, f = s.close - h > 0 ? 1 : 0;
      n.push(f), i += f, l >= t[0] - 1 && (d.psy = i / t[0] * 100, a += d.psy, l >= t[0] + t[1] - 2 && (d.maPsy = a / t[1], a -= (c = o[l - (t[1] - 1)].psy) !== null && c !== void 0 ? c : 0), i -= n[l - (t[0] - 1)]), o.push(d);
    }), o;
  }
}, ja = {
  name: "ROC",
  shortName: "ROC",
  calcParams: [12, 6],
  figures: [
    { key: "roc", title: "ROC: ", type: "line" },
    { key: "maRoc", title: "MAROC: ", type: "line" }
  ],
  calc: function(r, e) {
    var t = e.calcParams, i = [], a = 0;
    return r.forEach(function(n, o) {
      var s, l, u = {};
      if (o >= t[0] - 1) {
        var c = n.close, d = ((s = r[o - t[0]]) !== null && s !== void 0 ? s : r[o - (t[0] - 1)]).close;
        d !== 0 ? u.roc = (c - d) / d * 100 : u.roc = 0, a += u.roc, o >= t[0] - 1 + t[1] - 1 && (u.maRoc = a / t[1], a -= (l = i[o - (t[1] - 1)].roc) !== null && l !== void 0 ? l : 0);
      }
      i.push(u);
    }), i;
  }
}, Za = {
  name: "RSI",
  shortName: "RSI",
  calcParams: [6, 12, 24],
  figures: [
    { key: "rsi1", title: "RSI1: ", type: "line" },
    { key: "rsi2", title: "RSI2: ", type: "line" },
    { key: "rsi3", title: "RSI3: ", type: "line" }
  ],
  regenerateFigures: function(r) {
    return r.map(function(e, t) {
      var i = t + 1;
      return { key: "rsi".concat(i), title: "RSI".concat(i, ": "), type: "line" };
    });
  },
  calc: function(r, e) {
    var t = e.calcParams, i = e.figures, a = [], n = [], o = [], s = [];
    return r.map(function(l, u) {
      var c = {}, d = u === 0 ? 0 : l.close - r[u - 1].close, h = Math.max(d, 0), f = Math.max(-d, 0);
      return t.forEach(function(v, p) {
        var g, m;
        a[p] = ((g = a[p]) !== null && g !== void 0 ? g : 0) + h, n[p] = ((m = n[p]) !== null && m !== void 0 ? m : 0) + f, !(u < v) && (o[p] === void 0 || s[p] === void 0 ? (o[p] = a[p] / v, s[p] = n[p] / v) : (o[p] = (o[p] * (v - 1) + h) / v, s[p] = (s[p] * (v - 1) + f) / v), s[p] === 0 ? c[i[p].key] = 100 : o[p] === 0 ? c[i[p].key] = 0 : c[i[p].key] = 100 - 100 / (1 + o[p] / s[p]));
      }), c;
    });
  }
}, Ka = {
  name: "SMA",
  shortName: "SMA",
  series: "price",
  calcParams: [12, 2],
  precision: 2,
  figures: [
    { key: "sma", title: "SMA: ", type: "line" }
  ],
  shouldOhlc: !0,
  calc: function(r, e) {
    var t = e.calcParams, i = 0, a = 0;
    return r.map(function(n, o) {
      var s = {}, l = n.close;
      return i += l, o >= t[0] - 1 && (o > t[0] - 1 ? a = (l * t[1] + a * (t[0] - t[1] + 1)) / (t[0] + 1) : a = i / t[0], s.sma = a), s;
    });
  }
}, Ja = {
  name: "KDJ",
  shortName: "KDJ",
  calcParams: [9, 3, 3],
  figures: [
    { key: "k", title: "K: ", type: "line" },
    { key: "d", title: "D: ", type: "line" },
    { key: "j", title: "J: ", type: "line" }
  ],
  calc: function(r, e) {
    var t = e.calcParams, i = [];
    return r.forEach(function(a, n) {
      var o, s, l, u, c = {}, d = a.close;
      if (n >= t[0] - 1) {
        var h = ci(r.slice(n - (t[0] - 1), n + 1), "high", "low"), f = h[0], v = h[1], p = f - v, g = (d - v) / (p === 0 ? 1 : p) * 100;
        c.k = ((t[1] - 1) * ((s = (o = i[n - 1]) === null || o === void 0 ? void 0 : o.k) !== null && s !== void 0 ? s : 50) + g) / t[1], c.d = ((t[2] - 1) * ((u = (l = i[n - 1]) === null || l === void 0 ? void 0 : l.d) !== null && u !== void 0 ? u : 50) + c.k) / t[2], c.j = 3 * c.k - 2 * c.d;
      }
      i.push(c);
    }), i;
  }
}, Qa = {
  name: "SAR",
  shortName: "SAR",
  series: "price",
  calcParams: [2, 2, 20],
  precision: 2,
  shouldOhlc: !0,
  figures: [
    {
      key: "sar",
      title: "SAR: ",
      type: "circle",
      styles: function(r) {
        var e, t, i, a = r.data, n = r.indicator, o = r.defaultStyles, s = a.current, l = (e = s?.sar) !== null && e !== void 0 ? e : Number.MIN_SAFE_INTEGER, u = (((t = s?.high) !== null && t !== void 0 ? t : 0) + ((i = s?.low) !== null && i !== void 0 ? i : 0)) / 2, c = l < u ? ht(n.styles, "circles[0].upColor", o.circles[0].upColor) : ht(n.styles, "circles[0].downColor", o.circles[0].downColor);
        return { color: c };
      }
    }
  ],
  calc: function(r, e) {
    var t = e.calcParams, i = t[0] / 100, a = t[1] / 100, n = t[2] / 100, o = i, s = -100, l = !1, u = 0;
    return r.map(function(c, d) {
      var h = u, f = c.high, v = c.low;
      if (l) {
        (s === -100 || s < f) && (s = f, o = Math.min(o + a, n)), u = h + o * (s - h);
        var p = Math.min(r[Math.max(1, d) - 1].low, v);
        u > c.low ? (u = s, o = i, s = -100, l = !l) : u > p && (u = p);
      } else {
        (s === -100 || s > v) && (s = v, o = Math.min(o + a, n)), u = h + o * (s - h);
        var g = Math.max(r[Math.max(1, d) - 1].high, f);
        u < c.high ? (u = s, o = 0, s = -100, l = !l) : u < g && (u = g);
      }
      return { high: f, low: v, sar: u };
    });
  }
}, tn = {
  name: "TRIX",
  shortName: "TRIX",
  calcParams: [12, 9],
  figures: [
    { key: "trix", title: "TRIX: ", type: "line" },
    { key: "maTrix", title: "MATRIX: ", type: "line" }
  ],
  calc: function(r, e) {
    var t = e.calcParams, i = 0, a = 0, n = 0, o = 0, s = 0, l = 0, u = 0, c = [];
    return r.forEach(function(d, h) {
      var f, v = {}, p = d.close;
      if (i += p, h >= t[0] - 1 && (h > t[0] - 1 ? a = (2 * p + (t[0] - 1) * a) / (t[0] + 1) : a = i / t[0], s += a, h >= t[0] * 2 - 2 && (h > t[0] * 2 - 2 ? n = (2 * a + (t[0] - 1) * n) / (t[0] + 1) : n = s / t[0], l += n, h >= t[0] * 3 - 3))) {
        var g = 0, m = 0;
        h > t[0] * 3 - 3 ? (g = (2 * n + (t[0] - 1) * o) / (t[0] + 1), m = (g - o) / o * 100) : g = l / t[0], o = g, v.trix = m, u += m, h >= t[0] * 3 + t[1] - 4 && (v.maTrix = u / t[1], u -= (f = c[h - (t[1] - 1)].trix) !== null && f !== void 0 ? f : 0);
      }
      c.push(v);
    }), c;
  }
};
function Lr() {
  return {
    key: "volume",
    title: "VOLUME: ",
    type: "bar",
    baseValue: 0,
    styles: function(r) {
      var e = r.data, t = r.indicator, i = r.defaultStyles, a = e.current, n = ht(t.styles, "bars[0].noChangeColor", i.bars[0].noChangeColor);
      return C(a) && (a.close > a.open ? n = ht(t.styles, "bars[0].upColor", i.bars[0].upColor) : a.close < a.open && (n = ht(t.styles, "bars[0].downColor", i.bars[0].downColor))), { color: n };
    }
  };
}
var en = {
  name: "VOL",
  shortName: "VOL",
  series: "volume",
  calcParams: [5, 10, 20],
  shouldFormatBigNumber: !0,
  precision: 0,
  minValue: 0,
  figures: [
    { key: "ma1", title: "MA5: ", type: "line" },
    { key: "ma2", title: "MA10: ", type: "line" },
    { key: "ma3", title: "MA20: ", type: "line" },
    Lr()
  ],
  regenerateFigures: function(r) {
    var e = r.map(function(t, i) {
      return { key: "ma".concat(i + 1), title: "MA".concat(t, ": "), type: "line" };
    });
    return e.push(Lr()), e;
  },
  calc: function(r, e) {
    var t = e.calcParams, i = e.figures, a = [];
    return r.map(function(n, o) {
      var s, l = (s = n.volume) !== null && s !== void 0 ? s : 0, u = { volume: l, open: n.open, close: n.close };
      return t.forEach(function(c, d) {
        var h, f;
        a[d] = ((h = a[d]) !== null && h !== void 0 ? h : 0) + l, o >= c - 1 && (u[i[d].key] = a[d] / c, a[d] -= (f = r[o - (c - 1)].volume) !== null && f !== void 0 ? f : 0);
      }), u;
    });
  }
}, rn = {
  name: "VR",
  shortName: "VR",
  calcParams: [26, 6],
  figures: [
    { key: "vr", title: "VR: ", type: "line" },
    { key: "maVr", title: "MAVR: ", type: "line" }
  ],
  calc: function(r, e) {
    var t = e.calcParams, i = 0, a = 0, n = 0, o = 0, s = [];
    return r.forEach(function(l, u) {
      var c, d, h, f, v, p = {}, g = l.close, m = ((c = r[u - 1]) !== null && c !== void 0 ? c : l).close, x = (d = l.volume) !== null && d !== void 0 ? d : 0;
      if (g > m ? i += x : g < m ? a += x : n += x, u >= t[0] - 1) {
        var y = n / 2;
        a + y === 0 ? p.vr = 0 : p.vr = (i + y) / (a + y) * 100, o += p.vr, u >= t[0] + t[1] - 2 && (p.maVr = o / t[1], o -= (h = s[u - (t[1] - 1)].vr) !== null && h !== void 0 ? h : 0);
        var E = r[u - (t[0] - 1)], _ = (f = r[u - t[0]]) !== null && f !== void 0 ? f : E, I = E.close, w = (v = E.volume) !== null && v !== void 0 ? v : 0;
        I > _.close ? i -= w : I < _.close ? a -= w : n -= w;
      }
      s.push(p);
    }), s;
  }
}, an = {
  name: "WR",
  shortName: "WR",
  calcParams: [6, 10, 14],
  figures: [
    { key: "wr1", title: "WR1: ", type: "line" },
    { key: "wr2", title: "WR2: ", type: "line" },
    { key: "wr3", title: "WR3: ", type: "line" }
  ],
  regenerateFigures: function(r) {
    return r.map(function(e, t) {
      return { key: "wr".concat(t + 1), title: "WR".concat(t + 1, ": "), type: "line" };
    });
  },
  calc: function(r, e) {
    var t = e.calcParams, i = e.figures;
    return r.map(function(a, n) {
      var o = {}, s = a.close;
      return t.forEach(function(l, u) {
        var c = l - 1;
        if (n >= c) {
          var d = ci(r.slice(n - c, n + 1), "high", "low"), h = d[0], f = d[1], v = h - f;
          o[i[u].key] = v === 0 ? 0 : (s - h) / v * 100;
        }
      }), o;
    });
  }
}, mr = {}, nn = [
  Pa,
  Da,
  ka,
  Fa,
  Ba,
  Oa,
  La,
  Va,
  Na,
  Ya,
  Wa,
  za,
  $a,
  Xa,
  qa,
  Ha,
  Ua,
  Ga,
  ja,
  Za,
  Ka,
  Ja,
  Qa,
  tn,
  en,
  rn,
  an
];
nn.forEach(function(r) {
  mr[r.name] = di.extend(r);
});
function on(r) {
  mr[r.name] = di.extend(r);
}
function hi(r) {
  var e;
  return (e = mr[r]) !== null && e !== void 0 ? e : null;
}
function Ft(r, e) {
  var t, i = (t = e?.ignoreEvent) !== null && t !== void 0 ? t : !1;
  return we(i) ? !i : !i.includes(r);
}
var Vr = 1, We = -1, sn = "overlay_", ge = "overlay_figure_", ln = (
  /** @class */
  (function() {
    function r(e) {
      this.groupId = "", this.totalStep = 1, this.currentStep = Vr, this.drawingMode = "step", this.lock = !1, this.visible = !0, this.zLevel = 0, this.needDefaultPointFigure = !1, this.needDefaultXAxisFigure = !1, this.needDefaultYAxisFigure = !1, this.mode = "normal", this.modeSensitivity = 8, this.points = [], this.styles = null, this.createPointFigures = null, this.createXAxisFigures = null, this.createYAxisFigures = null, this.performEventPressedMove = null, this.performEventMoveForDrawing = null, this.onDrawStart = null, this.onDrawing = null, this.onDrawEnd = null, this.onClick = null, this.onDoubleClick = null, this.onRightClick = null, this.onPressedMoveStart = null, this.onPressedMoving = null, this.onPressedMoveEnd = null, this.onMouseMove = null, this.onMouseEnter = null, this.onMouseLeave = null, this.onRemoved = null, this.onSelected = null, this.onDeselected = null, this._prevZLevel = 0, this._prevPressedPoint = null, this._prevPressedPoints = [], this.override(e);
    }
    return r.prototype.override = function(e) {
      var t, i;
      this._prevOverlay = Be(M(M({}, this), { _prevOverlay: null }));
      var a = e.id, n = e.name;
      e.currentStep;
      var o = e.points, s = e.styles, l = Oe(e, ["id", "name", "currentStep", "points", "styles"]);
      if (dt(this, l), K(this.name) || (this.name = n ?? ""), !K(this.id) && K(a) && (this.id = a), C(s) && ((t = this.styles) !== null && t !== void 0 || (this.styles = {}), dt(this.styles, s)), Nt(o) && o.length > 0) {
        this.points = be([], Se(o), !1), this.currentStep = We;
        var u = this.points.length - 1, c = this.points[u];
        u > 0 && C(c) && ((i = this.performEventPressedMove) === null || i === void 0 || i.call(this, {
          currentStep: this.currentStep,
          mode: this.mode,
          points: this.points,
          performPointIndex: u,
          performPoint: c
        }));
      }
    }, r.prototype.getPrevZLevel = function() {
      return this._prevZLevel;
    }, r.prototype.setPrevZLevel = function(e) {
      this._prevZLevel = e;
    }, r.prototype.shouldUpdate = function() {
      var e = this._prevOverlay.zLevel !== this.zLevel, t = e || JSON.stringify(this._prevOverlay.points) !== JSON.stringify(this.points) || this._prevOverlay.visible !== this.visible || this._prevOverlay.extendData !== this.extendData || this._prevOverlay.styles !== this.styles;
      return { sort: e, draw: t };
    }, r.prototype.nextStep = function() {
      this.currentStep === this.totalStep - 1 ? this.currentStep = We : this.currentStep++;
    }, r.prototype.forceComplete = function() {
      this.currentStep = We;
    }, r.prototype.isDrawing = function() {
      return this.currentStep !== We;
    }, r.prototype.isStart = function() {
      return this.currentStep === Vr;
    }, r.prototype.isContinuousDrawingMode = function() {
      return this.drawingMode === "continuous";
    }, r.prototype.startContinuousDrawing = function(e) {
      this.points = [], this.continuousDrawingModeEventMoveForDrawing(e), this.currentStep = 2;
    }, r.prototype.continuousDrawingModeEventMoveForDrawing = function(e) {
      var t = {};
      return B(e.timestamp) && (t.timestamp = e.timestamp), B(e.dataIndex) && (t.dataIndex = e.dataIndex), B(e.value) && (t.value = e.value), this.points.push(t), !0;
    }, r.prototype.stepDrawingModeEventMoveForDrawing = function(e) {
      var t, i = this.currentStep - 1, a = {};
      B(e.timestamp) && (a.timestamp = e.timestamp), B(e.dataIndex) && (a.dataIndex = e.dataIndex), B(e.value) && (a.value = e.value), this.points[i] = a, (t = this.performEventMoveForDrawing) === null || t === void 0 || t.call(this, {
        currentStep: this.currentStep,
        mode: this.mode,
        points: this.points,
        performPointIndex: i,
        performPoint: a
      });
    }, r.prototype.eventPressedPointMove = function(e, t) {
      var i;
      this.points[t].timestamp = e.timestamp, B(e.dataIndex) && (this.points[t].dataIndex = e.dataIndex), B(e.value) && (this.points[t].value = e.value), (i = this.performEventPressedMove) === null || i === void 0 || i.call(this, {
        currentStep: this.currentStep,
        points: this.points,
        mode: this.mode,
        performPointIndex: t,
        performPoint: this.points[t]
      });
    }, r.prototype.startPressedMove = function(e) {
      this._prevPressedPoint = M({}, e), this._prevPressedPoints = Be(this.points);
    }, r.prototype.eventPressedOtherMove = function(e, t) {
      var i = this;
      if (this._prevPressedPoint !== null) {
        var a = null;
        B(e.dataIndex) && B(this._prevPressedPoint.dataIndex) && (a = e.dataIndex - this._prevPressedPoint.dataIndex);
        var n = null;
        B(e.value) && B(this._prevPressedPoint.value) && (n = e.value - this._prevPressedPoint.value), this.points = this._prevPressedPoints.map(function(o) {
          var s, l, u = M({}, o);
          if (B(a) && (B(o.dataIndex) || B(o.timestamp))) {
            var c = B(o.timestamp) ? i.isContinuousDrawingMode() ? t.timestampToFloatIndex(o.timestamp) : t.timestampToDataIndex(o.timestamp) : o.dataIndex;
            u.dataIndex = c + a, u.timestamp = i.isContinuousDrawingMode() ? (s = t.floatIndexToTimestamp(u.dataIndex)) !== null && s !== void 0 ? s : void 0 : (l = t.dataIndexToTimestamp(u.dataIndex)) !== null && l !== void 0 ? l : void 0;
          }
          return B(n) && B(o.value) && (u.value = o.value + n), u;
        });
      }
    }, r.extend = function(e) {
      var t = (
        /** @class */
        (function(i) {
          q(a, i);
          function a() {
            return i.call(this, e) || this;
          }
          return a;
        })(r)
      );
      return t;
    }, r;
  })()
), un = {
  name: "fibonacciLine",
  totalStep: 3,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(r) {
    var e, t, i, a = r.chart, n = r.coordinates, o = r.bounding, s = r.overlay, l = r.yAxis, u = s.points;
    if (n.length > 0) {
      var c = 0;
      if (!((e = l?.isInCandle()) !== null && e !== void 0) || e)
        c = (i = (t = a.getSymbol()) === null || t === void 0 ? void 0 : t.pricePrecision) !== null && i !== void 0 ? i : pt.PRICE;
      else {
        var d = a.getIndicators({ paneId: s.paneId });
        d.forEach(function(y) {
          c = Math.max(c, y.precision);
        });
      }
      var h = [], f = [], v = 0, p = o.width;
      if (n.length > 1 && B(u[0].value) && B(u[1].value)) {
        var g = [1, 0.786, 0.618, 0.5, 0.382, 0.236, 0], m = n[0].y - n[1].y, x = u[0].value - u[1].value;
        g.forEach(function(y) {
          var E, _ = n[1].y + m * y, I = a.getDecimalFold().format(a.getThousandsSeparator().format((((E = u[1].value) !== null && E !== void 0 ? E : 0) + x * y).toFixed(c)));
          h.push({ coordinates: [{ x: v, y: _ }, { x: p, y: _ }] }), f.push({
            x: v,
            y: _,
            text: "".concat(I, " (").concat((y * 100).toFixed(1), "%)"),
            baseline: "bottom"
          });
        });
      }
      return [
        {
          type: "line",
          attrs: h
        },
        {
          type: "text",
          isCheckEvent: !1,
          attrs: f
        }
      ];
    }
    return [];
  }
}, cn = {
  name: "horizontalRayLine",
  totalStep: 3,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(r) {
    var e = r.coordinates, t = r.bounding, i = { x: 0, y: e[0].y };
    return C(e[1]) && e[0].x < e[1].x && (i.x = t.width), [
      {
        type: "line",
        attrs: { coordinates: [e[0], i] }
      }
    ];
  },
  performEventPressedMove: function(r) {
    var e = r.points, t = r.performPoint;
    e[0].value = t.value, e[1].value = t.value;
  },
  performEventMoveForDrawing: function(r) {
    var e = r.currentStep, t = r.points, i = r.performPoint;
    e === 2 && (t[0].value = i.value);
  }
}, dn = {
  name: "horizontalSegment",
  totalStep: 3,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(r) {
    var e = r.coordinates, t = [];
    return e.length === 2 && t.push({ coordinates: e }), [
      {
        type: "line",
        attrs: t
      }
    ];
  },
  performEventPressedMove: function(r) {
    var e = r.points, t = r.performPoint;
    e[0].value = t.value, e[1].value = t.value;
  },
  performEventMoveForDrawing: function(r) {
    var e = r.currentStep, t = r.points, i = r.performPoint;
    e === 2 && (t[0].value = i.value);
  }
}, hn = {
  name: "horizontalStraightLine",
  totalStep: 2,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(r) {
    var e = r.coordinates, t = r.bounding;
    return [{
      type: "line",
      attrs: {
        coordinates: [
          {
            x: 0,
            y: e[0].y
          },
          {
            x: t.width,
            y: e[0].y
          }
        ]
      }
    }];
  }
}, _r = (
  /** @class */
  (function() {
    function r() {
      this._children = [], this._callbacks = /* @__PURE__ */ new Map();
    }
    return r.prototype.registerEvent = function(e, t) {
      return this._callbacks.set(e, t), this;
    }, r.prototype.onEvent = function(e, t) {
      var i = this._callbacks.get(e);
      return C(i) && this.checkEventOn(t) ? i(t) : !1;
    }, r.prototype.dispatchEventToChildren = function(e, t) {
      var i = this._children.length - 1;
      if (i > -1) {
        for (var a = i; a > -1; a--)
          if (this._children[a].dispatchEvent(e, t))
            return !0;
      }
      return !1;
    }, r.prototype.dispatchEvent = function(e, t) {
      return this.dispatchEventToChildren(e, t) ? !0 : this.onEvent(e, t);
    }, r.prototype.addChild = function(e) {
      return this._children.push(e), this;
    }, r.prototype.clear = function() {
      this._children = [];
    }, r;
  })()
), vt = 2, vn = (
  /** @class */
  (function(r) {
    q(e, r);
    function e(t) {
      var i = r.call(this) || this;
      return i.attrs = t.attrs, i.styles = t.styles, i;
    }
    return e.prototype.checkEventOn = function(t) {
      return this.checkEventOnImp(t, this.attrs, this.styles);
    }, e.prototype.setAttrs = function(t) {
      return this.attrs = t, this;
    }, e.prototype.setStyles = function(t) {
      return this.styles = t, this;
    }, e.prototype.draw = function(t) {
      this.drawImp(t, this.attrs, this.styles);
    }, e.extend = function(t) {
      var i = (
        /** @class */
        (function(a) {
          q(n, a);
          function n() {
            return a !== null && a.apply(this, arguments) || this;
          }
          return n.prototype.checkEventOnImp = function(o, s, l) {
            return t.checkEventOn(o, s, l);
          }, n.prototype.drawImp = function(o, s, l) {
            t.draw(o, s, l);
          }, n;
        })(e)
      );
      return i;
    }, e;
  })(_r)
);
function fn(r, e) {
  var t, i, a = [];
  a = a.concat(e);
  try {
    for (var n = bt(a), o = n.next(); !o.done; o = n.next()) {
      var s = o.value, l = s.coordinates;
      if (l.length > 1)
        for (var u = 1; u < l.length; u++) {
          var c = l[u - 1], d = l[u];
          if (c.x === d.x) {
            if (Math.abs(c.y - r.y) + Math.abs(d.y - r.y) - Math.abs(c.y - d.y) < vt + vt && Math.abs(r.x - c.x) < vt)
              return !0;
          } else {
            var h = yr(c, d), f = vi(h, r), v = Math.abs(f - r.y);
            if (Math.abs(c.x - r.x) + Math.abs(d.x - r.x) - Math.abs(c.x - d.x) < vt + vt && v * v / (h[0] * h[0] + 1) < vt * vt)
              return !0;
          }
        }
    }
  } catch (p) {
    t = { error: p };
  } finally {
    try {
      o && !o.done && (i = n.return) && i.call(n);
    } finally {
      if (t) throw t.error;
    }
  }
  return !1;
}
function vi(r, e) {
  return r !== null ? e.x * r[0] + r[1] : e.y;
}
function Xe(r, e, t) {
  var i = yr(r, e);
  return vi(i, t);
}
function yr(r, e) {
  var t = r.x - e.x;
  if (t !== 0) {
    var i = (r.y - e.y) / t, a = r.y - i * r.x;
    return [i, a];
  }
  return null;
}
function fi(r, e, t) {
  var i = e.length, a = B(t) ? t > 0 && t < 1 ? t : 0 : t ? 0.5 : 0;
  if (a > 0 && i > 2) {
    for (var n = e[0].x, o = e[0].y, s = 1; s < i - 1; s++) {
      var l = e[s - 1], u = e[s], c = e[s + 1], d = u.x - l.x, h = u.y - l.y, f = c.x - u.x, v = c.y - u.y, p = c.x - l.x, g = c.y - l.y, m = Math.sqrt(d * d + h * h), x = Math.sqrt(f * f + v * v), y = x / (x + m), E = u.x + p * a * y, _ = u.y + g * a * y;
      E = Math.min(E, Math.max(c.x, u.x)), _ = Math.min(_, Math.max(c.y, u.y)), E = Math.max(E, Math.min(c.x, u.x)), _ = Math.max(_, Math.min(c.y, u.y)), p = E - u.x, g = _ - u.y;
      var I = u.x - p * m / x, w = u.y - g * m / x;
      I = Math.min(I, Math.max(l.x, u.x)), w = Math.min(w, Math.max(l.y, u.y)), I = Math.max(I, Math.min(l.x, u.x)), w = Math.max(w, Math.min(l.y, u.y)), p = u.x - I, g = u.y - w, E = u.x + p * x / m, _ = u.y + g * x / m, r.bezierCurveTo(n, o, I, w, u.x, u.y), n = E, o = _;
    }
    var b = e[i - 1];
    r.bezierCurveTo(n, o, b.x, b.y, b.x, b.y);
  } else
    for (var s = 1; s < i; s++)
      r.lineTo(e[s].x, e[s].y);
}
function pn(r, e, t) {
  var i = [];
  i = i.concat(e);
  var a = t.style, n = a === void 0 ? "solid" : a, o = t.smooth, s = o === void 0 ? !1 : o, l = t.size, u = l === void 0 ? 1 : l, c = t.color, d = c === void 0 ? "currentColor" : c, h = t.dashedValue, f = h === void 0 ? [2, 2] : h, v = t.lineCap, p = t.lineJoin, g = B(s) ? s > 0 : s;
  r.lineWidth = u, r.strokeStyle = d, K(v) ? r.lineCap = v : g ? r.lineCap = "round" : r.lineCap = "butt", K(p) ? r.lineJoin = p : g ? r.lineJoin = "round" : r.lineJoin = "miter", n === "dashed" ? r.setLineDash(f) : r.setLineDash([]);
  var m = u % 2 === 1 ? 0.5 : 0;
  i.forEach(function(x) {
    var y = x.coordinates;
    y.length > 1 && (y.length === 2 && (y[0].x === y[1].x || y[0].y === y[1].y) ? (r.beginPath(), y[0].x === y[1].x ? (r.moveTo(y[0].x + m, y[0].y), r.lineTo(y[1].x + m, y[1].y)) : (r.moveTo(y[0].x, y[0].y + m), r.lineTo(y[1].x, y[1].y + m)), r.stroke(), r.closePath()) : (r.save(), u % 2 === 1 && r.translate(0.5, 0.5), r.beginPath(), r.moveTo(y[0].x, y[0].y), fi(r, y, s), r.stroke(), r.closePath(), r.restore()));
  });
}
var gn = {
  name: "line",
  checkEventOn: fn,
  draw: function(r, e, t) {
    pn(r, e, t);
  }
};
function pi(r, e, t) {
  var i = t ?? 0, a = [];
  if (r.length > 1)
    if (r[0].x === r[1].x) {
      var n = 0, o = e.height;
      if (a.push({ coordinates: [{ x: r[0].x, y: n }, { x: r[0].x, y: o }] }), r.length > 2) {
        a.push({ coordinates: [{ x: r[2].x, y: n }, { x: r[2].x, y: o }] });
        for (var s = r[0].x - r[2].x, l = 0; l < i; l++) {
          var u = s * (l + 1);
          a.push({ coordinates: [{ x: r[0].x + u, y: n }, { x: r[0].x + u, y: o }] });
        }
      }
    } else {
      var c = 0, d = e.width, h = yr(r[0], r[1]), f = h[0], v = h[1];
      if (a.push({ coordinates: [{ x: c, y: c * f + v }, { x: d, y: d * f + v }] }), r.length > 2) {
        var p = r[2].y - f * r[2].x;
        a.push({ coordinates: [{ x: c, y: c * f + p }, { x: d, y: d * f + p }] });
        for (var s = v - p, l = 0; l < i; l++) {
          var g = v + s * (l + 1);
          a.push({ coordinates: [{ x: c, y: c * f + g }, { x: d, y: d * f + g }] });
        }
      }
    }
  return a;
}
var mn = {
  name: "parallelStraightLine",
  totalStep: 4,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(r) {
    var e = r.coordinates, t = r.bounding;
    return [
      {
        type: "line",
        attrs: pi(e, t)
      }
    ];
  }
}, _n = {
  name: "priceChannelLine",
  totalStep: 4,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(r) {
    var e = r.coordinates, t = r.bounding;
    return [
      {
        type: "line",
        attrs: pi(e, t, 1)
      }
    ];
  }
}, yn = {
  name: "priceLine",
  totalStep: 2,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(r) {
    var e, t, i, a = r.chart, n = r.coordinates, o = r.bounding, s = r.overlay, l = r.yAxis, u = 0;
    if (!((e = l?.isInCandle()) !== null && e !== void 0) || e)
      u = (i = (t = a.getSymbol()) === null || t === void 0 ? void 0 : t.pricePrecision) !== null && i !== void 0 ? i : pt.PRICE;
    else {
      var c = a.getIndicators({ paneId: s.paneId });
      c.forEach(function(f) {
        u = Math.max(u, f.precision);
      });
    }
    var d = s.points[0].value, h = d === void 0 ? 0 : d;
    return [
      {
        type: "line",
        attrs: { coordinates: [n[0], { x: o.width, y: n[0].y }] }
      },
      {
        type: "text",
        ignoreEvent: !0,
        attrs: {
          x: n[0].x,
          y: n[0].y,
          text: a.getDecimalFold().format(a.getThousandsSeparator().format(h.toFixed(u))),
          baseline: "bottom"
        }
      }
    ];
  }
};
function xn(r, e) {
  if (r.length > 1) {
    var t = { x: 0, y: 0 };
    return r[0].x === r[1].x && r[0].y !== r[1].y ? r[0].y < r[1].y ? t = {
      x: r[0].x,
      y: e.height
    } : t = {
      x: r[0].x,
      y: 0
    } : r[0].x > r[1].x ? t = {
      x: 0,
      y: Xe(r[0], r[1], { x: 0, y: r[0].y })
    } : t = {
      x: e.width,
      y: Xe(r[0], r[1], { x: e.width, y: r[0].y })
    }, { coordinates: [r[0], t] };
  }
  return [];
}
var wn = {
  name: "rayLine",
  totalStep: 3,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(r) {
    var e = r.coordinates, t = r.bounding;
    return [
      {
        type: "line",
        attrs: xn(e, t)
      }
    ];
  }
}, bn = {
  name: "segment",
  totalStep: 3,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(r) {
    var e = r.coordinates;
    return e.length === 2 ? [
      {
        type: "line",
        attrs: { coordinates: e }
      }
    ] : [];
  }
}, Cn = {
  name: "straightLine",
  totalStep: 3,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(r) {
    var e = r.coordinates, t = r.bounding;
    return e.length === 2 ? e[0].x === e[1].x ? [
      {
        type: "line",
        attrs: {
          coordinates: [
            {
              x: e[0].x,
              y: 0
            },
            {
              x: e[0].x,
              y: t.height
            }
          ]
        }
      }
    ] : [
      {
        type: "line",
        attrs: {
          coordinates: [
            {
              x: 0,
              y: Xe(e[0], e[1], { x: 0, y: e[0].y })
            },
            {
              x: t.width,
              y: Xe(e[0], e[1], { x: t.width, y: e[0].y })
            }
          ]
        }
      }
    ] : [];
  }
}, En = {
  name: "verticalRayLine",
  totalStep: 3,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(r) {
    var e = r.coordinates, t = r.bounding;
    if (e.length === 2) {
      var i = { x: e[0].x, y: 0 };
      return e[0].y < e[1].y && (i.y = t.height), [
        {
          type: "line",
          attrs: { coordinates: [e[0], i] }
        }
      ];
    }
    return [];
  },
  performEventPressedMove: function(r) {
    var e = r.points, t = r.performPoint;
    e[0].timestamp = t.timestamp, e[0].dataIndex = t.dataIndex, e[1].timestamp = t.timestamp, e[1].dataIndex = t.dataIndex;
  },
  performEventMoveForDrawing: function(r) {
    var e = r.currentStep, t = r.points, i = r.performPoint;
    e === 2 && (t[0].timestamp = i.timestamp, t[0].dataIndex = i.dataIndex);
  }
}, In = {
  name: "verticalSegment",
  totalStep: 3,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(r) {
    var e = r.coordinates;
    return e.length === 2 ? [
      {
        type: "line",
        attrs: { coordinates: e }
      }
    ] : [];
  },
  performEventPressedMove: function(r) {
    var e = r.points, t = r.performPoint;
    e[0].timestamp = t.timestamp, e[0].dataIndex = t.dataIndex, e[1].timestamp = t.timestamp, e[1].dataIndex = t.dataIndex;
  },
  performEventMoveForDrawing: function(r) {
    var e = r.currentStep, t = r.points, i = r.performPoint;
    e === 2 && (t[0].timestamp = i.timestamp, t[0].dataIndex = i.dataIndex);
  }
}, Sn = {
  name: "verticalStraightLine",
  totalStep: 2,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(r) {
    var e = r.coordinates, t = r.bounding;
    return [
      {
        type: "line",
        attrs: {
          coordinates: [
            {
              x: e[0].x,
              y: 0
            },
            {
              x: e[0].x,
              y: t.height
            }
          ]
        }
      }
    ];
  }
}, Tn = {
  name: "simpleAnnotation",
  totalStep: 2,
  styles: {
    line: { style: "dashed" }
  },
  createPointFigures: function(r) {
    var e, t = r.overlay, i = r.coordinates, a = "";
    C(t.extendData) && (ct(t.extendData) ? a = t.extendData(t) : a = (e = t.extendData) !== null && e !== void 0 ? e : "");
    var n = i[0].x, o = i[0].y - 6, s = o - 50, l = s - 5;
    return [
      {
        type: "line",
        attrs: { coordinates: [{ x: n, y: o }, { x: n, y: s }] },
        ignoreEvent: !0
      },
      {
        type: "polygon",
        attrs: { coordinates: [{ x: n, y: s }, { x: n - 4, y: l }, { x: n + 4, y: l }] },
        ignoreEvent: !0
      },
      {
        type: "text",
        attrs: { x: n, y: l, text: a, align: "center", baseline: "bottom" },
        ignoreEvent: !0
      }
    ];
  }
}, An = {
  name: "simpleTag",
  totalStep: 2,
  styles: {
    line: { style: "dashed" }
  },
  createPointFigures: function(r) {
    var e = r.bounding, t = r.coordinates;
    return {
      type: "line",
      attrs: {
        coordinates: [
          { x: 0, y: t[0].y },
          { x: e.width, y: t[0].y }
        ]
      },
      ignoreEvent: !0
    };
  },
  createYAxisFigures: function(r) {
    var e, t, i, a, n = r.chart, o = r.overlay, s = r.coordinates, l = r.bounding, u = r.yAxis, c = (e = u?.isFromZero()) !== null && e !== void 0 ? e : !1, d = "left", h = 0;
    c ? (d = "left", h = 0) : (d = "right", h = l.width);
    var f = "";
    return C(o.extendData) && (ct(o.extendData) ? f = o.extendData(o) : f = (t = o.extendData) !== null && t !== void 0 ? t : ""), !C(f) && B(o.points[0].value) && (f = wt(o.points[0].value, (a = (i = n.getSymbol()) === null || i === void 0 ? void 0 : i.pricePrecision) !== null && a !== void 0 ? a : pt.PRICE)), { type: "text", attrs: { x: h, y: s[0].y, text: f, align: d, baseline: "middle" } };
  }
}, Mn = {
  name: "brush",
  totalStep: 2,
  drawingMode: "continuous",
  needDefaultPointFigure: !1,
  needDefaultXAxisFigure: !1,
  needDefaultYAxisFigure: !1,
  createPointFigures: function(r) {
    var e = r.coordinates;
    return e.length < 2 ? [] : [
      {
        type: "line",
        attrs: { coordinates: e },
        styles: {
          smooth: !1,
          lineCap: "round",
          lineJoin: "round"
        }
      }
    ];
  }
}, gi = {}, Pn = [
  un,
  cn,
  dn,
  hn,
  mn,
  _n,
  yn,
  wn,
  bn,
  Cn,
  En,
  In,
  Sn,
  Tn,
  An,
  Mn
];
Pn.forEach(function(r) {
  gi[r.name] = ln.extend(r);
});
function Dn(r) {
  var e;
  return (e = gi[r]) !== null && e !== void 0 ? e : null;
}
var kn = {
  grid: {
    horizontal: {
      color: "#EDEDED"
    },
    vertical: {
      color: "#EDEDED"
    }
  },
  candle: {
    priceMark: {
      high: {
        color: "#76808F"
      },
      low: {
        color: "#76808F"
      }
    },
    tooltip: {
      rect: {
        color: "#FEFEFE",
        borderColor: "#F2F3F5"
      },
      title: {
        color: "#76808F"
      },
      legend: {
        color: "#76808F"
      }
    }
  },
  indicator: {
    tooltip: {
      title: {
        color: "#76808F"
      },
      legend: {
        color: "#76808F"
      }
    }
  },
  xAxis: {
    axisLine: {
      color: "#DDDDDD"
    },
    tickText: {
      color: "#76808F"
    },
    tickLine: {
      color: "#DDDDDD"
    }
  },
  yAxis: {
    axisLine: {
      color: "#DDDDDD"
    },
    tickText: {
      color: "#76808F"
    },
    tickLine: {
      color: "#DDDDDD"
    }
  },
  separator: {
    color: "#DDDDDD"
  },
  crosshair: {
    horizontal: {
      line: {
        color: "#76808F"
      },
      text: {
        borderColor: "#686D76",
        backgroundColor: "#686D76"
      }
    },
    vertical: {
      line: {
        color: "#76808F"
      },
      text: {
        borderColor: "#686D76",
        backgroundColor: "#686D76"
      }
    }
  }
}, Rn = {
  grid: {
    horizontal: {
      color: "#292929"
    },
    vertical: {
      color: "#292929"
    }
  },
  candle: {
    priceMark: {
      high: {
        color: "#929AA5"
      },
      low: {
        color: "#929AA5"
      }
    },
    tooltip: {
      rect: {
        color: "rgba(10, 10, 10, .6)",
        borderColor: "rgba(10, 10, 10, .6)"
      },
      title: {
        color: "#929AA5"
      },
      legend: {
        color: "#929AA5"
      }
    }
  },
  indicator: {
    tooltip: {
      title: {
        color: "#929AA5"
      },
      legend: {
        color: "#929AA5"
      }
    }
  },
  xAxis: {
    axisLine: {
      color: "#333333"
    },
    tickText: {
      color: "#929AA5"
    },
    tickLine: {
      color: "#333333"
    }
  },
  yAxis: {
    axisLine: {
      color: "#333333"
    },
    tickText: {
      color: "#929AA5"
    },
    tickLine: {
      color: "#333333"
    }
  },
  separator: {
    color: "#333333"
  },
  crosshair: {
    horizontal: {
      line: {
        color: "#929AA5"
      },
      text: {
        borderColor: "#373a40",
        backgroundColor: "#373a40"
      }
    },
    vertical: {
      line: {
        color: "#929AA5"
      },
      text: {
        borderColor: "#373a40",
        backgroundColor: "#373a40"
      }
    }
  }
}, Fn = {
  light: kn,
  dark: Rn
};
function Bn(r) {
  var e;
  return (e = Fn[r]) !== null && e !== void 0 ? e : null;
}
var j = {
  CANDLE: "candle_pane",
  INDICATOR: "indicator_pane_",
  X_AXIS: "x_axis_pane"
}, On = 10, Ln = 80, Vn = 0.2, lr = 10, Nn = (
  /** @class */
  (function() {
    function r(e, t) {
      var i = this;
      this._styles = Ma(), this._formatter = {
        formatDate: function(v) {
          var p = v.dateTimeFormat, g = v.timestamp, m = v.template;
          return pa(p, g, m);
        },
        formatBigNumber: ga,
        formatExtendText: function(v) {
          return "";
        }
      }, this._innerFormatter = {
        formatDate: function(v, p, g) {
          return i._formatter.formatDate({ dateTimeFormat: i._dateTimeFormat, timestamp: v, template: p, type: g });
        },
        formatBigNumber: function(v) {
          return i._formatter.formatBigNumber(v);
        },
        formatExtendText: function(v) {
          return i._formatter.formatExtendText(v);
        }
      }, this._locale = "en-US", this._thousandsSeparator = {
        sign: ",",
        format: function(v) {
          return ma(v, i._thousandsSeparator.sign);
        }
      }, this._decimalFold = {
        threshold: 3,
        format: function(v) {
          return _a(v, i._decimalFold.threshold);
        }
      }, this._hotKey = {
        enabled: !0,
        exclude: []
      }, this._symbol = null, this._period = null, this._dataList = [], this._dataLoader = null, this._loading = !1, this._dataLoadMore = { forward: !1, backward: !1 }, this._zoomEnabled = !0, this._zoomAnchor = {
        main: "cursor",
        xAxis: "cursor"
      }, this._scrollEnabled = !0, this._totalBarSpace = 0, this._barSpace = On, this._offsetRightDistance = Ln, this._startLastBarRightSideDiffBarCount = 0, this._scrollLimitRole = "bar_count", this._minVisibleBarCount = { left: 2, right: 2 }, this._maxOffsetDistance = { left: 50, right: 50 }, this._visibleRange = Br(), this._visibleRangeDataList = [], this._visibleRangeHighLowPrice = [
        { x: 0, price: Number.MIN_SAFE_INTEGER },
        { x: 0, price: Number.MAX_SAFE_INTEGER }
      ], this._crosshair = {}, this._actions = /* @__PURE__ */ new Map(), this._indicators = /* @__PURE__ */ new Map(), this._overlays = /* @__PURE__ */ new Map(), this._progressOverlayInfo = null, this._lastPriceMarkExtendTextUpdateTimers = [], this._pressedOverlayInfo = {
        paneId: "",
        overlay: null,
        figureType: "none",
        figureIndex: -1,
        figure: null
      }, this._hoverOverlayInfo = {
        paneId: "",
        overlay: null,
        figureType: "none",
        figureIndex: -1,
        figure: null
      }, this._clickOverlayInfo = {
        paneId: "",
        overlay: null,
        figureType: "none",
        figureIndex: -1,
        figure: null
      }, this._layoutOptions = {
        barSpaceLimit: {
          min: 1,
          max: 50
        },
        pane: {
          minHeight: 30,
          dragEnabled: !0,
          order: 0,
          height: 100,
          state: "normal"
        },
        yAxis: {
          reverse: !1,
          inside: !1,
          position: "right",
          scrollZoomEnabled: !0,
          needWidget: !0,
          gap: {
            top: 0.2,
            bottom: 0.1
          }
        }
      }, this._chart = e;
      var a = t ?? {}, n = a.styles, o = a.locale, s = a.timezone, l = a.formatter, u = a.thousandsSeparator, c = a.decimalFold, d = a.zoomAnchor, h = a.hotkey, f = a.layout;
      C(f) && dt(this._layoutOptions, f), this._calcOptimalBarSpace(), this._lastBarRightSideDiffBarCount = this._offsetRightDistance / this._barSpace, C(n) && this.setStyles(n), K(o) && this.setLocale(o), this.setTimezone(s ?? ""), C(l) && this.setFormatter(l), C(u) && this.setThousandsSeparator(u), C(c) && this.setDecimalFold(c), C(d) && this.setZoomAnchor(d), C(h) && this.setHotkey(h), this._taskScheduler = new wa(function() {
        i._chart.layout({
          measureWidth: !0,
          update: !0,
          buildYAxisTick: !0
        });
      });
    }
    return r.prototype.setStyles = function(e) {
      var t = this, i, a, n, o, s, l, u = null;
      if (K(e) ? u = Bn(e) : u = e, dt(this._styles, u), Nt((n = (a = (i = u?.candle) === null || i === void 0 ? void 0 : i.tooltip) === null || a === void 0 ? void 0 : a.legend) === null || n === void 0 ? void 0 : n.template) && (this._styles.candle.tooltip.legend.template = u.candle.tooltip.legend.template), C((l = (s = (o = u?.candle) === null || o === void 0 ? void 0 : o.priceMark) === null || s === void 0 ? void 0 : s.last) === null || l === void 0 ? void 0 : l.extendTexts)) {
        this._clearLastPriceMarkExtendTextUpdateTimer();
        var c = [];
        this._styles.candle.priceMark.last.extendTexts.forEach(function(d) {
          var h = d.updateInterval;
          if (d.show && h > 0 && !c.includes(h)) {
            c.push(h);
            var f = setInterval(function() {
              t._chart.updatePane(0, j.CANDLE);
            }, h);
            t._lastPriceMarkExtendTextUpdateTimers.push(f);
          }
        });
      }
    }, r.prototype.getStyles = function() {
      return this._styles;
    }, r.prototype.setFormatter = function(e) {
      dt(this._formatter, e);
    }, r.prototype.getFormatter = function() {
      return this._formatter;
    }, r.prototype.getInnerFormatter = function() {
      return this._innerFormatter;
    }, r.prototype.setLocale = function(e) {
      this._locale = e;
    }, r.prototype.getLocale = function() {
      return this._locale;
    }, r.prototype.setTimezone = function(e) {
      if (!C(this._dateTimeFormat) || this.getTimezone() !== e) {
        var t = {
          hour12: !1,
          year: "numeric",
          month: "2-digit",
          day: "2-digit",
          hour: "2-digit",
          minute: "2-digit",
          second: "2-digit"
        };
        e.length > 0 && (t.timeZone = e);
        var i = null;
        try {
          i = new Intl.DateTimeFormat("en", t);
        } catch {
        }
        i !== null && (this._dateTimeFormat = i);
      }
    }, r.prototype.getTimezone = function() {
      return this._dateTimeFormat.resolvedOptions().timeZone;
    }, r.prototype.getDateTimeFormat = function() {
      return this._dateTimeFormat;
    }, r.prototype.setThousandsSeparator = function(e) {
      dt(this._thousandsSeparator, e);
    }, r.prototype.getThousandsSeparator = function() {
      return this._thousandsSeparator;
    }, r.prototype.setDecimalFold = function(e) {
      dt(this._decimalFold, e);
    }, r.prototype.getDecimalFold = function() {
      return this._decimalFold;
    }, r.prototype.setHotkey = function(e) {
      dt(this._hotKey, e);
    }, r.prototype.getHotkey = function() {
      return this._hotKey;
    }, r.prototype.getHotKey = function() {
      return this._hotKey;
    }, r.prototype.setSymbol = function(e) {
      var t = this;
      this.resetData(function() {
        t._symbol = M(M({ pricePrecision: pt.PRICE, volumePrecision: pt.VOLUME }, t._symbol), e), t._synchronizeIndicatorSeriesPrecision();
      });
    }, r.prototype.getSymbol = function() {
      return this._symbol;
    }, r.prototype.setPeriod = function(e) {
      var t = this;
      this.resetData(function() {
        t._period = e;
      });
    }, r.prototype.getPeriod = function() {
      return this._period;
    }, r.prototype.getDataList = function() {
      return this._dataList;
    }, r.prototype.getVisibleRangeDataList = function() {
      return this._visibleRangeDataList;
    }, r.prototype.getVisibleRangeHighLowPrice = function() {
      return this._visibleRangeHighLowPrice;
    }, r.prototype._addData = function(e, t, i) {
      var a, n, o = !1, s = !1;
      if (Nt(e)) {
        var l = { backward: !1, forward: !1 };
        switch (we(i) ? (l.backward = i, l.forward = i) : (l.backward = (a = i?.backward) !== null && a !== void 0 ? a : !1, l.forward = (n = i?.forward) !== null && n !== void 0 ? n : !1), t) {
          case "init": {
            this._clearData(), this._dataList = e, this._dataLoadMore.backward = l.backward, this._dataLoadMore.forward = l.forward, this.setOffsetRightDistance(this._offsetRightDistance), s = !0;
            break;
          }
          case "backward": {
            this._dataList = this._dataList.concat(e), this._dataLoadMore.backward = l.backward, this._lastBarRightSideDiffBarCount -= e.length, this._startLastBarRightSideDiffBarCount -= e.length, s = e.length > 0;
            break;
          }
          case "forward": {
            this._dataList = e.concat(this._dataList), this._dataLoadMore.forward = l.forward, s = e.length > 0;
            break;
          }
        }
        o = !0;
      } else {
        var u = this._dataList.length, c = e.timestamp, d = ht(this._dataList[u - 1], "timestamp", 0);
        if (c > d) {
          this._dataList.push(e);
          var h = this.getLastBarRightSideDiffBarCount();
          h < 0 && this.setLastBarRightSideDiffBarCount(--h), o = !0, s = !0;
        } else c === d && (this._dataList[u - 1] = e, o = !0, s = !0);
      }
      if (o && s) {
        this._adjustVisibleRange(), this.setCrosshair(this._crosshair, { notInvalidate: !0 });
        var f = this.getIndicatorsByFilter({});
        f.length > 0 ? this._calcIndicator(f) : this._chart.layout({
          measureWidth: !0,
          update: !0,
          buildYAxisTick: !0,
          cacheYAxisWidth: t !== "init"
        });
      }
    }, r.prototype.setDataLoader = function(e) {
      var t = this;
      this.resetData(function() {
        t._dataLoader = e;
      });
    }, r.prototype._calcOptimalBarSpace = function() {
      var e = 4, t = 1 - Vn * Math.atan(Math.max(e, this._barSpace) - e) / (Math.PI * 0.5), i = Math.min(Math.floor(this._barSpace * t), Math.floor(this._barSpace));
      i % 2 === 0 && i + 2 >= this._barSpace && --i, this._gapBarSpace = Math.max(1, i);
    }, r.prototype._adjustVisibleRange = function() {
      var e, t, i = this._dataList.length, a = this._totalBarSpace / this._barSpace, n = 0, o = 0;
      this._scrollLimitRole === "distance" ? (n = (this._totalBarSpace - this._maxOffsetDistance.right) / this._barSpace, o = (this._totalBarSpace - this._maxOffsetDistance.left) / this._barSpace) : (n = this._minVisibleBarCount.left, o = this._minVisibleBarCount.right), n = Math.max(0, n), o = Math.max(0, o);
      var s = a - Math.min(n, i);
      this._lastBarRightSideDiffBarCount > s && (this._lastBarRightSideDiffBarCount = s);
      var l = -i + Math.min(o, i);
      this._lastBarRightSideDiffBarCount < l && (this._lastBarRightSideDiffBarCount = l);
      var u = Math.round(this._lastBarRightSideDiffBarCount + i + 0.5), c = u;
      u > i && (u = i);
      var d = Math.round(u - a) - 1;
      d < 0 && (d = 0);
      var h = this._lastBarRightSideDiffBarCount > 0 ? Math.round(i + this._lastBarRightSideDiffBarCount - a) - 1 : d;
      this._visibleRange = { from: d, to: u, realFrom: h, realTo: c }, this.executeAction("onVisibleRangeChange", this._visibleRange), this._visibleRangeDataList = [], this._visibleRangeHighLowPrice = [
        { x: 0, price: Number.MIN_SAFE_INTEGER },
        { x: 0, price: Number.MAX_SAFE_INTEGER }
      ];
      for (var f = h; f < c; f++) {
        var v = this._dataList[f], p = this.dataIndexToCoordinate(f);
        this._visibleRangeDataList.push({
          dataIndex: f,
          x: p,
          data: {
            prev: (e = this._dataList[f - 1]) !== null && e !== void 0 ? e : v,
            current: v,
            next: (t = this._dataList[f + 1]) !== null && t !== void 0 ? t : v
          }
        }), C(v) && (this._visibleRangeHighLowPrice[0].price < v.high && (this._visibleRangeHighLowPrice[0].price = v.high, this._visibleRangeHighLowPrice[0].x = p), this._visibleRangeHighLowPrice[1].price > v.low && (this._visibleRangeHighLowPrice[1].price = v.low, this._visibleRangeHighLowPrice[1].x = p));
      }
      d === 0 ? this._dataLoadMore.forward && this._processDataLoad("forward") : u === i && this._dataLoadMore.backward && this._processDataLoad("backward");
    }, r.prototype._processDataLoad = function(e) {
      var t = this, i, a, n, o;
      if (!this._loading && C(this._dataLoader) && C(this._symbol) && C(this._period)) {
        this._loading = !0;
        var s = {
          type: e,
          symbol: this._symbol,
          period: this._period,
          timestamp: null,
          callback: function(l, u) {
            var c, d;
            t._loading = !1, t._addData(l, e, u), e === "init" && ((d = (c = t._dataLoader) === null || c === void 0 ? void 0 : c.subscribeBar) === null || d === void 0 || d.call(c, {
              symbol: t._symbol,
              period: t._period,
              callback: function(h) {
                t._addData(h, "update");
              }
            }));
          }
        };
        switch (e) {
          case "backward": {
            s.timestamp = (a = (i = this._dataList[this._dataList.length - 1]) === null || i === void 0 ? void 0 : i.timestamp) !== null && a !== void 0 ? a : null;
            break;
          }
          case "forward": {
            s.timestamp = (o = (n = this._dataList[0]) === null || n === void 0 ? void 0 : n.timestamp) !== null && o !== void 0 ? o : null;
            break;
          }
        }
        this._dataLoader.getBars(s);
      }
    }, r.prototype._processDataUnsubscribe = function() {
      var e, t;
      C(this._dataLoader) && C(this._symbol) && C(this._period) && ((t = (e = this._dataLoader).unsubscribeBar) === null || t === void 0 || t.call(e, {
        symbol: this._symbol,
        period: this._period
      }));
    }, r.prototype.resetData = function(e) {
      this._processDataUnsubscribe(), e?.(), this._loading = !1, this._processDataLoad("init");
    }, r.prototype.getBarSpace = function() {
      return {
        bar: this._barSpace,
        halfBar: this._barSpace / 2,
        gapBar: this._gapBarSpace,
        halfGapBar: Math.floor(this._gapBarSpace / 2)
      };
    }, r.prototype.setBarSpace = function(e, t) {
      e < this._layoutOptions.barSpaceLimit.min || e > this._layoutOptions.barSpaceLimit.max || this._barSpace === e || (this._barSpace = e, this._calcOptimalBarSpace(), t?.(), this._adjustVisibleRange(), this.setCrosshair(this._crosshair, { notInvalidate: !0 }), this._chart.layout({
        measureWidth: !0,
        update: !0,
        buildYAxisTick: !0,
        cacheYAxisWidth: !0
      }));
    }, r.prototype.getLayoutOptions = function() {
      return this._layoutOptions;
    }, r.prototype.setTotalBarSpace = function(e) {
      this._totalBarSpace !== e && (this._totalBarSpace = e, this._adjustVisibleRange(), this.setCrosshair(this._crosshair, { notInvalidate: !0 }));
    }, r.prototype.setOffsetRightDistance = function(e, t) {
      return this._offsetRightDistance = this._scrollLimitRole === "distance" ? Math.min(this._maxOffsetDistance.right, e) : e, this._lastBarRightSideDiffBarCount = this._offsetRightDistance / this._barSpace, (t ?? !1) && (this._adjustVisibleRange(), this.setCrosshair(this._crosshair, { notInvalidate: !0 }), this._chart.layout({
        measureWidth: !0,
        update: !0,
        buildYAxisTick: !0,
        cacheYAxisWidth: !0
      })), this;
    }, r.prototype.getInitialOffsetRightDistance = function() {
      return this._offsetRightDistance;
    }, r.prototype.getOffsetRightDistance = function() {
      return Math.max(0, this._lastBarRightSideDiffBarCount * this._barSpace);
    }, r.prototype.getLastBarRightSideDiffBarCount = function() {
      return this._lastBarRightSideDiffBarCount;
    }, r.prototype.setLastBarRightSideDiffBarCount = function(e) {
      this._lastBarRightSideDiffBarCount = e;
    }, r.prototype.setMaxOffsetLeftDistance = function(e) {
      this._scrollLimitRole = "distance", this._maxOffsetDistance.left = e;
    }, r.prototype.setMaxOffsetRightDistance = function(e) {
      this._scrollLimitRole = "distance", this._maxOffsetDistance.right = e;
    }, r.prototype.setLeftMinVisibleBarCount = function(e) {
      this._scrollLimitRole = "bar_count", this._minVisibleBarCount.left = e;
    }, r.prototype.setRightMinVisibleBarCount = function(e) {
      this._scrollLimitRole = "bar_count", this._minVisibleBarCount.right = e;
    }, r.prototype.getVisibleRange = function() {
      return this._visibleRange;
    }, r.prototype.startScroll = function() {
      this._startLastBarRightSideDiffBarCount = this._lastBarRightSideDiffBarCount;
    }, r.prototype.scroll = function(e) {
      if (this._scrollEnabled) {
        var t = e / this._barSpace, i = this._lastBarRightSideDiffBarCount * this._barSpace;
        this._lastBarRightSideDiffBarCount = this._startLastBarRightSideDiffBarCount - t, this._adjustVisibleRange(), this.setCrosshair(this._crosshair, { notInvalidate: !0 }), this._chart.layout({
          measureWidth: !0,
          update: !0,
          buildYAxisTick: !0,
          cacheYAxisWidth: !0
        });
        var a = Math.round(i - this._lastBarRightSideDiffBarCount * this._barSpace);
        a !== 0 && this.executeAction("onScroll", { distance: a });
      }
    }, r.prototype.getDataByDataIndex = function(e) {
      var t;
      return (t = this._dataList[e]) !== null && t !== void 0 ? t : null;
    }, r.prototype.coordinateToFloatIndex = function(e) {
      var t = this._dataList.length, i = (this._totalBarSpace - e) / this._barSpace, a = t + this._lastBarRightSideDiffBarCount - i;
      return Math.round(a * 1e6) / 1e6;
    }, r.prototype.dataIndexToTimestamp = function(e) {
      var t = this._dataList.length;
      if (t === 0)
        return null;
      var i = this.getDataByDataIndex(e);
      if (C(i))
        return i.timestamp;
      if (C(this._period)) {
        var a = t - 1, n = null, o = 0;
        if (e > a ? (n = this._dataList[a].timestamp, o = e - a) : e < 0 && (n = this._dataList[0].timestamp, o = e), B(n)) {
          var s = this._period, l = s.type, u = s.span;
          switch (l) {
            case "second":
              return n + u * 1e3 * o;
            case "minute":
              return n + u * 60 * 1e3 * o;
            case "hour":
              return n + u * 60 * 60 * 1e3 * o;
            case "day":
              return n + u * 24 * 60 * 60 * 1e3 * o;
            case "week":
              return n + u * 7 * 24 * 60 * 60 * 1e3 * o;
            case "month": {
              var c = new Date(n), d = c.getDate();
              c.setDate(1), c.setMonth(c.getMonth() + u * o);
              var h = new Date(c.getFullYear(), c.getMonth() + 1, 0).getDate();
              return c.setDate(Math.min(d, h)), c.getTime();
            }
            case "year": {
              var c = new Date(n);
              return c.setFullYear(c.getFullYear() + u * o), c.getTime();
            }
          }
        }
      }
      return null;
    }, r.prototype.timestampToDataIndex = function(e) {
      var t = this._dataList.length;
      if (t === 0)
        return 0;
      if (C(this._period)) {
        var i = null, a = 0, n = t - 1, o = this._dataList[n].timestamp;
        e > o && (i = o, a = n);
        var s = this._dataList[0].timestamp;
        if (e < s && (i = s, a = 0), B(i)) {
          var l = this._period, u = l.type, c = l.span;
          switch (u) {
            case "second":
              return a + Math.floor((e - i) / (c * 1e3));
            case "minute":
              return a + Math.floor((e - i) / (c * 60 * 1e3));
            case "hour":
              return a + Math.floor((e - i) / (c * 60 * 60 * 1e3));
            case "day":
              return a + Math.floor((e - i) / (c * 24 * 60 * 60 * 1e3));
            case "week":
              return a + Math.floor((e - i) / (c * 7 * 24 * 60 * 60 * 1e3));
            case "month": {
              var d = new Date(i), h = new Date(e), f = d.getFullYear(), v = h.getFullYear(), p = d.getMonth(), g = h.getMonth();
              return a + Math.floor(((v - f) * 12 + (g - p)) / c);
            }
            case "year": {
              var f = new Date(i).getFullYear(), v = new Date(e).getFullYear();
              return a + Math.floor((v - f) / c);
            }
          }
        }
      }
      return sr(this._dataList, "timestamp", e);
    }, r.prototype.dataIndexToCoordinate = function(e) {
      var t = this._dataList.length, i = t + this._lastBarRightSideDiffBarCount - e;
      return Math.floor(this._totalBarSpace - (i - 0.5) * this._barSpace + 0.5);
    }, r.prototype.coordinateToDataIndex = function(e) {
      return Math.ceil(this.coordinateToFloatIndex(e)) - 1;
    }, r.prototype.floatIndexToTimestamp = function(e) {
      var t = this._dataList.length;
      if (t === 0)
        return null;
      var i = t - 1;
      if (e > i && t >= 2) {
        var a = this._dataList[i].timestamp, n = this._dataList[i - 1].timestamp, o = a - n;
        if (o > 0) {
          var s = e - i;
          return Math.round(a + s * o);
        }
      }
      if (e < 0 && t >= 2) {
        var l = this._dataList[0].timestamp, u = this._dataList[1].timestamp, o = u - l;
        if (o > 0)
          return Math.round(l + e * o);
      }
      var c = Math.floor(e), d = e - c, h = this.dataIndexToTimestamp(c);
      if (d === 0 || !B(h))
        return h;
      var f = this.dataIndexToTimestamp(c + 1);
      return B(f) ? Math.round(h + (f - h) * d) : h;
    }, r.prototype.timestampToFloatIndex = function(e) {
      var t = this._dataList.length;
      if (t === 0)
        return 0;
      var i = this._dataList[0].timestamp, a = this._dataList[t - 1].timestamp;
      if (e > a && t >= 2) {
        var n = this._dataList[t - 2].timestamp, o = a - n;
        if (o > 0) {
          var s = e - a, l = s / o;
          return t - 1 + l;
        }
      }
      if (e < i && t >= 2) {
        var u = this._dataList[1].timestamp, o = u - i;
        if (o > 0) {
          var c = i - e, d = c / o;
          return -d;
        }
      }
      for (var h = 0, f = t - 1, v = 0; h <= f; ) {
        var p = Math.floor((h + f) / 2), g = this._dataList[p].timestamp;
        g <= e ? (v = p, h = p + 1) : f = p - 1;
      }
      var m = this._dataList[v], x = v + 1 < t ? this._dataList[v + 1] : null;
      if (C(m) && C(x)) {
        var y = m.timestamp, E = x.timestamp;
        if (e >= y && E > y) {
          var _ = (e - y) / (E - y);
          return v + Math.min(_, 1);
        }
      }
      return v;
    }, r.prototype.zoom = function(e, t, i) {
      var a = this, n;
      if (this._zoomEnabled) {
        var o = t ?? { x: (n = this._crosshair.x) !== null && n !== void 0 ? n : this._totalBarSpace / 2 };
        i === "xAxis" ? this._zoomAnchor.xAxis === "last_bar" && (o.x = this.dataIndexToCoordinate(this._dataList.length - 1)) : this._zoomAnchor.main === "last_bar" && (o.x = this.dataIndexToCoordinate(this._dataList.length - 1));
        var s = o.x, l = this.coordinateToFloatIndex(s), u = this._barSpace, c = this._barSpace + e * (this._barSpace / lr);
        this.setBarSpace(c, function() {
          a._lastBarRightSideDiffBarCount += l - a.coordinateToFloatIndex(s);
        });
        var d = this._barSpace / u;
        d !== 1 && this.executeAction("onZoom", { scale: d });
      }
    }, r.prototype.setZoomEnabled = function(e) {
      this._zoomEnabled = e;
    }, r.prototype.isZoomEnabled = function() {
      return this._zoomEnabled;
    }, r.prototype.setZoomAnchor = function(e) {
      K(e) ? (this._zoomAnchor.main = e, this._zoomAnchor.xAxis = e) : (K(e.main) && (this._zoomAnchor.main = e.main), K(e.xAxis) && (this._zoomAnchor.xAxis = e.xAxis));
    }, r.prototype.getZoomAnchor = function() {
      return M({}, this._zoomAnchor);
    }, r.prototype.setScrollEnabled = function(e) {
      this._scrollEnabled = e;
    }, r.prototype.isScrollEnabled = function() {
      return this._scrollEnabled;
    }, r.prototype.setCrosshair = function(e, t) {
      var i, a = t ?? {}, n = a.notInvalidate, o = a.notExecuteAction, s = a.forceInvalidate, l = e ?? {}, u = 0, c = 0;
      B(l.x) ? (u = this.coordinateToDataIndex(l.x), u < 0 ? c = 0 : u > this._dataList.length - 1 ? c = this._dataList.length - 1 : c = u) : (u = this._dataList.length - 1, c = u);
      var d = this._dataList[c], h = this.dataIndexToCoordinate(u), f = { x: this._crosshair.x, y: this._crosshair.y, paneId: this._crosshair.paneId };
      this._crosshair = M(M({}, l), { realX: h, kLineData: d, realDataIndex: u, dataIndex: c, timestamp: (i = this.dataIndexToTimestamp(u)) !== null && i !== void 0 ? i : void 0 }), (f.x !== l.x || f.y !== l.y || f.paneId !== l.paneId || (s ?? !1)) && (C(d) && !(o ?? !1) && this.hasAction("onCrosshairChange") && K(this._crosshair.paneId) && this.executeAction("onCrosshairChange", e), (n ?? !1) || this._chart.updatePane(
        1
        /* UpdateLevel.Overlay */
      ));
    }, r.prototype.getCrosshair = function() {
      return this._crosshair;
    }, r.prototype.executeAction = function(e, t) {
      var i;
      (i = this._actions.get(e)) === null || i === void 0 || i.execute(t);
    }, r.prototype.subscribeAction = function(e, t) {
      var i;
      this._actions.has(e) || this._actions.set(e, new ba()), (i = this._actions.get(e)) === null || i === void 0 || i.subscribe(t);
    }, r.prototype.unsubscribeAction = function(e, t) {
      var i = this._actions.get(e);
      C(i) && (i.unsubscribe(t), i.isEmpty() && this._actions.delete(e));
    }, r.prototype.hasAction = function(e) {
      var t = this._actions.get(e);
      return C(t) && !t.isEmpty();
    }, r.prototype._sortIndicators = function(e) {
      var t;
      K(e) ? (t = this._indicators.get(e)) === null || t === void 0 || t.sort(function(i, a) {
        return i.zLevel - a.zLevel;
      }) : this._indicators.forEach(function(i) {
        i.sort(function(a, n) {
          return a.zLevel - n.zLevel;
        });
      });
    }, r.prototype._calcIndicator = function(e) {
      var t = this, i = [];
      if (i = i.concat(e), i.length > 0) {
        var a = {};
        i.forEach(function(n) {
          a[n.id] = n.calcImp(t._dataList);
        }), this._taskScheduler.add(a);
      }
    }, r.prototype.addIndicator = function(e, t) {
      var i = e.name, a = this.getIndicatorsByFilter(e);
      if (a.length > 0)
        return !1;
      var n = e.paneId, o = this.getIndicatorsByPaneId(n), s = hi(i), l = new s();
      return this._synchronizeIndicatorSeriesPrecision(l), l.override(e), t || (this.removeIndicator({ paneId: n }), o = []), o.push(l), this._indicators.set(n, o), this._sortIndicators(n), this._calcIndicator(l), !0;
    }, r.prototype.getIndicatorsByPaneId = function(e) {
      var t;
      return (t = this._indicators.get(e)) !== null && t !== void 0 ? t : [];
    }, r.prototype.getIndicatorsByFilter = function(e) {
      var t = e.paneId, i = e.name, a = e.id, n = function(s) {
        return C(a) ? s.id === a : !C(i) || s.name === i;
      }, o = [];
      return C(t) ? o = o.concat(this.getIndicatorsByPaneId(t).filter(n)) : this._indicators.forEach(function(s) {
        o = o.concat(s.filter(n));
      }), o;
    }, r.prototype.removeIndicator = function(e) {
      var t = this, i = !1, a = this.getIndicatorsByFilter(e);
      return a.forEach(function(n) {
        var o = t.getIndicatorsByPaneId(n.paneId), s = o.findIndex(function(l) {
          return l.id === n.id;
        });
        s > -1 && (o.splice(s, 1), i = !0), o.length === 0 && t._indicators.delete(n.paneId);
      }), i;
    }, r.prototype.hasIndicators = function(e) {
      return this._indicators.has(e);
    }, r.prototype._synchronizeIndicatorSeriesPrecision = function(e) {
      if (C(this._symbol)) {
        var t = this._symbol, i = t.pricePrecision, a = i === void 0 ? pt.PRICE : i, n = t.volumePrecision, o = n === void 0 ? pt.VOLUME : n, s = function(l) {
          switch (l.series) {
            case "price": {
              l.setSeriesPrecision(a);
              break;
            }
            case "volume": {
              l.setSeriesPrecision(o);
              break;
            }
          }
        };
        C(e) ? s(e) : this._indicators.forEach(function(l) {
          l.forEach(function(u) {
            s(u);
          });
        });
      }
    }, r.prototype.overrideIndicator = function(e) {
      var t = this, i = !1, a = !1, n = this.getIndicatorsByFilter(e);
      return n.forEach(function(o) {
        var s = o.paneId;
        o.override(e);
        var l = o.paneId;
        if (s !== l) {
          var u = t.getIndicatorsByPaneId(s), c = u.findIndex(function(g) {
            return g.id === o.id;
          });
          c > -1 && u.splice(c, 1), u.length === 0 && t._indicators.delete(s);
          var d = t.getIndicatorsByPaneId(l);
          d.some(function(g) {
            return g.id === o.id;
          }) || (d.push(o), t._indicators.set(l, d)), a = !0;
        }
        var h = o.shouldUpdateImp(), f = h.calc, v = h.draw, p = h.sort;
        p && (a = !0), f ? t._calcIndicator(o) : v && (i = !0);
      }), a && this._sortIndicators(), i || a;
    }, r.prototype.getOverlaysByFilter = function(e) {
      var t, i = e.id, a = e.groupId, n = e.paneId, o = e.name, s = function(c) {
        return C(i) ? c.id === i : C(a) ? c.groupId === a && (!C(o) || c.name === o) : !C(o) || c.name === o;
      }, l = [];
      C(n) ? l = l.concat(this.getOverlaysByPaneId(n).filter(s)) : this._overlays.forEach(function(c) {
        l = l.concat(c.filter(s));
      });
      var u = (t = this._progressOverlayInfo) === null || t === void 0 ? void 0 : t.overlay;
      return C(u) && s(u) && l.push(u), l;
    }, r.prototype.getOverlaysByPaneId = function(e) {
      var t;
      if (!K(e)) {
        var i = [];
        return this._overlays.forEach(function(a) {
          i = i.concat(a);
        }), i;
      }
      return (t = this._overlays.get(e)) !== null && t !== void 0 ? t : [];
    }, r.prototype._sortOverlays = function(e) {
      var t;
      K(e) ? (t = this._overlays.get(e)) === null || t === void 0 || t.sort(function(i, a) {
        return i.zLevel - a.zLevel;
      }) : this._overlays.forEach(function(i) {
        i.sort(function(a, n) {
          return a.zLevel - n.zLevel;
        });
      });
    }, r.prototype.addOverlays = function(e, t) {
      var i = this, a = [], n = e.map(function(o, s) {
        var l, u, c, d, h, f, v, p;
        if (C(o.id)) {
          var g = null;
          try {
            for (var m = bt(i._overlays), x = m.next(); !x.done; x = m.next()) {
              var y = x.value, E = y[1], _ = E.find(function(T) {
                return T.id === o.id;
              });
              if (C(_)) {
                g = _;
                break;
              }
            }
          } catch (T) {
            l = { error: T };
          } finally {
            try {
              x && !x.done && (u = m.return) && u.call(m);
            } finally {
              if (l) throw l.error;
            }
          }
          if (C(g))
            return o.id;
        }
        var I = Dn(o.name);
        if (C(I)) {
          var w = (c = o.id) !== null && c !== void 0 ? c : xe(sn), _ = new I(), b = (d = o.paneId) !== null && d !== void 0 ? d : j.CANDLE;
          o.id = w, (h = o.groupId) !== null && h !== void 0 || (o.groupId = w);
          var S = i.getOverlaysByPaneId(b).length;
          return (f = o.zLevel) !== null && f !== void 0 || (o.zLevel = S), _.override(o), a.includes(b) || a.push(b), _.isDrawing() ? i._progressOverlayInfo = { paneId: b, overlay: _, appointPaneFlag: t[s] } : (i._overlays.has(b) || i._overlays.set(b, []), (v = i._overlays.get(b)) === null || v === void 0 || v.push(_)), _.isStart() && ((p = _.onDrawStart) === null || p === void 0 || p.call(_, { overlay: _, chart: i._chart })), w;
        }
        return null;
      });
      return a.length > 0 && (this._sortOverlays(), a.forEach(function(o) {
        i._chart.updatePane(1, o);
      }), this._chart.updatePane(1, j.X_AXIS)), n;
    }, r.prototype.getProgressOverlayInfo = function() {
      return this._progressOverlayInfo;
    }, r.prototype.progressOverlayComplete = function() {
      var e;
      if (this._progressOverlayInfo !== null) {
        var t = this._progressOverlayInfo, i = t.overlay, a = t.paneId;
        i.isDrawing() || (this._overlays.has(a) || this._overlays.set(a, []), (e = this._overlays.get(a)) === null || e === void 0 || e.push(i), this._sortOverlays(a), this._progressOverlayInfo = null);
      }
    }, r.prototype.updateProgressOverlayInfo = function(e, t) {
      this._progressOverlayInfo !== null && (we(t) && t && (this._progressOverlayInfo.appointPaneFlag = t), this._progressOverlayInfo.paneId = e, this._progressOverlayInfo.overlay.override({ paneId: e }));
    }, r.prototype.overrideOverlay = function(e) {
      var t = this, i = !1, a = [], n = this.getOverlaysByFilter(e);
      return n.forEach(function(o) {
        o.override(e);
        var s = o.shouldUpdate(), l = s.sort, u = s.draw;
        l && (i = !0), (l || u) && (a.includes(o.paneId) || a.push(o.paneId));
      }), i && this._sortOverlays(), a.length > 0 ? (a.forEach(function(o) {
        t._chart.updatePane(1, o);
      }), this._chart.updatePane(1, j.X_AXIS), !0) : !1;
    }, r.prototype.removeOverlay = function(e) {
      var t = this, i = [], a = this.getOverlaysByFilter(e);
      return a.forEach(function(n) {
        var o, s = n.paneId, l = t.getOverlaysByPaneId(n.paneId);
        if ((o = n.onRemoved) === null || o === void 0 || o.call(n, { overlay: n, chart: t._chart }), i.includes(s) || i.push(s), n.isDrawing())
          t._progressOverlayInfo = null;
        else {
          var u = l.findIndex(function(c) {
            return c.id === n.id;
          });
          u > -1 && l.splice(u, 1);
        }
        l.length === 0 && t._overlays.delete(s);
      }), i.length > 0 ? (i.forEach(function(n) {
        t._chart.updatePane(1, n);
      }), this._chart.updatePane(1, j.X_AXIS), !0) : !1;
    }, r.prototype.setPressedOverlayInfo = function(e) {
      this._pressedOverlayInfo = e;
    }, r.prototype.getPressedOverlayInfo = function() {
      return this._pressedOverlayInfo;
    }, r.prototype.setHoverOverlayInfo = function(e, t, i) {
      var a = this._hoverOverlayInfo, n = a.overlay, o = a.figureType, s = a.figureIndex, l = a.figure, u = e.overlay;
      if ((n?.id !== u?.id || o !== e.figureType || s !== e.figureIndex) && (this._hoverOverlayInfo = e, n?.id !== u?.id)) {
        var c = !1, d = !1;
        n !== null && (n.override({ zLevel: n.getPrevZLevel() }), d = !0, i(n, l) && (c = !0)), u !== null && (u.setPrevZLevel(u.zLevel), u.override({ zLevel: Number.MAX_SAFE_INTEGER }), d = !0, t(u, e.figure) && (c = !0)), d && this._sortOverlays(), c || this._chart.updatePane(
          1
          /* UpdateLevel.Overlay */
        );
      }
    }, r.prototype.getHoverOverlayInfo = function() {
      return this._hoverOverlayInfo;
    }, r.prototype.setClickOverlayInfo = function(e, t, i) {
      var a = this._clickOverlayInfo, n = a.paneId, o = a.overlay, s = a.figureType, l = a.figure, u = a.figureIndex, c = e.overlay;
      (o?.id !== c?.id || s !== e.figureType || u !== e.figureIndex) && (this._clickOverlayInfo = e, o?.id !== c?.id && (C(o) && i(o, l), C(c) && t(c, e.figure), this._chart.updatePane(1, e.paneId), n !== e.paneId && this._chart.updatePane(1, n), this._chart.updatePane(1, j.X_AXIS)));
    }, r.prototype.getClickOverlayInfo = function() {
      return this._clickOverlayInfo;
    }, r.prototype.isOverlayEmpty = function() {
      return this._overlays.size === 0 && this._progressOverlayInfo === null;
    }, r.prototype.isOverlayDrawing = function() {
      var e, t;
      return (t = (e = this._progressOverlayInfo) === null || e === void 0 ? void 0 : e.overlay.isDrawing()) !== null && t !== void 0 ? t : !1;
    }, r.prototype._clearLastPriceMarkExtendTextUpdateTimer = function() {
      this._lastPriceMarkExtendTextUpdateTimers.forEach(function(e) {
        clearInterval(e);
      }), this._lastPriceMarkExtendTextUpdateTimers = [];
    }, r.prototype._clearData = function() {
      this._dataLoadMore.backward = !1, this._dataLoadMore.forward = !1, this._loading = !1, this._dataList = [], this._visibleRangeDataList = [], this._visibleRangeHighLowPrice = [
        { x: 0, price: Number.MIN_SAFE_INTEGER },
        { x: 0, price: Number.MAX_SAFE_INTEGER }
      ], this._visibleRange = Br(), this._crosshair = {};
    }, r.prototype.getChart = function() {
      return this._chart;
    }, r.prototype.destroy = function() {
      this._clearData(), this._clearLastPriceMarkExtendTextUpdateTimer(), this._taskScheduler.clear(), this._overlays.clear(), this._indicators.clear(), this._actions.clear();
    }, r;
  })()
), U = {
  MAIN: "main",
  X_AXIS: "xAxis",
  Y_AXIS: "yAxis",
  SEPARATOR: "separator"
}, Re = 7;
function Yn() {
  return vr(this, void 0, void 0, function() {
    return fr(this, function(r) {
      switch (r.label) {
        case 0:
          return [4, new Promise(function(e) {
            var t = new ResizeObserver(function(i) {
              e(i.every(function(a) {
                return "devicePixelContentBoxSize" in a;
              })), t.disconnect();
            });
            t.observe(document.body, { box: "device-pixel-content-box" });
          }).catch(function() {
            return !1;
          })];
        case 1:
          return [2, r.sent()];
      }
    });
  });
}
var Nr = (
  /** @class */
  (function() {
    function r(e, t) {
      var i = this;
      this._supportedDevicePixelContentBox = !1, this._width = 0, this._height = 0, this._pixelWidth = 0, this._pixelHeight = 0, this._nextPixelWidth = 0, this._nextPixelHeight = 0, this._requestAnimationId = Qt, this._mediaQueryListener = function() {
        var a = te(i._element);
        i._nextPixelWidth = Math.round(i._element.clientWidth * a), i._nextPixelHeight = Math.round(i._element.clientHeight * a), i._resetPixelRatio();
      }, this._listener = t, this._element = Gt("canvas", e), this._ctx = this._element.getContext("2d"), Yn().then(function(a) {
        i._supportedDevicePixelContentBox = a, a ? (i._resizeObserver = new ResizeObserver(function(n) {
          var o = n.find(function(l) {
            return l.target === i._element;
          }), s = o?.devicePixelContentBoxSize[0];
          C(s) && (i._nextPixelWidth = s.inlineSize, i._nextPixelHeight = s.blockSize, (i._pixelWidth !== i._nextPixelWidth || i._pixelHeight !== i._nextPixelHeight) && i._resetPixelRatio());
        }), i._resizeObserver.observe(i._element, { box: "device-pixel-content-box" })) : (i._mediaQueryList = window.matchMedia("(resolution: ".concat(te(i._element), "dppx)")), i._mediaQueryList.addListener(i._mediaQueryListener));
      }).catch(function(a) {
        return !1;
      });
    }
    return r.prototype._resetPixelRatio = function() {
      var e = this;
      this._executeListener(function() {
        var t = e._element.clientWidth, i = e._element.clientHeight;
        e._width = t, e._height = i, e._pixelWidth = e._nextPixelWidth, e._pixelHeight = e._nextPixelHeight, e._element.width = e._nextPixelWidth, e._element.height = e._nextPixelHeight;
        var a = e._nextPixelWidth / t, n = e._nextPixelHeight / i;
        e._ctx.scale(a, n);
      });
    }, r.prototype._executeListener = function(e) {
      var t = this;
      this._requestAnimationId === Qt && (this._requestAnimationId = Le(function() {
        t._ctx.clearRect(0, 0, t._width, t._height), e?.(), t._listener(), t._requestAnimationId = Qt;
      }));
    }, r.prototype.update = function(e, t) {
      if (this._width !== e || this._height !== t) {
        if (this._element.style.width = "".concat(e, "px"), this._element.style.height = "".concat(t, "px"), !this._supportedDevicePixelContentBox) {
          var i = te(this._element);
          this._nextPixelWidth = Math.round(e * i), this._nextPixelHeight = Math.round(t * i), this._resetPixelRatio();
        }
      } else
        this._executeListener();
    }, r.prototype.getElement = function() {
      return this._element;
    }, r.prototype.getContext = function() {
      return this._ctx;
    }, r.prototype.destroy = function() {
      C(this._resizeObserver) && this._resizeObserver.unobserve(this._element), C(this._mediaQueryList) && this._mediaQueryList.removeListener(this._mediaQueryListener);
    }, r;
  })()
), mi = (
  /** @class */
  (function(r) {
    q(e, r);
    function e(t, i) {
      var a = r.call(this) || this;
      return a._bounding = pr(), a._cursor = "crosshair", a._forceCursor = null, a._pane = i, a._rootContainer = t, a._container = a.createContainer(), t.appendChild(a._container), a;
    }
    return e.prototype.setBounding = function(t) {
      return dt(this._bounding, t), this;
    }, e.prototype.getContainer = function() {
      return this._container;
    }, e.prototype.getBounding = function() {
      return this._bounding;
    }, e.prototype.getPane = function() {
      return this._pane;
    }, e.prototype.checkEventOn = function(t) {
      return !0;
    }, e.prototype.setCursor = function(t) {
      K(this._forceCursor) || t !== this._cursor && (this._cursor = t, this._container.style.cursor = this._cursor);
    }, e.prototype.setForceCursor = function(t) {
      var i;
      t !== this._forceCursor && (this._forceCursor = t, this._container.style.cursor = (i = this._forceCursor) !== null && i !== void 0 ? i : this._cursor);
    }, e.prototype.getForceCursor = function() {
      return this._forceCursor;
    }, e.prototype.update = function(t) {
      this.updateImp(
        this._container,
        this._bounding,
        t ?? 3
        /* UpdateLevel.Drawer */
      );
    }, e.prototype.destroy = function() {
      this._rootContainer.removeChild(this._container);
    }, e;
  })(_r)
), xr = (
  /** @class */
  (function(r) {
    q(e, r);
    function e(t, i) {
      var a = r.call(this, t, i) || this;
      a._mainCanvas = new Nr({
        position: "absolute",
        top: "0",
        left: "0",
        zIndex: "2",
        boxSizing: "border-box"
      }, function() {
        a.updateMain(a._mainCanvas.getContext());
      }), a._overlayCanvas = new Nr({
        position: "absolute",
        top: "0",
        left: "0",
        zIndex: "2",
        boxSizing: "border-box"
      }, function() {
        a.updateOverlay(a._overlayCanvas.getContext());
      });
      var n = a.getContainer();
      return n.appendChild(a._mainCanvas.getElement()), n.appendChild(a._overlayCanvas.getElement()), a;
    }
    return e.prototype.createContainer = function() {
      return Gt("div", {
        margin: "0",
        padding: "0",
        position: "absolute",
        top: "0",
        overflow: "hidden",
        boxSizing: "border-box",
        zIndex: "1"
      });
    }, e.prototype.updateImp = function(t, i, a) {
      var n = i.width, o = i.height, s = i.left;
      t.style.left = "".concat(s, "px");
      var l = a, u = t.clientWidth, c = t.clientHeight;
      switch ((n !== u || o !== c) && (t.style.width = "".concat(n, "px"), t.style.height = "".concat(o, "px"), l = 3), l) {
        case 0: {
          this._mainCanvas.update(n, o);
          break;
        }
        case 1: {
          this._overlayCanvas.update(n, o);
          break;
        }
        case 3:
        case 4: {
          this._mainCanvas.update(n, o), this._overlayCanvas.update(n, o);
          break;
        }
      }
    }, e.prototype.destroy = function() {
      this._mainCanvas.destroy(), this._overlayCanvas.destroy(), r.prototype.destroy.call(this);
    }, e.prototype.getImage = function(t) {
      var i = this.getBounding(), a = i.width, n = i.height, o = Gt("canvas", {
        width: "".concat(a, "px"),
        height: "".concat(n, "px"),
        boxSizing: "border-box"
      }), s = o.getContext("2d"), l = te(o);
      return o.width = a * l, o.height = n * l, s.scale(l, l), s.drawImage(this._mainCanvas.getElement(), 0, 0, a, n), t && s.drawImage(this._overlayCanvas.getElement(), 0, 0, a, n), o;
    }, e;
  })(mi)
);
function Wn(r, e) {
  var t, i, a = [];
  a = a.concat(e);
  try {
    for (var n = bt(a), o = n.next(); !o.done; o = n.next()) {
      var s = o.value, l = s.x, u = s.y, c = s.r, d = r.x - l, h = r.y - u;
      if (!(d * d + h * h > c * c))
        return !0;
    }
  } catch (f) {
    t = { error: f };
  } finally {
    try {
      o && !o.done && (i = n.return) && i.call(n);
    } finally {
      if (t) throw t.error;
    }
  }
  return !1;
}
function zn(r, e, t) {
  var i = [];
  i = i.concat(e);
  var a = t.style, n = a === void 0 ? "fill" : a, o = t.color, s = o === void 0 ? "currentColor" : o, l = t.borderSize, u = l === void 0 ? 1 : l, c = t.borderColor, d = c === void 0 ? "currentColor" : c, h = t.borderStyle, f = h === void 0 ? "solid" : h, v = t.borderDashedValue, p = v === void 0 ? [2, 2] : v, g = (n === "fill" || t.style === "stroke_fill") && (!K(s) || !Ce(s));
  g && (r.fillStyle = s, i.forEach(function(m) {
    var x = m.x, y = m.y, E = m.r;
    r.beginPath(), r.arc(x, y, E, 0, Math.PI * 2), r.closePath(), r.fill();
  })), (n === "stroke" || t.style === "stroke_fill") && u > 0 && !Ce(d) && (r.strokeStyle = d, r.lineWidth = u, f === "dashed" ? r.setLineDash(p) : r.setLineDash([]), i.forEach(function(m) {
    var x = m.x, y = m.y, E = m.r;
    (!g || E > u) && (r.beginPath(), r.arc(x, y, E, 0, Math.PI * 2), r.closePath(), r.stroke());
  }));
}
var $n = {
  name: "circle",
  checkEventOn: Wn,
  draw: function(r, e, t) {
    zn(r, e, t);
  }
};
function Xn(r, e) {
  var t, i, a = [];
  a = a.concat(e);
  try {
    for (var n = bt(a), o = n.next(); !o.done; o = n.next()) {
      for (var s = o.value, l = !1, u = s.coordinates, c = 0, d = u.length - 1; c < u.length; d = c++)
        u[c].y > r.y != u[d].y > r.y && r.x < (u[d].x - u[c].x) * (r.y - u[c].y) / (u[d].y - u[c].y) + u[c].x && (l = !l);
      if (l)
        return !0;
    }
  } catch (h) {
    t = { error: h };
  } finally {
    try {
      o && !o.done && (i = n.return) && i.call(n);
    } finally {
      if (t) throw t.error;
    }
  }
  return !1;
}
function qn(r, e, t) {
  var i = [];
  i = i.concat(e);
  var a = t.style, n = a === void 0 ? "fill" : a, o = t.color, s = o === void 0 ? "currentColor" : o, l = t.borderSize, u = l === void 0 ? 1 : l, c = t.borderColor, d = c === void 0 ? "currentColor" : c, h = t.borderStyle, f = h === void 0 ? "solid" : h, v = t.borderDashedValue, p = v === void 0 ? [2, 2] : v;
  (n === "fill" || t.style === "stroke_fill") && (!K(s) || !Ce(s)) && (r.fillStyle = s, i.forEach(function(g) {
    var m = g.coordinates;
    r.beginPath(), r.moveTo(m[0].x, m[0].y);
    for (var x = 1; x < m.length; x++)
      r.lineTo(m[x].x, m[x].y);
    r.closePath(), r.fill();
  })), (n === "stroke" || t.style === "stroke_fill") && u > 0 && !Ce(d) && (r.strokeStyle = d, r.lineWidth = u, f === "dashed" ? r.setLineDash(p) : r.setLineDash([]), i.forEach(function(g) {
    var m = g.coordinates;
    r.beginPath(), r.moveTo(m[0].x, m[0].y);
    for (var x = 1; x < m.length; x++)
      r.lineTo(m[x].x, m[x].y);
    r.closePath(), r.stroke();
  }));
}
var Hn = {
  name: "polygon",
  checkEventOn: Xn,
  draw: function(r, e, t) {
    qn(r, e, t);
  }
};
function _i(r, e) {
  var t, i, a = [];
  a = a.concat(e);
  try {
    for (var n = bt(a), o = n.next(); !o.done; o = n.next()) {
      var s = o.value, l = s.x, u = s.width;
      u < vt * 2 && (l -= vt, u = vt * 2);
      var c = s.y, d = s.height;
      if (d < vt * 2 && (c -= vt, d = vt * 2), r.x >= l && r.x <= l + u && r.y >= c && r.y <= c + d)
        return !0;
    }
  } catch (h) {
    t = { error: h };
  } finally {
    try {
      o && !o.done && (i = n.return) && i.call(n);
    } finally {
      if (t) throw t.error;
    }
  }
  return !1;
}
function yi(r, e, t) {
  var i, a = [];
  a = a.concat(e);
  var n = t.style, o = n === void 0 ? "fill" : n, s = t.color, l = s === void 0 ? "transparent" : s, u = t.borderSize, c = u === void 0 ? 1 : u, d = t.borderColor, h = d === void 0 ? "transparent" : d, f = t.borderStyle, v = f === void 0 ? "solid" : f, p = t.borderRadius, g = p === void 0 ? 0 : p, m = t.borderDashedValue, x = m === void 0 ? [2, 2] : m, y = (i = r.roundRect) !== null && i !== void 0 ? i : r.rect, E = (o === "fill" || t.style === "stroke_fill") && (!K(l) || !Ce(l));
  if (E && (r.fillStyle = l, a.forEach(function(w) {
    var b = w.x, S = w.y, T = w.width, A = w.height;
    r.beginPath(), y.call(r, b, S, T, A, g), r.closePath(), r.fill();
  })), (o === "stroke" || t.style === "stroke_fill") && c > 0 && !Ce(h)) {
    r.strokeStyle = h, r.fillStyle = h, r.lineWidth = c, v === "dashed" ? r.setLineDash(x) : r.setLineDash([]);
    var _ = c % 2 === 1 ? 0.5 : 0, I = Math.round(_ * 2);
    a.forEach(function(w) {
      var b = w.x, S = w.y, T = w.width, A = w.height;
      T > c * 2 && A > c * 2 ? (r.beginPath(), y.call(r, b + _, S + _, T - I, A - I, g), r.closePath(), r.stroke()) : E || r.fillRect(b, S, T, A);
    });
  }
}
var Un = {
  name: "rect",
  checkEventOn: _i,
  draw: function(r, e, t) {
    yi(r, e, t);
  }
};
function xi(r, e) {
  var t = e.size, i = t === void 0 ? 12 : t, a = e.paddingLeft, n = a === void 0 ? 0 : a, o = e.paddingTop, s = o === void 0 ? 0 : o, l = e.paddingRight, u = l === void 0 ? 0 : l, c = e.paddingBottom, d = c === void 0 ? 0 : c, h = e.weight, f = h === void 0 ? "normal" : h, v = e.family, p = r.x, g = r.y, m = r.text, x = r.align, y = x === void 0 ? "left" : x, E = r.baseline, _ = E === void 0 ? "top" : E, I = r.width, w = r.height, b = I ?? n + Wt(m, i, f, v) + u, S = w ?? s + i + d, T = 0;
  switch (y) {
    case "left":
    case "start": {
      T = p;
      break;
    }
    case "right":
    case "end": {
      T = p - b;
      break;
    }
    default: {
      T = p - b / 2;
      break;
    }
  }
  var A = 0;
  switch (_) {
    case "top":
    case "hanging": {
      A = g;
      break;
    }
    case "bottom":
    case "ideographic":
    case "alphabetic": {
      A = g - S;
      break;
    }
    default: {
      A = g - S / 2;
      break;
    }
  }
  return { x: T, y: A, width: b, height: S };
}
function Gn(r, e, t) {
  var i, a, n = [];
  n = n.concat(e);
  try {
    for (var o = bt(n), s = o.next(); !s.done; s = o.next()) {
      var l = s.value, u = xi(l, t), c = u.x, d = u.y, h = u.width, f = u.height;
      if (r.x >= c && r.x <= c + h && r.y >= d && r.y <= d + f)
        return !0;
    }
  } catch (v) {
    i = { error: v };
  } finally {
    try {
      s && !s.done && (a = o.return) && a.call(o);
    } finally {
      if (i) throw i.error;
    }
  }
  return !1;
}
function jn(r, e, t) {
  var i = [];
  i = i.concat(e);
  var a = t.color, n = a === void 0 ? "currentColor" : a, o = t.size, s = o === void 0 ? 12 : o, l = t.family, u = t.weight, c = t.paddingLeft, d = c === void 0 ? 0 : c, h = t.paddingTop, f = h === void 0 ? 0 : h, v = t.paddingRight, p = v === void 0 ? 0 : v, g = i.map(function(m) {
    return xi(m, t);
  });
  yi(r, g, M(M({}, t), { color: t.backgroundColor })), r.textAlign = "left", r.textBaseline = "top", r.font = he(s, u, l), r.fillStyle = n, i.forEach(function(m, x) {
    var y = g[x];
    r.fillText(m.text, y.x + d, y.y + f, y.width - d - p);
  });
}
var Zn = {
  name: "text",
  checkEventOn: Gn,
  draw: function(r, e, t) {
    jn(r, e, t);
  }
};
function Kn(r, e) {
  var t = r.x - e.x, i = r.y - e.y;
  return Math.sqrt(t * t + i * i);
}
function Jn(r, e) {
  var t, i, a = [];
  a = a.concat(e);
  try {
    for (var n = bt(a), o = n.next(); !o.done; o = n.next()) {
      var s = o.value;
      if (Math.abs(Kn(r, s) - s.r) < vt) {
        var l = s.r, u = s.startAngle, c = s.endAngle, d = l * Math.cos(u) + s.x, h = l * Math.sin(u) + s.y, f = l * Math.cos(c) + s.x, v = l * Math.sin(c) + s.y;
        if (r.x <= Math.max(d, f) + vt && r.x >= Math.min(d, f) - vt && r.y <= Math.max(h, v) + vt && r.y >= Math.min(h, v) - vt)
          return !0;
      }
    }
  } catch (p) {
    t = { error: p };
  } finally {
    try {
      o && !o.done && (i = n.return) && i.call(n);
    } finally {
      if (t) throw t.error;
    }
  }
  return !1;
}
function Qn(r, e, t) {
  var i = [];
  i = i.concat(e);
  var a = t.style, n = a === void 0 ? "solid" : a, o = t.size, s = o === void 0 ? 1 : o, l = t.color, u = l === void 0 ? "currentColor" : l, c = t.dashedValue, d = c === void 0 ? [2, 2] : c;
  r.lineWidth = s, r.strokeStyle = u, n === "dashed" ? r.setLineDash(d) : r.setLineDash([]), i.forEach(function(h) {
    var f = h.x, v = h.y, p = h.r, g = h.startAngle, m = h.endAngle;
    r.beginPath(), r.arc(f, v, p, g, m), r.stroke(), r.closePath();
  });
}
var to = {
  name: "arc",
  checkEventOn: Jn,
  draw: function(r, e, t) {
    Qn(r, e, t);
  }
};
function Yr(r, e, t, i, a, n, o) {
  var s = Se(i, 7), l = s[0], u = s[1], c = s[2], d = s[3], h = s[4], f = s[5], v = s[6], p = o ? e + f : f + a, g = o ? t + v : v + n, m = eo(e, t, l, u, c, d, h, p, g);
  m.forEach(function(x) {
    r.bezierCurveTo(x[0], x[1], x[2], x[3], x[4], x[5]);
  });
}
function eo(r, e, t, i, a, n, o, s, l) {
  for (var u = ro(r, e, t, i, a, n, o, s, l), c = u.cx, d = u.cy, h = u.startAngle, f = u.deltaAngle, v = [], p = Math.ceil(Math.abs(f) / (Math.PI / 2)), g = 0; g < p; g++) {
    var m = h + g * f / p, x = h + (g + 1) * f / p, y = io(c, d, t, i, a, m, x);
    v.push(y);
  }
  return v;
}
function ro(r, e, t, i, a, n, o, s, l) {
  var u = a * Math.PI / 180, c = (r - s) / 2, d = (e - l) / 2, h = Math.cos(u) * c + Math.sin(u) * d, f = -Math.sin(u) * c + Math.cos(u) * d, v = Math.pow(h, 2) / Math.pow(t, 2) + Math.pow(f, 2) / Math.pow(i, 2);
  v > 1 && (t *= Math.sqrt(v), i *= Math.sqrt(v));
  var p = n === o ? -1 : 1, g = Math.pow(t, 2) * Math.pow(i, 2) - Math.pow(t, 2) * Math.pow(f, 2) - Math.pow(i, 2) * Math.pow(h, 2), m = Math.pow(t, 2) * Math.pow(f, 2) + Math.pow(i, 2) * Math.pow(h, 2), x = p * Math.sqrt(Math.abs(g / m)) * (t * f / i), y = p * Math.sqrt(Math.abs(g / m)) * (-i * h / t), E = Math.cos(u) * x - Math.sin(u) * y + (r + s) / 2, _ = Math.sin(u) * x + Math.cos(u) * y + (e + l) / 2, I = Math.atan2((f - y) / i, (h - x) / t), w = Math.atan2((-f - y) / i, (-h - x) / t) - I;
  return w < 0 && o === 1 ? w += 2 * Math.PI : w > 0 && o === 0 && (w -= 2 * Math.PI), { cx: E, cy: _, startAngle: I, deltaAngle: w };
}
function io(r, e, t, i, a, n, o) {
  var s = Math.sin(o - n) * (Math.sqrt(4 + 3 * Math.pow(Math.tan((o - n) / 2), 2)) - 1) / 3, l = Math.cos(a), u = Math.sin(a), c = r + t * Math.cos(n) * l - i * Math.sin(n) * u, d = e + t * Math.cos(n) * u + i * Math.sin(n) * l, h = r + t * Math.cos(o) * l - i * Math.sin(o) * u, f = e + t * Math.cos(o) * u + i * Math.sin(o) * l, v = c + s * (-t * Math.sin(n) * l - i * Math.cos(n) * u), p = d + s * (-t * Math.sin(n) * u + i * Math.cos(n) * l), g = h - s * (-t * Math.sin(o) * l - i * Math.cos(o) * u), m = f - s * (-t * Math.sin(o) * u + i * Math.cos(o) * l);
  return [v, p, g, m, h, f];
}
function ao(r, e, t) {
  var i = [];
  i = i.concat(e);
  var a = t.lineWidth, n = a === void 0 ? 1 : a, o = t.color, s = o === void 0 ? "currentColor" : o;
  r.lineWidth = n, r.strokeStyle = s, r.setLineDash([]), i.forEach(function(l) {
    var u = l.x, c = l.y, d = l.path, h = d.match(/[MLHVCSQTAZ][^MLHVCSQTAZ]*/gi);
    if (C(h)) {
      var f = u, v = c;
      r.beginPath(), h.forEach(function(p) {
        var g = 0, m = 0, x = 0, y = 0, E = p[0], _ = p.slice(1).trim().split(/[\s,]+/).map(Number);
        switch (E) {
          case "M":
            g = _[0] + f, m = _[1] + v, r.moveTo(g, m), x = g, y = m;
            break;
          case "m":
            g += _[0], m += _[1], r.moveTo(g, m), x = g, y = m;
            break;
          case "L":
            g = _[0] + f, m = _[1] + v, r.lineTo(g, m);
            break;
          case "l":
            g += _[0], m += _[1], r.lineTo(g, m);
            break;
          case "H":
            g = _[0] + f, r.lineTo(g, m);
            break;
          case "h":
            g += _[0], r.lineTo(g, m);
            break;
          case "V":
            m = _[0] + v, r.lineTo(g, m);
            break;
          case "v":
            m += _[0], r.lineTo(g, m);
            break;
          case "C":
            r.bezierCurveTo(_[0] + f, _[1] + v, _[2] + f, _[3] + v, _[4] + f, _[5] + v), g = _[4] + f, m = _[5] + v;
            break;
          case "c":
            r.bezierCurveTo(g + _[0], m + _[1], g + _[2], m + _[3], g + _[4], m + _[5]), g += _[4], m += _[5];
            break;
          case "S":
            r.bezierCurveTo(g, m, _[0] + f, _[1] + v, _[2] + f, _[3] + v), g = _[2] + f, m = _[3] + v;
            break;
          case "s":
            r.bezierCurveTo(g, m, g + _[0], m + _[1], g + _[2], m + _[3]), g += _[2], m += _[3];
            break;
          case "Q":
            r.quadraticCurveTo(_[0] + f, _[1] + v, _[2] + f, _[3] + v), g = _[2] + f, m = _[3] + v;
            break;
          case "q":
            r.quadraticCurveTo(g + _[0], m + _[1], g + _[2], m + _[3]), g += _[2], m += _[3];
            break;
          case "T":
            r.quadraticCurveTo(g, m, _[0] + f, _[1] + v), g = _[0] + f, m = _[1] + v;
            break;
          case "t":
            r.quadraticCurveTo(g, m, g + _[0], m + _[1]), g += _[0], m += _[1];
            break;
          case "A":
            Yr(r, g, m, _, f, v, !1), g = _[5] + f, m = _[6] + v;
            break;
          case "a":
            Yr(r, g, m, _, f, v, !0), g += _[5], m += _[6];
            break;
          case "Z":
          case "z":
            r.closePath(), g = x, m = y;
            break;
        }
      }), t.style === "fill" ? r.fill() : r.stroke();
    }
  });
}
var no = {
  name: "path",
  checkEventOn: _i,
  draw: function(r, e, t) {
    ao(r, e, t);
  }
}, wi = {}, oo = [$n, gn, Hn, Un, Zn, to, no];
oo.forEach(function(r) {
  wi[r.name] = vn.extend(r);
});
function so(r) {
  var e;
  return (e = wi[r]) !== null && e !== void 0 ? e : null;
}
var Dt = (
  /** @class */
  (function(r) {
    q(e, r);
    function e(t) {
      var i = r.call(this) || this;
      return i._widget = t, i;
    }
    return e.prototype.getWidget = function() {
      return this._widget;
    }, e.prototype.createFigure = function(t, i) {
      var a = so(t.name);
      if (a !== null) {
        var n = new a(t);
        if (C(i)) {
          for (var o in i)
            i.hasOwnProperty(o) && n.registerEvent(o, i[o]);
          this.addChild(n);
        }
        return n;
      }
      return null;
    }, e.prototype.draw = function(t) {
      this.clear(), this.drawImp(t);
    }, e.prototype.checkEventOn = function(t) {
      return !0;
    }, e;
  })(_r)
), lo = (
  /** @class */
  (function(r) {
    q(e, r);
    function e() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return e.prototype.drawImp = function(t) {
      var i, a, n = this.getWidget(), o = this.getWidget().getPane(), s = o.getChart(), l = n.getBounding(), u = s.getStyles().grid, c = u.show;
      if (c) {
        t.save(), t.globalCompositeOperation = "destination-over";
        var d = u.horizontal, h = d.show;
        if (h) {
          var f = o.getYAxisComponentById(), v = f.getTicks().map(function(x) {
            return {
              coordinates: [
                { x: 0, y: x.coord },
                { x: l.width, y: x.coord }
              ]
            };
          });
          (i = this.createFigure({
            name: "line",
            attrs: v,
            styles: d
          })) === null || i === void 0 || i.draw(t);
        }
        var p = u.vertical, g = p.show;
        if (g) {
          var m = s.getXAxisPane().getXAxisComponent(), v = m.getTicks().map(function(y) {
            return {
              coordinates: [
                { x: y.coord, y: 0 },
                { x: y.coord, y: l.height }
              ]
            };
          });
          (a = this.createFigure({
            name: "line",
            attrs: v,
            styles: p
          })) === null || a === void 0 || a.draw(t);
        }
        t.restore();
      }
    }, e;
  })(Dt)
), bi = (
  /** @class */
  (function(r) {
    q(e, r);
    function e() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return e.prototype.eachChildren = function(t) {
      for (var i = this.getWidget().getPane(), a = i.getChart().getChartStore(), n = a.getVisibleRangeDataList(), o = a.getBarSpace(), s = n.length, l = 0; l < s; )
        t(n[l], o, l), ++l;
    }, e;
  })(Dt)
), Ci = (
  /** @class */
  (function(r) {
    q(e, r);
    function e() {
      var t = r.apply(this, be([], Se(arguments), !1)) || this;
      return t._boundCandleBarClickEvent = function(i) {
        return function() {
          return t.getWidget().getPane().getChart().getChartStore().executeAction("onCandleBarClick", i), !1;
        };
      }, t;
    }
    return e.prototype.drawImp = function(t) {
      var i = this, a = this.getWidget().getPane(), n = a.getId() === j.CANDLE, o = a.getChart().getChartStore(), s = this.getCandleBarOptions();
      if (s !== null) {
        var l = s.type, u = s.styles, c = 0, d = 0;
        if (s.type === "ohlc") {
          var h = o.getBarSpace().gapBar;
          c = Math.min(Math.max(Math.round(h * 0.2), 1), 8), c > 2 && c % 2 === 1 && c--, d = Math.floor(c / 2);
        }
        var f = a.getYAxisComponentById(s.yAxisId);
        this.eachChildren(function(v, p) {
          var g, m = v.x, x = v.data, y = x.current, E = x.prev;
          if (C(y)) {
            var _ = y.open, I = y.high, w = y.low, b = y.close, S = u.compareRule === "current_open" ? _ : (g = E?.close) !== null && g !== void 0 ? g : b, T = [];
            b > S ? (T[0] = u.upColor, T[1] = u.upBorderColor, T[2] = u.upWickColor) : b < S ? (T[0] = u.downColor, T[1] = u.downBorderColor, T[2] = u.downWickColor) : (T[0] = u.noChangeColor, T[1] = u.noChangeBorderColor, T[2] = u.noChangeWickColor);
            var A = f.convertToPixel(_), D = f.convertToPixel(b), R = [
              A,
              D,
              f.convertToPixel(I),
              f.convertToPixel(w)
            ];
            R.sort(function(L, F) {
              return L - F;
            });
            var P = p.gapBar % 2 === 0 ? 1 : 0, k = [];
            switch (l) {
              case "candle_solid": {
                k = i._createSolidBar(m, R, p, T, P);
                break;
              }
              case "candle_stroke": {
                k = i._createStrokeBar(m, R, p, T, P);
                break;
              }
              case "candle_up_stroke": {
                b > _ ? k = i._createStrokeBar(m, R, p, T, P) : k = i._createSolidBar(m, R, p, T, P);
                break;
              }
              case "candle_down_stroke": {
                _ > b ? k = i._createStrokeBar(m, R, p, T, P) : k = i._createSolidBar(m, R, p, T, P);
                break;
              }
              case "ohlc": {
                k = [
                  {
                    name: "rect",
                    attrs: [
                      {
                        x: m - d,
                        y: R[0],
                        width: c,
                        height: R[3] - R[0]
                      },
                      {
                        x: m - p.halfGapBar,
                        y: A + c > R[3] ? R[3] - c : A,
                        width: p.halfGapBar - d,
                        height: c
                      },
                      {
                        x: m + d,
                        y: D + c > R[3] ? R[3] - c : D,
                        width: p.halfGapBar - d,
                        height: c
                      }
                    ],
                    styles: { color: T[0] }
                  }
                ];
                break;
              }
            }
            k.forEach(function(L) {
              var F, O = null;
              n && (O = {
                mouseClickEvent: i._boundCandleBarClickEvent(v)
              }), (F = i.createFigure(L, O ?? void 0)) === null || F === void 0 || F.draw(t);
            });
          }
        });
      }
    }, e.prototype.getCandleBarOptions = function() {
      var t = this.getWidget().getPane(), i = t.getDefaultYAxisId();
      if (!C(i))
        return null;
      var a = t.getChart().getStyles().candle;
      return {
        yAxisId: i,
        type: a.type,
        styles: a.bar
      };
    }, e.prototype._createSolidBar = function(t, i, a, n, o) {
      return [
        {
          name: "rect",
          attrs: {
            x: t,
            y: i[0],
            width: 1,
            height: i[3] - i[0]
          },
          styles: { color: n[2] }
        },
        {
          name: "rect",
          attrs: {
            x: t - a.halfGapBar,
            y: i[1],
            width: a.gapBar + o,
            height: Math.max(1, i[2] - i[1])
          },
          styles: {
            style: "stroke_fill",
            color: n[0],
            borderColor: n[1]
          }
        }
      ];
    }, e.prototype._createStrokeBar = function(t, i, a, n, o) {
      return [
        {
          name: "rect",
          attrs: [
            {
              x: t,
              y: i[0],
              width: 1,
              height: i[1] - i[0]
            },
            {
              x: t,
              y: i[2],
              width: 1,
              height: i[3] - i[2]
            }
          ],
          styles: { color: n[2] }
        },
        {
          name: "rect",
          attrs: {
            x: t - a.halfGapBar,
            y: i[1],
            width: a.gapBar + o,
            height: Math.max(1, i[2] - i[1])
          },
          styles: {
            style: "stroke",
            borderColor: n[1]
          }
        }
      ];
    }, e;
  })(bi)
), uo = (
  /** @class */
  (function(r) {
    q(e, r);
    function e() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return e.prototype.getCandleBarOptions = function() {
      var t, i, a = this.getWidget().getPane(), n = a.getChart().getChartStore(), o = n.getIndicatorsByPaneId(a.getId());
      try {
        for (var s = bt(o), l = s.next(); !l.done; l = s.next()) {
          var u = l.value, c = a.getYAxisComponentById(u.yAxisId);
          if (u.shouldOhlc && u.visible && !c.isInCandle()) {
            var d = u.styles, h = n.getStyles().indicator, f = ht(d, "ohlc.compareRule", h.ohlc.compareRule), v = ht(d, "ohlc.upColor", h.ohlc.upColor), p = ht(d, "ohlc.downColor", h.ohlc.downColor), g = ht(d, "ohlc.noChangeColor", h.ohlc.noChangeColor);
            return {
              yAxisId: u.yAxisId,
              type: "ohlc",
              styles: {
                compareRule: f,
                upColor: v,
                downColor: p,
                noChangeColor: g,
                upBorderColor: v,
                downBorderColor: p,
                noChangeBorderColor: g,
                upWickColor: v,
                downWickColor: p,
                noChangeWickColor: g
              }
            };
          }
        }
      } catch (m) {
        t = { error: m };
      } finally {
        try {
          l && !l.done && (i = s.return) && i.call(s);
        } finally {
          if (t) throw t.error;
        }
      }
      return null;
    }, e.prototype.drawImp = function(t) {
      var i = this;
      r.prototype.drawImp.call(this, t);
      var a = this.getWidget(), n = a.getPane(), o = n.getChart(), s = a.getBounding(), l = o.getXAxisPane().getXAxisComponent(), u = o.getChartStore(), c = u.getIndicatorsByPaneId(n.getId()), d = u.getStyles().indicator;
      t.save(), c.forEach(function(h) {
        var f = n.getYAxisComponentById(h.yAxisId);
        if (h.visible) {
          h.zLevel < 0 ? t.globalCompositeOperation = "destination-over" : t.globalCompositeOperation = "source-over";
          var v = !1;
          if (h.draw !== null && (t.save(), v = h.draw({
            ctx: t,
            chart: o,
            indicator: h,
            bounding: s,
            xAxis: l,
            yAxis: f
          }), t.restore()), !v) {
            var p = h.result, g = [];
            i.eachChildren(function(m, x) {
              var y, E, _, I = x.halfGapBar, w = m.dataIndex, b = m.x, S = l.convertToPixel(w - 1), T = l.convertToPixel(w + 1), A = (y = p[w - 1]) !== null && y !== void 0 ? y : null, D = (E = p[w]) !== null && E !== void 0 ? E : null, R = (_ = p[w + 1]) !== null && _ !== void 0 ? _ : null, P = { x: S }, k = { x: b }, L = { x: T };
              h.figures.forEach(function(F) {
                var O = F.key, W = A?.[O];
                B(W) && (P[O] = f.convertToPixel(W));
                var Q = D?.[O];
                B(Q) && (k[O] = f.convertToPixel(Q));
                var it = R?.[O];
                B(it) && (L[O] = f.convertToPixel(it));
              }), gr(h, w, x, d, function(F, O, W) {
                var Q, it, tt, rt, nt;
                if (C(D?.[F.key])) {
                  var ut = k[F.key], z = (Q = F.attrs) === null || Q === void 0 ? void 0 : Q.call(F, {
                    data: { prev: A, current: D, next: R },
                    coordinate: { prev: P, current: k, next: L },
                    bounding: s,
                    barSpace: x,
                    xAxis: l,
                    yAxis: f
                  });
                  switch (F.type) {
                    case "text": {
                      z = M({
                        x: b,
                        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment -- ignore
                        y: ut,
                        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment -- ignore
                        text: D?.[F.key],
                        align: "center",
                        baseline: "middle"
                      }, z);
                      break;
                    }
                    case "circle": {
                      z = M({ x: b, y: ut, r: Math.max(1, I) }, z);
                      break;
                    }
                    case "rect":
                    case "bar": {
                      var G = (it = F.baseValue) !== null && it !== void 0 ? it : f.getRange().from, $ = f.convertToPixel(G), Z = Math.abs($ - ut);
                      G !== D?.[F.key] && (Z = Math.max(1, Z));
                      var H = 0;
                      ut > $ ? H = $ : H = ut;
                      var st = (tt = z?.width) !== null && tt !== void 0 ? tt : I * 2;
                      z = M({ x: b - st / 2, y: H, width: Math.max(1, st), height: Z }, z);
                      break;
                    }
                    case "line": {
                      C(g[W]) || (g[W] = []), B(k[F.key]) && B(L[F.key]) && g[W].push({
                        coordinates: (rt = z?.coordinates) !== null && rt !== void 0 ? rt : [
                          // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment -- ignore
                          { x: k.x, y: k[F.key] },
                          // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment -- ignore
                          { x: L.x, y: L[F.key] }
                        ],
                        styles: O
                      });
                      break;
                    }
                  }
                  var J = F.type;
                  C(z) && J !== "line" && ((nt = i.createFigure({
                    name: J === "bar" ? "rect" : J,
                    attrs: z,
                    styles: O
                  })) === null || nt === void 0 || nt.draw(t));
                }
              });
            }), g.forEach(function(m) {
              var x, y, E, _;
              if (m.length > 1) {
                for (var I = [
                  {
                    coordinates: [m[0].coordinates[0], m[0].coordinates[1]],
                    styles: m[0].styles
                  }
                ], w = 1; w < m.length; w++) {
                  var b = I[I.length - 1], S = m[w], T = b.coordinates[b.coordinates.length - 1];
                  T.x === S.coordinates[0].x && T.y === S.coordinates[0].y && b.styles.style === S.styles.style && b.styles.color === S.styles.color && b.styles.size === S.styles.size && b.styles.smooth === S.styles.smooth && ((x = b.styles.dashedValue) === null || x === void 0 ? void 0 : x[0]) === ((y = S.styles.dashedValue) === null || y === void 0 ? void 0 : y[0]) && ((E = b.styles.dashedValue) === null || E === void 0 ? void 0 : E[1]) === ((_ = S.styles.dashedValue) === null || _ === void 0 ? void 0 : _[1]) ? b.coordinates.push(S.coordinates[1]) : I.push({
                    coordinates: [S.coordinates[0], S.coordinates[1]],
                    styles: S.styles
                  });
                }
                I.forEach(function(A) {
                  var D, R = A.coordinates, P = A.styles;
                  (D = i.createFigure({
                    name: "line",
                    attrs: { coordinates: R },
                    styles: P
                  })) === null || D === void 0 || D.draw(t);
                });
              }
            });
          }
        }
      }), t.restore();
    }, e;
  })(Ci)
), co = (
  /** @class */
  (function(r) {
    q(e, r);
    function e() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return e.prototype.drawImp = function(t) {
      var i = this.getWidget(), a = i.getPane(), n = i.getBounding(), o = i.getPane().getChart().getChartStore(), s = o.getCrosshair(), l = o.getStyles().crosshair;
      if (K(s.paneId) && l.show) {
        if (s.paneId === a.getId()) {
          var u = s.y;
          this._drawLine(t, [
            { x: 0, y: u },
            { x: n.width, y: u }
          ], l.horizontal);
        }
        var c = s.realX;
        this._drawLine(t, [
          { x: c, y: 0 },
          { x: c, y: n.height }
        ], l.vertical);
      }
    }, e.prototype._drawLine = function(t, i, a) {
      var n;
      if (a.show) {
        var o = a.line;
        o.show && ((n = this.createFigure({
          name: "line",
          attrs: { coordinates: i },
          styles: o
        })) === null || n === void 0 || n.draw(t));
      }
    }, e;
  })(Dt)
), Ei = (
  /** @class */
  (function(r) {
    q(e, r);
    function e(t) {
      var i = r.call(this, t) || this;
      return i._activeFeatureInfo = null, i._featureClickEvent = function(a, n) {
        return function() {
          var o = i.getWidget().getPane();
          return o.getChart().getChartStore().executeAction(a, n), !0;
        };
      }, i._featureMouseMoveEvent = function(a) {
        return function() {
          return i._activeFeatureInfo = a, !0;
        };
      }, i.registerEvent("mouseMoveEvent", function(a) {
        return i._activeFeatureInfo = null, !1;
      }), i;
    }
    return e.prototype.drawImp = function(t) {
      var i = this.getWidget(), a = i.getPane(), n = a.getChart().getChartStore(), o = n.getCrosshair();
      if (C(o.kLineData)) {
        var s = i.getBounding(), l = n.getStyles().indicator.tooltip, u = l.offsetLeft, c = l.offsetTop, d = l.offsetRight;
        this.drawIndicatorTooltip(t, u, c, s.width - d);
      }
    }, e.prototype.drawIndicatorTooltip = function(t, i, a, n) {
      var o = this, s = this.getWidget().getPane(), l = s.getChart().getChartStore(), u = l.getStyles().indicator, c = u.tooltip;
      if (this.isDrawTooltip(l.getCrosshair(), c)) {
        var d = l.getIndicatorsByPaneId(s.getId()), h = c.title, f = c.legend;
        d.forEach(function(v) {
          var p = 0, g = { x: i, y: a }, m = o.getIndicatorTooltipData(v), x = m.name, y = m.calcParamsText, E = m.legends, _ = m.features, I = x.length > 0, w = E.length > 0;
          if (I || w) {
            var b = o.classifyTooltipFeatures(_);
            if (p = o.drawStandardTooltipFeatures(t, b[0], g, v, i, p, n), I) {
              var S = x;
              y.length > 0 && (S = "".concat(S).concat(y));
              var T = h.color;
              p = o.drawStandardTooltipLegends(t, [
                {
                  title: { text: "", color: T },
                  value: { text: S, color: T }
                }
              ], g, i, p, n, h);
            }
            p = o.drawStandardTooltipFeatures(t, b[1], g, v, i, p, n), w && (p = o.drawStandardTooltipLegends(t, E, g, i, p, n, f)), p = o.drawStandardTooltipFeatures(t, b[2], g, v, i, p, n), a = g.y + p;
          }
        });
      }
      return a;
    }, e.prototype.drawStandardTooltipFeatures = function(t, i, a, n, o, s, l) {
      var u = this;
      if (i.length > 0) {
        var c = 0, d = 0;
        i.forEach(function(v) {
          var p = v.marginLeft, g = p === void 0 ? 0 : p, m = v.marginTop, x = m === void 0 ? 0 : m, y = v.marginRight, E = y === void 0 ? 0 : y, _ = v.marginBottom, I = _ === void 0 ? 0 : _, w = v.paddingLeft, b = w === void 0 ? 0 : w, S = v.paddingTop, T = S === void 0 ? 0 : S, A = v.paddingRight, D = A === void 0 ? 0 : A, R = v.paddingBottom, P = R === void 0 ? 0 : R, k = v.size, L = k === void 0 ? 0 : k, F = v.type, O = v.content, W = 0;
          if (F === "icon_font") {
            var Q = O;
            t.font = he(L, "normal", Q.family), W = t.measureText(Q.code).width;
          } else
            W = L;
          c += g + b + W + D + E, d = Math.max(d, x + T + L + P + I);
        }), a.x + c > l ? (a.x = o, a.y += s, s = d) : s = Math.max(s, d);
        var h = this.getWidget().getPane(), f = h.getId();
        i.forEach(function(v) {
          var p, g, m, x, y, E = v.marginLeft, _ = E === void 0 ? 0 : E, I = v.marginTop, w = I === void 0 ? 0 : I, b = v.marginRight, S = b === void 0 ? 0 : b, T = v.paddingLeft, A = T === void 0 ? 0 : T, D = v.paddingTop, R = D === void 0 ? 0 : D, P = v.paddingRight, k = P === void 0 ? 0 : P, L = v.paddingBottom, F = L === void 0 ? 0 : L, O = v.backgroundColor, W = v.activeBackgroundColor, Q = v.borderRadius, it = v.size, tt = it === void 0 ? 0 : it, rt = v.color, nt = v.activeColor, ut = v.type, z = v.content, G = rt, $ = O;
          ((p = u._activeFeatureInfo) === null || p === void 0 ? void 0 : p.paneId) === f && ((g = u._activeFeatureInfo.indicator) === null || g === void 0 ? void 0 : g.id) === n?.id && u._activeFeatureInfo.feature.id === v.id && (G = nt ?? rt, $ = W ?? O);
          var Z = "onCandleTooltipFeatureClick", H = {
            paneId: f,
            feature: v
          };
          C(n) && (Z = "onIndicatorTooltipFeatureClick", H.indicator = n);
          var st = {
            mouseDownEvent: u._featureClickEvent(Z, H),
            mouseMoveEvent: u._featureMouseMoveEvent(H)
          }, J = 0;
          if (ut === "icon_font") {
            var et = z;
            (m = u.createFigure({
              name: "text",
              attrs: { text: et.code, x: a.x + _, y: a.y + w },
              styles: {
                paddingLeft: A,
                paddingTop: R,
                paddingRight: k,
                paddingBottom: F,
                borderRadius: Q,
                size: tt,
                family: et.family,
                color: G,
                backgroundColor: $
              }
            }, st)) === null || m === void 0 || m.draw(t), J = t.measureText(et.code).width;
          } else {
            (x = u.createFigure({
              name: "rect",
              attrs: { x: a.x + _, y: a.y + w, width: tt, height: tt },
              styles: {
                paddingLeft: A,
                paddingTop: R,
                paddingRight: k,
                paddingBottom: F,
                color: $
              }
            }, st)) === null || x === void 0 || x.draw(t);
            var Ct = z;
            (y = u.createFigure({
              name: "path",
              attrs: { path: Ct.path, x: a.x + _ + A, y: a.y + w + R, width: tt, height: tt },
              styles: {
                style: Ct.style,
                lineWidth: Ct.lineWidth,
                color: G
              }
            })) === null || y === void 0 || y.draw(t), J = tt;
          }
          a.x += _ + A + J + k + S;
        });
      }
      return s;
    }, e.prototype.drawStandardTooltipLegends = function(t, i, a, n, o, s, l) {
      var u = this;
      if (i.length > 0) {
        var c = l.marginLeft, d = l.marginTop, h = l.marginRight, f = l.marginBottom, v = l.size, p = l.family, g = l.weight;
        t.font = he(v, g, p), i.forEach(function(m) {
          var x, y, E = m.title, _ = m.value, I = t.measureText(E.text).width, w = t.measureText(_.text).width, b = I + w, S = d + v + f;
          a.x + c + b + h > s ? (a.x = n, a.y += o, o = S) : o = Math.max(o, S), E.text.length > 0 && ((x = u.createFigure({
            name: "text",
            attrs: { x: a.x + c, y: a.y + d, text: E.text },
            styles: { color: E.color, size: v, family: p, weight: g }
          })) === null || x === void 0 || x.draw(t)), (y = u.createFigure({
            name: "text",
            attrs: { x: a.x + c + I, y: a.y + d, text: _.text },
            styles: { color: _.color, size: v, family: p, weight: g }
          })) === null || y === void 0 || y.draw(t), a.x += c + b + h;
        });
      }
      return o;
    }, e.prototype.isDrawTooltip = function(t, i) {
      var a = i.showRule;
      return a === "always" || a === "follow_cross" && K(t.paneId);
    }, e.prototype.getIndicatorTooltipData = function(t) {
      var i, a = this.getWidget().getPane().getChart().getChartStore(), n = a.getStyles().indicator, o = n.tooltip, s = o.title, l = "", u = "";
      if (s.show && (s.showName && (l = t.shortName), s.showParams)) {
        var c = t.calcParams;
        c.length > 0 && (u = "(".concat(c.join(","), ")"));
      }
      var d = { name: l, calcParamsText: u, legends: [], features: o.features }, h = a.getCrosshair().dataIndex, f = t.result, v = a.getInnerFormatter(), p = a.getDecimalFold(), g = a.getThousandsSeparator(), m = [];
      if (t.visible) {
        var x = a.getBarSpace(), y = (i = f[h]) !== null && i !== void 0 ? i : {}, E = o.legend.defaultValue;
        gr(t, h, x, n, function(k, L) {
          if (K(k.title)) {
            var F = L.color, O = y[k.key];
            B(O) && (O = wt(O, t.precision), t.shouldFormatBigNumber && (O = v.formatBigNumber(O)), O = p.format(g.format(O))), m.push({ title: { text: k.title, color: F }, value: { text: O ?? E, color: F } });
          }
        }), d.legends = m;
      }
      if (ct(t.createTooltipDataSource)) {
        var _ = this.getWidget(), I = _.getPane(), w = I.getChart(), b = t.createTooltipDataSource({
          chart: w,
          indicator: t,
          crosshair: a.getCrosshair(),
          bounding: _.getBounding(),
          xAxis: I.getChart().getXAxisPane().getXAxisComponent(),
          yAxis: I.getYAxisComponentById(t.yAxisId)
        }), S = b.name, T = b.calcParamsText, A = b.legends, D = b.features;
        if (s.show && (K(S) && s.showName && (d.name = S), K(T) && s.showParams && (d.calcParamsText = T)), C(D) && (d.features = D), C(A) && t.visible) {
          var R = [], P = n.tooltip.legend.color;
          A.forEach(function(k) {
            var L = { text: "", color: P };
            Yt(k.title) ? L = k.title : L.text = k.title;
            var F = { text: "", color: P };
            Yt(k.value) ? F = k.value : F.text = k.value, B(Number(F.text)) && (F.text = p.format(g.format(F.text))), R.push({ title: L, value: F });
          }), d.legends = R;
        }
      }
      return d;
    }, e.prototype.classifyTooltipFeatures = function(t) {
      var i = [], a = [], n = [];
      return t.forEach(function(o) {
        switch (o.position) {
          case "left": {
            i.push(o);
            break;
          }
          case "middle": {
            a.push(o);
            break;
          }
          case "right": {
            n.push(o);
            break;
          }
        }
      }), [i, a, n];
    }, e;
  })(Dt)
), Ii = (
  /** @class */
  (function(r) {
    q(e, r);
    function e(t) {
      var i = r.call(this, t) || this;
      return i._initEvent(), i;
    }
    return e.prototype._initEvent = function() {
      var t = this, i = this.getWidget(), a = i.getPane(), n = a.getId(), o = a.getChart(), s = o.getChartStore();
      this.registerEvent("mouseMoveEvent", function(l) {
        var u, c = s.getProgressOverlayInfo();
        if (c !== null) {
          var d = c.overlay, h = c.paneId;
          d.isStart() && (s.updateProgressOverlayInfo(n), h = n);
          var f = d.points.length - 1;
          return d.isDrawing() && h === n && (d.stepDrawingModeEventMoveForDrawing(t._coordinateToPoint(d, l)), (u = d.onDrawing) === null || u === void 0 || u.call(d, M({ chart: o, overlay: d }, l))), t._figureMouseMoveEvent(d, "point", f, { key: "".concat(ge, "point_").concat(f), type: "circle", attrs: {} })(l);
        }
        return s.setHoverOverlayInfo({
          paneId: n,
          overlay: null,
          figureType: "none",
          figureIndex: -1,
          figure: null
        }, function(v, p) {
          return t._processOverlayMouseEnterEvent(v, p, l);
        }, function(v, p) {
          return t._processOverlayMouseLeaveEvent(v, p, l);
        }), i.setForceCursor(null), !1;
      }).registerEvent("mouseClickEvent", function(l) {
        var u, c, d = s.getProgressOverlayInfo();
        if (d !== null) {
          var h = d.overlay, f = d.paneId;
          h.isStart() && (s.updateProgressOverlayInfo(n, !0), f = n);
          var v = h.points.length - 1;
          return h.isDrawing() && f === n && (h.stepDrawingModeEventMoveForDrawing(t._coordinateToPoint(h, l)), (u = h.onDrawing) === null || u === void 0 || u.call(h, M({ chart: o, overlay: h }, l)), h.nextStep(), h.isDrawing() || (s.progressOverlayComplete(), (c = h.onDrawEnd) === null || c === void 0 || c.call(h, M({ chart: o, overlay: h }, l)))), t._figureMouseClickEvent(h, "point", v, {
            key: "".concat(ge, "point_").concat(v),
            type: "circle",
            attrs: {}
          })(l);
        }
        return s.setClickOverlayInfo({
          paneId: n,
          overlay: null,
          figureType: "none",
          figureIndex: -1,
          figure: null
        }, function(p, g) {
          return t._processOverlaySelectedEvent(p, g, l);
        }, function(p, g) {
          return t._processOverlayDeselectedEvent(p, g, l);
        }), !1;
      }).registerEvent("mouseDoubleClickEvent", function(l) {
        var u, c = s.getProgressOverlayInfo();
        if (c !== null) {
          var d = c.overlay, h = c.paneId;
          d.isDrawing() && h === n && (d.forceComplete(), d.isDrawing() || (s.progressOverlayComplete(), (u = d.onDrawEnd) === null || u === void 0 || u.call(d, M({ chart: o, overlay: d }, l))));
          var f = d.points.length - 1;
          return t._figureMouseClickEvent(d, "point", f, {
            key: "".concat(ge, "point_").concat(f),
            type: "circle",
            attrs: {}
          })(l);
        }
        return !1;
      }).registerEvent("mouseRightClickEvent", function(l) {
        var u = s.getProgressOverlayInfo();
        if (u !== null) {
          var c = u.overlay;
          if (c.isDrawing()) {
            var d = c.points.length - 1;
            return t._figureMouseRightClickEvent(c, "point", d, {
              key: "".concat(ge, "point_").concat(d),
              type: "circle",
              attrs: {}
            })(l);
          }
        }
        return !1;
      }).registerEvent("mouseDownEvent", function(l) {
        var u, c = s.getProgressOverlayInfo();
        if (c !== null) {
          var d = c.overlay;
          if (d.isContinuousDrawingMode() && d.isStart()) {
            s.updateProgressOverlayInfo(n, !0);
            var h = t._coordinateToPoint(d, l);
            return d.startContinuousDrawing(h), (u = d.onDrawStart) === null || u === void 0 || u.call(d, M({ chart: o, overlay: d }, l)), !0;
          }
        }
        return !1;
      }).registerEvent("mouseUpEvent", function(l) {
        var u, c, d = s.getProgressOverlayInfo();
        if (d !== null) {
          var h = d.overlay;
          if (h.isContinuousDrawingMode() && h.isDrawing() && !h.isStart())
            return h.forceComplete(), s.progressOverlayComplete(), (u = h.onDrawEnd) === null || u === void 0 || u.call(h, M({ chart: o, overlay: h }, l)), !0;
        }
        var f = s.getPressedOverlayInfo(), v = f.overlay, p = f.figure;
        return v !== null && Ft("onPressedMoveEnd", p) && ((c = v.onPressedMoveEnd) === null || c === void 0 || c.call(v, M({ chart: o, overlay: v, figure: p ?? void 0 }, l))), s.setPressedOverlayInfo({
          paneId: n,
          overlay: null,
          figureType: "none",
          figureIndex: -1,
          figure: null
        }), !1;
      }).registerEvent("pressedMouseMoveEvent", function(l) {
        var u, c, d = s.getProgressOverlayInfo();
        if (d !== null) {
          var h = d.overlay;
          if (h.isContinuousDrawingMode() && h.isDrawing() && !h.isStart()) {
            var f = t._coordinateToPoint(h, l);
            return h.continuousDrawingModeEventMoveForDrawing(f), (u = h.onDrawing) === null || u === void 0 || u.call(h, M({ chart: o, overlay: h }, l)), t.getWidget().setForceCursor("pointer"), !0;
          }
        }
        var v = s.getPressedOverlayInfo(), p = v.overlay, g = v.figureType, m = v.figureIndex, x = v.figure;
        if (p !== null && Ft("onPressedMoving", x) && !p.lock) {
          var f = t._coordinateToPoint(p, l);
          g === "point" ? p.eventPressedPointMove(f, m) : p.eventPressedOtherMove(f, t.getWidget().getPane().getChart().getChartStore());
          var y = !1;
          return (c = p.onPressedMoving) === null || c === void 0 || c.call(p, M(M({ chart: o, overlay: p, figure: x ?? void 0 }, l), { preventDefault: function() {
            y = !0;
          } })), y ? t.getWidget().setForceCursor(null) : t.getWidget().setForceCursor("pointer"), !0;
        }
        return t.getWidget().setForceCursor(null), !1;
      });
    }, e.prototype._createFigureEvents = function(t, i, a, n) {
      return t.isDrawing() ? null : {
        mouseMoveEvent: this._figureMouseMoveEvent(t, i, a, n),
        mouseDownEvent: this._figureMouseDownEvent(t, i, a, n),
        mouseClickEvent: this._figureMouseClickEvent(t, i, a, n),
        mouseRightClickEvent: this._figureMouseRightClickEvent(t, i, a, n),
        mouseDoubleClickEvent: this._figureMouseDoubleClickEvent(t, i, a, n)
      };
    }, e.prototype._processOverlayMouseEnterEvent = function(t, i, a) {
      return ct(t.onMouseEnter) && Ft("onMouseEnter", i) ? (t.onMouseEnter(M({ chart: this.getWidget().getPane().getChart(), overlay: t, figure: i ?? void 0 }, a)), !0) : !1;
    }, e.prototype._processOverlayMouseLeaveEvent = function(t, i, a) {
      return ct(t.onMouseLeave) && Ft("onMouseLeave", i) ? (t.onMouseLeave(M({ chart: this.getWidget().getPane().getChart(), overlay: t, figure: i ?? void 0 }, a)), !0) : !1;
    }, e.prototype._processOverlaySelectedEvent = function(t, i, a) {
      var n;
      return Ft("onSelected", i) ? ((n = t.onSelected) === null || n === void 0 || n.call(t, M({ chart: this.getWidget().getPane().getChart(), overlay: t, figure: i ?? void 0 }, a)), !0) : !1;
    }, e.prototype._processOverlayDeselectedEvent = function(t, i, a) {
      var n;
      return Ft("onDeselected", i) ? ((n = t.onDeselected) === null || n === void 0 || n.call(t, M({ chart: this.getWidget().getPane().getChart(), overlay: t, figure: i ?? void 0 }, a)), !0) : !1;
    }, e.prototype._figureMouseMoveEvent = function(t, i, a, n) {
      var o = this;
      return function(s) {
        var l, u = o.getWidget().getPane(), c = !t.isDrawing() && Ft("onMouseMove", n);
        if (c) {
          var d = !1;
          (l = t.onMouseMove) === null || l === void 0 || l.call(t, M(M({ chart: u.getChart(), overlay: t, figure: n }, s), { preventDefault: function() {
            d = !0;
          } })), d ? o.getWidget().setForceCursor(null) : o.getWidget().setForceCursor("pointer");
        }
        return u.getChart().getChartStore().setHoverOverlayInfo({ paneId: u.getId(), overlay: t, figureType: i, figure: n, figureIndex: a }, function(h, f) {
          return o._processOverlayMouseEnterEvent(h, f, s);
        }, function(h, f) {
          return o._processOverlayMouseLeaveEvent(h, f, s);
        }), c;
      };
    }, e.prototype._figureMouseDownEvent = function(t, i, a, n) {
      var o = this;
      return function(s) {
        var l;
        if (t.lock)
          return !1;
        var u = o.getWidget().getPane(), c = u.getId();
        return t.startPressedMove(o._coordinateToPoint(t, s)), Ft("onPressedMoveStart", n) ? ((l = t.onPressedMoveStart) === null || l === void 0 || l.call(t, M({ chart: u.getChart(), overlay: t, figure: n }, s)), u.getChart().getChartStore().setPressedOverlayInfo({ paneId: c, overlay: t, figureType: i, figureIndex: a, figure: n }), !t.isDrawing()) : !1;
      };
    }, e.prototype._figureMouseClickEvent = function(t, i, a, n) {
      var o = this;
      return function(s) {
        var l, u = o.getWidget().getPane(), c = u.getId(), d = !t.isDrawing() && Ft("onClick", n);
        return d && ((l = t.onClick) === null || l === void 0 || l.call(t, M({ chart: o.getWidget().getPane().getChart(), overlay: t, figure: n }, s))), u.getChart().getChartStore().setClickOverlayInfo({ paneId: c, overlay: t, figureType: i, figureIndex: a, figure: n }, function(h, f) {
          return o._processOverlaySelectedEvent(h, f, s);
        }, function(h, f) {
          return o._processOverlayDeselectedEvent(h, f, s);
        }), d;
      };
    }, e.prototype._figureMouseDoubleClickEvent = function(t, i, a, n) {
      var o = this;
      return function(s) {
        var l;
        return Ft("onDoubleClick", n) ? ((l = t.onDoubleClick) === null || l === void 0 || l.call(t, M(M({}, s), { chart: o.getWidget().getPane().getChart(), figure: n, overlay: t })), !t.isDrawing()) : !1;
      };
    }, e.prototype._figureMouseRightClickEvent = function(t, i, a, n) {
      var o = this;
      return function(s) {
        var l;
        if (Ft("onRightClick", n)) {
          var u = !1;
          return (l = t.onRightClick) === null || l === void 0 || l.call(t, M(M({ chart: o.getWidget().getPane().getChart(), overlay: t, figure: n }, s), { preventDefault: function() {
            u = !0;
          } })), u || o.getWidget().getPane().getChart().getChartStore().removeOverlay(t), !t.isDrawing();
        }
        return !1;
      };
    }, e.prototype._coordinateToPoint = function(t, i) {
      var a, n, o = {}, s = this.getWidget().getPane(), l = s.getChart(), u = s.getId(), c = l.getChartStore();
      if (this.coordinateToPointTimestampDataIndexFlag()) {
        var d = t;
        if (d.isContinuousDrawingMode()) {
          var h = c.coordinateToFloatIndex(i.x);
          o.dataIndex = h, o.timestamp = (a = c.floatIndexToTimestamp(h)) !== null && a !== void 0 ? a : void 0;
        } else {
          var f = l.getXAxisPane().getXAxisComponent(), v = f.convertFromPixel(i.x);
          o.dataIndex = v, o.timestamp = (n = c.dataIndexToTimestamp(v)) !== null && n !== void 0 ? n : void 0;
        }
      }
      if (this.coordinateToPointValueFlag()) {
        var p = s.getYAxisComponentById(), g = p.convertFromPixel(i.y);
        if (t.mode !== "normal" && u === j.CANDLE && B(o.dataIndex)) {
          var m = c.getDataByDataIndex(o.dataIndex);
          if (m !== null) {
            var x = t.modeSensitivity;
            if (g > m.high)
              if (t.mode === "weak_magnet") {
                var y = p.convertToPixel(m.high), E = p.reverse ? y + x : y - x, _ = p.convertFromPixel(E);
                g < _ && (g = m.high);
              } else
                g = m.high;
            else if (g < m.low)
              if (t.mode === "weak_magnet") {
                var I = p.convertToPixel(m.low), E = p.reverse ? I - x : I + x, _ = p.convertFromPixel(E);
                g > _ && (g = m.low);
              } else
                g = m.low;
            else {
              var w = Math.max(m.open, m.close), b = Math.min(m.open, m.close);
              g > w ? g - w < m.high - g ? g = w : g = m.high : g < b ? g - m.low < b - g ? g = m.low : g = b : w - g < g - b ? g = w : g = b;
            }
          }
        }
        o.value = g;
      }
      return o;
    }, e.prototype.coordinateToPointValueFlag = function() {
      return !0;
    }, e.prototype.coordinateToPointTimestampDataIndexFlag = function() {
      return !0;
    }, e.prototype.dispatchEvent = function(t, i) {
      var a = this.getWidget().getPane().getChart().getChartStore().isOverlayDrawing();
      return a ? this.onEvent(t, i) : r.prototype.dispatchEvent.call(this, t, i);
    }, e.prototype.drawImp = function(t) {
      var i = this, a = this.getCompleteOverlays();
      a.forEach(function(o) {
        o.visible && i._drawOverlay(t, o);
      });
      var n = this.getProgressOverlay();
      C(n) && n.visible && this._drawOverlay(t, n);
    }, e.prototype._drawOverlay = function(t, i) {
      var a = i.points, n = this.getWidget().getPane(), o = n.getChart(), s = o.getChartStore(), l = n.getYAxisComponentById(), u = i.isContinuousDrawingMode(), c = a.map(function(h) {
        var f, v = null;
        u && B(h.timestamp) ? v = s.timestampToFloatIndex(h.timestamp) : B(h.timestamp) ? v = s.timestampToDataIndex(h.timestamp) : B(h.dataIndex) && (v = h.dataIndex);
        var p = { x: 0, y: 0 };
        return B(v) && (p.x = s.dataIndexToCoordinate(v)), B(h.value) && (p.y = (f = l?.convertToPixel(h.value)) !== null && f !== void 0 ? f : 0), p;
      });
      if (c.length > 0) {
        var d = [].concat(this.getFigures(i, c));
        this.drawFigures(t, i, d);
      }
      this.drawDefaultFigures(t, i, c);
    }, e.prototype.drawFigures = function(t, i, a) {
      var n = this, o = this.getWidget().getPane().getChart().getStyles().overlay;
      a.forEach(function(s, l) {
        var u = s.type, c = s.styles, d = s.attrs, h = [].concat(d);
        h.forEach(function(f) {
          var v, p, g = n._createFigureEvents(i, "other", l, s), m = M(M(M({}, o[u]), (v = i.styles) === null || v === void 0 ? void 0 : v[u]), c);
          (p = n.createFigure({
            name: u,
            attrs: f,
            styles: m
          }, g ?? void 0)) === null || p === void 0 || p.draw(t);
        });
      });
    }, e.prototype.getCompleteOverlays = function() {
      var t = this.getWidget().getPane();
      return t.getChart().getChartStore().getOverlaysByPaneId(t.getId());
    }, e.prototype.getProgressOverlay = function() {
      var t = this.getWidget().getPane(), i = t.getChart().getChartStore().getProgressOverlayInfo();
      return C(i) && i.paneId === t.getId() ? i.overlay : null;
    }, e.prototype.getFigures = function(t, i) {
      var a, n, o = this.getWidget(), s = o.getPane(), l = s.getChart(), u = s.getYAxisComponentById(), c = l.getXAxisPane().getXAxisComponent(), d = o.getBounding();
      return (n = (a = t.createPointFigures) === null || a === void 0 ? void 0 : a.call(t, { chart: l, overlay: t, coordinates: i, bounding: d, xAxis: c, yAxis: u })) !== null && n !== void 0 ? n : [];
    }, e.prototype.drawDefaultFigures = function(t, i, a) {
      var n = this, o, s;
      if (i.needDefaultPointFigure) {
        var l = this.getWidget().getPane().getChart().getChartStore(), u = l.getHoverOverlayInfo(), c = l.getClickOverlayInfo();
        if (((o = u.overlay) === null || o === void 0 ? void 0 : o.id) === i.id && u.figureType !== "none" || ((s = c.overlay) === null || s === void 0 ? void 0 : s.id) === i.id && c.figureType !== "none") {
          var d = l.getStyles().overlay, h = i.styles, f = M(M({}, d.point), h?.point);
          a.forEach(function(v, p) {
            var g, m, x, y, E, _ = v.x, I = v.y, w = f.radius, b = f.color, S = f.borderColor, T = f.borderSize;
            ((g = u.overlay) === null || g === void 0 ? void 0 : g.id) === i.id && u.figureType === "point" && ((m = u.figure) === null || m === void 0 ? void 0 : m.key) === "".concat(ge, "point_").concat(p) && (w = f.activeRadius, b = f.activeColor, S = f.activeBorderColor, T = f.activeBorderSize), (y = n.createFigure({
              name: "circle",
              attrs: { x: _, y: I, r: w + T },
              styles: { color: S }
            }, (x = n._createFigureEvents(i, "point", p, {
              key: "".concat(ge, "point_").concat(p),
              type: "circle",
              attrs: { x: _, y: I, r: w + T },
              styles: { color: S }
            })) !== null && x !== void 0 ? x : void 0)) === null || y === void 0 || y.draw(t), (E = n.createFigure({
              name: "circle",
              attrs: { x: _, y: I, r: w },
              styles: { color: b }
            })) === null || E === void 0 || E.draw(t);
          });
        }
      }
    }, e;
  })(Dt)
), Si = (
  /** @class */
  (function(r) {
    q(e, r);
    function e(t, i) {
      var a = r.call(this, t, i) || this;
      return a._gridView = new lo(a), a._indicatorView = new uo(a), a._crosshairLineView = new co(a), a._tooltipView = a.createTooltipView(), a._overlayView = new Ii(a), a.addChild(a._tooltipView), a.addChild(a._overlayView), a;
    }
    return e.prototype.getName = function() {
      return U.MAIN;
    }, e.prototype.updateMain = function(t) {
      this.updateMainContent(t), this._indicatorView.draw(t), this._gridView.draw(t);
    }, e.prototype.createTooltipView = function() {
      return new Ei(this);
    }, e.prototype.updateMainContent = function(t) {
    }, e.prototype.updateOverlayContent = function(t) {
    }, e.prototype.updateOverlay = function(t) {
      this._overlayView.draw(t), this._crosshairLineView.draw(t), this.updateOverlayContent(t), this._tooltipView.draw(t);
    }, e;
  })(xr)
), ho = (
  /** @class */
  (function(r) {
    q(e, r);
    function e() {
      var t = r.apply(this, be([], Se(arguments), !1)) || this;
      return t._ripplePoint = t.createFigure({
        name: "circle",
        attrs: {
          x: 0,
          y: 0,
          r: 0
        },
        styles: {
          style: "fill"
        }
      }), t._animationFrameTime = 0, t._animation = new or({ iterationCount: 1 / 0 }).doFrame(function(i) {
        t._animationFrameTime = i;
        var a = t.getWidget().getPane();
        a.getChart().updatePane(0, a.getId());
      }), t;
    }
    return e.prototype.drawImp = function(t) {
      var i, a, n, o = this.getWidget(), s = o.getPane(), l = s.getChart(), u = l.getDataList(), c = u.length - 1, d = o.getBounding(), h = s.getYAxisComponentById(), f = l.getStyles().candle.area, v = [], p = Number.MAX_SAFE_INTEGER, g = Number.MIN_SAFE_INTEGER, m = null;
      if (this.eachChildren(function(w) {
        var b = w.x, S = w.data.current, T = S?.[f.value];
        if (B(T)) {
          var A = h.convertToPixel(T);
          g === Number.MIN_SAFE_INTEGER && (g = b), v.push({ x: b, y: A }), p = Math.min(p, A), w.dataIndex === c && (m = { x: b, y: A });
        }
      }), v.length > 0) {
        (i = this.createFigure({
          name: "line",
          attrs: { coordinates: v },
          styles: {
            color: f.lineColor,
            size: f.lineSize,
            smooth: f.smooth
          }
        })) === null || i === void 0 || i.draw(t);
        var x = f.backgroundColor, y = "";
        if (Nt(x)) {
          var E = t.createLinearGradient(0, d.height, 0, p);
          try {
            x.forEach(function(w) {
              var b = w.offset, S = w.color;
              E.addColorStop(b, S);
            });
          } catch {
          }
          y = E;
        } else
          y = x;
        t.fillStyle = y, t.beginPath(), t.moveTo(g, d.height), t.lineTo(v[0].x, v[0].y), fi(t, v, f.smooth), t.lineTo(v[v.length - 1].x, d.height), t.closePath(), t.fill();
      }
      var _ = f.point;
      if (_.show && C(m)) {
        (a = this.createFigure({
          name: "circle",
          attrs: {
            x: m.x,
            y: m.y,
            r: _.radius
          },
          styles: {
            style: "fill",
            color: _.color
          }
        })) === null || a === void 0 || a.draw(t);
        var I = _.rippleRadius;
        _.animation && (I = _.radius + this._animationFrameTime / _.animationDuration * (_.rippleRadius - _.radius), this._animation.setDuration(_.animationDuration).start()), (n = this._ripplePoint) === null || n === void 0 || n.setAttrs({
          x: m.x,
          y: m.y,
          r: I
        }).setStyles({ style: "fill", color: _.rippleColor }).draw(t);
      } else
        this.stopAnimation();
    }, e.prototype.stopAnimation = function() {
      this._animation.stop();
    }, e;
  })(bi)
), vo = (
  /** @class */
  (function(r) {
    q(e, r);
    function e() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return e.prototype.drawImp = function(t) {
      var i, a, n = this.getWidget(), o = n.getPane(), s = o.getChart().getChartStore(), l = s.getStyles().candle.priceMark, u = l.high, c = l.low;
      if (l.show && (u.show || c.show)) {
        var d = s.getVisibleRangeHighLowPrice(), h = (a = (i = s.getSymbol()) === null || i === void 0 ? void 0 : i.pricePrecision) !== null && a !== void 0 ? a : pt.PRICE, f = o.getYAxisComponentById(), v = d[0], p = v.price, g = v.x, m = d[1], x = m.price, y = m.x, E = f.convertToPixel(p), _ = f.convertToPixel(x), I = s.getDecimalFold(), w = s.getThousandsSeparator();
        u.show && p !== Number.MIN_SAFE_INTEGER && this._drawMark(t, I.format(w.format(wt(p, h))), { x: g, y: E }, E < _ ? [-2, -5] : [2, 5], u), c.show && x !== Number.MAX_SAFE_INTEGER && this._drawMark(t, I.format(w.format(wt(x, h))), { x: y, y: _ }, E < _ ? [2, 5] : [-2, -5], c);
      }
    }, e.prototype._drawMark = function(t, i, a, n, o) {
      var s, l, u, c = a.x, d = a.y + n[0];
      (s = this.createFigure({
        name: "line",
        attrs: {
          coordinates: [
            { x: c - 2, y: d + n[0] },
            { x: c, y: d },
            { x: c + 2, y: d + n[0] }
          ]
        },
        styles: { color: o.color }
      })) === null || s === void 0 || s.draw(t);
      var h = 0, f = 0, v = "left", p = this.getWidget().getBounding().width;
      c > p / 2 ? (h = c - 5, f = h - o.textOffset, v = "right") : (h = c + 5, v = "left", f = h + o.textOffset);
      var g = d + n[1];
      (l = this.createFigure({
        name: "line",
        attrs: {
          coordinates: [
            { x: c, y: d },
            { x: c, y: g },
            { x: h, y: g }
          ]
        },
        styles: { color: o.color }
      })) === null || l === void 0 || l.draw(t), (u = this.createFigure({
        name: "text",
        attrs: {
          x: f,
          y: g,
          text: i,
          align: v,
          baseline: "middle"
        },
        styles: {
          color: o.color,
          size: o.textSize,
          family: o.textFamily,
          weight: o.textWeight
        }
      })) === null || u === void 0 || u.draw(t);
    }, e;
  })(Dt)
), fo = (
  /** @class */
  (function(r) {
    q(e, r);
    function e() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return e.prototype.drawImp = function(t) {
      var i, a, n, o = this.getWidget(), s = o.getPane(), l = o.getBounding(), u = s.getChart().getChartStore(), c = u.getStyles().candle.priceMark, d = c.last, h = d.line;
      if (c.show && d.show && h.show) {
        var f = s.getYAxisComponentById(), v = u.getDataList(), p = v[v.length - 1];
        if (C(p)) {
          var g = p.close, m = p.open, x = d.compareRule === "current_open" ? m : (a = (i = v[v.length - 2]) === null || i === void 0 ? void 0 : i.close) !== null && a !== void 0 ? a : g, y = f.convertToNicePixel(g), E = "";
          g > x ? E = d.upColor : g < x ? E = d.downColor : E = d.noChangeColor, (n = this.createFigure({
            name: "line",
            attrs: {
              coordinates: [
                { x: 0, y },
                { x: l.width, y }
              ]
            },
            styles: {
              style: h.style,
              color: E,
              size: h.size,
              dashedValue: h.dashedValue
            }
          })) === null || n === void 0 || n.draw(t);
        }
      }
    }, e;
  })(Dt)
), po = {
  second: "HH:mm:ss",
  minute: "HH:mm",
  hour: "MM-DD HH:mm",
  day: "YYYY-MM-DD",
  week: "YYYY-MM-DD",
  month: "YYYY-MM",
  year: "YYYY"
}, Ti = {
  second: "HH:mm:ss",
  minute: "YYYY-MM-DD HH:mm",
  hour: "YYYY-MM-DD HH:mm",
  day: "YYYY-MM-DD",
  week: "YYYY-MM-DD",
  month: "YYYY-MM",
  year: "YYYY"
}, go = {
  time: "时间：",
  open: "开：",
  high: "高：",
  low: "低：",
  close: "收：",
  volume: "成交量：",
  turnover: "成交额：",
  change: "涨幅：",
  second: "秒",
  minute: "",
  hour: "小时",
  day: "天",
  week: "周",
  month: "月",
  year: "年"
}, mo = {
  time: "Time: ",
  open: "Open: ",
  high: "High: ",
  low: "Low: ",
  close: "Close: ",
  volume: "Volume: ",
  turnover: "Turnover: ",
  change: "Change: ",
  second: "S",
  minute: "",
  hour: "H",
  day: "D",
  week: "W",
  month: "M",
  year: "Y"
}, _o = {
  "zh-CN": go,
  "en-US": mo
};
function Wr(r, e) {
  var t;
  return (t = _o[e][r]) !== null && t !== void 0 ? t : r;
}
var yo = (
  /** @class */
  (function(r) {
    q(e, r);
    function e() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return e.prototype.drawImp = function(t) {
      var i = this.getWidget(), a = i.getPane().getChart().getChartStore(), n = a.getCrosshair();
      if (C(n.kLineData)) {
        var o = i.getBounding(), s = a.getStyles(), l = s.candle, u = s.indicator;
        if (l.tooltip.showType === "rect" && u.tooltip.showType === "rect") {
          var c = this.isDrawTooltip(n, l.tooltip), d = this.isDrawTooltip(n, u.tooltip);
          this._drawRectTooltip(t, c, d, l.tooltip.offsetTop);
        } else if (l.tooltip.showType === "standard" && u.tooltip.showType === "standard") {
          var h = l.tooltip, f = h.offsetLeft, v = h.offsetTop, p = h.offsetRight, g = o.width - p, m = this._drawCandleStandardTooltip(t, f, v, g);
          this.drawIndicatorTooltip(t, f, m, g);
        } else if (l.tooltip.showType === "rect" && u.tooltip.showType === "standard") {
          var x = l.tooltip, f = x.offsetLeft, v = x.offsetTop, p = x.offsetRight, g = o.width - p, y = this.drawIndicatorTooltip(t, f, v, g), c = this.isDrawTooltip(n, l.tooltip);
          this._drawRectTooltip(t, c, !1, y);
        } else {
          var E = l.tooltip, f = E.offsetLeft, v = E.offsetTop, p = E.offsetRight, g = o.width - p, _ = this._drawCandleStandardTooltip(t, f, v, g), d = this.isDrawTooltip(n, u.tooltip);
          this._drawRectTooltip(t, !1, d, _);
        }
      }
    }, e.prototype._drawCandleStandardTooltip = function(t, i, a, n) {
      var o, s = this.getWidget().getPane().getChart().getChartStore(), l = s.getStyles().candle, u = l.tooltip, c = u.legend, d = 0, h = { x: i, y: a }, f = s.getCrosshair();
      if (this.isDrawTooltip(f, u)) {
        var v = u.title;
        if (v.show) {
          var p = (o = s.getPeriod()) !== null && o !== void 0 ? o : {}, g = p.type, m = g === void 0 ? "" : g, x = p.span, y = x === void 0 ? "" : x, E = kr(v.template, M(M({}, s.getSymbol()), { period: "".concat(y).concat(Wr(m, s.getLocale())) })), _ = v.color, I = this.drawStandardTooltipLegends(t, [
            {
              title: { text: "", color: _ },
              value: { text: E, color: _ }
            }
          ], { x: i, y: a }, i, 0, n, v);
          h.y = h.y + I;
        }
        var w = this._getCandleTooltipLegends(), b = this.classifyTooltipFeatures(u.features);
        d = this.drawStandardTooltipFeatures(t, b[0], h, null, i, d, n), d = this.drawStandardTooltipFeatures(t, b[1], h, null, i, d, n), w.length > 0 && (d = this.drawStandardTooltipLegends(t, w, h, i, d, n, c)), d = this.drawStandardTooltipFeatures(t, b[2], h, null, i, d, n);
      }
      return h.y + d;
    }, e.prototype._drawRectTooltip = function(t, i, a, n) {
      var o = this, s, l, u = this.getWidget(), c = u.getPane(), d = c.getChart().getChartStore(), h = d.getStyles(), f = h.candle, v = h.indicator, p = f.tooltip, g = v.tooltip;
      if (i || a) {
        var m = this._getCandleTooltipLegends(), x = p.offsetLeft, y = p.offsetTop, E = p.offsetRight, _ = p.offsetBottom, I = p.legend, w = I.marginLeft, b = I.marginRight, S = I.marginTop, T = I.marginBottom, A = I.size, D = I.weight, R = I.family, P = p.rect, k = P.position, L = P.paddingLeft, F = P.paddingRight, O = P.paddingTop, W = P.paddingBottom, Q = P.offsetLeft, it = P.offsetRight, tt = P.offsetTop, rt = P.offsetBottom, nt = P.borderSize, ut = P.borderRadius, z = P.borderColor, G = P.color, $ = 0, Z = 0, H = 0;
        i && (t.font = he(A, D, R), m.forEach(function(Xt) {
          var Rt = Xt.title, Mt = Xt.value, Lt = "".concat(Rt.text).concat(Mt.text), qt = t.measureText(Lt).width + w + b;
          $ = Math.max($, qt);
        }), H += (T + S + A) * m.length);
        var st = g.legend, J = st.marginLeft, et = st.marginRight, Ct = st.marginTop, St = st.marginBottom, gt = st.size, Tt = st.weight, It = st.family, se = [];
        if (a) {
          var Ot = d.getIndicatorsByPaneId(c.getId());
          t.font = he(gt, Tt, It), Ot.forEach(function(Xt) {
            var Rt = o.getIndicatorTooltipData(Xt).legends;
            se.push(Rt), Rt.forEach(function(Mt) {
              var Lt = Mt.title, qt = Mt.value, Ye = "".concat(Lt.text).concat(qt.text), na = t.measureText(Ye).width + J + et;
              $ = Math.max($, na), H += Ct + St + gt;
            });
          });
        }
        if (Z += $, Z !== 0 && H !== 0) {
          var pe = d.getCrosshair(), le = u.getBounding(), At = c.getYAxisWidget().getBounding();
          Z += nt * 2 + L + F, H += nt * 2 + O + W;
          var mt = le.width / 2, _t = k === "pointer" && pe.paneId === j.CANDLE, Et = ((s = pe.realX) !== null && s !== void 0 ? s : 0) > mt, kt = 0;
          if (_t) {
            var Pr = pe.realX;
            Et ? kt = Pr - it - Z : kt = Pr + Q;
          } else {
            var Ne = this.getWidget().getPane().getYAxisComponentById();
            Et ? (kt = Q + x, Ne.inside && Ne.position === "left" && (kt += At.width)) : (kt = le.width - it - Z - E, Ne.inside && Ne.position === "right" && (kt -= At.width));
          }
          var ue = n + tt;
          if (_t) {
            var ra = pe.y;
            ue = ra - H / 2, ue + H > le.height - rt - _ && (ue = le.height - rt - H - _), ue < n + tt && (ue = n + tt + y);
          }
          (l = this.createFigure({
            name: "rect",
            attrs: {
              x: kt,
              y: ue,
              width: Z,
              height: H
            },
            styles: {
              style: "stroke_fill",
              color: G,
              borderColor: z,
              borderSize: nt,
              borderRadius: ut
            }
          })) === null || l === void 0 || l.draw(t);
          var ia = kt + nt + L + w, Jt = ue + nt + O;
          if (i && m.forEach(function(Xt) {
            var Rt, Mt;
            Jt += S;
            var Lt = Xt.title;
            (Rt = o.createFigure({
              name: "text",
              attrs: {
                x: ia,
                y: Jt,
                text: Lt.text
              },
              styles: {
                color: Lt.color,
                size: A,
                family: R,
                weight: D
              }
            })) === null || Rt === void 0 || Rt.draw(t);
            var qt = Xt.value;
            (Mt = o.createFigure({
              name: "text",
              attrs: {
                x: kt + Z - nt - b - F,
                y: Jt,
                text: qt.text,
                align: "right"
              },
              styles: {
                color: qt.color,
                size: A,
                family: R,
                weight: D
              }
            })) === null || Mt === void 0 || Mt.draw(t), Jt += A + T;
          }), a) {
            var aa = kt + nt + L + J;
            se.forEach(function(Xt) {
              Xt.forEach(function(Rt) {
                var Mt, Lt;
                Jt += Ct;
                var qt = Rt.title, Ye = Rt.value;
                (Mt = o.createFigure({
                  name: "text",
                  attrs: {
                    x: aa,
                    y: Jt,
                    text: qt.text
                  },
                  styles: {
                    color: qt.color,
                    size: gt,
                    family: It,
                    weight: Tt
                  }
                })) === null || Mt === void 0 || Mt.draw(t), (Lt = o.createFigure({
                  name: "text",
                  attrs: {
                    x: kt + Z - nt - et - F,
                    y: Jt,
                    text: Ye.text,
                    align: "right"
                  },
                  styles: {
                    color: Ye.color,
                    size: gt,
                    family: It,
                    weight: Tt
                  }
                })) === null || Lt === void 0 || Lt.draw(t), Jt += gt + St;
              });
            });
          }
        }
      }
    }, e.prototype._getCandleTooltipLegends = function() {
      var t, i, a, n, o, s, l, u, c = this.getWidget().getPane().getChart().getChartStore(), d = c.getStyles().candle, h = c.getDataList(), f = c.getInnerFormatter(), v = c.getDecimalFold(), p = c.getThousandsSeparator(), g = c.getLocale(), m = (t = c.getSymbol()) !== null && t !== void 0 ? t : {}, x = m.pricePrecision, y = x === void 0 ? pt.PRICE : x, E = m.volumePrecision, _ = E === void 0 ? pt.VOLUME : E, I = c.getPeriod(), w = (i = c.getCrosshair().dataIndex) !== null && i !== void 0 ? i : 0, b = d.tooltip, S = b.legend, T = S.color, A = S.defaultValue, D = S.template, R = (a = h[w - 1]) !== null && a !== void 0 ? a : null, P = h[w], k = (n = R?.close) !== null && n !== void 0 ? n : P.close, L = P.close - k, F = M(M({}, P), { time: f.formatDate(P.timestamp, Ti[(o = I?.type) !== null && o !== void 0 ? o : "day"], "tooltip"), open: v.format(p.format(wt(P.open, y))), high: v.format(p.format(wt(P.high, y))), low: v.format(p.format(wt(P.low, y))), close: v.format(p.format(wt(P.close, y))), volume: v.format(p.format(f.formatBigNumber(wt((s = P.volume) !== null && s !== void 0 ? s : A, _)))), turnover: v.format(p.format(wt((l = P.turnover) !== null && l !== void 0 ? l : A, y))), change: k === 0 ? A : "".concat(p.format(wt(L / k * 100)), "%") }), O = ct(D) ? D({ prev: R, current: P, next: (u = h[w + 1]) !== null && u !== void 0 ? u : null }, d) : D;
      return O.map(function(W) {
        var Q = W.title, it = W.value, tt = { text: "", color: T };
        Yt(Q) ? tt = M({}, Q) : tt.text = Q, tt.text = Wr(tt.text, g);
        var rt = { text: A, color: T };
        return Yt(it) ? rt = M({}, it) : rt.text = it, C(/{change}/.exec(rt.text)) && (rt.color = L === 0 ? d.priceMark.last.noChangeColor : L > 0 ? d.priceMark.last.upColor : d.priceMark.last.downColor), rt.text = kr(rt.text, F), { title: tt, value: rt };
      });
    }, e;
  })(Ei)
), xo = (
  /** @class */
  (function(r) {
    q(e, r);
    function e(t) {
      var i = r.call(this, t) || this;
      return i._activeFeatureInfo = null, i._featureClickEvent = function(a) {
        return function() {
          var n = i.getWidget().getPane();
          return n.getChart().getChartStore().executeAction("onCrosshairFeatureClick", a), !0;
        };
      }, i._featureMouseMoveEvent = function(a) {
        return function() {
          return i._activeFeatureInfo = a, i.getWidget().setForceCursor("pointer"), !0;
        };
      }, i.registerEvent("mouseMoveEvent", function(a) {
        return i._activeFeatureInfo = null, i.getWidget().setForceCursor(null), !1;
      }), i;
    }
    return e.prototype.drawImp = function(t) {
      var i = this, a, n, o = this.getWidget(), s = o.getPane(), l = o.getPane().getChart().getChartStore(), u = l.getCrosshair(), c = this.getWidget(), d = c.getPane().getYAxisComponentById();
      if (K(u.paneId) && u.paneId === s.getId() && d.isInCandle()) {
        var h = l.getStyles().crosshair, f = h.horizontal.features;
        if (h.show && h.horizontal.show && f.length > 0) {
          var v = d.position === "right", p = c.getBounding(), g = 0, m = h.horizontal.text;
          if (d.inside && m.show) {
            var x = d.convertFromPixel(u.y), y = d.getRange(), E = d.displayValueToText(d.realValueToDisplayValue(d.valueToRealValue(x, { range: y }), { range: y }), (n = (a = l.getSymbol()) === null || a === void 0 ? void 0 : a.pricePrecision) !== null && n !== void 0 ? n : pt.PRICE);
            E = l.getDecimalFold().format(l.getThousandsSeparator().format(E)), g = m.paddingLeft + Wt(E, m.size, m.weight, m.family) + m.paddingRight;
          }
          var _ = g;
          v && (_ = p.width - g);
          var I = u.y;
          f.forEach(function(w) {
            var b, S, T, A, D = w.marginLeft, R = D === void 0 ? 0 : D, P = w.marginTop, k = P === void 0 ? 0 : P, L = w.marginRight, F = L === void 0 ? 0 : L, O = w.paddingLeft, W = O === void 0 ? 0 : O, Q = w.paddingTop, it = Q === void 0 ? 0 : Q, tt = w.paddingRight, rt = tt === void 0 ? 0 : tt, nt = w.paddingBottom, ut = nt === void 0 ? 0 : nt, z = w.color, G = w.activeColor, $ = w.backgroundColor, Z = w.activeBackgroundColor, H = w.borderRadius, st = w.size, J = st === void 0 ? 0 : st, et = w.type, Ct = w.content, St = J;
            if (et === "icon_font") {
              var gt = Ct;
              St = W + Wt(gt.code, J, "normal", gt.family) + rt;
            }
            v ? _ -= St + F : _ += R;
            var Tt = z, It = $;
            ((b = i._activeFeatureInfo) === null || b === void 0 ? void 0 : b.feature.id) === w.id && (Tt = G ?? z, It = Z ?? $);
            var se = {
              mouseDownEvent: i._featureClickEvent({ crosshair: u, feature: w }),
              mouseMoveEvent: i._featureMouseMoveEvent({ crosshair: u, feature: w })
            };
            if (et === "icon_font") {
              var gt = Ct;
              (S = i.createFigure({
                name: "text",
                attrs: {
                  text: gt.code,
                  x: _,
                  y: I + k,
                  baseline: "middle"
                },
                styles: {
                  paddingLeft: W,
                  paddingTop: it,
                  paddingRight: rt,
                  paddingBottom: ut,
                  borderRadius: H,
                  size: J,
                  family: gt.family,
                  color: Tt,
                  backgroundColor: It
                }
              }, se)) === null || S === void 0 || S.draw(t);
            } else {
              (T = i.createFigure({
                name: "rect",
                attrs: { x: _, y: I + k - J / 2, width: J, height: J },
                styles: {
                  paddingLeft: W,
                  paddingTop: it,
                  paddingRight: rt,
                  paddingBottom: ut,
                  color: It
                }
              }, se)) === null || T === void 0 || T.draw(t);
              var Ot = Ct;
              (A = i.createFigure({
                name: "path",
                attrs: { path: Ot.path, x: _, y: I + k + it - J / 2, width: J, height: J },
                styles: {
                  style: Ot.style,
                  lineWidth: Ot.lineWidth,
                  color: Tt
                }
              })) === null || A === void 0 || A.draw(t);
            }
            v ? _ -= R : _ += St + F;
          });
        }
      }
    }, e;
  })(Dt)
), wo = (
  /** @class */
  (function(r) {
    q(e, r);
    function e(t, i) {
      var a = r.call(this, t, i) || this;
      return a._candleBarView = new Ci(a), a._candleAreaView = new ho(a), a._candleHighLowPriceView = new vo(a), a._candleLastPriceLineView = new fo(a), a._crosshairFeatureView = new xo(a), a.addChild(a._candleBarView), a.addChild(a._crosshairFeatureView), a;
    }
    return e.prototype.updateMainContent = function(t) {
      var i = this.getPane().getChart().getStyles().candle;
      i.type !== "area" ? (this._candleBarView.draw(t), this._candleHighLowPriceView.draw(t), this._candleAreaView.stopAnimation()) : this._candleAreaView.draw(t), this._candleLastPriceLineView.draw(t);
    }, e.prototype.updateOverlayContent = function(t) {
      this._crosshairFeatureView.draw(t);
    }, e.prototype.createTooltipView = function() {
      return new yo(this);
    }, e;
  })(Si)
), Ai = (
  /** @class */
  (function(r) {
    q(e, r);
    function e() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return e.prototype.drawImp = function(t) {
      var i = this, a, n, o = this.getWidget(), s = o.getPane(), l = o.getBounding(), u = this.getAxis(), c = this.getAxisStyles(s.getChart().getStyles());
      if (c.show) {
        c.axisLine.show && ((a = this.createFigure({
          name: "line",
          attrs: this.createAxisLine(l, c),
          styles: c.axisLine
        })) === null || a === void 0 || a.draw(t));
        var d = u.getTicks();
        if (c.tickLine.show) {
          var h = this.createTickLines(d, l, c);
          h.forEach(function(v) {
            var p;
            (p = i.createFigure({
              name: "line",
              attrs: v,
              styles: c.tickLine
            })) === null || p === void 0 || p.draw(t);
          });
        }
        if (c.tickText.show) {
          var f = this.createTickTexts(d, l, c);
          (n = this.createFigure({
            name: "text",
            attrs: f,
            styles: c.tickText
          })) === null || n === void 0 || n.draw(t);
        }
      }
    }, e;
  })(Dt)
), bo = (
  /** @class */
  (function(r) {
    q(e, r);
    function e() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return e.prototype.getAxis = function() {
      return this.getWidget().getAxisComponent();
    }, e.prototype.getAxisStyles = function(t) {
      return t.yAxis;
    }, e.prototype.createAxisLine = function(t, i) {
      var a = this.getAxis(), n = i.axisLine.size, o = 0;
      return a.isFromZero() ? o = 0 : o = t.width - n, {
        coordinates: [
          { x: o, y: 0 },
          { x: o, y: t.height }
        ]
      };
    }, e.prototype.createTickLines = function(t, i, a) {
      var n = this.getAxis(), o = a.axisLine, s = a.tickLine, l = 0, u = 0;
      return n.isFromZero() ? (l = 0, o.show && (l += o.size), u = l + s.length) : (l = i.width, o.show && (l -= o.size), u = l - s.length), t.map(function(c) {
        return {
          coordinates: [
            { x: l, y: c.coord },
            { x: u, y: c.coord }
          ]
        };
      });
    }, e.prototype.createTickTexts = function(t, i, a) {
      var n = this.getAxis(), o = a.axisLine, s = a.tickLine, l = a.tickText, u = 0;
      n.isFromZero() ? (u = l.marginStart, o.show && (u += o.size), s.show && (u += s.length)) : (u = i.width - l.marginEnd, o.show && (u -= o.size), s.show && (u -= s.length));
      var c = this.getAxis().isFromZero() ? "left" : "right";
      return t.map(function(d) {
        return {
          x: u,
          y: d.coord,
          text: d.text,
          align: c,
          baseline: "middle"
        };
      });
    }, e;
  })(Ai)
), Co = (
  /** @class */
  (function(r) {
    q(e, r);
    function e() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return e.prototype.drawImp = function(t) {
      var i = this, a, n, o, s, l = this.getWidget(), u = l.getPane(), c = l.getBounding(), d = u.getChart().getChartStore(), h = d.getStyles().candle.priceMark, f = h.last, v = f.text;
      if (h.show && f.show && v.show) {
        var p = (n = (a = d.getSymbol()) === null || a === void 0 ? void 0 : a.pricePrecision) !== null && n !== void 0 ? n : pt.PRICE, g = l.getAxisComponent(), m = d.getDataList(), x = m[m.length - 1];
        if (C(x)) {
          var y = x.close, E = x.open, _ = f.compareRule === "current_open" ? E : (s = (o = m[m.length - 2]) === null || o === void 0 ? void 0 : o.close) !== null && s !== void 0 ? s : y, I = g.convertToNicePixel(y), w = "";
          y > _ ? w = f.upColor : y < _ ? w = f.downColor : w = f.noChangeColor;
          var b = 0, S = "left";
          g.isFromZero() ? (b = 0, S = "left") : (b = c.width, S = "right");
          var T = [], A = g.getRange(), D = g.displayValueToText(g.realValueToDisplayValue(g.valueToRealValue(y, { range: A }), { range: A }), p);
          D = d.getDecimalFold().format(d.getThousandsSeparator().format(D));
          var R = v.paddingLeft, P = v.paddingRight, k = v.paddingTop, L = v.paddingBottom, F = v.size, O = v.family, W = v.weight, Q = R + Wt(D, F, W, O) + P, it = k + F + L;
          T.push({
            name: "text",
            attrs: {
              x: b,
              y: I,
              width: Q,
              height: it,
              text: D,
              align: S,
              baseline: "middle"
            },
            styles: M(M({}, v), { backgroundColor: w })
          });
          var tt = d.getInnerFormatter().formatExtendText, rt = F / 2, nt = I - rt - k, ut = I + rt + L;
          f.extendTexts.forEach(function(z, G) {
            var $ = tt({ type: "last_price", data: x, index: G });
            if ($.length > 0 && z.show) {
              var Z = z.size / 2, H = 0;
              z.position === "above_price" ? (nt -= z.paddingBottom + Z, H = nt, nt -= Z + z.paddingTop) : (ut += z.paddingTop + Z, H = ut, ut += Z + z.paddingBottom), Q = Math.max(Q, z.paddingLeft + Wt($, z.size, z.weight, z.family) + z.paddingRight), T.push({
                name: "text",
                attrs: {
                  x: b,
                  y: H,
                  width: Q,
                  height: z.paddingTop + z.size + z.paddingBottom,
                  text: $,
                  align: S,
                  baseline: "middle"
                },
                styles: M(M({}, z), { backgroundColor: w })
              });
            }
          }), T.forEach(function(z) {
            var G;
            z.attrs.width = Q, (G = i.createFigure(z)) === null || G === void 0 || G.draw(t);
          });
        }
      }
    }, e;
  })(Dt)
), Eo = (
  /** @class */
  (function(r) {
    q(e, r);
    function e() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return e.prototype.drawImp = function(t) {
      var i = this, a = this.getWidget(), n = a.getPane(), o = a.getBounding(), s = n.getChart().getChartStore(), l = s.getStyles().indicator, u = l.lastValueMark, c = u.text;
      if (u.show) {
        var d = a, h = d.getAxisComponent(), f = h.getRange(), v = s.getDataList(), p = s.getBarSpace(), g = v.length - 1, m = /* @__PURE__ */ new Set([h.id]), x = n.getDefaultYAxisId();
        n.isManualYAxis(h.id) && C(x) && m.add(x);
        var y = s.getIndicatorsByPaneId(n.getId()).filter(function(w) {
          return m.has(w.yAxisId);
        }), E = s.getInnerFormatter(), _ = s.getDecimalFold(), I = s.getThousandsSeparator();
        y.forEach(function(w) {
          var b, S = w.result, T = (b = S[g]) !== null && b !== void 0 ? b : {};
          if (C(T) && w.visible) {
            var A = w.precision;
            gr(w, g, p, l, function(D, R) {
              var P, k = T[D.key];
              if (B(k)) {
                var L = h.convertToNicePixel(k), F = h.displayValueToText(h.realValueToDisplayValue(h.valueToRealValue(k, { range: f }), { range: f }), A);
                w.shouldFormatBigNumber && (F = E.formatBigNumber(F)), F = _.format(I.format(F));
                var O = 0, W = "left";
                h.isFromZero() ? (O = 0, W = "left") : (O = o.width, W = "right"), (P = i.createFigure({
                  name: "text",
                  attrs: {
                    x: O,
                    y: L,
                    text: F,
                    align: W,
                    baseline: "middle"
                  },
                  styles: M(M({}, c), { backgroundColor: R.color })
                })) === null || P === void 0 || P.draw(t);
              }
            });
          }
        });
      }
    }, e;
  })(Dt)
), Mi = (
  /** @class */
  (function(r) {
    q(e, r);
    function e() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return e.prototype.coordinateToPointTimestampDataIndexFlag = function() {
      return !1;
    }, e.prototype.drawDefaultFigures = function(t, i, a) {
      this.drawFigures(t, i, this.getDefaultFigures(i, a));
    }, e.prototype.getDefaultFigures = function(t, i) {
      var a, n = this.getWidget(), o = n.getPane(), s = o.getChart().getChartStore(), l = s.getClickOverlayInfo(), u = [];
      if (t.needDefaultYAxisFigure && t.id === ((a = l.overlay) === null || a === void 0 ? void 0 : a.id) && l.paneId === o.getId()) {
        var c = o.getYAxisComponentById(), d = n.getBounding(), h = Number.MAX_SAFE_INTEGER, f = Number.MIN_SAFE_INTEGER, v = c.isFromZero(), p = "left", g = 0;
        v ? (p = "left", g = 0) : (p = "right", g = d.width);
        var m = s.getDecimalFold(), x = s.getThousandsSeparator();
        i.forEach(function(y, E) {
          var _, I, w = t.points[E];
          if (B(w.value)) {
            h = Math.min(h, y.y), f = Math.max(f, y.y);
            var b = m.format(x.format(wt(w.value, (I = (_ = s.getSymbol()) === null || _ === void 0 ? void 0 : _.pricePrecision) !== null && I !== void 0 ? I : pt.PRICE)));
            u.push({ type: "text", attrs: { x: g, y: y.y, text: b, align: p, baseline: "middle" }, ignoreEvent: !0 });
          }
        }), i.length > 1 && u.unshift({ type: "rect", attrs: { x: 0, y: h, width: d.width, height: f - h }, ignoreEvent: !0 });
      }
      return u;
    }, e.prototype.getFigures = function(t, i) {
      var a, n, o = this.getWidget(), s = o.getPane(), l = s.getChart(), u = s.getYAxisComponentById(), c = l.getXAxisPane().getXAxisComponent(), d = o.getBounding();
      return (n = (a = t.createYAxisFigures) === null || a === void 0 ? void 0 : a.call(t, { chart: l, overlay: t, coordinates: i, bounding: d, xAxis: c, yAxis: u })) !== null && n !== void 0 ? n : [];
    }, e;
  })(Ii)
), Pi = (
  /** @class */
  (function(r) {
    q(e, r);
    function e() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return e.prototype.drawImp = function(t) {
      var i, a = this.getWidget(), n = a.getPane(), o = a.getPane().getChart().getChartStore(), s = o.getCrosshair();
      if (K(s.paneId) && this.compare(s, n.getId())) {
        var l = o.getStyles().crosshair;
        if (l.show) {
          var u = this.getDirectionStyles(l), c = u.text;
          if (u.show && c.show) {
            var d = a.getBounding(), h = "getAxisComponent" in a ? a.getAxisComponent() : n.getYAxisComponentById(), f = this.getText(s, o, h);
            t.font = he(c.size, c.weight, c.family), (i = this.createFigure({
              name: "text",
              attrs: this.getTextAttrs(f, t.measureText(f).width, s, d, h, c),
              styles: c
            })) === null || i === void 0 || i.draw(t);
          }
        }
      }
    }, e.prototype.compare = function(t, i) {
      return t.paneId === i;
    }, e.prototype.getDirectionStyles = function(t) {
      return t.horizontal;
    }, e.prototype.getText = function(t, i, a) {
      var n, o, s, l = a, u = a.convertFromPixel(t.y), c = 0, d = !1;
      if (l.isInCandle())
        c = (o = (n = i.getSymbol()) === null || n === void 0 ? void 0 : n.pricePrecision) !== null && o !== void 0 ? o : pt.PRICE;
      else {
        var h = l.id, f = this.getWidget().getPane();
        f.isManualYAxis(h) && (h = (s = f.getDefaultYAxisId()) !== null && s !== void 0 ? s : h);
        var v = i.getIndicatorsByPaneId(t.paneId).filter(function(m) {
          return m.yAxisId === h;
        });
        v.forEach(function(m) {
          c = Math.max(m.precision, c), d || (d = m.shouldFormatBigNumber);
        });
      }
      var p = l.getRange(), g = l.displayValueToText(l.realValueToDisplayValue(l.valueToRealValue(u, { range: p }), { range: p }), c);
      return d && (g = i.getInnerFormatter().formatBigNumber(g)), i.getDecimalFold().format(i.getThousandsSeparator().format(g));
    }, e.prototype.getTextAttrs = function(t, i, a, n, o, s) {
      var l = o, u = 0, c = "left";
      return l.isFromZero() ? (u = 0, c = "left") : (u = n.width, c = "right"), { x: u, y: a.y, text: t, align: c, baseline: "middle" };
    }, e;
  })(Dt)
), Io = (
  /** @class */
  (function(r) {
    q(e, r);
    function e(t, i, a) {
      var n = r.call(this, t, i) || this;
      return n._yAxisView = new bo(n), n._candleLastPriceLabelView = new Co(n), n._indicatorLastValueView = new Eo(n), n._overlayYAxisView = new Mi(n), n._crosshairHorizontalLabelView = new Pi(n), n._yAxis = a, n.setCursor("ns-resize"), n.addChild(n._overlayYAxisView), n;
    }
    return e.prototype.getAxisComponent = function() {
      return this._yAxis;
    }, e.prototype.getName = function() {
      return U.Y_AXIS;
    }, e.prototype.updateMain = function(t) {
      this._yAxisView.draw(t);
      var i = this.getPane(), a = i.isDefaultYAxis(this._yAxis.id) || i.isManualYAxis(this._yAxis.id);
      a && this.getAxisComponent().isInCandle() && this._candleLastPriceLabelView.draw(t), this._indicatorLastValueView.draw(t);
    }, e.prototype.updateOverlay = function(t) {
      this._overlayYAxisView.draw(t), this._crosshairHorizontalLabelView.draw(t);
    }, e;
  })(xr)
), De = 8;
function zr() {
  return {
    from: 0,
    to: 0,
    range: 0,
    realFrom: 0,
    realTo: 0,
    realRange: 0,
    displayFrom: 0,
    displayTo: 0,
    displayRange: 0
  };
}
var Di = (
  /** @class */
  (function() {
    function r(e) {
      this.scrollZoomEnabled = !0, this._range = zr(), this._prevRange = zr(), this._ticks = [], this._autoCalcTickFlag = !0, this._parent = e;
    }
    return r.prototype.getParent = function() {
      return this._parent;
    }, r.prototype.buildTicks = function(e) {
      return this._autoCalcTickFlag && (this._range = this.createRangeImp()), this._prevRange.from !== this._range.from || this._prevRange.to !== this._range.to || e ? (this._prevRange = this._range, this._ticks = this.createTicksImp(), !0) : !1;
    }, r.prototype.getTicks = function() {
      return this._ticks;
    }, r.prototype.setRange = function(e) {
      this._autoCalcTickFlag = !1, this._range = e;
    }, r.prototype.getRange = function() {
      return this._range;
    }, r.prototype.setAutoCalcTickFlag = function(e) {
      this._autoCalcTickFlag = e;
    }, r.prototype.getAutoCalcTickFlag = function() {
      return this._autoCalcTickFlag;
    }, r;
  })()
), Ke = "yAxis_", Je = (
  /** @class */
  (function(r) {
    q(e, r);
    function e(t, i) {
      var a = r.call(this, t) || this;
      a.id = "", a.paneId = "", a.reverse = !1, a.inside = !1, a.position = "right", a.gap = {
        top: 0.2,
        bottom: 0.1
      }, a.createRange = function(h) {
        return h.defaultRange;
      }, a.minSpan = function(h) {
        return de(-h);
      }, a.valueToRealValue = function(h) {
        return h;
      }, a.realValueToDisplayValue = function(h) {
        return h;
      }, a.displayValueToRealValue = function(h) {
        return h;
      }, a.realValueToValue = function(h) {
        return h;
      }, a.displayValueToText = function(h, f) {
        return wt(h, f);
      };
      var n = i.minSpan, o = i.valueToRealValue, s = i.realValueToDisplayValue, l = i.displayValueToRealValue, u = i.realValueToValue, c = i.displayValueToText, d = Oe(i, ["minSpan", "valueToRealValue", "realValueToDisplayValue", "displayValueToRealValue", "realValueToValue", "displayValueToText"]);
      return ct(n) && (a.minSpan = n), ct(o) && (a.valueToRealValue = o), ct(s) && (a.realValueToDisplayValue = s), ct(l) && (a.displayValueToRealValue = l), ct(u) && (a.realValueToValue = u), ct(c) && (a.displayValueToText = c), a.override(d), a;
    }
    return e.prototype.override = function(t) {
      var i = t.id, a = t.name, n = t.gap, o = Oe(t, ["id", "name", "gap"]);
      C(i) && this.id.length === 0 && (this.id = i), !K(this.name) && K(a) && (this.name = a), dt(this.gap, n), dt(this, o);
    }, e.prototype._getIndicatorsByYAxisIds = function() {
      var t = this.getParent(), i = /* @__PURE__ */ new Set([this.id]);
      if (t.isManualYAxis(this.id)) {
        var a = t.getDefaultYAxisId();
        C(a) && i.add(a);
      }
      return t.getChart().getChartStore().getIndicatorsByPaneId(t.getId()).filter(function(n) {
        return i.has(n.yAxisId);
      });
    }, e.prototype._shouldUseCandleData = function() {
      var t = this.getParent();
      return this.isInCandle() && (t.isDefaultYAxis(this.id) || t.isManualYAxis(this.id));
    }, e.prototype.createRangeImp = function() {
      var t, i, a = this.getParent(), n = a.getChart(), o = n.getChartStore(), s = a.getId(), l = Number.MAX_SAFE_INTEGER, u = Number.MIN_SAFE_INTEGER, c = !1, d = Number.MAX_SAFE_INTEGER, h = Number.MIN_SAFE_INTEGER, f = Number.MAX_SAFE_INTEGER, v = this._getIndicatorsByYAxisIds();
      v.forEach(function($) {
        c || (c = $.shouldOhlc), f = Math.min(f, $.precision), B($.minValue) && (d = Math.min(d, $.minValue)), B($.maxValue) && (h = Math.max(h, $.maxValue));
      });
      var p = 4, g = this.isInCandle();
      if (g) {
        var m = (i = (t = o.getSymbol()) === null || t === void 0 ? void 0 : t.pricePrecision) !== null && i !== void 0 ? i : pt.PRICE;
        f !== Number.MAX_SAFE_INTEGER ? p = Math.min(f, m) : p = m;
      } else
        f !== Number.MAX_SAFE_INTEGER && (p = f);
      var x = o.getVisibleRangeDataList(), y = n.getStyles().candle, E = y.type === "area", _ = y.area.value, I = this._shouldUseCandleData(), w = I && !E || !g && c;
      x.forEach(function($) {
        var Z = $.dataIndex, H = $.data.current;
        if (C(H) && (w && (l = Math.min(l, H.low), u = Math.max(u, H.high)), I && E)) {
          var st = H[_];
          B(st) && (l = Math.min(l, st), u = Math.max(u, st));
        }
        v.forEach(function(J) {
          var et, Ct = J.result, St = J.figures, gt = (et = Ct[Z]) !== null && et !== void 0 ? et : {};
          St.forEach(function(Tt) {
            var It = gt[Tt.key];
            B(It) && (l = Math.min(l, It), u = Math.max(u, It));
          });
        });
      }), l !== Number.MAX_SAFE_INTEGER && u !== Number.MIN_SAFE_INTEGER ? (l = Math.min(d, l), u = Math.max(h, u)) : (l = 0, u = 10);
      var b = u - l, S = {
        from: l,
        to: u,
        range: b,
        realFrom: l,
        realTo: u,
        realRange: b,
        displayFrom: l,
        displayTo: u,
        displayRange: b
      }, T = this.createRange({
        chart: n,
        paneId: s,
        defaultRange: S
      }), A = T.realFrom, D = T.realTo, R = T.realRange, P = this.minSpan(p);
      if (A === D || R < P) {
        var k = d === A, L = h === D, F = De / 2;
        A = k ? A : L ? A - De * P : A - F * P, D = L ? D : k ? D + De * P : D + F * P;
      }
      var O = this.getBounding().height, W = this.gap, Q = W.top, it = W.bottom, tt = Q;
      tt >= 1 && (tt = tt / O);
      var rt = it;
      rt >= 1 && (rt = rt / O), R = D - A, A = A - R * rt, D = D + R * tt;
      var nt = this.realValueToValue(A, { range: T }), ut = this.realValueToValue(D, { range: T }), z = this.realValueToDisplayValue(A, { range: T }), G = this.realValueToDisplayValue(D, { range: T });
      return {
        from: nt,
        to: ut,
        range: ut - nt,
        realFrom: A,
        realTo: D,
        realRange: D - A,
        displayFrom: z,
        displayTo: G,
        displayRange: G - z
      };
    }, e.prototype.isInCandle = function() {
      return this.getParent().getId() === j.CANDLE;
    }, e.prototype.isFromZero = function() {
      return this.position === "left" && this.inside || this.position === "right" && !this.inside;
    }, e.prototype.createTicksImp = function() {
      var t = this, i, a, n = this.getRange(), o = n.displayFrom, s = n.displayTo, l = n.displayRange, u = [];
      if (l >= 0) {
        var c = ya(l / De), d = xa(c), h = Fr(Math.ceil(o / c) * c, d), f = Fr(Math.floor(s / c) * c, d), v = 0, p = h;
        if (c !== 0)
          for (; p <= f; ) {
            var g = p.toFixed(d);
            u[v] = { text: g, coord: 0, value: g }, ++v, p += c;
          }
      }
      var m = this.getParent(), x = this.getBounding().height, y = m.getChart().getChartStore(), E = [], _ = this._getIndicatorsByYAxisIds(), I = y.getStyles(), w = 0, b = !1;
      this._shouldUseCandleData() ? w = (a = (i = y.getSymbol()) === null || i === void 0 ? void 0 : i.pricePrecision) !== null && a !== void 0 ? a : pt.PRICE : _.forEach(function(P) {
        w = Math.max(w, P.precision), b || (b = P.shouldFormatBigNumber);
      });
      var S = y.getInnerFormatter(), T = y.getThousandsSeparator(), A = y.getDecimalFold(), D = I.xAxis.tickText.size, R = NaN;
      return u.forEach(function(P) {
        var k = P.value, L = t.displayValueToText(+k, w), F = t.convertToPixel(t.realValueToValue(t.displayValueToRealValue(+k, { range: n }), { range: n }));
        b && (L = S.formatBigNumber(k)), L = A.format(T.format(L));
        var O = B(R);
        F > D && F < x - D && (O && Math.abs(R - F) > D * 2 || !O) && (E.push({ text: L, coord: F, value: k }), R = F);
      }), ct(this.createTicks) ? this.createTicks({
        range: this.getRange(),
        bounding: this.getBounding(),
        defaultTicks: E
      }) : E;
    }, e.prototype.getAutoSize = function() {
      var t, i, a = this.getParent(), n = a.getChart(), o = n.getChartStore(), s = o.getStyles(), l = s.yAxis, u = l.size;
      if (u !== "auto")
        return u;
      var c = 0;
      if (l.show && (l.axisLine.show && (c += l.axisLine.size), l.tickLine.show && (c += l.tickLine.length), l.tickText.show)) {
        var d = 0;
        this.getTicks().forEach(function(W) {
          d = Math.max(d, Wt(W.text, l.tickText.size, l.tickText.weight, l.tickText.family));
        }), c += l.tickText.marginStart + l.tickText.marginEnd + d;
      }
      var h = s.candle.priceMark, f = h.show && h.last.show && h.last.text.show, v = 0, p = s.crosshair, g = p.show && p.horizontal.show && p.horizontal.text.show, m = 0;
      if (f || g) {
        var x = (i = (t = o.getSymbol()) === null || t === void 0 ? void 0 : t.pricePrecision) !== null && i !== void 0 ? i : pt.PRICE, y = this.getRange().displayTo;
        if (f) {
          var E = o.getDataList(), _ = E[E.length - 1];
          if (C(_)) {
            var I = h.last.text, w = I.paddingLeft, b = I.paddingRight, S = I.size, T = I.family, A = I.weight;
            v = w + Wt(wt(_.close, x), S, A, T) + b;
            var D = o.getInnerFormatter().formatExtendText;
            h.last.extendTexts.forEach(function(W, Q) {
              var it = D({ type: "last_price", data: _, index: Q });
              it.length > 0 && W.show && (v = Math.max(v, W.paddingLeft + Wt(it, W.size, W.weight, W.family) + W.paddingRight));
            });
          }
        }
        if (g) {
          var R = this._getIndicatorsByYAxisIds(), P = 0, k = !1;
          R.forEach(function(W) {
            P = Math.max(W.precision, P), k || (k = W.shouldFormatBigNumber);
          });
          var L = 2;
          if (this._shouldUseCandleData()) {
            var F = s.indicator.lastValueMark;
            F.show && F.text.show ? L = Math.max(P, x) : L = x;
          } else
            L = P;
          var O = wt(y, L);
          k && (O = o.getInnerFormatter().formatBigNumber(O)), O = o.getDecimalFold().format(O), m += p.horizontal.text.paddingLeft + p.horizontal.text.paddingRight + p.horizontal.text.borderSize * 2 + Wt(O, p.horizontal.text.size, p.horizontal.text.weight, p.horizontal.text.family);
        }
      }
      return Math.max(c, v, m);
    }, e.prototype.getBounding = function() {
      var t, i;
      return (i = (t = this.getParent().getYAxisWidgetById(this.id)) === null || t === void 0 ? void 0 : t.getBounding()) !== null && i !== void 0 ? i : this.getParent().getMainWidget().getBounding();
    }, e.prototype.convertFromPixel = function(t) {
      var i = this.getBounding().height, a = this.getRange(), n = a.realFrom, o = a.realRange, s = this.reverse ? t / i : 1 - t / i, l = s * o + n;
      return this.realValueToValue(l, { range: a });
    }, e.prototype.convertToPixel = function(t) {
      var i = this.getRange(), a = this.valueToRealValue(t, { range: i }), n = this.getBounding().height, o = i.realFrom, s = i.realRange, l = (a - o) / s;
      return this.reverse ? Math.round(l * n) : Math.round((1 - l) * n);
    }, e.prototype.convertToNicePixel = function(t) {
      var i = this.getBounding().height, a = this.convertToPixel(t);
      return Math.round(Math.max(i * 0.05, Math.min(a, i * 0.98)));
    }, e.extend = function(t) {
      var i = (
        /** @class */
        (function(a) {
          q(n, a);
          function n(o) {
            return a.call(this, o, t) || this;
          }
          return n;
        })(e)
      );
      return i;
    }, e;
  })(Di)
), So = {
  name: "normal"
}, To = {
  name: "percentage",
  minSpan: function() {
    return Math.pow(10, -2);
  },
  displayValueToText: function(r) {
    return "".concat(wt(r, 2), "%");
  },
  valueToRealValue: function(r, e) {
    var t = e.range;
    return (r - t.from) / t.range * t.realRange + t.realFrom;
  },
  realValueToValue: function(r, e) {
    var t = e.range;
    return (r - t.realFrom) / t.realRange * t.range + t.from;
  },
  createRange: function(r) {
    var e = r.chart, t = r.defaultRange, i = e.getDataList(), a = e.getVisibleRange(), n = i[a.from];
    if (C(n)) {
      var o = t.from, s = t.to, l = t.range, u = (t.from - n.close) / n.close * 100, c = (t.to - n.close) / n.close * 100, d = c - u;
      return {
        from: o,
        to: s,
        range: l,
        realFrom: u,
        realTo: c,
        realRange: d,
        displayFrom: u,
        displayTo: c,
        displayRange: d
      };
    }
    return t;
  }
}, Ao = {
  name: "logarithm",
  minSpan: function(r) {
    return 0.05 * de(-r);
  },
  valueToRealValue: function(r) {
    return r < 0 ? -Ht(Math.abs(r)) : Ht(r);
  },
  realValueToDisplayValue: function(r) {
    return r < 0 ? -de(Math.abs(r)) : de(r);
  },
  displayValueToRealValue: function(r) {
    return r < 0 ? -Ht(Math.abs(r)) : Ht(r);
  },
  realValueToValue: function(r) {
    return r < 0 ? -de(Math.abs(r)) : de(r);
  },
  createRange: function(r) {
    var e = r.defaultRange, t = e.from, i = e.to, a = e.range, n = t < 0 ? -Ht(Math.abs(t)) : Ht(t), o = i < 0 ? -Ht(Math.abs(i)) : Ht(i);
    return {
      from: t,
      to: i,
      range: a,
      realFrom: n,
      realTo: o,
      realRange: o - n,
      displayFrom: t,
      displayTo: i,
      displayRange: a
    };
  }
}, $r = {
  normal: Je.extend(So),
  percentage: Je.extend(To),
  logarithm: Je.extend(Ao)
};
function Mo(r) {
  var e;
  return (e = $r[r]) !== null && e !== void 0 ? e : $r.normal;
}
var ki = (
  /** @class */
  (function() {
    function r(e, t) {
      this._bounding = pr(), this._chart = e, this._id = t, this._container = Gt("div", {
        width: "100%",
        margin: "0",
        padding: "0",
        position: "relative",
        overflow: "hidden",
        boxSizing: "border-box"
      });
    }
    return r.prototype.getContainer = function() {
      return this._container;
    }, r.prototype.getId = function() {
      return this._id;
    }, r.prototype.getChart = function() {
      return this._chart;
    }, r.prototype.getBounding = function() {
      return this._bounding;
    }, r.prototype.update = function(e) {
      this._bounding.height !== this._container.clientHeight && (this._container.style.height = "".concat(this._bounding.height, "px")), this.updateImp(e ?? 3, this._container, this._bounding);
    }, r;
  })()
), Ri = (
  /** @class */
  (function(r) {
    q(e, r);
    function e(t, i) {
      var a = r.call(this, t, i.id) || this;
      a._yAxisWidgets = /* @__PURE__ */ new Map(), a._yAxisComponents = /* @__PURE__ */ new Map(), a._manualYAxisIds = /* @__PURE__ */ new Set(), a._defaultYAxisId = null, a._yAxesBounding = {};
      var n = a.getContainer();
      return a._mainWidget = a.createMainWidget(n), a._options = i, a;
    }
    return e.prototype.setOptions = function(t) {
      return dt(this._options, t), B(t.height) && t.height > 0 && this.setBounding({ height: this._options.height }), this;
    }, e.prototype.setAxisCursor = function(t, i) {
      var a, n, o = null, s = "default";
      this.getId() === j.X_AXIS ? (o = this.getMainWidget().getContainer(), s = "ew-resize") : (o = (n = (a = this.getYAxisWidgetById(i)) === null || a === void 0 ? void 0 : a.getContainer()) !== null && n !== void 0 ? n : null, s = "ns-resize"), !(!C(o) || !we(t)) && (t ? o.style.cursor = s : o.style.cursor = "default");
    }, e.prototype.createOrOverrideYAxis = function(t) {
      var i, a, n, o, s, l = M(M({}, t), { paneId: this.getId() }), u = l.id, c = (i = l.name) !== null && i !== void 0 ? i : "normal", d = (a = l.needWidget) !== null && a !== void 0 ? a : !0, h = this._yAxisComponents.get(u), f = !C(h) || C(l.name) && h.name !== l.name;
      if (f) {
        if ((n = this._yAxisWidgets.get(u)) === null || n === void 0 || n.destroy(), this._yAxisWidgets.delete(u), h = this.createYAxisComponent(c), h.id = u, h.paneId = this.getId(), this._yAxisComponents.set(u, h), (o = this._defaultYAxisId) !== null && o !== void 0 || (this._defaultYAxisId = u), d) {
          var v = this.createYAxisWidget(this.getContainer(), h);
          C(v) && this._yAxisWidgets.set(u, v);
        }
      } else if (we(l.needWidget) && C(h)) {
        var v = this._yAxisWidgets.get(u);
        if (l.needWidget && !C(v)) {
          var p = this.createYAxisWidget(this.getContainer(), h);
          C(p) && this._yAxisWidgets.set(u, p);
        } else !l.needWidget && C(v) && (v.destroy(), this._yAxisWidgets.delete(u));
      }
      if (!C(h))
        throw new Error("create yAxis failed.");
      h.setAutoCalcTickFlag(!0), h.override(M(M({}, l), { name: c })), this.setAxisCursor(h.scrollZoomEnabled, u);
      var g = this.getBounding();
      return (s = this._yAxisWidgets.get(u)) === null || s === void 0 || s.setBounding({ height: g.height, top: g.top }), h;
    }, e.prototype.getOptions = function() {
      return this._options;
    }, e.prototype.getYAxisComponents = function() {
      return Array.from(this._yAxisComponents.values());
    }, e.prototype.getWidgetYAxisComponents = function() {
      var t = this;
      return Array.from(this._yAxisWidgets.keys()).map(function(i) {
        return t._yAxisComponents.get(i);
      });
    }, e.prototype.hasYAxisComponent = function(t) {
      return this._yAxisComponents.has(t);
    }, e.prototype.setManualYAxis = function(t, i) {
      i ? this._manualYAxisIds.add(t) : this._manualYAxisIds.delete(t);
    }, e.prototype.isManualYAxis = function(t) {
      return this._manualYAxisIds.has(t);
    }, e.prototype.removeYAxis = function(t) {
      var i = this, a, n = this._yAxisComponents.get(t);
      if (!C(n))
        return !1;
      this._yAxisComponents.delete(t), this._manualYAxisIds.delete(t), this._defaultYAxisId === t && (this._defaultYAxisId = (a = this._yAxisComponents.keys().next().value) !== null && a !== void 0 ? a : null);
      var o = this._yAxisWidgets.get(t);
      return C(o) && (o.destroy(), this._yAxisWidgets.delete(t)), this._yAxesBounding = Object.keys(this._yAxesBounding).reduce(function(s, l) {
        return l !== t && (s[l] = i._yAxesBounding[l]), s;
      }, {}), !0;
    }, e.prototype.getDefaultYAxisId = function() {
      return this._defaultYAxisId;
    }, e.prototype.isDefaultYAxis = function(t) {
      return this._defaultYAxisId === t;
    }, e.prototype.getYAxisComponentById = function(t) {
      var i = t ?? this.getDefaultYAxisId();
      return this._yAxisComponents.get(i);
    }, e.prototype.getYAxisWidgetById = function(t) {
      var i, a = t ?? this.getDefaultYAxisId();
      return C(a) && (i = this._yAxisWidgets.get(a)) !== null && i !== void 0 ? i : null;
    }, e.prototype.setYAxesBounding = function(t) {
      this._yAxesBounding = t;
    }, e.prototype.setBounding = function(t, i, a, n) {
      var o = this;
      dt(this.getBounding(), t);
      var s = {};
      C(t.height) && (s.height = t.height), C(t.top) && (s.top = t.top), this._mainWidget.setBounding(s);
      var l = C(i);
      return l && this._mainWidget.setBounding(i), this._yAxisWidgets.size > 0 && this._yAxisWidgets.forEach(function(u, c) {
        var d, h, f, v;
        if (u.setBounding(s), C(o._yAxesBounding[c])) {
          u.setBounding(o._yAxesBounding[c]);
          return;
        }
        var p = o.getYAxisComponentById(c);
        p.position === "left" ? C(a) && u.setBounding(M(M({}, a), { left: 0 })) : C(n) && (u.setBounding(n), l && u.setBounding({
          left: ((d = i.left) !== null && d !== void 0 ? d : 0) + ((h = i.width) !== null && h !== void 0 ? h : 0) + ((f = i.right) !== null && f !== void 0 ? f : 0) - ((v = n.width) !== null && v !== void 0 ? v : 0)
        }));
      }), this;
    }, e.prototype.getMainWidget = function() {
      return this._mainWidget;
    }, e.prototype.getYAxisWidget = function() {
      return this.getYAxisWidgetById();
    }, e.prototype.getYAxisWidgets = function() {
      return Array.from(this._yAxisWidgets.values());
    }, e.prototype.updateImp = function(t) {
      this._mainWidget.update(t), this._yAxisWidgets.forEach(function(i) {
        i.update(t);
      });
    }, e.prototype.destroy = function() {
      this._mainWidget.destroy(), this._yAxisWidgets.forEach(function(t) {
        t.destroy();
      });
    }, e.prototype.getImage = function(t) {
      var i = this.getBounding(), a = i.width, n = i.height, o = Gt("canvas", {
        width: "".concat(a, "px"),
        height: "".concat(n, "px"),
        boxSizing: "border-box"
      }), s = o.getContext("2d"), l = te(o);
      o.width = a * l, o.height = n * l, s.scale(l, l);
      var u = this._mainWidget.getBounding();
      return s.drawImage(this._mainWidget.getImage(t), u.left, 0, u.width, u.height), this._yAxisWidgets.forEach(function(c) {
        var d = c.getBounding();
        s.drawImage(c.getImage(t), d.left, 0, d.width, d.height);
      }), o;
    }, e.prototype.createYAxisComponent = function(t) {
      throw new Error("createYAxisComponent is not implemented.");
    }, e.prototype.createYAxisWidget = function(t, i) {
      return null;
    }, e;
  })(ki)
), Fi = (
  /** @class */
  (function(r) {
    q(e, r);
    function e() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return e.prototype.createYAxisComponent = function(t) {
      var i = Mo(t ?? "default");
      return new i(this);
    }, e.prototype.createMainWidget = function(t) {
      return new Si(t, this);
    }, e.prototype.createYAxisWidget = function(t, i) {
      return new Io(t, this, i);
    }, e;
  })(Ri)
), Po = (
  /** @class */
  (function(r) {
    q(e, r);
    function e() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return e.prototype.createMainWidget = function(t) {
      return new wo(t, this);
    }, e;
  })(Fi)
), Do = (
  /** @class */
  (function(r) {
    q(e, r);
    function e() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return e.prototype.getAxis = function() {
      return this.getWidget().getPane().getXAxisComponent();
    }, e.prototype.getAxisStyles = function(t) {
      return t.xAxis;
    }, e.prototype.createAxisLine = function(t) {
      return {
        coordinates: [
          { x: 0, y: 0 },
          { x: t.width, y: 0 }
        ]
      };
    }, e.prototype.createTickLines = function(t, i, a) {
      var n = a.tickLine, o = a.axisLine.size;
      return t.map(function(s) {
        return {
          coordinates: [
            { x: s.coord, y: 0 },
            { x: s.coord, y: o + n.length }
          ]
        };
      });
    }, e.prototype.createTickTexts = function(t, i, a) {
      var n = a.tickText, o = a.axisLine.size, s = a.tickLine.length;
      return t.map(function(l) {
        return {
          x: l.coord,
          y: o + s + n.marginStart,
          text: l.text,
          align: "center",
          baseline: "top"
        };
      });
    }, e;
  })(Ai)
), ko = (
  /** @class */
  (function(r) {
    q(e, r);
    function e() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return e.prototype.coordinateToPointTimestampDataIndexFlag = function() {
      return !0;
    }, e.prototype.coordinateToPointValueFlag = function() {
      return !1;
    }, e.prototype.getCompleteOverlays = function() {
      return this.getWidget().getPane().getChart().getChartStore().getOverlaysByPaneId();
    }, e.prototype.getProgressOverlay = function() {
      var t, i;
      return (i = (t = this.getWidget().getPane().getChart().getChartStore().getProgressOverlayInfo()) === null || t === void 0 ? void 0 : t.overlay) !== null && i !== void 0 ? i : null;
    }, e.prototype.getDefaultFigures = function(t, i) {
      var a, n = [], o = this.getWidget(), s = o.getPane(), l = s.getChart().getChartStore(), u = l.getClickOverlayInfo();
      if (t.needDefaultXAxisFigure && t.id === ((a = u.overlay) === null || a === void 0 ? void 0 : a.id)) {
        var c = Number.MAX_SAFE_INTEGER, d = Number.MIN_SAFE_INTEGER;
        i.forEach(function(h, f) {
          c = Math.min(c, h.x), d = Math.max(d, h.x);
          var v = t.points[f];
          if (B(v.timestamp)) {
            var p = l.getInnerFormatter().formatDate(v.timestamp, "YYYY-MM-DD HH:mm", "crosshair");
            n.push({ type: "text", attrs: { x: h.x, y: 0, text: p, align: "center" }, ignoreEvent: !0 });
          }
        }), i.length > 1 && n.unshift({ type: "rect", attrs: { x: c, y: 0, width: d - c, height: o.getBounding().height }, ignoreEvent: !0 });
      }
      return n;
    }, e.prototype.getFigures = function(t, i) {
      var a, n, o = this.getWidget(), s = o.getPane(), l = s.getChart(), u = s.getYAxisComponentById(), c = l.getXAxisPane().getXAxisComponent(), d = o.getBounding();
      return (n = (a = t.createXAxisFigures) === null || a === void 0 ? void 0 : a.call(t, { chart: l, overlay: t, coordinates: i, bounding: d, xAxis: c, yAxis: u })) !== null && n !== void 0 ? n : [];
    }, e;
  })(Mi)
), Ro = (
  /** @class */
  (function(r) {
    q(e, r);
    function e() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return e.prototype.compare = function(t) {
      return C(t.timestamp);
    }, e.prototype.getDirectionStyles = function(t) {
      return t.vertical;
    }, e.prototype.getText = function(t, i) {
      var a, n, o = t.timestamp;
      return i.getInnerFormatter().formatDate(o, Ti[(n = (a = i.getPeriod()) === null || a === void 0 ? void 0 : a.type) !== null && n !== void 0 ? n : "day"], "crosshair");
    }, e.prototype.getTextAttrs = function(t, i, a, n, o, s) {
      var l = a.realX, u = 0, c = "center";
      return l - i / 2 - s.paddingLeft < 0 ? (u = 0, c = "left") : l + i / 2 + s.paddingRight > n.width ? (u = n.width, c = "right") : u = l, { x: u, y: 0, text: t, align: c, baseline: "top" };
    }, e;
  })(Pi)
), Fo = (
  /** @class */
  (function(r) {
    q(e, r);
    function e(t, i) {
      var a = r.call(this, t, i) || this;
      return a._xAxisView = new Do(a), a._overlayXAxisView = new ko(a), a._crosshairVerticalLabelView = new Ro(a), a.setCursor("ew-resize"), a.addChild(a._overlayXAxisView), a;
    }
    return e.prototype.getName = function() {
      return U.X_AXIS;
    }, e.prototype.updateMain = function(t) {
      this._xAxisView.draw(t);
    }, e.prototype.updateOverlay = function(t) {
      this._overlayXAxisView.draw(t), this._crosshairVerticalLabelView.draw(t);
    }, e;
  })(xr)
), Bo = (
  /** @class */
  (function(r) {
    q(e, r);
    function e(t, i) {
      var a = r.call(this, t) || this;
      return a.override(i), a;
    }
    return e.prototype.override = function(t) {
      var i = t.name, a = t.scrollZoomEnabled, n = t.createTicks;
      !K(this.name) && K(i) && (this.name = i), this.scrollZoomEnabled = a ?? this.scrollZoomEnabled, this.createTicks = n ?? this.createTicks;
    }, e.prototype.createRangeImp = function() {
      var t = this.getParent().getChart().getChartStore(), i = t.getVisibleRange(), a = i.realFrom, n = i.realTo, o = a, s = n, l = n - a + 1, u = {
        from: o,
        to: s,
        range: l,
        realFrom: o,
        realTo: s,
        realRange: l,
        displayFrom: o,
        displayTo: s,
        displayRange: l
      };
      return u;
    }, e.prototype.createTicksImp = function() {
      var t, i = this.getRange(), a = i.realFrom, n = i.realTo, o = i.from, s = this.getParent().getChart().getChartStore(), l = s.getInnerFormatter().formatDate, u = s.getPeriod(), c = [], d = s.getBarSpace().bar, h = s.getStyles().xAxis.tickText, f = Math.max(Wt("YYYY-MM-DD HH:mm:ss", h.size, h.weight, h.family), this.getBounding().width / De), v = Math.ceil(f / d);
      v % 2 !== 0 && (v += 1);
      for (var p = Math.max(0, Math.floor(a / v) * v), g = p; g < n; g += v)
        if (g >= o) {
          var m = s.dataIndexToTimestamp(g);
          B(m) && c.push({
            coord: this.convertToPixel(g),
            value: m,
            text: l(m, po[(t = u?.type) !== null && t !== void 0 ? t : "day"], "xAxis")
          });
        }
      return ct(this.createTicks) ? this.createTicks({
        range: this.getRange(),
        bounding: this.getBounding(),
        defaultTicks: c
      }) : c;
    }, e.prototype.getAutoSize = function() {
      var t = this.getParent().getChart().getStyles(), i = t.xAxis, a = i.size;
      if (a !== "auto")
        return a;
      var n = t.crosshair, o = 0;
      i.show && (i.axisLine.show && (o += i.axisLine.size), i.tickLine.show && (o += i.tickLine.length), i.tickText.show && (o += i.tickText.marginStart + i.tickText.marginEnd + i.tickText.size));
      var s = 0;
      return n.show && n.vertical.show && n.vertical.text.show && (s += n.vertical.text.paddingTop + n.vertical.text.paddingBottom + n.vertical.text.borderSize * 2 + n.vertical.text.size), Math.max(o, s);
    }, e.prototype.getBounding = function() {
      return this.getParent().getMainWidget().getBounding();
    }, e.prototype.convertTimestampFromPixel = function(t) {
      var i = this.getParent().getChart().getChartStore(), a = i.coordinateToDataIndex(t);
      return i.dataIndexToTimestamp(a);
    }, e.prototype.convertTimestampToPixel = function(t) {
      var i = this.getParent().getChart().getChartStore(), a = i.timestampToDataIndex(t);
      return i.dataIndexToCoordinate(a);
    }, e.prototype.convertFromPixel = function(t) {
      return this.getParent().getChart().getChartStore().coordinateToDataIndex(t);
    }, e.prototype.convertToPixel = function(t) {
      return this.getParent().getChart().getChartStore().dataIndexToCoordinate(t);
    }, e.extend = function(t) {
      var i = (
        /** @class */
        (function(a) {
          q(n, a);
          function n(o) {
            return a.call(this, o, t) || this;
          }
          return n;
        })(e)
      );
      return i;
    }, e;
  })(Di)
), Oo = {
  name: "normal"
}, Xr = {
  normal: Bo.extend(Oo)
};
function Lo(r) {
  var e;
  return (e = Xr[r]) !== null && e !== void 0 ? e : Xr.normal;
}
var Vo = (
  /** @class */
  (function(r) {
    q(e, r);
    function e(t, i) {
      var a = r.call(this, t, i) || this;
      return a.overrideXAxis({ name: "normal", scrollZoomEnabled: !0 }), a;
    }
    return e.prototype.setOptions = function(t) {
      return r.prototype.setOptions.call(this, t);
    }, e.prototype.overrideXAxis = function(t) {
      var i = t.name;
      return (!C(this._xAxis) || C(i) && this._xAxis.name !== i) && (this._xAxis = this.createXAxisComponent(i ?? "normal")), this._xAxis.override(t), this.setAxisCursor(this._xAxis.scrollZoomEnabled), this;
    }, e.prototype.getXAxisComponent = function() {
      return this._xAxis;
    }, e.prototype.createXAxisComponent = function(t) {
      var i = Lo(t);
      return new i(this);
    }, e.prototype.createMainWidget = function(t) {
      return new Fo(t, this);
    }, e;
  })(Ri)
);
function No(r, e) {
  var t = 0;
  return function() {
    var i = Date.now();
    i - t > e && (r.apply(this, arguments), t = i);
  };
}
var Yo = (
  /** @class */
  (function(r) {
    q(e, r);
    function e(t, i) {
      var a = r.call(this, t, i) || this;
      return a._dragFlag = !1, a._dragStartY = 0, a._topPaneHeight = 0, a._bottomPaneHeight = 0, a._topPane = null, a._bottomPane = null, a._pressedMouseMoveEvent = No(a._pressedTouchMouseMoveEvent, 20), a.registerEvent("touchStartEvent", a._mouseDownEvent.bind(a)).registerEvent("touchMoveEvent", a._pressedMouseMoveEvent.bind(a)).registerEvent("touchEndEvent", a._mouseUpEvent.bind(a)).registerEvent("mouseDownEvent", a._mouseDownEvent.bind(a)).registerEvent("mouseUpEvent", a._mouseUpEvent.bind(a)).registerEvent("pressedMouseMoveEvent", a._pressedMouseMoveEvent.bind(a)).registerEvent("mouseEnterEvent", a._mouseEnterEvent.bind(a)).registerEvent("mouseLeaveEvent", a._mouseLeaveEvent.bind(a)), a;
    }
    return e.prototype.getName = function() {
      return U.SEPARATOR;
    }, e.prototype._dragEnabled = function(t, i) {
      return t.getOptions().state === "normal" && i.getOptions().state === "normal" && i.getOptions().dragEnabled;
    }, e.prototype._findAdjustablePane = function(t, i) {
      for (var a = this.getPane().getChart().getDrawPanes(), n = t; n >= 0 && n < a.length; n += i) {
        var o = a[n];
        if (o.getId() !== j.X_AXIS && o.getOptions().state === "normal")
          return o;
      }
      return null;
    }, e.prototype._findDragPanes = function() {
      var t = this.getPane(), i = t.getChart().getDrawPanes(), a = i.indexOf(t.getTopPane()), n = i.indexOf(t.getBottomPane());
      if (a === -1 || n === -1)
        return null;
      var o = this._findAdjustablePane(a, -1), s = this._findAdjustablePane(n, 1);
      return C(o) && C(s) && this._dragEnabled(o, s) ? { topPane: o, bottomPane: s } : null;
    }, e.prototype._mouseDownEvent = function(t) {
      var i = this._findDragPanes();
      return C(i) ? (this._topPane = i.topPane, this._bottomPane = i.bottomPane, this._dragFlag = !0, this._dragStartY = t.pageY, this._topPaneHeight = this._topPane.getBounding().height, this._bottomPaneHeight = this._bottomPane.getBounding().height, !0) : (this._topPane = null, this._bottomPane = null, !1);
    }, e.prototype._mouseUpEvent = function() {
      return this._dragFlag = !1, this._topPane = null, this._bottomPane = null, this._topPaneHeight = 0, this._bottomPaneHeight = 0, this._mouseLeaveEvent();
    }, e.prototype._pressedTouchMouseMoveEvent = function(t) {
      var i = t.pageY - this._dragStartY, a = i < 0;
      if (C(this._topPane) && C(this._bottomPane) && this._dragEnabled(this._topPane, this._bottomPane)) {
        var n = null, o = null, s = 0, l = 0;
        a ? (n = this._topPane, o = this._bottomPane, s = this._topPaneHeight, l = this._bottomPaneHeight) : (n = this._bottomPane, o = this._topPane, s = this._bottomPaneHeight, l = this._topPaneHeight);
        var u = n.getOptions().minHeight;
        if (s > u) {
          var c = Math.max(s - Math.abs(i), u), d = s - c;
          n.setBounding({ height: c });
          var h = l + d;
          o.setBounding({ height: h }), n.setOptions({ height: c }), o.setOptions({ height: h });
          var f = this.getPane(), v = f.getChart();
          v.getChartStore().executeAction("onPaneDrag", { paneId: f.getId() }), v.layout({
            measureHeight: !0,
            measureWidth: !0,
            update: !0,
            buildYAxisTick: !0,
            forceBuildYAxisTick: !0
          });
        }
      }
      return !0;
    }, e.prototype._mouseEnterEvent = function() {
      var t = this._findDragPanes();
      if (C(t)) {
        var i = this.getPane().getChart(), a = i.getStyles().separator;
        return this.getContainer().style.background = a.activeBackgroundColor, !0;
      }
      return !1;
    }, e.prototype._mouseLeaveEvent = function() {
      return this._dragFlag ? !1 : (this.getContainer().style.background = "transparent", !0);
    }, e.prototype.createContainer = function() {
      return Gt("div", {
        width: "100%",
        height: "".concat(Re, "px"),
        margin: "0",
        padding: "0",
        position: "absolute",
        top: "-3px",
        zIndex: "20",
        boxSizing: "border-box",
        cursor: "ns-resize"
      });
    }, e.prototype.updateImp = function(t, i, a) {
      if (a === 4 || a === 2) {
        var n = this.getPane().getChart().getStyles().separator;
        t.style.top = "".concat(-Math.floor((Re - n.size) / 2), "px"), t.style.height = "".concat(Re, "px");
      }
    }, e;
  })(mi)
), Wo = (
  /** @class */
  (function(r) {
    q(e, r);
    function e(t, i, a, n) {
      var o = r.call(this, t, i) || this;
      return o.getContainer().style.overflow = "", o._topPane = a, o._bottomPane = n, o._separatorWidget = new Yo(o.getContainer(), o), o;
    }
    return e.prototype.setBounding = function(t) {
      return dt(this.getBounding(), t), this;
    }, e.prototype.getTopPane = function() {
      return this._topPane;
    }, e.prototype.setTopPane = function(t) {
      return this._topPane = t, this;
    }, e.prototype.getBottomPane = function() {
      return this._bottomPane;
    }, e.prototype.setBottomPane = function(t) {
      return this._bottomPane = t, this;
    }, e.prototype.getWidget = function() {
      return this._separatorWidget;
    }, e.prototype.getImage = function(t) {
      var i = this.getBounding(), a = i.width, n = i.height, o = this.getChart().getStyles().separator, s = Gt("canvas", {
        width: "".concat(a, "px"),
        height: "".concat(n, "px"),
        boxSizing: "border-box"
      }), l = s.getContext("2d"), u = te(s);
      return s.width = a * u, s.height = n * u, l.scale(u, u), l.fillStyle = o.color, l.fillRect(0, 0, a, n), s;
    }, e.prototype.updateImp = function(t, i, a) {
      if (t === 4 || t === 2) {
        var n = this.getChart().getStyles().separator;
        i.style.backgroundColor = n.color, i.style.height = "".concat(a.height, "px"), i.style.marginLeft = "".concat(a.left, "px"), i.style.width = "".concat(a.width, "px"), this._separatorWidget.update(t);
      }
    }, e;
  })(ki)
);
function qr() {
  return typeof window > "u" ? !1 : window.navigator.userAgent.toLowerCase().includes("firefox");
}
function Qe() {
  return typeof window > "u" ? !1 : /iPhone|iPad|iPod|iOS/.test(window.navigator.userAgent);
}
function zo() {
  return /Mac|iPhone|iPad|iPod|iOS/.test(window.navigator.userAgent);
}
var ze = {
  ResetClick: 500,
  LongTap: 500,
  PreventFiresTouchEvents: 500
}, me = {
  CancelClick: 5,
  CancelTap: 5,
  DoubleClick: 5,
  DoubleTap: 30
}, Ae = {
  Left: 0,
  Middle: 1,
  Right: 2
}, $o = 10, Xo = (
  /** @class */
  (function() {
    function r(e, t, i) {
      var a = this;
      this._clickCount = 0, this._clickTimeoutId = null, this._clickCoordinate = { x: Number.NEGATIVE_INFINITY, y: Number.POSITIVE_INFINITY }, this._tapCount = 0, this._tapTimeoutId = null, this._tapCoordinate = { x: Number.NEGATIVE_INFINITY, y: Number.POSITIVE_INFINITY }, this._longTapTimeoutId = null, this._longTapActive = !1, this._mouseMoveStartCoordinate = null, this._touchMoveStartCoordinate = null, this._touchMoveExceededManhattanDistance = !1, this._cancelClick = !1, this._cancelTap = !1, this._unsubscribeOutsideMouseEvents = null, this._unsubscribeOutsideTouchEvents = null, this._unsubscribeMobileSafariEvents = null, this._unsubscribeMousemove = null, this._unsubscribeMouseWheel = null, this._unsubscribeContextMenu = null, this._unsubscribeRootMouseEvents = null, this._unsubscribeRootTouchEvents = null, this._startPinchMiddleCoordinate = null, this._startPinchDistance = 0, this._pinchPrevented = !1, this._preventTouchDragProcess = !1, this._mousePressed = !1, this._lastTouchEventTimeStamp = 0, this._activeTouchId = null, this._acceptMouseLeave = !Qe(), this._onFirefoxOutsideMouseUp = function(n) {
        a._mouseUpHandler(n);
      }, this._onMobileSafariDoubleClick = function(n) {
        if (a._firesTouchEvents(n)) {
          if (++a._tapCount, a._tapTimeoutId !== null && a._tapCount > 1) {
            var o = a._mouseTouchMoveWithDownInfo(a._getCoordinate(n), a._tapCoordinate).manhattanDistance;
            o < me.DoubleTap && !a._cancelTap && a._processEvent(a._makeCompatEvent(n), a._handler.doubleTapEvent), a._resetTapTimeout();
          }
        } else if (++a._clickCount, a._clickTimeoutId !== null && a._clickCount > 1) {
          var o = a._mouseTouchMoveWithDownInfo(a._getCoordinate(n), a._clickCoordinate).manhattanDistance;
          o < me.DoubleClick && !a._cancelClick && a._processEvent(a._makeCompatEvent(n), a._handler.mouseDoubleClickEvent), a._resetClickTimeout();
        }
      }, this._target = e, this._handler = t, this._options = i, this._init();
    }
    return r.prototype.destroy = function() {
      this._unsubscribeOutsideMouseEvents !== null && (this._unsubscribeOutsideMouseEvents(), this._unsubscribeOutsideMouseEvents = null), this._unsubscribeOutsideTouchEvents !== null && (this._unsubscribeOutsideTouchEvents(), this._unsubscribeOutsideTouchEvents = null), this._unsubscribeMousemove !== null && (this._unsubscribeMousemove(), this._unsubscribeMousemove = null), this._unsubscribeMouseWheel !== null && (this._unsubscribeMouseWheel(), this._unsubscribeMouseWheel = null), this._unsubscribeContextMenu !== null && (this._unsubscribeContextMenu(), this._unsubscribeContextMenu = null), this._unsubscribeRootMouseEvents !== null && (this._unsubscribeRootMouseEvents(), this._unsubscribeRootMouseEvents = null), this._unsubscribeRootTouchEvents !== null && (this._unsubscribeRootTouchEvents(), this._unsubscribeRootTouchEvents = null), this._unsubscribeMobileSafariEvents !== null && (this._unsubscribeMobileSafariEvents(), this._unsubscribeMobileSafariEvents = null), this._clearLongTapTimeout(), this._resetClickTimeout();
    }, r.prototype._mouseEnterHandler = function(e) {
      var t = this, i, a, n;
      (i = this._unsubscribeMousemove) === null || i === void 0 || i.call(this), (a = this._unsubscribeMouseWheel) === null || a === void 0 || a.call(this), (n = this._unsubscribeContextMenu) === null || n === void 0 || n.call(this);
      var o = this._mouseMoveHandler.bind(this);
      this._unsubscribeMousemove = function() {
        t._target.removeEventListener("mousemove", o);
      }, this._target.addEventListener("mousemove", o);
      var s = this._mouseWheelHandler.bind(this);
      this._unsubscribeMouseWheel = function() {
        t._target.removeEventListener("wheel", s);
      }, this._target.addEventListener("wheel", s, { passive: !1 });
      var l = this._contextMenuHandler.bind(this);
      this._unsubscribeContextMenu = function() {
        t._target.removeEventListener("contextmenu", l);
      }, this._target.addEventListener("contextmenu", l, { passive: !1 }), !this._firesTouchEvents(e) && (this._processEvent(this._makeCompatEvent(e), this._handler.mouseEnterEvent), this._acceptMouseLeave = !0);
    }, r.prototype._resetClickTimeout = function() {
      this._clickTimeoutId !== null && clearTimeout(this._clickTimeoutId), this._clickCount = 0, this._clickTimeoutId = null, this._clickCoordinate = { x: Number.NEGATIVE_INFINITY, y: Number.POSITIVE_INFINITY };
    }, r.prototype._resetTapTimeout = function() {
      this._tapTimeoutId !== null && clearTimeout(this._tapTimeoutId), this._tapCount = 0, this._tapTimeoutId = null, this._tapCoordinate = { x: Number.NEGATIVE_INFINITY, y: Number.POSITIVE_INFINITY };
    }, r.prototype._mouseMoveHandler = function(e) {
      this._mousePressed || this._touchMoveStartCoordinate !== null || this._firesTouchEvents(e) || (this._processEvent(this._makeCompatEvent(e), this._handler.mouseMoveEvent), this._acceptMouseLeave = !0);
    }, r.prototype._mouseWheelHandler = function(e) {
      if (Math.abs(e.deltaX) > Math.abs(e.deltaY)) {
        if (!C(this._handler.mouseWheelHortEvent) || (this._preventDefault(e), Math.abs(e.deltaX) === 0))
          return;
        this._handler.mouseWheelHortEvent(this._makeCompatEvent(e), -e.deltaX);
      } else {
        if (!C(this._handler.mouseWheelVertEvent))
          return;
        var t = -(e.deltaY / 100);
        if (t === 0)
          return;
        switch (this._preventDefault(e), e.deltaMode) {
          case e.DOM_DELTA_PAGE: {
            t *= 120;
            break;
          }
          case e.DOM_DELTA_LINE: {
            t *= 32;
            break;
          }
        }
        if (t !== 0) {
          var i = Math.sign(t) * Math.min(1, Math.abs(t));
          this._handler.mouseWheelVertEvent(this._makeCompatEvent(e), i);
        }
      }
    }, r.prototype._contextMenuHandler = function(e) {
      this._preventDefault(e);
    }, r.prototype._touchMoveHandler = function(e) {
      var t = this._touchWithId(e.changedTouches, this._activeTouchId);
      if (t !== null && (this._lastTouchEventTimeStamp = this._eventTimeStamp(e), this._startPinchMiddleCoordinate === null && !this._preventTouchDragProcess)) {
        this._pinchPrevented = !0;
        var i = this._mouseTouchMoveWithDownInfo(this._getCoordinate(t), this._touchMoveStartCoordinate), a = i.xOffset, n = i.yOffset, o = i.manhattanDistance;
        if (!(!this._touchMoveExceededManhattanDistance && o < me.CancelTap)) {
          if (!this._touchMoveExceededManhattanDistance) {
            var s = a * 0.5, l = n >= s && !this._options.treatVertDragAsPageScroll(), u = s > n && !this._options.treatHorzDragAsPageScroll();
            !l && !u && (this._preventTouchDragProcess = !0), this._touchMoveExceededManhattanDistance = !0, this._cancelTap = !0, this._clearLongTapTimeout(), this._resetTapTimeout();
          }
          this._preventTouchDragProcess || this._processEvent(this._makeCompatEvent(e, t), this._handler.touchMoveEvent);
        }
      }
    }, r.prototype._mouseMoveWithDownHandler = function(e) {
      if (e.button === Ae.Left) {
        var t = this._mouseTouchMoveWithDownInfo(this._getCoordinate(e), this._mouseMoveStartCoordinate), i = t.manhattanDistance;
        i >= me.CancelClick && (this._cancelClick = !0, this._resetClickTimeout()), this._cancelClick && this._processEvent(this._makeCompatEvent(e), this._handler.pressedMouseMoveEvent);
      }
    }, r.prototype._mouseTouchMoveWithDownInfo = function(e, t) {
      var i = Math.abs(t.x - e.x), a = Math.abs(t.y - e.y), n = i + a;
      return { xOffset: i, yOffset: a, manhattanDistance: n };
    }, r.prototype._touchEndHandler = function(e) {
      var t = this._touchWithId(e.changedTouches, this._activeTouchId);
      if (t === null && e.touches.length === 0 && (t = e.changedTouches[0]), t !== null) {
        this._activeTouchId = null, this._lastTouchEventTimeStamp = this._eventTimeStamp(e), this._clearLongTapTimeout(), this._touchMoveStartCoordinate = null, this._unsubscribeRootTouchEvents !== null && (this._unsubscribeRootTouchEvents(), this._unsubscribeRootTouchEvents = null);
        var i = this._makeCompatEvent(e, t);
        if (this._processEvent(i, this._handler.touchEndEvent), ++this._tapCount, this._tapTimeoutId !== null && this._tapCount > 1) {
          var a = this._mouseTouchMoveWithDownInfo(this._getCoordinate(t), this._tapCoordinate).manhattanDistance;
          a < me.DoubleTap && !this._cancelTap && this._processEvent(i, this._handler.doubleTapEvent), this._resetTapTimeout();
        } else
          this._cancelTap || (this._processEvent(i, this._handler.tapEvent), C(this._handler.tapEvent) && this._preventDefault(e));
        this._tapCount === 0 && this._preventDefault(e), e.touches.length === 0 && this._longTapActive && (this._longTapActive = !1, this._preventDefault(e));
      }
    }, r.prototype._mouseUpHandler = function(e) {
      if (e.button === Ae.Left) {
        var t = this._makeCompatEvent(e);
        if (this._mouseMoveStartCoordinate = null, this._mousePressed = !1, this._unsubscribeRootMouseEvents !== null && (this._unsubscribeRootMouseEvents(), this._unsubscribeRootMouseEvents = null), qr()) {
          var i = this._target.ownerDocument.documentElement;
          i.removeEventListener("mouseleave", this._onFirefoxOutsideMouseUp);
        }
        if (!this._firesTouchEvents(e))
          if (this._processEvent(t, this._handler.mouseUpEvent), ++this._clickCount, this._clickTimeoutId !== null && this._clickCount > 1) {
            var a = this._mouseTouchMoveWithDownInfo(this._getCoordinate(e), this._clickCoordinate).manhattanDistance;
            a < me.DoubleClick && !this._cancelClick && this._processEvent(t, this._handler.mouseDoubleClickEvent), this._resetClickTimeout();
          } else
            this._cancelClick || this._processEvent(t, this._handler.mouseClickEvent);
      }
    }, r.prototype._clearLongTapTimeout = function() {
      this._longTapTimeoutId !== null && (clearTimeout(this._longTapTimeoutId), this._longTapTimeoutId = null);
    }, r.prototype._touchStartHandler = function(e) {
      if (this._activeTouchId === null) {
        var t = e.changedTouches[0];
        this._activeTouchId = t.identifier, this._lastTouchEventTimeStamp = this._eventTimeStamp(e);
        var i = this._target.ownerDocument.documentElement;
        this._cancelTap = !1, this._touchMoveExceededManhattanDistance = !1, this._preventTouchDragProcess = !1, this._touchMoveStartCoordinate = this._getCoordinate(t), this._unsubscribeRootTouchEvents !== null && (this._unsubscribeRootTouchEvents(), this._unsubscribeRootTouchEvents = null);
        {
          var a = this._touchMoveHandler.bind(this), n = this._touchEndHandler.bind(this);
          this._unsubscribeRootTouchEvents = function() {
            i.removeEventListener("touchmove", a), i.removeEventListener("touchend", n);
          }, i.addEventListener("touchmove", a, { passive: !1 }), i.addEventListener("touchend", n, { passive: !1 }), this._clearLongTapTimeout(), this._longTapTimeoutId = setTimeout(this._longTapHandler.bind(this, e), ze.LongTap);
        }
        this._processEvent(this._makeCompatEvent(e, t), this._handler.touchStartEvent), this._tapTimeoutId === null && (this._tapCount = 0, this._tapTimeoutId = setTimeout(this._resetTapTimeout.bind(this), ze.ResetClick), this._tapCoordinate = this._getCoordinate(t));
      }
    }, r.prototype._mouseDownHandler = function(e) {
      if (e.button === Ae.Right) {
        this._preventDefault(e), this._processEvent(this._makeCompatEvent(e), this._handler.mouseRightClickEvent);
        return;
      }
      if (e.button === Ae.Left) {
        var t = this._target.ownerDocument.documentElement;
        qr() && t.addEventListener("mouseleave", this._onFirefoxOutsideMouseUp), this._cancelClick = !1, this._mouseMoveStartCoordinate = this._getCoordinate(e), this._unsubscribeRootMouseEvents !== null && (this._unsubscribeRootMouseEvents(), this._unsubscribeRootMouseEvents = null);
        {
          var i = this._mouseMoveWithDownHandler.bind(this), a = this._mouseUpHandler.bind(this);
          this._unsubscribeRootMouseEvents = function() {
            t.removeEventListener("mousemove", i), t.removeEventListener("mouseup", a);
          }, t.addEventListener("mousemove", i), t.addEventListener("mouseup", a);
        }
        this._mousePressed = !0, !this._firesTouchEvents(e) && (this._processEvent(this._makeCompatEvent(e), this._handler.mouseDownEvent), this._clickTimeoutId === null && (this._clickCount = 0, this._clickTimeoutId = setTimeout(this._resetClickTimeout.bind(this), ze.ResetClick), this._clickCoordinate = this._getCoordinate(e)));
      }
    }, r.prototype._init = function() {
      var e = this;
      this._target.addEventListener("mouseenter", this._mouseEnterHandler.bind(this)), this._target.addEventListener("touchcancel", this._clearLongTapTimeout.bind(this));
      {
        var t = this._target.ownerDocument, i = function(a) {
          e._handler.mouseDownOutsideEvent != null && (a.composed && e._target.contains(a.composedPath()[0]) || a.target !== null && e._target.contains(a.target) || e._handler.mouseDownOutsideEvent({ x: 0, y: 0, pageX: 0, pageY: 0 }));
        };
        this._unsubscribeOutsideTouchEvents = function() {
          t.removeEventListener("touchstart", i);
        }, this._unsubscribeOutsideMouseEvents = function() {
          t.removeEventListener("mousedown", i);
        }, t.addEventListener("mousedown", i), t.addEventListener("touchstart", i, { passive: !0 });
      }
      Qe() && (this._unsubscribeMobileSafariEvents = function() {
        e._target.removeEventListener("dblclick", e._onMobileSafariDoubleClick);
      }, this._target.addEventListener("dblclick", this._onMobileSafariDoubleClick)), this._target.addEventListener("mouseleave", this._mouseLeaveHandler.bind(this)), this._target.addEventListener("touchstart", this._touchStartHandler.bind(this), { passive: !0 }), this._target.addEventListener("mousedown", function(a) {
        if (a.button === Ae.Middle)
          return a.preventDefault(), !1;
      }), this._target.addEventListener("mousedown", this._mouseDownHandler.bind(this)), this._initPinch(), this._target.addEventListener("touchmove", function() {
      }, { passive: !1 });
    }, r.prototype._initPinch = function() {
      var e = this;
      !C(this._handler.pinchStartEvent) && !C(this._handler.pinchEvent) && !C(this._handler.pinchEndEvent) || (this._target.addEventListener("touchstart", function(t) {
        e._checkPinchState(t.touches);
      }, { passive: !0 }), this._target.addEventListener("touchmove", function(t) {
        if (!(t.touches.length !== 2 || e._startPinchMiddleCoordinate === null) && C(e._handler.pinchEvent)) {
          var i = e._getTouchDistance(t.touches[0], t.touches[1]), a = i / e._startPinchDistance;
          e._handler.pinchEvent(M(M({}, e._startPinchMiddleCoordinate), { pageX: 0, pageY: 0 }), a), e._preventDefault(t);
        }
      }, { passive: !1 }), this._target.addEventListener("touchend", function(t) {
        e._checkPinchState(t.touches);
      }));
    }, r.prototype._checkPinchState = function(e) {
      e.length === 1 && (this._pinchPrevented = !1), e.length !== 2 || this._pinchPrevented || this._longTapActive ? this._stopPinch() : this._startPinch(e);
    }, r.prototype._startPinch = function(e) {
      var t = this._target.getBoundingClientRect();
      this._startPinchMiddleCoordinate = {
        x: (e[0].clientX - t.left + (e[1].clientX - t.left)) / 2,
        y: (e[0].clientY - t.top + (e[1].clientY - t.top)) / 2
      }, this._startPinchDistance = this._getTouchDistance(e[0], e[1]), C(this._handler.pinchStartEvent) && this._handler.pinchStartEvent({ x: 0, y: 0, pageX: 0, pageY: 0 }), this._clearLongTapTimeout();
    }, r.prototype._stopPinch = function() {
      this._startPinchMiddleCoordinate !== null && (this._startPinchMiddleCoordinate = null, C(this._handler.pinchEndEvent) && this._handler.pinchEndEvent({ x: 0, y: 0, pageX: 0, pageY: 0 }));
    }, r.prototype._mouseLeaveHandler = function(e) {
      var t, i, a;
      (t = this._unsubscribeMousemove) === null || t === void 0 || t.call(this), (i = this._unsubscribeMouseWheel) === null || i === void 0 || i.call(this), (a = this._unsubscribeContextMenu) === null || a === void 0 || a.call(this), !this._firesTouchEvents(e) && this._acceptMouseLeave && (this._processEvent(this._makeCompatEvent(e), this._handler.mouseLeaveEvent), this._acceptMouseLeave = !Qe());
    }, r.prototype._longTapHandler = function(e) {
      var t = this._touchWithId(e.touches, this._activeTouchId);
      t !== null && (this._processEvent(this._makeCompatEvent(e, t), this._handler.longTapEvent), this._cancelTap = !0, this._longTapActive = !0);
    }, r.prototype._firesTouchEvents = function(e) {
      var t;
      return C((t = e.sourceCapabilities) === null || t === void 0 ? void 0 : t.firesTouchEvents) ? e.sourceCapabilities.firesTouchEvents : this._eventTimeStamp(e) < this._lastTouchEventTimeStamp + ze.PreventFiresTouchEvents;
    }, r.prototype._processEvent = function(e, t) {
      t?.call(this._handler, e);
    }, r.prototype._makeCompatEvent = function(e, t) {
      var i = this, a = t ?? e, n = this._target.getBoundingClientRect();
      return {
        x: a.clientX - n.left,
        y: a.clientY - n.top,
        pageX: a.pageX,
        pageY: a.pageY,
        isTouch: !e.type.startsWith("mouse") && e.type !== "contextmenu" && e.type !== "click" && e.type !== "wheel",
        preventDefault: function() {
          e.type !== "touchstart" && i._preventDefault(e);
        }
      };
    }, r.prototype._getTouchDistance = function(e, t) {
      var i = e.clientX - t.clientX, a = e.clientY - t.clientY;
      return Math.sqrt(i * i + a * a);
    }, r.prototype._preventDefault = function(e) {
      e.cancelable && e.preventDefault();
    }, r.prototype._getCoordinate = function(e) {
      return {
        x: e.pageX,
        y: e.pageY
      };
    }, r.prototype._eventTimeStamp = function(e) {
      var t;
      return (t = e.timeStamp) !== null && t !== void 0 ? t : performance.now();
    }, r.prototype._touchWithId = function(e, t) {
      for (var i = 0; i < e.length; ++i)
        if (e[i].identifier === t)
          return e[i];
      return null;
    }, r;
  })()
), Hr = {
  name: "scrollLeft",
  keys: "Shift+ArrowLeft",
  action: function(r) {
    var e = r.chart;
    e.scrollByDistance(-3 * e.getBarSpace().bar);
  }
}, Ur = {
  name: "scrollRight",
  keys: "Shift+ArrowRight",
  action: function(r) {
    var e = r.chart;
    e.scrollByDistance(3 * e.getBarSpace().bar);
  }
}, Gr = {
  name: "zoomIn",
  keys: ["Shift+Equal", "Shift+NumpadAdd"],
  action: function(r) {
    var e = r.chart;
    e.zoomAtCoordinate(1.05);
  }
}, jr = {
  name: "zoomOut",
  keys: ["Shift+Minus", "Shift+NumpadSubtract"],
  action: function(r) {
    var e = r.chart;
    e.zoomAtCoordinate(0.95);
  }
}, _e, Bi = (_e = {}, _e[Hr.name] = Hr, _e[Ur.name] = Ur, _e[Gr.name] = Gr, _e[jr.name] = jr, _e);
function qo(r) {
  var e;
  return (e = Bi[r]) !== null && e !== void 0 ? e : null;
}
function Ho() {
  return Object.keys(Bi);
}
var Uo = {
  command: "meta",
  cmd: "meta",
  control: "ctrl",
  option: "alt",
  mod: zo() ? "meta" : "ctrl"
}, Zr = {
  "+": "equal",
  plus: "equal",
  add: "equal",
  numpadadd: "equal",
  "-": "minus",
  subtract: "minus",
  numpadsubtract: "minus",
  esc: "escape",
  del: "delete",
  left: "arrowleft",
  right: "arrowright",
  up: "arrowup",
  down: "arrowdown"
}, tr = ["ctrl", "alt", "shift", "meta"], Go = (
  /** @class */
  (function() {
    function r(e, t) {
      var i = this;
      this._flingStartTime = (/* @__PURE__ */ new Date()).getTime(), this._flingScrollRequestId = null, this._startScrollCoordinate = null, this._touchCoordinate = null, this._touchCancelCrosshair = !1, this._touchZoomed = !1, this._pinchScale = 1, this._mouseDownWidget = null, this._prevYAxisRanges = /* @__PURE__ */ new Map(), this._xAxisStartScaleCoordinate = null, this._xAxisStartScaleDistance = 0, this._xAxisScale = 1, this._yAxisStartScaleDistance = 0, this._mouseMoveTriggerWidgetInfo = { pane: null, widget: null }, this._boundKeyBoardDownEvent = function(a) {
        var n, o, s, l = a.target, u = l?.tagName.toLowerCase();
        if (!(u === "input" || u === "textarea" || l?.isContentEditable === !0)) {
          var c = i._chart.getHotKey(), d = c.enabled, h = c.exclude;
          if (d) {
            var f = [];
            a.ctrlKey && f.push("ctrl"), a.altKey && f.push("alt"), a.shiftKey && f.push("shift"), a.metaKey && f.push("meta");
            var v = a.code.trim().toLowerCase();
            /^key[a-z]$/.test(v) ? f.push(v.slice(3)) : /^digit[0-9]$/.test(v) ? f.push(v.slice(5)) : f.push((n = Zr[v]) !== null && n !== void 0 ? n : v);
            for (var p = f.join("+"), g = Ho(), m = g.length - 1; m >= 0; m--) {
              var x = g[m], y = qo(x);
              if (!h.includes(x) && C(y)) {
                var E = Nt(y.keys) ? y.keys : [y.keys], _ = E.some(function(w) {
                  var b = [], S = "";
                  return w.replace(/\+\+$/, "+Plus").replace(/\+=$/, "+Equal").split("+").forEach(function(T) {
                    var A, D, R = (A = Uo[T.trim().toLowerCase()]) !== null && A !== void 0 ? A : T, P = R.trim().toLowerCase(), k = "";
                    /^key[a-z]$/.test(P) ? k = P.slice(3) : /^digit[0-9]$/.test(P) ? k = P.slice(5) : k = (D = Zr[P]) !== null && D !== void 0 ? D : P, tr.includes(k) ? b.includes(k) || b.push(k) : k.length > 0 && (S = k);
                  }), b.sort(function(T, A) {
                    return tr.indexOf(T) - tr.indexOf(A);
                  }), be(be([], Se(b), !1), [S], !1).filter(function(T) {
                    return T.length > 0;
                  }).join("+") === p;
                });
                if (_) {
                  var I = { chart: i._chart, event: a, key: p, hotkey: y };
                  if (!ct(y.check) || y.check(I)) {
                    (!((o = y.preventDefault) !== null && o !== void 0) || o) && a.preventDefault(), (s = y.stopPropagation) !== null && s !== void 0 && s && a.stopPropagation(), y.action(I);
                    return;
                  }
                }
              }
            }
          }
        }
      }, this._chart = t, this._event = new Xo(e, this, {
        treatVertDragAsPageScroll: function() {
          return !1;
        },
        treatHorzDragAsPageScroll: function() {
          return !1;
        }
      }), document.addEventListener("keydown", this._boundKeyBoardDownEvent);
    }
    return r.prototype._getYAxisByWidget = function(e) {
      return e.getName() === U.Y_AXIS ? e.getAxisComponent() : e.getPane().getYAxisComponentById();
    }, r.prototype._getYAxisScaleTargetByWidget = function(e) {
      var t = this._getYAxisByWidget(e), i = e.getPane();
      return i.isManualYAxis(t.id) ? i.getYAxisComponentById() : t;
    }, r.prototype._syncYAxisValueRange = function(e, t) {
      var i = e.getRange(), a = t.from, n = t.to, o = e.valueToRealValue(a, { range: i }), s = e.valueToRealValue(n, { range: i }), l = e.realValueToDisplayValue(o, { range: i }), u = e.realValueToDisplayValue(s, { range: i });
      e.setRange({
        from: a,
        to: n,
        range: n - a,
        realFrom: o,
        realTo: s,
        realRange: s - o,
        displayFrom: l,
        displayTo: u,
        displayRange: u - l
      });
    }, r.prototype._syncManualYAxesValueRange = function(e, t) {
      var i = this, a = t.getRange();
      e.getPane().getYAxisComponents().forEach(function(n) {
        var o = n;
        o !== t && e.getPane().isManualYAxis(o.id) && i._syncYAxisValueRange(o, a);
      });
    }, r.prototype._resetYAxisAndManualYAxes = function(e, t) {
      t.setAutoCalcTickFlag(!0), e.getPane().getYAxisComponents().forEach(function(i) {
        var a = i;
        e.getPane().isManualYAxis(a.id) && a.setAutoCalcTickFlag(!0);
      }), this._chart.layout({
        measureWidth: !0,
        update: !0,
        buildYAxisTick: !0
      });
    }, r.prototype.pinchStartEvent = function() {
      return this._touchZoomed = !0, this._pinchScale = 1, !0;
    }, r.prototype.pinchEvent = function(e, t) {
      var i = this._findWidgetByEvent(e), a = i.pane, n = i.widget;
      if (a?.getId() !== j.X_AXIS && n?.getName() === U.MAIN) {
        var o = this._makeWidgetEvent(e, n), s = (t - this._pinchScale) * 5;
        return this._pinchScale = t, this._chart.getChartStore().zoom(s, { x: o.x, y: o.y }, "main"), !0;
      }
      return !1;
    }, r.prototype.mouseWheelHortEvent = function(e, t) {
      var i = this._chart.getChartStore();
      return i.startScroll(), i.scroll(t), !0;
    }, r.prototype.mouseWheelVertEvent = function(e, t) {
      var i = this._findWidgetByEvent(e).widget, a = this._makeWidgetEvent(e, i), n = i?.getName();
      if (n === U.MAIN)
        return this._chart.getChartStore().zoom(t, { x: a.x, y: a.y }, "main"), !0;
      if (n === U.Y_AXIS) {
        var o = i, s = this._getYAxisByWidget(o);
        if (s.scrollZoomEnabled) {
          var l = 1 + t * 0.05, u = this._getYAxisScaleTargetByWidget(o);
          return this._zoomYAxis(u, l), this._syncManualYAxesValueRange(o, u), !0;
        }
      }
      return !1;
    }, r.prototype.mouseDownEvent = function(e) {
      var t, i, a = this._findWidgetByEvent(e), n = a.pane, o = a.widget;
      if (this._mouseDownWidget = o, o !== null) {
        var s = this._makeWidgetEvent(e, o), l = o.getName();
        switch (l) {
          case U.SEPARATOR:
            return o.dispatchEvent("mouseDownEvent", s);
          case U.MAIN: {
            var u = o.dispatchEvent("mouseDownEvent", s);
            if (!u) {
              var c = n.getYAxisComponents();
              try {
                for (var d = bt(c), h = d.next(); !h.done; h = d.next()) {
                  var f = h.value, v = f;
                  if (!v.getAutoCalcTickFlag()) {
                    var p = v.getRange();
                    this._prevYAxisRanges.set(v, M({}, p));
                  }
                }
              } catch (g) {
                t = { error: g };
              } finally {
                try {
                  h && !h.done && (i = d.return) && i.call(d);
                } finally {
                  if (t) throw t.error;
                }
              }
              this._startScrollCoordinate = { x: s.x, y: s.y }, this._chart.getChartStore().startScroll();
            }
            return u;
          }
          case U.X_AXIS:
            return this._processXAxisScrollStartEvent(o, s);
          case U.Y_AXIS:
            return this._processYAxisScaleStartEvent(o, s);
        }
      }
      return !1;
    }, r.prototype.mouseMoveEvent = function(e) {
      var t, i, a, n = this._findWidgetByEvent(e), o = n.pane, s = n.widget, l = this._makeWidgetEvent(e, s);
      if ((((t = this._mouseMoveTriggerWidgetInfo.pane) === null || t === void 0 ? void 0 : t.getId()) !== o?.getId() || ((i = this._mouseMoveTriggerWidgetInfo.widget) === null || i === void 0 ? void 0 : i.getName()) !== s?.getName()) && (s?.dispatchEvent("mouseEnterEvent", l), (a = this._mouseMoveTriggerWidgetInfo.widget) === null || a === void 0 || a.dispatchEvent("mouseLeaveEvent", l), this._mouseMoveTriggerWidgetInfo = { pane: o, widget: s }), s !== null) {
        var u = s.getName();
        switch (u) {
          case U.MAIN: {
            var c = s.dispatchEvent("mouseMoveEvent", l), d = { x: l.x, y: l.y, paneId: o?.getId() };
            return c ? (s.getForceCursor() !== "pointer" && (d = void 0), s.setCursor("pointer")) : s.setCursor("crosshair"), this._chart.getChartStore().setCrosshair(d), c;
          }
          case U.SEPARATOR:
          case U.X_AXIS:
          case U.Y_AXIS: {
            var c = s.dispatchEvent("mouseMoveEvent", l);
            return this._chart.getChartStore().setCrosshair(), c;
          }
        }
      }
      return !1;
    }, r.prototype.pressedMouseMoveEvent = function(e) {
      var t, i;
      if (this._mouseDownWidget !== null && this._mouseDownWidget.getName() === U.SEPARATOR)
        return this._mouseDownWidget.dispatchEvent("pressedMouseMoveEvent", e);
      var a = this._findWidgetByEvent(e), n = a.pane, o = a.widget;
      if (o !== null && ((t = this._mouseDownWidget) === null || t === void 0 ? void 0 : t.getPane().getId()) === n?.getId() && ((i = this._mouseDownWidget) === null || i === void 0 ? void 0 : i.getName()) === o.getName()) {
        var s = this._makeWidgetEvent(e, o), l = o.getName();
        switch (l) {
          case U.MAIN: {
            var u = void 0, c = o.dispatchEvent("pressedMouseMoveEvent", s);
            return c ? this._chart.updatePane(
              1
              /* UpdateLevel.Overlay */
            ) : this._processMainScrollingEvent(o, s), (!c || o.getForceCursor() === "pointer") && (u = { x: s.x, y: s.y, paneId: n?.getId() }), this._chart.getChartStore().setCrosshair(u, { forceInvalidate: !0 }), c;
          }
          case U.X_AXIS:
            return this._processXAxisScrollingEvent(o, s);
          case U.Y_AXIS:
            return this._processYAxisScalingEvent(o, s);
        }
      }
      return !1;
    }, r.prototype.mouseUpEvent = function(e) {
      var t = this._findWidgetByEvent(e).widget, i = !1;
      if (t !== null) {
        var a = this._makeWidgetEvent(e, t), n = t.getName();
        switch (n) {
          case U.MAIN:
          case U.SEPARATOR:
          case U.X_AXIS:
          case U.Y_AXIS: {
            i = t.dispatchEvent("mouseUpEvent", a);
            break;
          }
        }
        i && this._chart.updatePane(
          1
          /* UpdateLevel.Overlay */
        );
      }
      return this._mouseDownWidget = null, this._startScrollCoordinate = null, this._prevYAxisRanges.clear(), this._xAxisStartScaleCoordinate = null, this._xAxisStartScaleDistance = 0, this._xAxisScale = 1, this._yAxisStartScaleDistance = 0, i;
    }, r.prototype.mouseClickEvent = function(e) {
      var t = this._findWidgetByEvent(e).widget;
      if (t !== null) {
        var i = this._makeWidgetEvent(e, t);
        return t.dispatchEvent("mouseClickEvent", i);
      }
      return !1;
    }, r.prototype.mouseRightClickEvent = function(e) {
      var t = this._findWidgetByEvent(e).widget, i = !1;
      if (t !== null) {
        var a = this._makeWidgetEvent(e, t), n = t.getName();
        switch (n) {
          case U.MAIN:
          case U.X_AXIS:
          case U.Y_AXIS: {
            i = t.dispatchEvent("mouseRightClickEvent", a);
            break;
          }
        }
        i && this._chart.updatePane(
          1
          /* UpdateLevel.Overlay */
        );
      }
      return !1;
    }, r.prototype.mouseDoubleClickEvent = function(e) {
      var t = this._findWidgetByEvent(e).widget;
      if (t !== null) {
        var i = t.getName();
        switch (i) {
          case U.MAIN: {
            var a = this._makeWidgetEvent(e, t);
            return t.dispatchEvent("mouseDoubleClickEvent", a);
          }
          case U.Y_AXIS: {
            var n = t, o = this._getYAxisByWidget(n), s = this._getYAxisScaleTargetByWidget(n);
            if (!s.getAutoCalcTickFlag() || !o.getAutoCalcTickFlag())
              return this._resetYAxisAndManualYAxes(n, s), !0;
            break;
          }
        }
      }
      return !1;
    }, r.prototype.mouseLeaveEvent = function() {
      return this._chart.getChartStore().setCrosshair(), !0;
    }, r.prototype.touchStartEvent = function(e) {
      var t, i, a, n = this._findWidgetByEvent(e), o = n.pane, s = n.widget;
      if (s !== null) {
        var l = this._makeWidgetEvent(e, s);
        (a = l.preventDefault) === null || a === void 0 || a.call(l);
        var u = s.getName();
        switch (u) {
          case U.MAIN: {
            var c = this._chart.getChartStore();
            if (s.dispatchEvent("mouseDownEvent", l))
              return this._touchCancelCrosshair = !0, this._touchCoordinate = null, c.setCrosshair(void 0, { notInvalidate: !0 }), this._chart.updatePane(
                1
                /* UpdateLevel.Overlay */
              ), !0;
            this._flingScrollRequestId !== null && (nr(this._flingScrollRequestId), this._flingScrollRequestId = null), this._flingStartTime = (/* @__PURE__ */ new Date()).getTime();
            var d = o.getYAxisComponents();
            try {
              for (var h = bt(d), f = h.next(); !f.done; f = h.next()) {
                var v = f.value, p = v;
                if (!p.getAutoCalcTickFlag()) {
                  var g = p.getRange();
                  this._prevYAxisRanges.set(p, M({}, g));
                }
              }
            } catch (E) {
              t = { error: E };
            } finally {
              try {
                f && !f.done && (i = h.return) && i.call(h);
              } finally {
                if (t) throw t.error;
              }
            }
            if (this._startScrollCoordinate = { x: l.x, y: l.y }, c.startScroll(), this._touchZoomed = !1, this._touchCoordinate !== null) {
              var m = l.x - this._touchCoordinate.x, x = l.y - this._touchCoordinate.y, y = Math.sqrt(m * m + x * x);
              y < $o ? (this._touchCoordinate = { x: l.x, y: l.y }, c.setCrosshair({ x: l.x, y: l.y, paneId: o?.getId() })) : (this._touchCoordinate = null, this._touchCancelCrosshair = !0, c.setCrosshair());
            }
            return !0;
          }
          case U.X_AXIS:
            return this._processXAxisScrollStartEvent(s, l);
          case U.Y_AXIS:
            return this._processYAxisScaleStartEvent(s, l);
        }
      }
      return !1;
    }, r.prototype.touchMoveEvent = function(e) {
      var t, i, a, n = this._findWidgetByEvent(e), o = n.pane, s = n.widget;
      if (s !== null) {
        var l = this._makeWidgetEvent(e, s), u = s.getName(), c = this._chart.getChartStore();
        switch (u) {
          case U.MAIN:
            return s.dispatchEvent("pressedMouseMoveEvent", l) ? ((t = l.preventDefault) === null || t === void 0 || t.call(l), c.setCrosshair(void 0, { notInvalidate: !0 }), this._chart.updatePane(
              1
              /* UpdateLevel.Overlay */
            ), !0) : (this._touchCoordinate !== null ? ((i = l.preventDefault) === null || i === void 0 || i.call(l), c.setCrosshair({ x: l.x, y: l.y, paneId: o?.getId() })) : this._processMainScrollingEvent(s, l), !0);
          case U.X_AXIS:
            return (a = l.preventDefault) === null || a === void 0 || a.call(l), this._processXAxisScrollingEvent(s, l);
          case U.Y_AXIS:
            return this._processYAxisScalingEvent(s, l);
        }
      }
      return !1;
    }, r.prototype.touchEndEvent = function(e) {
      var t = this, i = this._findWidgetByEvent(e).widget;
      if (i !== null) {
        var a = this._makeWidgetEvent(e, i), n = i.getName();
        switch (n) {
          case U.MAIN: {
            if (i.dispatchEvent("mouseUpEvent", a), this._startScrollCoordinate !== null) {
              var o = (/* @__PURE__ */ new Date()).getTime() - this._flingStartTime, s = a.x - this._startScrollCoordinate.x, l = s / (o > 0 ? o : 1) * 20;
              if (o < 200 && Math.abs(l) > 0) {
                var u = this._chart.getChartStore(), c = function() {
                  t._flingScrollRequestId = Le(function() {
                    u.startScroll(), u.scroll(l), l = l * (1 - 0.025), Math.abs(l) < 1 ? t._flingScrollRequestId !== null && (nr(t._flingScrollRequestId), t._flingScrollRequestId = null) : c();
                  });
                };
                c();
              }
            }
            return !0;
          }
          case U.X_AXIS:
          case U.Y_AXIS: {
            var d = i.dispatchEvent("mouseUpEvent", a);
            d && this._chart.updatePane(
              1
              /* UpdateLevel.Overlay */
            );
          }
        }
        this._startScrollCoordinate = null, this._prevYAxisRanges.clear(), this._xAxisStartScaleCoordinate = null, this._xAxisStartScaleDistance = 0, this._xAxisScale = 1, this._yAxisStartScaleDistance = 0;
      }
      return !1;
    }, r.prototype.tapEvent = function(e) {
      var t = this._findWidgetByEvent(e), i = t.pane, a = t.widget, n = !1;
      if (a !== null) {
        var o = this._makeWidgetEvent(e, a), s = a.dispatchEvent("mouseClickEvent", o);
        if (a.getName() === U.MAIN) {
          var l = this._makeWidgetEvent(e, a), u = this._chart.getChartStore();
          s ? (this._touchCancelCrosshair = !0, this._touchCoordinate = null, u.setCrosshair(void 0, { notInvalidate: !0 }), n = !0) : (!this._touchCancelCrosshair && !this._touchZoomed && (this._touchCoordinate = { x: l.x, y: l.y }, u.setCrosshair({ x: l.x, y: l.y, paneId: i?.getId() }, { notInvalidate: !0 }), n = !0), this._touchCancelCrosshair = !1);
        }
        (n || s) && this._chart.updatePane(
          1
          /* UpdateLevel.Overlay */
        );
      }
      return n;
    }, r.prototype.doubleTapEvent = function(e) {
      return this.mouseDoubleClickEvent(e);
    }, r.prototype.longTapEvent = function(e) {
      var t = this._findWidgetByEvent(e), i = t.pane, a = t.widget;
      if (a !== null && a.getName() === U.MAIN) {
        var n = this._makeWidgetEvent(e, a);
        return this._touchCoordinate = { x: n.x, y: n.y }, this._chart.getChartStore().setCrosshair({ x: n.x, y: n.y, paneId: i?.getId() }), !0;
      }
      return !1;
    }, r.prototype._processMainScrollingEvent = function(e, t) {
      var i, a, n;
      if (this._startScrollCoordinate !== null) {
        var o = e.getPane().getYAxisComponents();
        try {
          for (var s = bt(o), l = s.next(); !l.done; l = s.next()) {
            var u = l.value, c = u, d = this._prevYAxisRanges.get(c);
            if (C(d) && !c.getAutoCalcTickFlag() && c.scrollZoomEnabled) {
              (n = t.preventDefault) === null || n === void 0 || n.call(t);
              var h = d.from, f = d.to, v = d.range, p = 0;
              c.reverse ? p = this._startScrollCoordinate.y - t.y : p = t.y - this._startScrollCoordinate.y;
              var g = e.getBounding(), m = p / g.height, x = v * m, y = h + x, E = f + x, _ = c.valueToRealValue(y, { range: d }), I = c.valueToRealValue(E, { range: d }), w = c.realValueToDisplayValue(_, { range: d }), b = c.realValueToDisplayValue(I, { range: d });
              c.setRange({
                from: y,
                to: E,
                range: E - y,
                realFrom: _,
                realTo: I,
                realRange: I - _,
                displayFrom: w,
                displayTo: b,
                displayRange: b - w
              });
            }
          }
        } catch (T) {
          i = { error: T };
        } finally {
          try {
            l && !l.done && (a = s.return) && a.call(s);
          } finally {
            if (i) throw i.error;
          }
        }
        var S = t.x - this._startScrollCoordinate.x;
        this._chart.getChartStore().scroll(S);
      }
    }, r.prototype._processXAxisScrollStartEvent = function(e, t) {
      var i = e.dispatchEvent("mouseDownEvent", t);
      return i && this._chart.updatePane(
        1
        /* UpdateLevel.Overlay */
      ), this._xAxisStartScaleCoordinate = { x: t.x, y: t.y }, this._xAxisStartScaleDistance = t.pageX, i;
    }, r.prototype._processXAxisScrollingEvent = function(e, t) {
      var i = e.dispatchEvent("pressedMouseMoveEvent", t);
      if (i)
        this._chart.updatePane(
          1
          /* UpdateLevel.Overlay */
        );
      else {
        var a = e.getPane().getXAxisComponent();
        if (a.scrollZoomEnabled && this._xAxisStartScaleDistance !== 0) {
          var n = this._xAxisStartScaleDistance / t.pageX;
          if (Number.isFinite(n)) {
            var o = (n - this._xAxisScale) * 10;
            this._xAxisScale = n, this._chart.getChartStore().zoom(o, this._xAxisStartScaleCoordinate, "xAxis");
          }
        }
      }
      return i;
    }, r.prototype._processYAxisScaleStartEvent = function(e, t) {
      var i = e.dispatchEvent("mouseDownEvent", t);
      i && this._chart.updatePane(
        1
        /* UpdateLevel.Overlay */
      );
      var a = this._getYAxisScaleTargetByWidget(e), n = a.getRange();
      return this._prevYAxisRanges.set(a, M({}, n)), this._yAxisStartScaleDistance = t.pageY, i;
    }, r.prototype._processYAxisScalingEvent = function(e, t) {
      var i, a = e.dispatchEvent("pressedMouseMoveEvent", t);
      if (a)
        this._chart.updatePane(
          1
          /* UpdateLevel.Overlay */
        );
      else {
        var n = this._getYAxisByWidget(e), o = this._getYAxisScaleTargetByWidget(e), s = this._prevYAxisRanges.get(o);
        if (C(s) && n.scrollZoomEnabled && this._yAxisStartScaleDistance !== 0) {
          (i = t.preventDefault) === null || i === void 0 || i.call(t);
          var l = t.pageY / this._yAxisStartScaleDistance;
          this._zoomYAxis(o, l, s), this._syncManualYAxesValueRange(e, o);
        }
      }
      return a;
    }, r.prototype._zoomYAxis = function(e, t, i) {
      var a = i ?? e.getRange(), n = a.from, o = a.to, s = a.range, l = s * t, u = (l - s) / 2, c = n - u, d = o + u, h = e.valueToRealValue(c, { range: a }), f = e.valueToRealValue(d, { range: a }), v = e.realValueToDisplayValue(h, { range: a }), p = e.realValueToDisplayValue(f, { range: a });
      e.setRange({
        from: c,
        to: d,
        range: l,
        realFrom: h,
        realTo: f,
        realRange: f - h,
        displayFrom: v,
        displayTo: p,
        displayRange: p - v
      }), this._chart.layout({
        measureWidth: !0,
        update: !0,
        buildYAxisTick: !0
      });
    }, r.prototype._findWidgetByEvent = function(e) {
      var t, i, a, n, o, s, l = e.x, u = e.y, c = this._chart.getSeparatorPanes(), d = this._chart.getStyles().separator.size;
      try {
        for (var h = bt(c), f = h.next(); !f.done; f = h.next()) {
          var v = f.value, p = v[1], g = p.getBounding(), m = g.top - Math.round((Re - d) / 2);
          if (l >= g.left && l <= g.left + g.width && u >= m && u <= m + Re)
            return { pane: p, widget: p.getWidget() };
        }
      } catch (P) {
        t = { error: P };
      } finally {
        try {
          f && !f.done && (i = h.return) && i.call(h);
        } finally {
          if (t) throw t.error;
        }
      }
      var x = this._chart.getDrawPanes(), y = null;
      try {
        for (var E = bt(x), _ = E.next(); !_.done; _ = E.next()) {
          var I = _.value, g = I.getBounding();
          if (l >= g.left && l <= g.left + g.width && u >= g.top && u <= g.top + g.height) {
            y = I;
            break;
          }
        }
      } catch (P) {
        a = { error: P };
      } finally {
        try {
          _ && !_.done && (n = E.return) && n.call(E);
        } finally {
          if (a) throw a.error;
        }
      }
      var w = null;
      if (y !== null) {
        if (!C(w)) {
          var b = y.getMainWidget(), S = b.getBounding();
          l >= S.left && l <= S.left + S.width && u >= S.top && u <= S.top + S.height && (w = b);
        }
        if (!C(w))
          try {
            for (var T = bt(y.getYAxisWidgets()), A = T.next(); !A.done; A = T.next()) {
              var D = A.value, R = D.getBounding();
              if (l >= R.left && l <= R.left + R.width && u >= R.top && u <= R.top + R.height) {
                w = D;
                break;
              }
            }
          } catch (P) {
            o = { error: P };
          } finally {
            try {
              A && !A.done && (s = T.return) && s.call(T);
            } finally {
              if (o) throw o.error;
            }
          }
      }
      return { pane: y, widget: w };
    }, r.prototype._makeWidgetEvent = function(e, t) {
      var i, a, n, o = (i = t?.getBounding()) !== null && i !== void 0 ? i : null;
      return M(M({}, e), { x: e.x - ((a = o?.left) !== null && a !== void 0 ? a : 0), y: e.y - ((n = o?.top) !== null && n !== void 0 ? n : 0) });
    }, r.prototype.destroy = function() {
      document.removeEventListener("keydown", this._boundKeyBoardDownEvent), this._event.destroy();
    }, r;
  })()
), Oi = (
  /** @class */
  (function() {
    function r(e, t) {
      var i = this;
      this._chartBounding = pr(), this._drawPanes = [], this._separatorPanes = /* @__PURE__ */ new Map(), this._layoutUpdateOptions = {
        sort: !0,
        measureHeight: !0,
        measureWidth: !0,
        secondMeasureWidth: !1,
        update: !0,
        buildYAxisTick: !1,
        cacheYAxisWidth: !1,
        forceBuildYAxisTick: !1
      }, this._layoutPending = !1, this._resizeObserver = null, this._resizeRequestAnimationId = Qt, this._scheduleResize = function() {
        i._resizeRequestAnimationId === Qt && (i._resizeRequestAnimationId = Le(function() {
          i._resizeRequestAnimationId = Qt, (i._chartBounding.width !== Math.floor(i._chartContainer.clientWidth) || i._chartBounding.height !== Math.floor(i._chartContainer.clientHeight)) && i.resize();
        }));
      }, this._cacheYAxisWidth = { left: 0, right: 0 }, this._initContainer(e), this._chartEvent = new Go(this._chartContainer, this), this._chartStore = new Nn(this, t);
      var a = this._chartStore.getLayoutOptions(), n = a.pane;
      this._candlePane = this._createPane(Po, M(M({}, n), { id: j.CANDLE })), this._candlePane.createOrOverrideYAxis(M(M({}, a.yAxis), { id: xe(Ke) })), this._xAxisPane = this._createPane(Vo, M(M({}, n), { id: j.X_AXIS, order: Number.MAX_SAFE_INTEGER })), this._layout(), this._initResizeListener();
    }
    return r.prototype._initContainer = function(e) {
      this._container = e, this._chartContainer = Gt("div", {
        position: "relative",
        width: "100%",
        height: "100%",
        outline: "none",
        borderStyle: "none",
        cursor: "crosshair",
        boxSizing: "border-box",
        userSelect: "none",
        webkitUserSelect: "none",
        overflow: "hidden",
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment -- ignore
        // @ts-expect-error
        msUserSelect: "none",
        MozUserSelect: "none",
        webkitTapHighlightColor: "transparent"
      }), this._chartContainer.tabIndex = 1, e.appendChild(this._chartContainer), this._cacheChartBounding();
    }, r.prototype._cacheChartBounding = function() {
      this._chartBounding.width = Math.floor(this._chartContainer.clientWidth), this._chartBounding.height = Math.floor(this._chartContainer.clientHeight);
    }, r.prototype._initResizeListener = function() {
      var e = this;
      C(ResizeObserver) ? (this._resizeObserver = new ResizeObserver(function() {
        e._scheduleResize();
      }), this._resizeObserver.observe(this._chartContainer)) : window.addEventListener("resize", this._scheduleResize);
    }, r.prototype._createPane = function(e, t) {
      var i = new e(this, t);
      return this._drawPanes.push(i), i;
    }, r.prototype.getDrawPaneById = function(e) {
      if (e === j.CANDLE)
        return this._candlePane;
      if (e === j.X_AXIS)
        return this._xAxisPane;
      var t = this._drawPanes.find(function(i) {
        return i.getId() === e;
      });
      return t ?? null;
    }, r.prototype.getContainer = function() {
      return this._container;
    }, r.prototype.getChartStore = function() {
      return this._chartStore;
    }, r.prototype.getXAxisPane = function() {
      return this._xAxisPane;
    }, r.prototype.getDrawPanes = function() {
      return this._drawPanes;
    }, r.prototype.getSeparatorPanes = function() {
      return this._separatorPanes;
    }, r.prototype.layout = function(e) {
      var t = this, i, a, n, o, s, l, u, c;
      (i = e.sort) !== null && i !== void 0 && i && (this._layoutUpdateOptions.sort = e.sort), (a = e.measureHeight) !== null && a !== void 0 && a && (this._layoutUpdateOptions.measureHeight = e.measureHeight), (n = e.measureWidth) !== null && n !== void 0 && n && (this._layoutUpdateOptions.measureWidth = e.measureWidth), (o = e.secondMeasureWidth) !== null && o !== void 0 && o && (this._layoutUpdateOptions.secondMeasureWidth = e.secondMeasureWidth), (s = e.update) !== null && s !== void 0 && s && (this._layoutUpdateOptions.update = e.update), (l = e.buildYAxisTick) !== null && l !== void 0 && l && (this._layoutUpdateOptions.buildYAxisTick = e.buildYAxisTick), (u = e.cacheYAxisWidth) !== null && u !== void 0 && u && (this._layoutUpdateOptions.cacheYAxisWidth = e.cacheYAxisWidth), (c = e.forceBuildYAxisTick) !== null && c !== void 0 && c && (this._layoutUpdateOptions.forceBuildYAxisTick = e.forceBuildYAxisTick), this._layoutPending || (this._layoutPending = !0, Promise.resolve().then(function(d) {
        t._layout(), t._layoutPending = !1;
      }).catch(function(d) {
      }));
    }, r.prototype._layout = function() {
      var e = this, t, i = this._layoutUpdateOptions, a = i.sort, n = i.measureHeight, o = i.measureWidth, s = i.secondMeasureWidth, l = i.update, u = i.buildYAxisTick, c = i.cacheYAxisWidth, d = i.forceBuildYAxisTick;
      if (a) {
        for (; C(this._chartContainer.firstChild); )
          this._chartContainer.removeChild(this._chartContainer.firstChild);
        this._separatorPanes.clear(), this._drawPanes.sort(function(b, S) {
          return b.getOptions().order - S.getOptions().order;
        });
        var h = null;
        this._drawPanes.forEach(function(b) {
          if (b.getId() !== j.X_AXIS) {
            if (C(h)) {
              var S = new Wo(e, "", h, b);
              e._chartContainer.appendChild(S.getContainer()), e._separatorPanes.set(b, S);
            }
            h = b;
          }
          e._chartContainer.appendChild(b.getContainer());
        });
      }
      if (n) {
        var f = this._chartBounding.height, v = this.getStyles().separator.size, p = this._xAxisPane.getXAxisComponent().getAutoSize(), g = this._drawPanes.filter(function(b) {
          return b.getId() !== j.X_AXIS;
        }), m = g.find(function(b) {
          return b.getOptions().state === "maximize";
        }), x = Math.max(f - p, 0), y = /* @__PURE__ */ new Map(), E = v;
        if (C(m))
          E = 0, g.forEach(function(b) {
            y.set(b, b === m ? x : 0);
          });
        else {
          x = Math.max(x - this._separatorPanes.size * v, 0);
          var _ = (t = g.find(function(b) {
            return b.getId() === j.CANDLE && b.getOptions().state === "normal";
          })) !== null && t !== void 0 ? t : g.find(function(b) {
            return b.getOptions().state === "normal";
          });
          g.forEach(function(b) {
            if (b !== _) {
              var S = b.getOptions(), T = S.minHeight;
              if (S.state === "normal") {
                T = Math.max(S.minHeight, S.height);
                var A = Math.max(x, 0);
                T > A && (T = A);
              }
              x -= T, y.set(b, T);
            }
          }), C(_) && y.set(_, Math.max(x, 0));
        }
        this._drawPanes.forEach(function(b) {
          var S;
          b.getId() !== j.X_AXIS && b.setBounding({ height: (S = y.get(b)) !== null && S !== void 0 ? S : 0 });
        }), this._xAxisPane.setBounding({ height: p });
        var I = 0;
        this._drawPanes.forEach(function(b) {
          var S = e._separatorPanes.get(b);
          C(S) && (S.setBounding({ height: E, top: I }), I += E), b.setBounding({ top: I }), I += b.getBounding().height;
        });
      }
      var w = function() {
        var b = o;
        if ((u || d) && e._drawPanes.forEach(function(G) {
          G.getYAxisComponents().forEach(function($) {
            var Z = $.buildTicks(d);
            b || (b = Z);
          });
        }), b) {
          var S = e._chartBounding.width, T = e.getStyles(), A = [], D = [], R = [], P = [], k = function(G, $, Z) {
            var H;
            G[$] = Math.max((H = G[$]) !== null && H !== void 0 ? H : 0, Z);
          };
          e._drawPanes.forEach(function(G) {
            var $ = [], Z = [], H = [], st = [];
            G.getId() !== j.X_AXIS && G.getWidgetYAxisComponents().forEach(function(J) {
              var et = J;
              et.position === "left" ? et.inside ? Z.push(et) : $.push(et) : et.inside ? H.push(et) : st.push(et);
            }), $.forEach(function(J, et) {
              k(A, et, J.getAutoSize());
            }), Z.forEach(function(J, et) {
              k(D, et, J.getAutoSize());
            }), H.forEach(function(J, et) {
              k(R, et, J.getAutoSize());
            }), st.forEach(function(J, et) {
              k(P, et, J.getAutoSize());
            });
          });
          var L = A.reduce(function(G, $) {
            return G + $;
          }, 0), F = P.reduce(function(G, $) {
            return G + $;
          }, 0);
          c && (L = Math.max(e._cacheYAxisWidth.left, L), F = Math.max(e._cacheYAxisWidth.right, F)), e._cacheYAxisWidth.left = L, e._cacheYAxisWidth.right = F;
          var O = S, W = 0, Q = 0;
          O -= L, W = L, O -= F, Q = F, e._chartStore.setTotalBarSpace(O);
          var it = { width: S }, tt = { width: O, left: W, right: Q }, rt = { width: L }, nt = { width: F }, ut = T.separator.fill, z = {};
          ut ? z = it : z = tt, e._drawPanes.forEach(function(G) {
            var $, Z;
            ($ = e._separatorPanes.get(G)) === null || $ === void 0 || $.setBounding(z);
            var H = {}, st = 0, J = 0, et = 0, Ct = 0, St = [], gt = [], Tt = [], It = [];
            G.getId() !== j.X_AXIS && G.getWidgetYAxisComponents().forEach(function(At) {
              var mt = At;
              mt.position === "left" ? mt.inside ? gt.push(mt) : St.push(mt) : mt.inside ? Tt.push(mt) : It.push(mt);
            });
            var se = St.reduce(function(At, mt, _t) {
              var Et;
              return At + ((Et = A[_t]) !== null && Et !== void 0 ? Et : 0);
            }, 0);
            st = L - se;
            for (var Ot = St.length - 1; Ot >= 0; Ot--) {
              var pe = St[Ot], le = (Z = A[Ot]) !== null && Z !== void 0 ? Z : 0;
              H[pe.id] = { width: le, left: st }, st += le;
            }
            gt.forEach(function(At, mt) {
              var _t, Et = (_t = D[mt]) !== null && _t !== void 0 ? _t : 0;
              H[At.id] = { width: Et, left: W + J }, J += Et;
            }), Tt.forEach(function(At, mt) {
              var _t, Et = (_t = R[mt]) !== null && _t !== void 0 ? _t : 0;
              et += Et, H[At.id] = { width: Et, left: W + O - et };
            }), It.forEach(function(At, mt) {
              var _t, Et = (_t = P[mt]) !== null && _t !== void 0 ? _t : 0;
              H[At.id] = { width: Et, left: W + O + Ct }, Ct += Et;
            }), G.setYAxesBounding(H), G.setBounding(it, tt, rt, nt);
          });
        }
      };
      w(), s && w(), l && (this._xAxisPane.getXAxisComponent().buildTicks(!0), this.updatePane(
        4
        /* UpdateLevel.All */
      )), this._layoutUpdateOptions = {
        sort: !1,
        measureHeight: !1,
        measureWidth: !1,
        secondMeasureWidth: !1,
        update: !1,
        buildYAxisTick: !1,
        cacheYAxisWidth: !1,
        forceBuildYAxisTick: !1
      };
    }, r.prototype.updatePane = function(e, t) {
      var i = this;
      if (C(t)) {
        var a = this.getDrawPaneById(t);
        a?.update(e);
      } else
        this._drawPanes.forEach(function(n) {
          var o;
          n.update(e), (o = i._separatorPanes.get(n)) === null || o === void 0 || o.update(e);
        });
    }, r.prototype.getDom = function(e, t) {
      var i, a;
      if (C(e)) {
        var n = this.getDrawPaneById(e);
        if (C(n)) {
          var o = t ?? "root";
          switch (o) {
            case "root":
              return n.getContainer();
            case "main":
              return n.getMainWidget().getContainer();
            case "yAxis":
              return (a = (i = n.getYAxisWidget()) === null || i === void 0 ? void 0 : i.getContainer()) !== null && a !== void 0 ? a : null;
          }
        }
      } else
        return this._chartContainer;
      return null;
    }, r.prototype.getSize = function(e, t) {
      var i, a;
      if (C(e)) {
        var n = this.getDrawPaneById(e);
        if (C(n)) {
          var o = t ?? "root";
          switch (o) {
            case "root":
              return n.getBounding();
            case "main":
              return n.getMainWidget().getBounding();
            case "yAxis":
              return (a = (i = n.getYAxisWidget()) === null || i === void 0 ? void 0 : i.getBounding()) !== null && a !== void 0 ? a : null;
          }
        }
      } else
        return this._chartBounding;
      return null;
    }, r.prototype._resetYAxisAutoCalcTickFlag = function() {
      this._drawPanes.forEach(function(e) {
        e.getYAxisComponents().forEach(function(t) {
          t.setAutoCalcTickFlag(!0);
        });
      });
    }, r.prototype.setSymbol = function(e) {
      e !== this.getSymbol() && (this._resetYAxisAutoCalcTickFlag(), this._chartStore.setSymbol(e));
    }, r.prototype.getSymbol = function() {
      return this._chartStore.getSymbol();
    }, r.prototype.setPeriod = function(e) {
      e !== this.getPeriod() && (this._resetYAxisAutoCalcTickFlag(), this._chartStore.setPeriod(e));
    }, r.prototype.getPeriod = function() {
      return this._chartStore.getPeriod();
    }, r.prototype.setStyles = function(e) {
      var t = this;
      this._setOptions(function() {
        t._chartStore.setStyles(e);
      });
    }, r.prototype.getStyles = function() {
      return this._chartStore.getStyles();
    }, r.prototype.setFormatter = function(e) {
      var t = this;
      this._setOptions(function() {
        t._chartStore.setFormatter(e);
      });
    }, r.prototype.getFormatter = function() {
      return this._chartStore.getFormatter();
    }, r.prototype.setLocale = function(e) {
      var t = this;
      this._setOptions(function() {
        t._chartStore.setLocale(e);
      });
    }, r.prototype.getLocale = function() {
      return this._chartStore.getLocale();
    }, r.prototype.setTimezone = function(e) {
      var t = this;
      this._setOptions(function() {
        t._chartStore.setTimezone(e);
      });
    }, r.prototype.getTimezone = function() {
      return this._chartStore.getTimezone();
    }, r.prototype.setThousandsSeparator = function(e) {
      var t = this;
      this._setOptions(function() {
        t._chartStore.setThousandsSeparator(e);
      });
    }, r.prototype.getThousandsSeparator = function() {
      return this._chartStore.getThousandsSeparator();
    }, r.prototype.setDecimalFold = function(e) {
      var t = this;
      this._setOptions(function() {
        t._chartStore.setDecimalFold(e);
      });
    }, r.prototype.getDecimalFold = function() {
      return this._chartStore.getDecimalFold();
    }, r.prototype.setHotkey = function(e) {
      this._chartStore.setHotkey(e);
    }, r.prototype.getHotkey = function() {
      return this._chartStore.getHotkey();
    }, r.prototype.getHotKey = function() {
      return this._chartStore.getHotKey();
    }, r.prototype._setOptions = function(e) {
      e(), this.layout({
        measureHeight: !0,
        measureWidth: !0,
        update: !0,
        buildYAxisTick: !0,
        forceBuildYAxisTick: !0
      });
    }, r.prototype.setOffsetRightDistance = function(e) {
      this._chartStore.setOffsetRightDistance(e, !0);
    }, r.prototype.getOffsetRightDistance = function() {
      return this._chartStore.getOffsetRightDistance();
    }, r.prototype.setMaxOffsetLeftDistance = function(e) {
      e < 0 || this._chartStore.setMaxOffsetLeftDistance(e);
    }, r.prototype.setMaxOffsetRightDistance = function(e) {
      e < 0 || this._chartStore.setMaxOffsetRightDistance(e);
    }, r.prototype.setLeftMinVisibleBarCount = function(e) {
      e < 0 || this._chartStore.setLeftMinVisibleBarCount(Math.ceil(e));
    }, r.prototype.setRightMinVisibleBarCount = function(e) {
      e < 0 || this._chartStore.setRightMinVisibleBarCount(Math.ceil(e));
    }, r.prototype.setBarSpace = function(e) {
      this._chartStore.setBarSpace(e);
    }, r.prototype.getBarSpace = function() {
      return this._chartStore.getBarSpace();
    }, r.prototype.getVisibleRange = function() {
      return this._chartStore.getVisibleRange();
    }, r.prototype._removeOrphanYAxes = function() {
      var e = this, t = !1;
      return this._drawPanes.forEach(function(i) {
        var a = i.getId();
        if (a !== j.X_AXIS) {
          var n = /* @__PURE__ */ new Set(), o = i.getDefaultYAxisId();
          C(o) && n.add(o), e._chartStore.getIndicatorsByPaneId(a).forEach(function(s) {
            n.add(s.yAxisId);
          }), i.getYAxisComponents().forEach(function(s) {
            !n.has(s.id) && !i.isManualYAxis(s.id) && (t = i.removeYAxis(s.id) || t);
          });
        }
      }), t;
    }, r.prototype._createOrUseIndicatorYAxis = function(e, t) {
      var i = !1;
      return e.hasYAxisComponent(t) || (e.createOrOverrideYAxis(M(M({}, this._chartStore.getLayoutOptions().yAxis), { id: t })), i = !0), e.isManualYAxis(t) && (e.setManualYAxis(t, !1), i = !0), i;
    }, r.prototype.resetData = function() {
      this._chartStore.resetData();
    }, r.prototype.getDataList = function() {
      return this._chartStore.getDataList();
    }, r.prototype.setDataLoader = function(e) {
      this._resetYAxisAutoCalcTickFlag(), this._chartStore.setDataLoader(e);
    }, r.prototype.createIndicator = function(e, t) {
      var i, a, n, o, s = K(e) ? { name: e } : e;
      if (hi(s.name) === null)
        return null;
      (i = s.id) !== null && i !== void 0 || (s.id = xe("".concat(s.name, "_"))), (a = s.paneId) !== null && a !== void 0 || (s.paneId = xe(j.INDICATOR));
      var l = this.getDrawPaneById(s.paneId);
      (n = s.yAxisId) !== null && n !== void 0 || (s.yAxisId = (o = l?.getDefaultYAxisId()) !== null && o !== void 0 ? o : xe(Ke));
      var u = this._chartStore.addIndicator(s, t ?? !1);
      if (u) {
        var c = !1, d = this.getDrawPaneById(s.paneId);
        return C(d) || (d = this._createPane(Fi, M(M({}, this._chartStore.getLayoutOptions().pane), { id: s.paneId })), c = !0), this._createOrUseIndicatorYAxis(d, s.yAxisId), this._removeOrphanYAxes(), this.layout({
          sort: c,
          measureHeight: !0,
          measureWidth: !0,
          update: !0,
          buildYAxisTick: !0,
          forceBuildYAxisTick: !0
        }), s.id;
      }
      return null;
    }, r.prototype.overrideIndicator = function(e) {
      var t = this, i = this._chartStore.getIndicatorsByFilter(e);
      if (i.length === 0)
        return !1;
      var a = this._chartStore.overrideIndicator(e);
      return i.forEach(function(n) {
        var o = t.getDrawPaneById(n.paneId);
        C(o) && (a = t._createOrUseIndicatorYAxis(o, n.yAxisId) || a);
      }), a && (this._removeOrphanYAxes(), this.layout({
        measureWidth: !0,
        update: !0,
        buildYAxisTick: !0,
        forceBuildYAxisTick: !0
      })), a;
    }, r.prototype.getIndicators = function(e) {
      return this._chartStore.getIndicatorsByFilter(e ?? {});
    }, r.prototype.removeIndicator = function(e) {
      var t = this, i = this._chartStore.removeIndicator(e ?? {});
      if (i) {
        this._removeOrphanYAxes();
        var a = !1, n = [];
        this._drawPanes.forEach(function(o) {
          var s = o.getId();
          if (s !== j.X_AXIS && s !== j.CANDLE) {
            var l = t._chartStore.getIndicatorsByPaneId(s);
            l.length === 0 && n.push(s);
          }
        }), n.forEach(function(o) {
          var s = t._drawPanes.findIndex(function(u) {
            return u.getId() === o;
          }), l = t._drawPanes[s];
          C(l) && (t._drawPanes.splice(s, 1), l.destroy(), a = !0);
        }), this.layout({
          sort: a,
          measureHeight: a,
          measureWidth: !0,
          update: !0,
          buildYAxisTick: !0,
          forceBuildYAxisTick: !0
        });
      }
      return i;
    }, r.prototype.createOverlay = function(e) {
      var t = this, i = [], a = [], n = function(s) {
        !C(s.paneId) || t.getDrawPaneById(s.paneId) === null ? (s.paneId = j.CANDLE, a.push(!1)) : a.push(!0), i.push(s);
      };
      K(e) ? n({ name: e }) : Nt(e) ? e.forEach(function(s) {
        var l = null;
        K(s) ? l = { name: s } : l = s, n(l);
      }) : n(e);
      var o = this._chartStore.addOverlays(i, a);
      return Nt(e) ? o : o[0];
    }, r.prototype.getOverlays = function(e) {
      return this._chartStore.getOverlaysByFilter(e ?? {});
    }, r.prototype.overrideOverlay = function(e) {
      return this._chartStore.overrideOverlay(e);
    }, r.prototype.removeOverlay = function(e) {
      return this._chartStore.removeOverlay(e ?? {});
    }, r.prototype.setPaneOptions = function(e) {
      var t, i, a, n = !1, o = !1, s = !1, l = C(e.id);
      try {
        for (var u = bt(this._drawPanes), c = u.next(); !c.done; c = u.next()) {
          var d = c.value, h = d.getId();
          if (l && e.id === h || !l) {
            if (h !== j.X_AXIS) {
              var f = d.getOptions(), v = f.state;
              if (B(e.height) && e.height > 0) {
                var p = Math.max((a = e.minHeight) !== null && a !== void 0 ? a : f.minHeight, 0), g = Math.max(p, e.height);
                o = !0, n = !0, d.setBounding({ height: g });
              }
              C(e.state) && (o = !0, n = !0, v === "normal" && e.state !== "normal" ? d.setOptions({ height: d.getBounding().height }) : v !== "normal" && e.state === "normal" && !B(e.height) && d.setBounding({
                height: Math.max(f.minHeight, f.height)
              }));
            }
            if (B(e.order) && (o = !0, s = !0), d.setOptions(e), h === e.id)
              break;
          }
        }
      } catch (m) {
        t = { error: m };
      } finally {
        try {
          c && !c.done && (i = u.return) && i.call(u);
        } finally {
          if (t) throw t.error;
        }
      }
      o && this.layout({
        sort: s,
        measureHeight: n,
        measureWidth: !0,
        update: !0,
        buildYAxisTick: !0,
        forceBuildYAxisTick: !0
      });
    }, r.prototype.createYAxis = function(e) {
      var t, i, a = (t = e.paneId) !== null && t !== void 0 ? t : j.CANDLE, n = this.getDrawPaneById(a);
      if (!C(n) || a === j.X_AXIS)
        return null;
      var o = (i = e.id) !== null && i !== void 0 ? i : xe(Ke);
      return n.hasYAxisComponent(o) || (n.createOrOverrideYAxis(M(M(M({}, this._chartStore.getLayoutOptions().yAxis), e), { id: o, paneId: a })), n.setManualYAxis(o, !0), this.layout({
        measureWidth: !0,
        update: !0,
        buildYAxisTick: !0,
        forceBuildYAxisTick: !0
      })), o;
    }, r.prototype.removeYAxis = function(e) {
      var t, i, a = e.id, n = e.name;
      if (!C(a) && !C(n))
        return !1;
      var o = !1, s = function(h) {
        var f = l.getDrawPaneById(h.paneId);
        if (!C(f) || f.isDefaultYAxis(h.id) && h.paneId === j.CANDLE)
          return "continue";
        var v = l._chartStore.getIndicatorsByPaneId(h.paneId);
        if (v.some(function(p) {
          return p.yAxisId === h.id;
        }))
          return "continue";
        o = f.removeYAxis(h.id) || o;
      }, l = this;
      try {
        for (var u = bt(this.getYAxes(e)), c = u.next(); !c.done; c = u.next()) {
          var d = c.value;
          s(d);
        }
      } catch (h) {
        t = { error: h };
      } finally {
        try {
          c && !c.done && (i = u.return) && i.call(u);
        } finally {
          if (t) throw t.error;
        }
      }
      return o && this.layout({
        measureWidth: !0,
        update: !0,
        buildYAxisTick: !0,
        forceBuildYAxisTick: !0
      }), o;
    }, r.prototype.getYAxes = function(e) {
      var t, i, a = e.paneId, n = e.id, o = e.name, s = function(u) {
        return C(n) ? u.id === n : !C(o) || u.name === o;
      }, l = [];
      return C(a) ? l = l.concat((i = (t = this.getDrawPaneById(a)) === null || t === void 0 ? void 0 : t.getYAxisComponents().filter(s)) !== null && i !== void 0 ? i : []) : this._drawPanes.forEach(function(u) {
        u.getId() !== j.X_AXIS && (l = l.concat(u.getYAxisComponents().filter(s)));
      }), l;
    }, r.prototype.overrideYAxis = function(e) {
      var t = this, i = this.getYAxes({ paneId: e.paneId, id: e.id });
      i.length !== 0 && (i.forEach(function(a) {
        var n;
        (n = t.getDrawPaneById(a.paneId)) === null || n === void 0 || n.createOrOverrideYAxis(M(M({}, e), { id: a.id }));
      }), this.layout({
        measureWidth: !0,
        update: !0,
        buildYAxisTick: !0,
        forceBuildYAxisTick: !0
      }));
    }, r.prototype.overrideXAxis = function(e) {
      this._xAxisPane.overrideXAxis(e), this.layout({
        measureHeight: !0,
        update: !0,
        buildYAxisTick: !0,
        forceBuildYAxisTick: !0
      });
    }, r.prototype.getPaneOptions = function(e) {
      var t;
      if (C(e)) {
        var i = this.getDrawPaneById(e);
        return (t = i?.getOptions()) !== null && t !== void 0 ? t : null;
      }
      return this._drawPanes.map(function(a) {
        return a.getOptions();
      });
    }, r.prototype.setZoomEnabled = function(e) {
      this._chartStore.setZoomEnabled(e);
    }, r.prototype.isZoomEnabled = function() {
      return this._chartStore.isZoomEnabled();
    }, r.prototype.setZoomAnchor = function(e) {
      this._chartStore.setZoomAnchor(e);
    }, r.prototype.getZoomAnchor = function() {
      return this._chartStore.getZoomAnchor();
    }, r.prototype.setScrollEnabled = function(e) {
      this._chartStore.setScrollEnabled(e);
    }, r.prototype.isScrollEnabled = function() {
      return this._chartStore.isScrollEnabled();
    }, r.prototype.scrollByDistance = function(e, t) {
      var i = this, a = B(t) && t > 0 ? t : 0;
      if (this._chartStore.startScroll(), a > 0) {
        var n = new or({ duration: a });
        n.doFrame(function(o) {
          var s = e * (o / a);
          i._chartStore.scroll(s);
        }), n.start();
      } else
        this._chartStore.scroll(e);
    }, r.prototype.scrollToRealTime = function(e) {
      var t = this._chartStore.getBarSpace().bar, i = this._chartStore.getLastBarRightSideDiffBarCount() - this._chartStore.getInitialOffsetRightDistance() / t, a = i * t;
      this.scrollByDistance(a, e);
    }, r.prototype.scrollToDataIndex = function(e, t) {
      var i = (this._chartStore.getLastBarRightSideDiffBarCount() + (this.getDataList().length - 1 - e)) * this._chartStore.getBarSpace().bar;
      this.scrollByDistance(i, t);
    }, r.prototype.scrollToTimestamp = function(e, t) {
      var i = sr(this.getDataList(), "timestamp", e);
      this.scrollToDataIndex(i, t);
    }, r.prototype.zoomAtCoordinate = function(e, t, i) {
      var a = this, n = B(i) && i > 0 ? i : 0, o = this._chartStore.getBarSpace().bar, s = o * e, l = s - o;
      if (n > 0) {
        var u = 0, c = new or({ duration: n });
        c.doFrame(function(d) {
          var h = l * (d / n), f = (h - u) / a._chartStore.getBarSpace().bar * lr;
          a._chartStore.zoom(f, t ?? null, "main"), u = h;
        }), c.start();
      } else
        this._chartStore.zoom(l / o * lr, t ?? null, "main");
    }, r.prototype.zoomAtDataIndex = function(e, t, i) {
      var a = this._chartStore.dataIndexToCoordinate(t);
      this.zoomAtCoordinate(e, { x: a, y: 0 }, i);
    }, r.prototype.zoomAtTimestamp = function(e, t, i) {
      var a = sr(this.getDataList(), "timestamp", t);
      this.zoomAtDataIndex(e, a, i);
    }, r.prototype.convertToPixel = function(e, t) {
      var i = this, a, n = t ?? {}, o = n.paneId, s = o === void 0 ? j.CANDLE : o, l = n.yAxisId, u = n.absolute, c = u === void 0 ? !1 : u, d = [];
      if (s !== j.X_AXIS) {
        var h = this.getDrawPaneById(s);
        if (h !== null) {
          var f = h.getBounding(), v = [].concat(e), p = this._xAxisPane.getXAxisComponent(), g = h.getYAxisComponentById(l);
          d = v.map(function(m) {
            var x = {}, y = m.dataIndex;
            if (B(m.timestamp) && (y = i._chartStore.timestampToDataIndex(m.timestamp)), B(y) && (x.x = p.convertToPixel(y)), B(m.value)) {
              var E = g.convertToPixel(m.value);
              x.y = c ? f.top + E : E;
            }
            return x;
          });
        }
      }
      return Nt(e) ? d : (a = d[0]) !== null && a !== void 0 ? a : {};
    }, r.prototype.convertFromPixel = function(e, t) {
      var i = this, a, n = t ?? {}, o = n.paneId, s = o === void 0 ? j.CANDLE : o, l = n.yAxisId, u = n.absolute, c = u === void 0 ? !1 : u, d = [];
      if (s !== j.X_AXIS) {
        var h = this.getDrawPaneById(s);
        if (h !== null) {
          var f = h.getBounding(), v = [].concat(e), p = this._xAxisPane.getXAxisComponent(), g = h.getYAxisComponentById(l);
          d = v.map(function(m) {
            var x, y = {};
            if (B(m.x)) {
              var E = p.convertFromPixel(m.x);
              y.dataIndex = E, y.timestamp = (x = i._chartStore.dataIndexToTimestamp(E)) !== null && x !== void 0 ? x : void 0;
            }
            if (B(m.y)) {
              var _ = c ? m.y - f.top : m.y;
              y.value = g.convertFromPixel(_);
            }
            return y;
          });
        }
      }
      return Nt(e) ? d : (a = d[0]) !== null && a !== void 0 ? a : {};
    }, r.prototype.executeAction = function(e, t) {
      var i;
      if (e === "onCrosshairChange") {
        var a = null;
        C(t) && (a = M({}, t), (i = a.paneId) !== null && i !== void 0 || (a.paneId = j.CANDLE)), this._chartStore.setCrosshair(a, { notExecuteAction: !0 });
      }
    }, r.prototype.subscribeAction = function(e, t) {
      this._chartStore.subscribeAction(e, t);
    }, r.prototype.unsubscribeAction = function(e, t) {
      this._chartStore.unsubscribeAction(e, t);
    }, r.prototype.getConvertPictureUrl = function(e, t, i) {
      var a = this, n = this._chartBounding, o = n.width, s = n.height, l = Gt("canvas", {
        width: "".concat(o, "px"),
        height: "".concat(s, "px"),
        boxSizing: "border-box"
      }), u = l.getContext("2d"), c = te(l);
      l.width = o * c, l.height = s * c, u.scale(c, c), u.fillStyle = i ?? "#FFFFFF", u.fillRect(0, 0, o, s);
      var d = e ?? !1;
      return this._drawPanes.forEach(function(h) {
        var f = a._separatorPanes.get(h);
        if (C(f)) {
          var v = f.getBounding();
          u.drawImage(f.getImage(d), v.left, v.top, v.width, v.height);
        }
        var p = h.getBounding();
        u.drawImage(h.getImage(d), 0, p.top, o, p.height);
      }), l.toDataURL("image/".concat(t ?? "jpeg"));
    }, r.prototype.resize = function() {
      this._cacheChartBounding(), this.layout({
        measureHeight: !0,
        measureWidth: !0,
        secondMeasureWidth: !0,
        update: !0,
        buildYAxisTick: !0,
        forceBuildYAxisTick: !0
      });
    }, r.prototype.destroy = function() {
      this._resizeRequestAnimationId !== Qt && (nr(this._resizeRequestAnimationId), this._resizeRequestAnimationId = Qt), C(this._resizeObserver) ? (this._resizeObserver.disconnect(), this._resizeObserver = null) : window.removeEventListener("resize", this._scheduleResize), this._chartEvent.destroy(), this._drawPanes.forEach(function(e) {
        e.destroy();
      }), this._drawPanes = [], this._separatorPanes.clear(), this._chartStore.destroy(), this._container.removeChild(this._chartContainer);
    }, r;
  })()
), qe = /* @__PURE__ */ new Map(), jo = 1;
function Zo(r, e) {
  var t = null;
  if (K(r) ? t = document.getElementById(r) : t = r, t === null)
    return null;
  var i = qe.get(t.id);
  if (C(i))
    return i;
  var a = "k_line_chart_".concat(jo++);
  return i = new Oi(t, e), i.id = a, t.setAttribute("k-line-chart-id", a), qe.set(a, i), i;
}
function Ko(r) {
  var e, t, i = null;
  if (r instanceof Oi)
    i = r.id;
  else {
    var a = null;
    K(r) ? a = document.getElementById(r) : a = r, i = (e = a?.getAttribute("k-line-chart-id")) !== null && e !== void 0 ? e : null;
  }
  i !== null && ((t = qe.get(i)) === null || t === void 0 || t.destroy(), qe.delete(i));
}
const Li = "STOCK_EVA_API_MA", Vi = "STOCK_EVA_API_VOLUME", Ni = "STOCK_EVA_API_MACD", Yi = "STOCK_EVA_API_RSI", Kr = ["ma5", "ma10", "ma20", "ma60", "ma120", "ma250"], Jo = ["macd", "macd_signal", "macd_hist"], Jr = /* @__PURE__ */ new WeakSet();
function Qo(r) {
  return Date.parse(`${r}T00:00:00+08:00`);
}
function ts(r) {
  return r.map((e) => ({
    timestamp: Qo(e.trade_date),
    open: e.open,
    high: e.high,
    low: e.low,
    close: e.close,
    volume: e.volume,
    turnover: e.amount,
    amount: e.amount,
    ma5: e.ma5,
    ma10: e.ma10,
    ma20: e.ma20,
    ma60: e.ma60,
    ma120: e.ma120,
    ma250: e.ma250,
    macd: e.macd,
    macd_signal: e.macd_signal,
    macd_hist: e.macd_hist,
    rsi14: e.rsi14
  }));
}
function er(r, e) {
  return r.map(
    (t) => Object.fromEntries(e.map((i) => [i, t[i] ?? null]))
  );
}
function es(r) {
  Jr.has(r) || (r({
    name: Li,
    shortName: "API MA",
    series: "price",
    figures: Kr.map((e) => ({
      key: e,
      title: `${e.toUpperCase()}: `,
      type: "line"
    })),
    calc: (e) => er(e, Kr)
  }), r({
    name: Vi,
    shortName: "成交量",
    series: "volume",
    shouldFormatBigNumber: !0,
    figures: [{ key: "volume", title: "VOL: ", type: "bar", baseValue: 0 }],
    calc: (e) => e.map((t) => ({
      volume: typeof t.volume == "number" && Number.isFinite(t.volume) ? t.volume : null
    }))
  }), r({
    name: Ni,
    shortName: "API MACD",
    figures: [
      { key: "macd", title: "MACD: ", type: "line" },
      { key: "macd_signal", title: "Signal: ", type: "line" },
      {
        key: "macd_hist",
        title: "Hist: ",
        type: "bar",
        baseValue: 0
      }
    ],
    calc: (e) => er(e, Jo)
  }), r({
    name: Yi,
    shortName: "API RSI14",
    figures: [{ key: "rsi14", title: "RSI14: ", type: "line" }],
    calc: (e) => er(e, ["rsi14"])
  }), Jr.add(r));
}
const rs = {
  init: (r) => {
    const e = Zo(r, {
      locale: "zh-CN",
      timezone: "Asia/Shanghai",
      styles: {
        grid: {
          horizontal: { color: "#273440" },
          vertical: { color: "#273440" }
        },
        candle: {
          bar: {
            upColor: "#f05b67",
            downColor: "#24b884",
            noChangeColor: "#70808e",
            upBorderColor: "#f05b67",
            downBorderColor: "#24b884",
            noChangeBorderColor: "#70808e",
            upWickColor: "#f05b67",
            downWickColor: "#24b884",
            noChangeWickColor: "#70808e"
          }
        }
      }
    });
    if (!e) throw new Error("KLineChart initialization failed");
    return e;
  },
  dispose: (r) => {
    Ko(r);
  },
  registerIndicator: on
};
function is(r, e, t = rs) {
  const i = ts(e.series);
  es(t.registerIndicator);
  const a = t.init(r);
  return a.setSymbol({
    ticker: e.symbol,
    pricePrecision: 4,
    volumePrecision: 0
  }), a.setPeriod({ span: 1, type: "day" }), a.setDataLoader({
    getBars: ({ callback: n }) => {
      n(i, { forward: !1, backward: !1 });
    }
  }), a.createIndicator({ name: Li, paneId: "candle_pane" }, !0), a.createIndicator(Vi), a.createIndicator(Ni), a.createIndicator(Yi), () => t.dispose(r);
}
const as = "http://127.0.0.1:8000/api/v1", ns = /^\d{4}-\d{2}-\d{2}$/, os = /^(sh|sz)\.[0-9]{6}$/;
class ur extends Error {
  status;
  detail;
  constructor(e, t) {
    super(t), this.name = "ApiError", this.status = e, this.detail = t;
  }
}
function ft(r, e) {
  if (typeof r != "object" || r === null || Array.isArray(r))
    throw new TypeError(`${e} must be an object`);
  return r;
}
function Y(r, e, t) {
  if (typeof r != "string" || t && !t.includes(r))
    throw new TypeError(`${e} must be a valid string`);
  return r;
}
function jt(r, e) {
  const t = Y(r, e);
  if (!ns.test(t))
    throw new TypeError(`${e} must be an ISO date`);
  return t;
}
function wr(r, e) {
  return r === null ? null : jt(r, e);
}
function Zt(r, e) {
  if (typeof r != "number" || !Number.isFinite(r))
    throw new TypeError(`${e} must be a finite number`);
  return r;
}
function Ut(r, e) {
  return r === null ? null : Zt(r, e);
}
function $t(r, e) {
  const t = Zt(r, e);
  if (!Number.isInteger(t) || t < 0)
    throw new TypeError(`${e} must be a non-negative integer`);
  return t;
}
function Ve(r, e) {
  if (typeof r != "boolean") throw new TypeError(`${e} must be boolean`);
  return r;
}
function ot(r, e) {
  if (!Array.isArray(r) || !r.every((t) => typeof t == "string"))
    throw new TypeError(`${e} must be a string array`);
  return [...r];
}
function ve(r, e, t) {
  if (!Array.isArray(r)) throw new TypeError(`${e} must be an array`);
  return r.map(t);
}
function Qr(r, e) {
  const t = ft(r, e);
  return Object.fromEntries(
    Object.entries(t).map(([i, a]) => [
      i,
      Zt(a, `${e}.${i}`)
    ])
  );
}
function Ee(r, e) {
  return Y(r, e, [
    "empty",
    "ready",
    "degraded"
  ]);
}
function je(r, e) {
  return Y(r, e, [
    "ready",
    "degraded",
    "missing"
  ]);
}
function br(r, e) {
  const t = ft(r, e);
  return {
    value: Zt(t.value, `${e}.value`),
    level: Y(t.level, `${e}.level`, [
      "low",
      "medium",
      "high"
    ]),
    reasons: ot(t.reasons, `${e}.reasons`)
  };
}
function ss(r, e) {
  const t = ft(r, e), i = {
    code: Y(t.code, `${e}.code`),
    detail: Y(t.detail, `${e}.detail`)
  };
  for (const a of ["component", "metric", "source"])
    t[a] !== void 0 && (i[a] = Y(t[a], `${e}.${a}`));
  for (const a of ["value", "raw_value", "score"])
    t[a] !== void 0 && (i[a] = Ut(t[a], `${e}.${a}`));
  return t.as_of !== void 0 && (i.as_of = jt(t.as_of, `${e}.as_of`)), i;
}
function ie(r, e) {
  return ve(
    r,
    e,
    (t, i) => ss(t, `${e}[${i}]`)
  );
}
function Cr(r, e) {
  const t = ft(r, e);
  return {
    source: Y(t.source, `${e}.source`),
    source_version: Y(
      t.source_version,
      `${e}.source_version`
    ),
    earliest_input_date: jt(
      t.earliest_input_date,
      `${e}.earliest_input_date`
    ),
    latest_input_date: jt(
      t.latest_input_date,
      `${e}.latest_input_date`
    ),
    record_count: $t(t.record_count, `${e}.record_count`),
    content_hash: Y(t.content_hash, `${e}.content_hash`)
  };
}
function Wi(r, e) {
  return ve(
    r,
    e,
    (t, i) => Cr(t, `${e}[${i}]`)
  );
}
function ls(r, e) {
  const t = ft(r, e);
  return {
    name: Y(t.name, `${e}.name`),
    score: Ut(t.score, `${e}.score`),
    weight: Zt(t.weight, `${e}.weight`),
    weighted_score: Ut(
      t.weighted_score,
      `${e}.weighted_score`
    ),
    formula_version: Y(
      t.formula_version,
      `${e}.formula_version`
    ),
    quality_status: je(
      t.quality_status,
      `${e}.quality_status`
    ),
    missing_inputs: ot(t.missing_inputs, `${e}.missing_inputs`),
    quality_issues: ot(t.quality_issues, `${e}.quality_issues`),
    supporting_evidence: ie(
      t.supporting_evidence,
      `${e}.supporting_evidence`
    ),
    contrary_evidence: ie(
      t.contrary_evidence,
      `${e}.contrary_evidence`
    ),
    source_lineage: Wi(
      t.source_lineage,
      `${e}.source_lineage`
    )
  };
}
function us(r, e) {
  const t = ft(r, e);
  return {
    expected_boards: ot(t.expected_boards, `${e}.expected_boards`),
    observed_boards: ot(t.observed_boards, `${e}.observed_boards`),
    missing_boards: ot(t.missing_boards, `${e}.missing_boards`),
    expected_index_series: ot(
      t.expected_index_series,
      `${e}.expected_index_series`
    ),
    observed_index_series: ot(
      t.observed_index_series,
      `${e}.observed_index_series`
    ),
    missing_index_series: ot(
      t.missing_index_series,
      `${e}.missing_index_series`
    ),
    coverage_basis: Y(
      t.coverage_basis,
      `${e}.coverage_basis`
    ),
    coverage_evidence_status: Y(
      t.coverage_evidence_status,
      `${e}.coverage_evidence_status`
    ),
    observed_universe_count: $t(
      t.observed_universe_count,
      `${e}.observed_universe_count`
    ),
    index_coverage_ratio: Zt(
      t.index_coverage_ratio,
      `${e}.index_coverage_ratio`
    ),
    scope_status: Y(t.scope_status, `${e}.scope_status`),
    can_support_full_a_share_conclusion: Ve(
      t.can_support_full_a_share_conclusion,
      `${e}.can_support_full_a_share_conclusion`
    ),
    conclusion_disclaimer: Y(
      t.conclusion_disclaimer,
      `${e}.conclusion_disclaimer`
    )
  };
}
function cs(r) {
  const e = ft(r, "market regime");
  return {
    result_id: Y(e.result_id, "result_id"),
    formula_version: Y(e.formula_version, "formula_version"),
    as_of: jt(e.as_of, "as_of"),
    data_as_of: wr(e.data_as_of, "data_as_of"),
    status: Ee(e.status, "status"),
    quality_status: Ee(e.quality_status, "quality_status"),
    strategic_state: Y(e.strategic_state, "strategic_state", [
      "bull",
      "range",
      "bear"
    ]),
    tactical_state: Y(e.tactical_state, "tactical_state", [
      "risk_on",
      "neutral",
      "risk_off"
    ]),
    total_score: Ut(e.total_score, "total_score"),
    component_scores: ve(
      e.component_scores,
      "component_scores",
      (t, i) => ls(t, `component_scores[${i}]`)
    ),
    weights: Qr(e.weights, "weights"),
    thresholds: Qr(e.thresholds, "thresholds"),
    confidence: br(e.confidence, "confidence"),
    missing_inputs: ot(e.missing_inputs, "missing_inputs"),
    supporting_evidence: ie(
      e.supporting_evidence,
      "supporting_evidence"
    ),
    contrary_evidence: ie(
      e.contrary_evidence,
      "contrary_evidence"
    ),
    quality_issues: ot(e.quality_issues, "quality_issues"),
    actual_market_scope: us(
      e.actual_market_scope,
      "actual_market_scope"
    ),
    source_lineage: Wi(e.source_lineage, "source_lineage")
  };
}
function ds(r, e) {
  const t = ft(r, e), i = {
    metric: Y(t.metric, `${e}.metric`),
    raw_value: Ut(t.raw_value, `${e}.raw_value`),
    unit: Y(t.unit, `${e}.unit`),
    score: Ut(t.score, `${e}.score`),
    weight: Zt(t.weight, `${e}.weight`),
    weighted_score: Ut(
      t.weighted_score,
      `${e}.weighted_score`
    ),
    formula_version: Y(
      t.formula_version,
      `${e}.formula_version`
    ),
    quality_status: je(
      t.quality_status,
      `${e}.quality_status`
    ),
    missing_inputs: ot(t.missing_inputs, `${e}.missing_inputs`),
    quality_issues: ot(t.quality_issues, `${e}.quality_issues`)
  };
  for (const a of ["effective_count", "target_count"])
    t[a] !== void 0 && (i[a] = $t(t[a], `${e}.${a}`));
  return t.coverage_ratio !== void 0 && (i.coverage_ratio = Zt(
    t.coverage_ratio,
    `${e}.coverage_ratio`
  )), i;
}
function zi(r, e) {
  return ve(
    r,
    e,
    (t, i) => ds(t, `${e}[${i}]`)
  );
}
function $i(r, e) {
  const t = ft(r, e), i = {
    generation_id: Y(t.generation_id, `${e}.generation_id`),
    schema_version: Y(t.schema_version, `${e}.schema_version`),
    source: Y(t.source, `${e}.source`),
    source_version: Y(
      t.source_version,
      `${e}.source_version`
    ),
    source_snapshot_date: jt(
      t.source_snapshot_date,
      `${e}.source_snapshot_date`
    ),
    taxonomy_id: Y(t.taxonomy_id, `${e}.taxonomy_id`),
    coverage_ratio: Zt(
      t.coverage_ratio,
      `${e}.coverage_ratio`
    )
  };
  return t.source_date_semantics !== void 0 && (i.source_date_semantics = Y(
    t.source_date_semantics,
    `${e}.source_date_semantics`
  )), i;
}
function Xi(r, e) {
  const t = ft(r, e);
  return {
    scope_status: Y(t.scope_status, `${e}.scope_status`),
    included_markets: ot(
      t.included_markets,
      `${e}.included_markets`
    ),
    included_boards: ot(
      t.included_boards,
      `${e}.included_boards`
    ),
    excluded_classification_boards: ot(
      t.excluded_classification_boards,
      `${e}.excluded_classification_boards`
    ),
    classification_eligible_symbols: $t(
      t.classification_eligible_symbols,
      `${e}.classification_eligible_symbols`
    ),
    observed_market_symbols: $t(
      t.observed_market_symbols,
      `${e}.observed_market_symbols`
    ),
    priced_classified_symbols: $t(
      t.priced_classified_symbols,
      `${e}.priced_classified_symbols`
    ),
    coverage_basis: Y(
      t.coverage_basis,
      `${e}.coverage_basis`
    ),
    can_support_full_a_share_conclusion: Ve(
      t.can_support_full_a_share_conclusion,
      `${e}.can_support_full_a_share_conclusion`
    ),
    can_support_all_industry_conclusion: Ve(
      t.can_support_all_industry_conclusion,
      `${e}.can_support_all_industry_conclusion`
    ),
    conclusion_disclaimer: Y(
      t.conclusion_disclaimer,
      `${e}.conclusion_disclaimer`
    )
  };
}
function qi(r, e) {
  const t = ft(r, e);
  if (t.evidence_tier !== null)
    throw new TypeError(`${e}.evidence_tier must be null`);
  return {
    status: Y(t.status, `${e}.status`, [
      "missing"
    ]),
    evidence_tier: null,
    reason: Y(t.reason, `${e}.reason`)
  };
}
function hs(r, e) {
  const t = ft(r, e);
  return {
    rank: $t(t.rank, `${e}.rank`),
    ranking_id: Y(t.ranking_id, `${e}.ranking_id`),
    sector_id: Y(t.sector_id, `${e}.sector_id`),
    sector_name: Y(t.sector_name, `${e}.sector_name`),
    member_count: $t(t.member_count, `${e}.member_count`),
    priced_member_count: $t(
      t.priced_member_count,
      `${e}.priced_member_count`
    ),
    ranking_eligible: Ve(
      t.ranking_eligible,
      `${e}.ranking_eligible`
    ),
    ranking_exclusion_reasons: ot(
      t.ranking_exclusion_reasons,
      `${e}.ranking_exclusion_reasons`
    ),
    total_score: Ut(t.total_score, `${e}.total_score`),
    confidence: br(t.confidence, `${e}.confidence`),
    metric_scores: zi(t.metric_scores, `${e}.metric_scores`),
    supporting_evidence: ie(
      t.supporting_evidence,
      `${e}.supporting_evidence`
    ),
    contrary_evidence: ie(
      t.contrary_evidence,
      `${e}.contrary_evidence`
    ),
    quality_status: je(
      t.quality_status,
      `${e}.quality_status`
    ),
    missing_inputs: ot(t.missing_inputs, `${e}.missing_inputs`),
    quality_issues: ot(t.quality_issues, `${e}.quality_issues`)
  };
}
function vs(r) {
  const e = ft(r, "sector rotation");
  return {
    result_id: Y(e.result_id, "result_id"),
    formula_version: Y(e.formula_version, "formula_version"),
    status: Ee(e.status, "status"),
    quality_status: Ee(e.quality_status, "quality_status"),
    as_of: jt(e.as_of, "as_of"),
    data_as_of: wr(e.data_as_of, "data_as_of"),
    taxonomy_id: Y(e.taxonomy_id, "taxonomy_id"),
    classification_lineage: e.classification_lineage === null ? null : $i(
      e.classification_lineage,
      "classification_lineage"
    ),
    market_lineage: e.market_lineage === null ? null : Cr(e.market_lineage, "market_lineage"),
    actual_scope: Xi(e.actual_scope, "actual_scope"),
    rankings: ve(
      e.rankings,
      "rankings",
      (t, i) => hs(t, `rankings[${i}]`)
    ),
    fund_flow_evidence: qi(
      e.fund_flow_evidence,
      "fund_flow_evidence"
    ),
    missing_inputs: ot(e.missing_inputs, "missing_inputs"),
    quality_issues: ot(e.quality_issues, "quality_issues")
  };
}
function fs(r, e) {
  const t = ft(r, e);
  if (t.actionable_primary !== !1)
    throw new TypeError(`${e}.actionable_primary must be false`);
  const i = Y(t.symbol, `${e}.symbol`);
  if (!os.test(i))
    throw new TypeError(`${e}.symbol must be an A-share symbol`);
  return {
    rank: $t(t.rank, `${e}.rank`),
    candidate_id: Y(t.candidate_id, `${e}.candidate_id`),
    symbol: i,
    name: Y(t.name, `${e}.name`),
    total_score: Ut(t.total_score, `${e}.total_score`),
    confidence: br(t.confidence, `${e}.confidence`),
    actionable_primary: !1,
    actionability_status: Y(
      t.actionability_status,
      `${e}.actionability_status`
    ),
    limit_lock_status: Y(
      t.limit_lock_status,
      `${e}.limit_lock_status`
    ),
    leader_qualified: Ve(
      t.leader_qualified,
      `${e}.leader_qualified`
    ),
    qualification_version: Y(
      t.qualification_version,
      `${e}.qualification_version`
    ),
    qualification_reasons: ot(
      t.qualification_reasons,
      `${e}.qualification_reasons`
    ),
    disqualification_reasons: ot(
      t.disqualification_reasons,
      `${e}.disqualification_reasons`
    ),
    metric_scores: zi(t.metric_scores, `${e}.metric_scores`),
    supporting_evidence: ie(
      t.supporting_evidence,
      `${e}.supporting_evidence`
    ),
    contrary_evidence: ie(
      t.contrary_evidence,
      `${e}.contrary_evidence`
    ),
    quality_status: je(
      t.quality_status,
      `${e}.quality_status`
    ),
    missing_inputs: ot(t.missing_inputs, `${e}.missing_inputs`),
    quality_issues: ot(t.quality_issues, `${e}.quality_issues`)
  };
}
function ps(r, e) {
  const t = ft(r, e);
  return {
    symbol: Y(t.symbol, `${e}.symbol`),
    reasons: ot(t.reasons, `${e}.reasons`)
  };
}
function gs(r) {
  const e = ft(r, "sector leaders");
  return {
    result_id: Y(e.result_id, "result_id"),
    formula_version: Y(e.formula_version, "formula_version"),
    status: Ee(e.status, "status"),
    quality_status: Ee(e.quality_status, "quality_status"),
    as_of: jt(e.as_of, "as_of"),
    data_as_of: wr(e.data_as_of, "data_as_of"),
    taxonomy_id: Y(e.taxonomy_id, "taxonomy_id"),
    sector_id: Y(e.sector_id, "sector_id"),
    sector_name: e.sector_name === null ? null : Y(e.sector_name, "sector_name"),
    classification_lineage: e.classification_lineage === null ? null : $i(
      e.classification_lineage,
      "classification_lineage"
    ),
    market_lineage: e.market_lineage === null ? null : Cr(e.market_lineage, "market_lineage"),
    actual_scope: Xi(e.actual_scope, "actual_scope"),
    candidates: ve(
      e.candidates,
      "candidates",
      (t, i) => fs(t, `candidates[${i}]`)
    ),
    exclusions: ve(
      e.exclusions,
      "exclusions",
      (t, i) => ps(t, `exclusions[${i}]`)
    ),
    fund_flow_evidence: qi(
      e.fund_flow_evidence,
      "fund_flow_evidence"
    ),
    missing_inputs: ot(e.missing_inputs, "missing_inputs"),
    quality_issues: ot(e.quality_issues, "quality_issues")
  };
}
async function ms(r) {
  try {
    const e = ft(await r.json(), "error response");
    if (typeof e.detail == "string") return e.detail;
    if (e.detail !== void 0) {
      const t = ft(e.detail, "error detail");
      if (typeof t.code == "string") return t.code;
    }
  } catch {
  }
  return `HTTP ${r.status}`;
}
async function Er(r, e, t) {
  const i = await e(`${as}${r}`, { signal: t });
  if (!i.ok)
    throw new ur(i.status, await ms(i));
  return i.json();
}
function Ir(r, e) {
  const t = new URLSearchParams({ as_of: jt(r, "as_of") });
  return e !== void 0 && t.set("taxonomy_id", e), t.toString();
}
async function _s(r, e = fetch, t = new AbortController().signal) {
  return cs(
    await Er(`/analysis/market-regime?${Ir(r)}`, e, t)
  );
}
async function ys(r, e, t = fetch, i = new AbortController().signal) {
  return vs(
    await Er(
      `/analysis/sector-rotation?${Ir(r, e)}`,
      t,
      i
    )
  );
}
async function xs(r, e, t, i = fetch, a = new AbortController().signal) {
  return gs(
    await Er(
      `/analysis/sectors/${encodeURIComponent(t)}/leaders?${Ir(
        r,
        e
      )}`,
      i,
      a
    )
  );
}
class ws {
  constructor(e = fetch) {
    this.fetcher = e;
  }
  fetcher;
  overviewController = null;
  leaderController = null;
  async loadOverview(e) {
    this.overviewController?.abort(), this.leaderController?.abort();
    const t = new AbortController();
    this.overviewController = t;
    try {
      const [i, a] = await Promise.all([
        _s(e.asOf, this.fetcher, t.signal),
        ys(
          e.asOf,
          e.taxonomyId,
          this.fetcher,
          t.signal
        )
      ]);
      return t.signal.aborted || this.overviewController !== t ? null : { market: i, sectors: a };
    } catch (i) {
      if (t.signal.aborted || this.overviewController !== t)
        return null;
      throw i;
    } finally {
      this.overviewController === t && (this.overviewController = null);
    }
  }
  async loadLeaders(e) {
    this.leaderController?.abort();
    const t = new AbortController();
    this.leaderController = t;
    try {
      const i = await xs(
        e.asOf,
        e.taxonomyId,
        e.sectorId,
        this.fetcher,
        t.signal
      );
      return t.signal.aborted || this.leaderController !== t ? null : i;
    } catch (i) {
      if (t.signal.aborted || this.leaderController !== t)
        return null;
      throw i;
    } finally {
      this.leaderController === t && (this.leaderController = null);
    }
  }
  dispose() {
    this.overviewController?.abort(), this.leaderController?.abort(), this.overviewController = null, this.leaderController = null;
  }
}
const Sr = /^(sh|sz)\.[0-9]{6}$/, Tr = /^\d{4}-\d{2}-\d{2}$/;
function Hi(r) {
  return r === "portfolio" || r === "watchlists" || r === "sectors";
}
function Ar(r) {
  return typeof r.asOf == "string" && Tr.test(r.asOf) && typeof r.taxonomyId == "string" && r.taxonomyId.length > 0 && typeof r.sectorId == "string" && r.sectorId.length > 0;
}
function Ui(r) {
  if (!Tr.test(r.asOf) || !r.taxonomyId || r.sectorId === "")
    throw new TypeError("invalid decision route");
  const e = new URLSearchParams({
    as_of: r.asOf,
    taxonomy_id: r.taxonomyId
  });
  return r.sectorId !== null && e.set("sector_id", r.sectorId), `#overview?${e}`;
}
function Gi(r) {
  const e = /^#overview(?:\?(.*))?$/.exec(r);
  if (!e) return null;
  const t = new URLSearchParams(e[1] ?? ""), i = t.get("as_of"), a = t.get("taxonomy_id"), n = t.get("sector_id");
  return i === null || !Tr.test(i) || a === null || a.length === 0 || n === "" ? null : { asOf: i, taxonomyId: a, sectorId: n };
}
function bs(r, e, t) {
  if (!Sr.test(r)) throw new TypeError("invalid security symbol");
  if (t !== void 0 && !Ar(t))
    throw new TypeError("invalid decision context");
  const i = new URLSearchParams({ from: e });
  return t && (i.set("as_of", t.asOf), i.set("taxonomy_id", t.taxonomyId), i.set("sector_id", t.sectorId)), `#security/${encodeURIComponent(r)}?${i}`;
}
function Cs(r) {
  const e = /^#security\/([^?]+)(?:\?(.*))?$/.exec(r);
  if (!e) return null;
  let t;
  try {
    t = decodeURIComponent(e[1]).toLowerCase();
  } catch (u) {
    if (u instanceof URIError) return null;
    throw u;
  }
  const i = new URLSearchParams(e[2] ?? ""), a = i.get("from");
  if (!Sr.test(t) || !Hi(a)) return null;
  const n = i.get("as_of"), o = i.get("taxonomy_id"), s = i.get("sector_id");
  return n !== null || o !== null || s !== null ? Ar({
    asOf: n ?? void 0,
    taxonomyId: o ?? void 0,
    sectorId: s ?? void 0
  }) ? {
    symbol: t,
    sourceView: a,
    asOf: n,
    taxonomyId: o,
    sectorId: s
  } : null : { symbol: t, sourceView: a };
}
function Es(r, e) {
  const t = (o) => {
    if (!(o instanceof Element)) return null;
    const s = o.closest("[data-security-symbol]");
    if (!s) return null;
    const l = s.dataset.securitySymbol?.toLowerCase() ?? "", u = s.dataset.securitySource ?? "";
    if (!Sr.test(l) || !Hi(u)) return null;
    const c = {
      asOf: s.dataset.securityAsOf,
      taxonomyId: s.dataset.securityTaxonomy,
      sectorId: s.dataset.securitySector
    };
    return Object.values(c).some((d) => d !== void 0) ? Ar(c) ? { symbol: l, sourceView: u, decisionContext: c } : null : { symbol: l, sourceView: u };
  }, i = (o) => {
    o.decisionContext ? e(o.symbol, o.sourceView, o.decisionContext) : e(o.symbol, o.sourceView);
  }, a = (o) => {
    const s = t(o.target);
    s && (o.preventDefault(), i(s));
  }, n = (o) => {
    if (o.repeat || o.key !== "Enter" && o.key !== " ") return;
    const s = t(o.target);
    s && (o.preventDefault(), i(s));
  };
  return r.addEventListener("click", a), r.addEventListener("keydown", n), () => {
    r.removeEventListener("click", a), r.removeEventListener("keydown", n);
  };
}
const cr = { phase: "idle" };
function Is(r) {
  if (r.phase === "idle")
    throw new Error("cockpit response received before load");
  return {
    symbol: r.symbol,
    sourceView: r.sourceView,
    ...r.decisionContext ? { decisionContext: r.decisionContext } : {}
  };
}
function $e(r, e) {
  if (e.type === "load")
    return {
      phase: "loading",
      symbol: e.symbol,
      sourceView: e.sourceView,
      ...e.decisionContext ? { decisionContext: e.decisionContext } : {}
    };
  const t = Is(r);
  if (e.type === "success") {
    if (e.response.status === "empty") {
      const i = e.response.quality_issues.includes(
        "no_effective_trading_data"
      ) ? "no_effective_trading_data" : "no_market_data";
      return {
        ...t,
        phase: "empty",
        reason: i,
        response: e.response
      };
    }
    return { ...t, phase: "ready", response: e.response };
  }
  return e.type === "empty" ? { ...t, phase: "empty", reason: e.reason } : e.status === 409 ? { ...t, phase: "quality-error", message: e.message } : { ...t, phase: "connection-error", message: e.message };
}
function V(r, e, t) {
  const i = document.createElement(r);
  return e !== void 0 && (i.textContent = e), t && (i.className = t), i;
}
function Pt(r, e = 2) {
  return r == null ? "—" : new Intl.NumberFormat("zh-CN", {
    maximumFractionDigits: e
  }).format(r);
}
function Ie(r) {
  return r == null ? "—" : `${Math.round(r * 100)}%`;
}
function Me(r, e, t) {
  const i = V("li", void 0, "decision-stage panel");
  i.dataset.decisionStage = r, i.append(V("p", e, "panel-kicker"), V("h3", t));
  const a = V("div", void 0, "decision-stage-body");
  return i.append(a), { item: i, body: a };
}
function fe(r, e = "degraded") {
  const t = V("p", r, `decision-status decision-status-${e}`);
  return t.setAttribute("role", "status"), t;
}
function Kt(r) {
  const e = V("dl", void 0, "decision-facts");
  for (const [t, i] of r) {
    const a = V("div");
    a.append(
      V("dt", t),
      document.createTextNode(" "),
      V("dd", i, "mono")
    ), e.append(a);
  }
  return e;
}
function at(r, e) {
  const t = V("div", void 0, "decision-token-row");
  return t.append(V("strong", r)), t.append(
    V("span", e.length ? e.join(" · ") : "无", "mono")
  ), t;
}
function ae(r) {
  return r.length ? r.map((e) => `${e.code}${e.detail ? `：${e.detail}` : ""}`).join(" · ") : "无";
}
function ji(r) {
  const e = V("div", void 0, "decision-metric-list");
  for (const t of r) {
    const i = V("details", void 0, "decision-detail"), a = V(
      "summary",
      `${t.metric} · 得分 ${Pt(t.score)}`
    ), n = t.effective_count !== void 0 && t.target_count !== void 0 ? `${t.effective_count} / ${t.target_count}` : "—";
    i.append(
      a,
      Kt([
        ["原始值", `${Pt(t.raw_value, 4)} ${t.unit}`],
        ["覆盖样本", n],
        ["覆盖率", Ie(t.coverage_ratio)],
        ["权重", Pt(t.weight, 4)],
        ["加权分", Pt(t.weighted_score, 4)],
        ["公式版本", t.formula_version],
        ["质量", t.quality_status]
      ]),
      at("缺失输入", t.missing_inputs),
      at("质量问题", t.quality_issues)
    ), e.append(i);
  }
  return e;
}
function ti(r) {
  const e = V("details", void 0, "decision-detail");
  if (e.append(V("summary", "查看市场分项、支持证据与反例")), !r.length)
    return e.append(V("p", "没有可展示的市场分项。", "state-message")), e;
  for (const t of r) {
    const i = V("section", void 0, "decision-component");
    i.append(
      V(
        "h4",
        `${t.name} · ${Pt(t.score)} · ${t.quality_status}`
      ),
      Kt([
        ["权重", Pt(t.weight, 4)],
        ["加权分", Pt(t.weighted_score, 4)],
        ["公式版本", t.formula_version]
      ]),
      at("支持", [ae(t.supporting_evidence)]),
      at("反例", [ae(t.contrary_evidence)]),
      at("缺失输入", t.missing_inputs),
      at("质量问题", t.quality_issues)
    ), e.append(i);
  }
  return e;
}
function Mr() {
  return fe(
    "仅沪深主板价格样本 / 不能代表全 A 股；以下状态是受限样本的后端分析结果。",
    "warning"
  );
}
function Ss(r, e) {
  if (r.append(Mr()), e.status === "empty") {
    r.append(
      V("p", "市场状态证据不足，未形成可用结论。", "decision-empty"),
      at("缺失输入", e.missing_inputs),
      at("质量问题", e.quality_issues),
      ti(e.component_scores)
    );
    return;
  }
  r.append(
    Kt([
      ["战略样本状态", e.strategic_state],
      ["战术样本状态", e.tactical_state],
      ["后端总分", Pt(e.total_score)],
      ["置信度", `${Ie(e.confidence.value)} / ${e.confidence.level}`],
      ["请求时点", e.as_of],
      ["实际数据日", e.data_as_of ?? "—"],
      ["公式版本", e.formula_version],
      ["样本范围", e.actual_market_scope.scope_status],
      ["覆盖证据", e.actual_market_scope.coverage_evidence_status]
    ]),
    ti(e.component_scores),
    at("支持证据", [ae(e.supporting_evidence)]),
    at("反例", [ae(e.contrary_evidence)]),
    at("缺失输入", e.missing_inputs),
    at("质量问题", e.quality_issues)
  );
}
function Ts(r) {
  r.append(
    fe("资金证据尚不可用", "missing"),
    V(
      "p",
      "Release 1 未发布 L1/L2/L3 资金证据；成交额仅属于量价证据，不等于净流入，也不用于推断主力方向。",
      "state-message"
    ),
    Kt([
      ["发布阶段", "Release 2"],
      ["当前证据层级", "missing / unavailable"],
      ["可发布趋势", "否"]
    ])
  );
}
function ei(r) {
  const e = V("details", void 0, "decision-detail");
  e.append(V("summary", "查看分类、行情血缘与实际覆盖"));
  const t = r.classification_lineage, i = r.market_lineage;
  return e.append(
    Kt([
      ["分类代际", t?.generation_id ?? "—"],
      ["分类版本", t?.schema_version ?? "—"],
      ["分类源日期", t?.source_snapshot_date ?? "—"],
      ["日期语义", t?.source_date_semantics ?? "—"],
      ["分类覆盖率", Ie(t?.coverage_ratio)],
      ["行情版本", i?.source_version ?? "—"],
      ["行情最新输入", i?.latest_input_date ?? "—"],
      ["行情内容哈希", i?.content_hash ?? "—"],
      ["实际范围", r.actual_scope.scope_status],
      ["定价/分类", `${r.actual_scope.priced_classified_symbols} / ${r.actual_scope.classification_eligible_symbols}`]
    ])
  ), e;
}
function As(r, e, t) {
  const i = (d) => r.metric_scores.find((h) => h.metric === d)?.raw_value, a = i("leader_count"), n = i("leader_diffusion"), o = i("leader_persistence_days"), s = V(
    "article",
    void 0,
    `decision-ranking${e ? " is-selected" : ""}`
  ), l = V("div", void 0, "decision-ranking-heading"), u = V("div");
  u.append(
    V("span", `后端顺序 ${r.rank}`, "quiet-tag"),
    V("h4", r.sector_name),
    V(
      "p",
      `${r.sector_id} · 得分 ${Pt(r.total_score)} · 置信度 ${Ie(r.confidence.value)}`,
      "meta-line mono"
    )
  );
  const c = V("button", `查看 ${r.sector_name} 龙头`);
  return c.type = "button", c.dataset.decisionSector = r.sector_id, c.dataset.decisionAsOf = t.asOf, c.dataset.decisionTaxonomy = t.taxonomyId, l.append(u, c), s.append(
    l,
    Kt([
      ["成员/有价", `${r.priced_member_count} / ${r.member_count}`],
      ["质量", r.quality_status],
      ["排名资格", r.ranking_eligible ? "eligible" : "excluded"],
      ["龙头数量", a == null ? "—" : Pt(a, 0)],
      ["扩散度", Ie(n)],
      ["持续", o == null ? "—" : `${Pt(o)} 日`]
    ]),
    ji(r.metric_scores),
    at("支持证据", [ae(r.supporting_evidence)]),
    at("反例", [ae(r.contrary_evidence)]),
    at("缺失输入", r.missing_inputs),
    at("排名排除原因", r.ranking_exclusion_reasons),
    at("质量问题", r.quality_issues)
  ), s;
}
function Ms(r, e, t, i) {
  if (r.append(Mr()), !e.rankings.length) {
    r.append(
      V(
        "p",
        "板块数据为空不代表市场没有热点；当前没有足够、已发布的分类与行情证据。",
        "decision-empty"
      ),
      at("缺失输入", e.missing_inputs),
      at("质量问题", e.quality_issues),
      ei(e)
    );
    return;
  }
  r.append(
    fe(`后端返回 ${e.rankings.length} 个板块；保持原始顺序`, e.quality_status)
  );
  const a = V("div", void 0, "decision-rankings");
  for (const n of e.rankings)
    a.append(
      As(n, n.sector_id === t, i)
    );
  r.append(
    a,
    ei(e),
    Kt([
      ["响应公式", e.formula_version],
      ["分类体系", e.taxonomy_id],
      ["实际数据日", e.data_as_of ?? "—"]
    ]),
    at("缺失输入", e.missing_inputs),
    at("质量问题", e.quality_issues)
  );
}
function Ps(r) {
  r.append(
    fe("尚无本地组合风险输入", "missing"),
    V(
      "p",
      "组合风险与目标暴露属于 Release 2。没有现金、净值和风险预算时，不生成仓位区间，也不猜测个人仓位。",
      "state-message"
    )
  );
}
function rr(r, e, t, i) {
  if (r.append(
    fe(
      "龙头仅为后端受限样本候选；不可作为操作首选，不构成买卖建议。",
      "warning"
    )
  ), e.phase === "idle") {
    r.append(V("p", "先选择一个有证据的板块。", "decision-empty"));
    return;
  }
  if (e.phase === "loading") {
    r.append(V("p", "正在读取后端龙头原因与风险…", "state-message"));
    return;
  }
  if (e.phase === "error") {
    r.append(
      V("p", `龙头证据读取失败：${e.message}`, "decision-empty")
    );
    return;
  }
  const a = e.response;
  a.candidates.length || r.append(
    V("p", "当前没有可展示候选；这不等于板块没有龙头。", "decision-empty"),
    at("缺失输入", a.missing_inputs)
  );
  const n = V("div", void 0, "decision-candidates");
  for (const o of a.candidates) {
    const s = V("article", void 0, "decision-candidate"), l = V("div", void 0, "decision-ranking-heading"), u = V("div");
    u.append(
      V("span", `后端顺序 ${o.rank}`, "quiet-tag"),
      V("h4", `${o.name} · ${o.symbol}`),
      V(
        "p",
        `得分 ${Pt(o.total_score)} · 置信度 ${Ie(o.confidence.value)}`,
        "meta-line mono"
      )
    );
    const c = V(
      "button",
      `打开 ${o.symbol} 技术驾驶舱`
    );
    c.type = "button", c.dataset.securitySymbol = o.symbol, c.dataset.securitySource = "sectors", c.dataset.securityAsOf = t.asOf, c.dataset.securityTaxonomy = t.taxonomyId, c.dataset.securitySector = i ?? a.sector_id, l.append(u, c), s.append(
      l,
      Kt([
        ["可操作性", `${o.actionable_primary ? "可" : "不可"}作为操作首选`],
        ["研究龙头资格", o.leader_qualified ? "qualified" : "not qualified"],
        ["资格版本", o.qualification_version],
        ["操作状态", o.actionability_status],
        ["涨跌停锁定", o.limit_lock_status === "unavailable" ? "涨跌停锁定状态不可用" : o.limit_lock_status],
        ["质量", o.quality_status]
      ]),
      ji(o.metric_scores),
      at("支持证据", [ae(o.supporting_evidence)]),
      at("反例", [ae(o.contrary_evidence)]),
      at("缺失输入", o.missing_inputs),
      at("资格原因", o.qualification_reasons),
      at("不合格原因", o.disqualification_reasons),
      at("风险/质量", o.quality_issues)
    ), n.append(s);
  }
  if (r.append(n), a.exclusions.length) {
    const o = V("details", void 0, "decision-detail");
    o.append(V("summary", "查看被排除证券与原因"));
    for (const s of a.exclusions)
      o.append(
        V(
          "p",
          `${s.symbol} · ${s.reasons.join(" · ")}`,
          "mono state-message"
        )
      );
    r.append(o);
  }
  r.append(
    Kt([
      ["响应公式", a.formula_version],
      ["板块", a.sector_name ?? a.sector_id],
      ["资金证据", `${a.fund_flow_evidence.status} / Release 2`],
      ["实际数据日", a.data_as_of ?? "—"]
    ]),
    at("缺失输入", a.missing_inputs),
    at("质量问题", a.quality_issues)
  );
}
function Ds(r, e) {
  return r === 422 ? `日期不在可读取范围：${e}` : r === 503 ? `本地行情存储暂不可用：${e}` : `本地分析接口不可用：${e}`;
}
function ks(r, e) {
  r.replaceChildren();
  const t = V("header", void 0, "decision-header"), i = V("div");
  i.append(
    V("p", "RELEASE 1 / EVIDENCE FIRST", "panel-kicker"),
    V("h2", "市场 → 板块 → 龙头 → 个股"),
    V(
      "p",
      `请求时点 ${e.query.asOf} · 分类 ${e.query.taxonomyId} · 后端结论只读展示`,
      "meta-line mono"
    )
  ), t.append(i), r.append(t);
  const a = V("ol", void 0, "decision-flow-list");
  a.setAttribute("aria-label", "收盘后证据决策顺序");
  const n = Me("market", "01 / REGIME", "市场状态"), o = Me("fund-flow", "02 / FUND EVIDENCE", "资金证据"), s = Me("sectors", "03 / ROTATION", "板块轮动"), l = Me(
    "portfolio-risk",
    "04 / PORTFOLIO",
    "组合风险"
  ), u = Me("leaders", "05 / LEADERS", "龙头候选 → 个股");
  if (a.append(
    n.item,
    o.item,
    s.item,
    l.item,
    u.item
  ), r.append(a), Ts(o.body), Ps(l.body), e.phase === "loading") {
    n.body.append(fe("正在读取市场状态与板块轮动…", "loading")), s.body.append(fe("等待后端板块证据…", "loading")), rr(u.body, { phase: "idle" }, e.query, null);
    return;
  }
  if (e.phase === "error") {
    const c = V("p", Ds(e.status, e.message), "decision-empty");
    c.setAttribute("role", "alert"), n.body.append(c, Mr()), s.body.append(
      V("p", "板块数据未读取；不解释为无热点。", "decision-empty")
    ), rr(u.body, { phase: "idle" }, e.query, null);
    return;
  }
  Ss(n.body, e.overview.market), Ms(
    s.body,
    e.overview.sectors,
    e.selectedSectorId,
    e.query
  ), rr(
    u.body,
    e.leaders,
    e.query,
    e.selectedSectorId
  );
}
function X(r, e, t) {
  const i = document.createElement(r);
  return e !== void 0 && (i.textContent = e), t && (i.className = t), i;
}
function xt(r, e = 4) {
  return r === null ? "—" : new Intl.NumberFormat("zh-CN", {
    maximumFractionDigits: e
  }).format(r);
}
function Rs(r) {
  return r === "portfolio" ? "持仓" : r === "watchlists" ? "自选预警" : "盘后决策流";
}
function ri(r) {
  const e = X("dl", void 0, "security-metadata"), t = [
    ["证券", r.symbol],
    ["实际数据日", r.as_of ?? "—"],
    ["来源", r.source],
    ["价格口径", r.price_adjustment],
    ["公式版本", r.formula_version],
    ["状态", r.status],
    [
      "质量问题",
      r.quality_issues.length ? r.quality_issues.join("、") : "无"
    ]
  ];
  for (const [i, a] of t) {
    const n = X("div");
    n.append(X("dt", i), X("dd", a, "mono")), e.append(n);
  }
  return e;
}
function ii(r, e) {
  const t = X("article", void 0, "panel indicator-panel");
  t.append(X("h3", r));
  const i = X("dl", void 0, "indicator-values");
  for (const [a, n] of e) {
    const o = X("div");
    o.append(X("dt", a), X("dd", xt(n), "mono")), i.append(o);
  }
  return t.append(i), t;
}
function yt(r) {
  return X("td", r);
}
function Fs(r) {
  const e = X("div", void 0, "table-wrap security-table"), t = X("table");
  t.setAttribute("aria-label", "技术指标数值替代");
  const i = X(
    "caption",
    `最近 ${Math.min(r.length, 20)} 个有效交易日；完整图表共 ${r.length} 条`
  ), a = X("thead"), n = X("tr");
  for (const s of [
    "日期",
    "开",
    "高",
    "低",
    "收",
    "成交量",
    "MA5",
    "MA10",
    "MA20",
    "MA60",
    "MA120",
    "MA250",
    "MACD",
    "Signal",
    "Hist",
    "RSI14"
  ]) {
    const l = X("th", s);
    l.scope = "col", n.append(l);
  }
  a.append(n);
  const o = X("tbody");
  for (const s of r.slice(-20)) {
    const l = X("tr");
    l.append(
      yt(s.trade_date),
      yt(xt(s.open)),
      yt(xt(s.high)),
      yt(xt(s.low)),
      yt(xt(s.close)),
      yt(xt(s.volume, 0)),
      yt(xt(s.ma5)),
      yt(xt(s.ma10)),
      yt(xt(s.ma20)),
      yt(xt(s.ma60)),
      yt(xt(s.ma120)),
      yt(xt(s.ma250)),
      yt(xt(s.macd)),
      yt(xt(s.macd_signal)),
      yt(xt(s.macd_hist)),
      yt(xt(s.rsi14))
    ), o.append(l);
  }
  return t.append(i, a, o), e.append(t), e;
}
function Bs(r, e) {
  const t = e === "no_effective_trading_data" ? "该窗口没有有效交易数据（记录可能全部为停牌占位）。" : "该窗口没有可用行情。", i = X("div", void 0, "panel security-state");
  i.append(
    X("p", "EMPTY / 真实空态", "panel-kicker"),
    X(
      "h2",
      e === "no_effective_trading_data" ? "没有有效交易数据" : "没有可用行情"
    ),
    X("p", t, "state-message")
  ), r.append(i);
}
function Os(r, e, t) {
  const i = r.querySelector("#security-back"), a = r.querySelector("#security-status"), n = r.querySelector("#security-content");
  if (!i || !a || !n)
    throw new Error("security cockpit root is incomplete");
  if (n.replaceChildren(), e.phase === "idle")
    return a.textContent = "尚未选择证券", null;
  if (e.decisionContext) {
    const v = new URLSearchParams({
      as_of: e.decisionContext.asOf,
      taxonomy_id: e.decisionContext.taxonomyId,
      sector_id: e.decisionContext.sectorId
    });
    i.href = `#overview?${v}`, i.dataset.decisionReturn = "true", delete i.dataset.viewTarget;
  } else
    i.href = `#${e.sourceView}`, i.dataset.viewTarget = e.sourceView, delete i.dataset.decisionReturn;
  if (i.textContent = `← 返回${Rs(e.sourceView)}`, e.phase === "loading") {
    a.textContent = `正在读取 ${e.symbol} 的交易日和后端分析…`;
    const v = X("div", void 0, "panel security-state");
    return v.append(
      X("p", "LOADING / 后端分析", "panel-kicker"),
      X("h2", e.symbol),
      X("p", "先读取有效交易日，再请求最多 260 日分析。")
    ), n.append(v), null;
  }
  if (e.phase === "empty")
    return a.textContent = `${e.symbol} 无可绘制数据`, e.response && n.append(ri(e.response)), Bs(n, e.reason), null;
  if (e.phase === "quality-error") {
    a.textContent = `${e.symbol} 未通过数据质量门禁`;
    const v = X("div", void 0, "panel security-state");
    return v.append(
      X("p", "HTTP 409 / FAIL CLOSED", "panel-kicker"),
      X("h2", "数据质量门禁"),
      X("p", e.message, "state-message"),
      X("p", "未使用未复权数据降级，也未绘制蜡烛。", "state-message")
    ), n.append(v), null;
  }
  if (e.phase === "connection-error") {
    a.textContent = `${e.symbol} 连接失败`;
    const v = X("div", void 0, "panel security-state");
    return v.append(
      X("p", "CONNECTION ERROR / 本地服务", "panel-kicker"),
      X("h2", "无法连接本地 API"),
      X("p", e.message, "state-message"),
      X("p", "请确认 Stock EVA API 仅在本机运行。", "state-message")
    ), n.append(v), null;
  }
  const o = e.response;
  a.textContent = `${o.symbol} 已加载 ${o.series.length} 个有效交易日`, n.append(ri(o));
  const s = X("article", void 0, "panel security-chart-panel"), l = X("div", void 0, "panel-heading"), u = X("div");
  u.append(
    X("p", "QFQ DAILY / API INDICATORS", "panel-kicker"),
    X("h2", `${o.symbol} 技术驾驶舱`)
  ), l.append(u, X("span", `截至 ${o.as_of ?? "—"}`, "quiet-tag"));
  const c = X(
    "p",
    `前复权日 K、真实成交量和后端 MA，共 ${o.series.length} 个有效交易日。`,
    "state-message"
  ), d = X("div", void 0, "security-chart");
  d.id = "security-chart", d.dataset.testid = "security-chart", d.setAttribute("role", "img"), d.setAttribute(
    "aria-label",
    `${o.symbol} 前复权日 K、成交量、MA5、10、20、60、120、250、MACD、RSI14 图表`
  ), s.append(l, c, d), n.append(s);
  const h = o.series[o.series.length - 1], f = X("div", void 0, "indicator-grid");
  return f.append(
    ii("MACD（12, 26, 9）", [
      ["MACD", h.macd],
      ["Signal", h.macd_signal],
      ["Histogram", h.macd_hist]
    ]),
    ii("RSI（14）", [["RSI14", h.rsi14]])
  ), n.append(f, Fs(o.series)), t(d, o);
}
const Ls = "baostock.industry_classification";
let Bt = cr, lt = null, zt = null, re = null, ne = null, oe = null, ke = null, He = !1;
function Vs() {
  const r = document.querySelector("#security-analysis");
  if (!r) throw new Error("security cockpit section is missing");
  return r;
}
function Ue() {
  return document.querySelector("#decision-flow");
}
function ai() {
  ne?.(), ne = Os(
    Vs(),
    Bt,
    is
  );
}
function Fe() {
  const r = Ue();
  r && lt && ks(r, lt);
}
function Ns() {
  const r = new Intl.DateTimeFormat("en-US", {
    timeZone: "Asia/Shanghai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  }).formatToParts(/* @__PURE__ */ new Date()), e = Object.fromEntries(r.map((t) => [t.type, t.value]));
  return `${e.year}-${e.month}-${e.day}`;
}
function Ys() {
  return { asOf: Ns(), taxonomyId: Ls };
}
function Zi(r) {
  return { asOf: r.asOf, taxonomyId: r.taxonomyId };
}
function Ki(r) {
  return {
    status: r instanceof ur ? r.status : null,
    message: r instanceof ur ? r.detail : r instanceof Error ? r.message : "未知连接错误"
  };
}
function Ws(r, e) {
  const t = Ui({ ...r, sectorId: e });
  oe = t, window.history.replaceState(null, "", t);
}
async function Ji(r, e) {
  if (!re || lt?.phase !== "ready") return;
  const t = lt.overview;
  lt = {
    phase: "ready",
    query: r,
    overview: t,
    selectedSectorId: e,
    leaders: { phase: "loading" }
  }, Fe();
  try {
    const i = await re.loadLeaders({
      ...r,
      sectorId: e
    });
    if (i === null || lt?.phase !== "ready" || lt.query.asOf !== r.asOf || lt.query.taxonomyId !== r.taxonomyId || lt.selectedSectorId !== e)
      return;
    lt = {
      ...lt,
      leaders: { phase: "ready", response: i }
    };
  } catch (i) {
    if (lt?.phase !== "ready" || lt.selectedSectorId !== e)
      return;
    lt = {
      ...lt,
      leaders: { phase: "error", ...Ki(i) }
    };
  }
  Fe();
}
async function dr(r, e) {
  if (!(!Ue() || !re)) {
    zt?.abort(), zt = null, ne?.(), ne = null, window.stockEvaActivateView?.("overview", !1), lt = { phase: "loading", query: r }, Fe();
    try {
      const i = await re.loadOverview(r);
      if (i === null) return;
      const n = i.sectors.rankings.some(
        (o) => o.sector_id === e
      ) ? e : i.sectors.rankings[0]?.sector_id ?? null;
      lt = {
        phase: "ready",
        query: r,
        overview: i,
        selectedSectorId: n,
        leaders: { phase: "idle" }
      }, Ws(r, n), Fe(), n && await Ji(r, n);
    } catch (i) {
      lt = {
        phase: "error",
        query: r,
        ...Ki(i)
      }, Fe();
    }
  }
}
async function Qi(r, e, t) {
  zt?.abort();
  const i = new AbortController();
  zt = i, re?.dispose(), window.stockEvaActivateView?.("security", !1), Bt = $e(Bt, {
    type: "load",
    symbol: r,
    sourceView: e,
    ...t ? { decisionContext: t } : {}
  }), ai();
  try {
    const a = await da(
      r,
      fetch,
      i.signal,
      t?.asOf
    );
    if (zt !== i || i.signal.aborted) return;
    Bt = $e(Bt, {
      type: "success",
      response: a
    });
  } catch (a) {
    if (zt !== i || i.signal.aborted) return;
    a instanceof li ? Bt = $e(Bt, {
      type: "empty",
      reason: "no_market_data"
    }) : Bt = $e(Bt, {
      type: "failure",
      status: a instanceof ir ? a.status : null,
      message: a instanceof ir ? a.detail : a instanceof Error ? a.message : "未知连接错误"
    });
  }
  ai();
}
function zs(r, e, t) {
  const i = bs(r, e, t);
  oe = i, window.history.pushState(null, "", i), Qi(r, e, t);
}
function Pe() {
  const r = window.location.hash;
  if (r === oe) return;
  oe = r;
  const e = Cs(r);
  if (e) {
    const i = e.asOf && e.taxonomyId && e.sectorId ? {
      asOf: e.asOf,
      taxonomyId: e.taxonomyId,
      sectorId: e.sectorId
    } : void 0;
    Qi(
      e.symbol,
      e.sourceView,
      i
    );
    return;
  }
  const t = Gi(r);
  if (t) {
    dr(Zi(t), t.sectorId);
    return;
  }
  if (r === "" || r === "#overview") {
    dr(Ys(), null);
    return;
  }
  zt?.abort(), zt = null, ne?.(), ne = null;
}
function ni(r) {
  if (!(r.target instanceof Element)) return;
  const t = r.target.closest("[data-decision-sector]")?.dataset.decisionSector;
  if (!t || lt?.phase !== "ready") return;
  r.preventDefault();
  const i = lt.query, a = Ui({ ...i, sectorId: t });
  oe = a, window.history.pushState(null, "", a), Ji(i, t);
}
function oi(r) {
  if (!(r.target instanceof Element)) return;
  const e = r.target.closest(
    "#security-back[data-decision-return]"
  );
  if (!e) return;
  r.preventDefault(), r.stopImmediatePropagation();
  const t = Gi(e.hash);
  t && (oe = e.hash, window.history.pushState(null, "", e.hash), dr(Zi(t), t.sectorId));
}
function ta() {
  if (ke) return;
  Bt = cr, lt = null, oe = null, re = new ws(fetch);
  const r = Es(document, zs);
  Ue()?.addEventListener("click", ni), document.addEventListener("click", oi, !0), window.addEventListener("popstate", Pe), window.addEventListener("hashchange", Pe);
  const e = () => {
    ke === e && (r(), Ue()?.removeEventListener("click", ni), document.removeEventListener("click", oi, !0), window.removeEventListener("popstate", Pe), window.removeEventListener("hashchange", Pe), zt?.abort(), zt = null, re?.dispose(), re = null, ne?.(), ne = null, Bt = cr, lt = null, oe = null, ke = null);
  };
  ke = e, Pe();
}
function ea() {
  He = !1, ta();
}
function Xs() {
  He && (document.removeEventListener("DOMContentLoaded", ea), He = !1), ke?.();
}
document.readyState === "loading" ? (He = !0, document.addEventListener("DOMContentLoaded", ea, { once: !0 })) : ta();
export {
  Xs as disposeSecurityCockpit,
  ta as initializeSecurityCockpit
};
