const ka = "http://127.0.0.1:8000/api/v1", Ii = /^(sh|sz)\.[0-9]{6}$/, ir = /^\d{4}-\d{2}-\d{2}$/;
let fr = class extends Error {
  status;
  detail;
  constructor(t, e) {
    super(e), this.name = "ApiError", this.status = t, this.detail = e;
  }
};
class Si extends Error {
  constructor() {
    super("no backend-provided trading dates"), this.name = "NoTradingDatesError";
  }
}
function Er(r) {
  return typeof r == "object" && r !== null && !Array.isArray(r);
}
function Se(r, t) {
  const e = r[t];
  if (typeof e != "string") throw new TypeError(`${t} must be a string`);
  return e;
}
function ge(r, t) {
  const e = r[t];
  if (typeof e != "number" || !Number.isFinite(e))
    throw new TypeError(`${t} must be a finite number`);
  return e;
}
function Wt(r, t) {
  return r[t] === null ? null : ge(r, t);
}
function Da(r) {
  if (!Er(r)) throw new TypeError("series point must be an object");
  const t = Se(r, "trade_date");
  if (!ir.test(t))
    throw new TypeError("trade_date must be an ISO date");
  return {
    trade_date: t,
    open: ge(r, "open"),
    high: ge(r, "high"),
    low: ge(r, "low"),
    close: ge(r, "close"),
    volume: ge(r, "volume"),
    amount: ge(r, "amount"),
    ma5: Wt(r, "ma5"),
    ma10: Wt(r, "ma10"),
    ma20: Wt(r, "ma20"),
    ma60: Wt(r, "ma60"),
    ma120: Wt(r, "ma120"),
    ma250: Wt(r, "ma250"),
    macd: Wt(r, "macd"),
    macd_signal: Wt(r, "macd_signal"),
    macd_hist: Wt(r, "macd_hist"),
    rsi14: Wt(r, "rsi14")
  };
}
function Ra(r) {
  if (!Er(r)) throw new TypeError("analysis response must be an object");
  const t = Se(r, "symbol"), e = Se(r, "status"), i = Se(r, "source"), a = Se(r, "price_adjustment"), n = Se(r, "formula_version");
  if (!Ii.test(t)) throw new TypeError("invalid response symbol");
  if (e !== "empty" && e !== "ready")
    throw new TypeError("invalid analysis status");
  if (i !== "baostock") throw new TypeError("invalid analysis source");
  if (a !== "qfq")
    throw new TypeError("invalid price adjustment");
  if (r.as_of !== null && typeof r.as_of != "string")
    throw new TypeError("as_of must be an ISO date or null");
  if (typeof r.as_of == "string" && !ir.test(r.as_of))
    throw new TypeError("as_of must be an ISO date or null");
  if (!Array.isArray(r.quality_issues) || !r.quality_issues.every((o) => typeof o == "string"))
    throw new TypeError("quality_issues must be a string array");
  if (!Array.isArray(r.series))
    throw new TypeError("series must be an array");
  return {
    symbol: t,
    status: e,
    as_of: r.as_of,
    source: i,
    price_adjustment: a,
    formula_version: n,
    quality_issues: r.quality_issues,
    series: r.series.map(Da)
  };
}
function Ti(r) {
  if (!Array.isArray(r) || !r.every(
    (t) => typeof t == "string" && ir.test(t)
  ))
    throw new TypeError("trading dates must be an ISO date array");
  for (let t = 1; t < r.length; t += 1)
    if (r[t] <= r[t - 1])
      throw new TypeError("trading dates must be strictly ascending");
  return r;
}
function Fa(r, t) {
  const e = Ti(r);
  if (t !== void 0 && !ir.test(t))
    throw new TypeError("as_of must be an ISO date");
  const i = t === void 0 ? e : e.filter((n) => n <= t);
  if (i.length === 0) throw new Si();
  const a = i.slice(-260);
  return { start: a[0], end: a[a.length - 1] };
}
async function Ba(r) {
  try {
    const t = await r.json();
    if (Er(t) && typeof t.detail == "string")
      return t.detail;
  } catch {
  }
  return `HTTP ${r.status}`;
}
async function Ur(r, t, e) {
  const i = await t(`${ka}${r}`, { signal: e });
  if (!i.ok)
    throw new fr(i.status, await Ba(i));
  return i.json();
}
async function Oa(r, t = fetch, e = new AbortController().signal, i) {
  if (!Ii.test(r)) throw new TypeError("invalid security symbol");
  const a = Ti(
    await Ur("/market/history/dates", t, e)
  ), { start: n, end: o } = Fa(a, i), s = new URLSearchParams({ start: n, end: o }), l = await Ur(
    `/securities/${encodeURIComponent(r)}/analysis?${s}`,
    t,
    e
  );
  return Ra(l);
}
function ht(r, t) {
  if (!(!zt(r) && !zt(t))) {
    for (var e in t)
      if (Object.prototype.hasOwnProperty.call(t, e)) {
        var i = r[e], a = t[e];
        zt(a) && zt(i) ? ht(i, a) : r[e] = $e(a);
      }
  }
}
function $e(r) {
  if (!zt(r))
    return r;
  var t = null;
  $t(r) ? t = [] : t = {};
  for (var e in r)
    if (Object.prototype.hasOwnProperty.call(r, e)) {
      var i = r[e];
      zt(i) ? t[e] = $e(i) : t[e] = i;
    }
  return t;
}
function $t(r) {
  return Object.prototype.toString.call(r) === "[object Array]";
}
function dt(r) {
  return typeof r == "function";
}
function zt(r) {
  return typeof r == "object" && C(r);
}
function O(r) {
  return typeof r == "number" && Number.isFinite(r);
}
function C(r) {
  return r != null;
}
function Me(r) {
  return typeof r == "boolean";
}
function K(r) {
  return typeof r == "string";
}
var La = /\\(\\)?/g, Va = RegExp(`[^.[\\]]+|\\[(?:([^"'][^[]*)|(["'])((?:(?!\\2)[^\\\\]|\\\\.)*?)\\2)\\]|(?=(?:\\.|\\[\\])(?:\\.|\\[\\]|$))`, "g");
function vt(r, t, e) {
  if (C(r)) {
    var i = [];
    t.replace(Va, function(s) {
      for (var l = [], u = 1; u < arguments.length; u++)
        l[u - 1] = arguments[u];
      var c = s;
      return C(l[1]) ? c = l[2].replace(La, "$1") : C(l[0]) && (c = l[0].trim()), i.push(c), "";
    });
    for (var a = r, n = 0, o = i.length; C(a) && n < o; )
      a = a?.[i[n++]];
    return C(a) ? a : e ?? "--";
  }
  return e ?? "--";
}
function Na(r, t) {
  var e = {};
  return r.formatToParts(new Date(t)).forEach(function(i) {
    var a = i.type, n = i.value;
    switch (a) {
      case "year": {
        e.YYYY = n;
        break;
      }
      case "month": {
        e.MM = n;
        break;
      }
      case "day": {
        e.DD = n;
        break;
      }
      case "hour": {
        e.HH = n === "24" ? "00" : n;
        break;
      }
      case "minute": {
        e.mm = n;
        break;
      }
      case "second": {
        e.ss = n;
        break;
      }
    }
  }), e;
}
function Ya(r, t, e) {
  var i = Na(r, t);
  return e.replace(/YYYY|MM|DD|HH|mm|ss/g, function(a) {
    return i[a];
  });
}
function bt(r, t) {
  var e = +r;
  return O(e) ? e.toFixed(t ?? 2) : "".concat(r);
}
function Wa(r) {
  var t = +r;
  if (O(t)) {
    if (t > 1e9)
      return "".concat(+(t / 1e9).toFixed(3), "B");
    if (t > 1e6)
      return "".concat(+(t / 1e6).toFixed(3), "M");
    if (t > 1e3)
      return "".concat(+(t / 1e3).toFixed(3), "K");
  }
  return "".concat(r);
}
function $a(r, t) {
  var e = "".concat(r);
  if (t.length === 0)
    return e;
  if (e.includes(".")) {
    var i = e.split(".");
    return "".concat(i[0].replace(/(\d)(?=(\d{3})+$)/g, function(a) {
      return "".concat(a).concat(t);
    }), ".").concat(i[1]);
  }
  return e.replace(/(\d)(?=(\d{3})+$)/g, function(a) {
    return "".concat(a).concat(t);
  });
}
function za(r, t) {
  var e = "".concat(r), i = new RegExp("\\.0{" + t + ",}[1-9][0-9]*$");
  if (i.test(e)) {
    var a = e.split("."), n = a.length - 1, o = a[n], s = /0*/.exec(o);
    if (C(s)) {
      var l = s[0].length;
      return a[n] = o.replace(/0*/, "0{".concat(l, "}")), a.join(".");
    }
  }
  return e;
}
function Gr(r, t) {
  return r.replace(/\{(\w+)\}/g, function(e, i) {
    var a = t[i];
    return C(a) ? a : "{".concat(i, "}");
  });
}
var Fe = null;
function oe(r) {
  var t, e;
  return (e = (t = r.ownerDocument.defaultView) === null || t === void 0 ? void 0 : t.devicePixelRatio) !== null && e !== void 0 ? e : 1;
}
function ye(r, t, e) {
  return "".concat(t ?? "normal", " ").concat(r ?? 12, "px ").concat(e ?? "Helvetica Neue");
}
function qt(r, t, e, i) {
  if (!C(Fe)) {
    var a = document.createElement("canvas"), n = oe(a);
    Fe = a.getContext("2d"), Fe.scale(n, n);
  }
  return Fe.font = ye(t, e, i), Math.round(Fe.measureText(r).width);
}
var pr = function(r, t) {
  return pr = Object.setPrototypeOf || { __proto__: [] } instanceof Array && function(e, i) {
    e.__proto__ = i;
  } || function(e, i) {
    for (var a in i) Object.prototype.hasOwnProperty.call(i, a) && (e[a] = i[a]);
  }, pr(r, t);
};
function X(r, t) {
  if (typeof t != "function" && t !== null)
    throw new TypeError("Class extends value " + String(t) + " is not a constructor or null");
  pr(r, t);
  function e() {
    this.constructor = r;
  }
  r.prototype = t === null ? Object.create(t) : (e.prototype = t.prototype, new e());
}
var M = function() {
  return M = Object.assign || function(t) {
    for (var e, i = 1, a = arguments.length; i < a; i++) {
      e = arguments[i];
      for (var n in e) Object.prototype.hasOwnProperty.call(e, n) && (t[n] = e[n]);
    }
    return t;
  }, M.apply(this, arguments);
};
function ze(r, t) {
  var e = {};
  for (var i in r) Object.prototype.hasOwnProperty.call(r, i) && t.indexOf(i) < 0 && (e[i] = r[i]);
  if (r != null && typeof Object.getOwnPropertySymbols == "function")
    for (var a = 0, i = Object.getOwnPropertySymbols(r); a < i.length; a++)
      t.indexOf(i[a]) < 0 && Object.prototype.propertyIsEnumerable.call(r, i[a]) && (e[i[a]] = r[i[a]]);
  return e;
}
function Ir(r, t, e, i) {
  function a(n) {
    return n instanceof e ? n : new e(function(o) {
      o(n);
    });
  }
  return new (e || (e = Promise))(function(n, o) {
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
function Sr(r, t) {
  var e = { label: 0, sent: function() {
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
    for (; o && (o = 0, u[0] && (e = 0)), e; ) try {
      if (i = 1, a && (n = u[0] & 2 ? a.return : u[0] ? a.throw || ((n = a.return) && n.call(a), 0) : a.next) && !(n = n.call(a, u[1])).done) return n;
      switch (a = 0, n && (u = [u[0] & 2, n.value]), u[0]) {
        case 0:
        case 1:
          n = u;
          break;
        case 4:
          return e.label++, { value: u[1], done: !1 };
        case 5:
          e.label++, a = u[1], u = [0];
          continue;
        case 7:
          u = e.ops.pop(), e.trys.pop();
          continue;
        default:
          if (n = e.trys, !(n = n.length > 0 && n[n.length - 1]) && (u[0] === 6 || u[0] === 2)) {
            e = 0;
            continue;
          }
          if (u[0] === 3 && (!n || u[1] > n[0] && u[1] < n[3])) {
            e.label = u[1];
            break;
          }
          if (u[0] === 6 && e.label < n[1]) {
            e.label = n[1], n = u;
            break;
          }
          if (n && e.label < n[2]) {
            e.label = n[2], e.ops.push(u);
            break;
          }
          n[2] && e.ops.pop(), e.trys.pop();
          continue;
      }
      u = t.call(r, e);
    } catch (c) {
      u = [6, c], a = 0;
    } finally {
      i = n = 0;
    }
    if (u[0] & 5) throw u[1];
    return { value: u[0] ? u[1] : void 0, done: !0 };
  }
}
function Ct(r) {
  var t = typeof Symbol == "function" && Symbol.iterator, e = t && r[t], i = 0;
  if (e) return e.call(r);
  if (r && typeof r.length == "number") return {
    next: function() {
      return r && i >= r.length && (r = void 0), { value: r && r[i++], done: !r };
    }
  };
  throw new TypeError(t ? "Object is not iterable." : "Symbol.iterator is not defined.");
}
function Re(r, t) {
  var e = typeof Symbol == "function" && r[Symbol.iterator];
  if (!e) return r;
  var i = e.call(r), a, n = [], o;
  try {
    for (; (t === void 0 || t-- > 0) && !(a = i.next()).done; ) n.push(a.value);
  } catch (s) {
    o = { error: s };
  } finally {
    try {
      a && !a.done && (e = i.return) && e.call(i);
    } finally {
      if (o) throw o.error;
    }
  }
  return n;
}
function Pe(r, t, e) {
  if (arguments.length === 2) for (var i = 0, a = t.length, n; i < a; i++)
    (n || !(i in t)) && (n || (n = Array.prototype.slice.call(t, 0, i)), n[i] = t[i]);
  return r.concat(n || Array.prototype.slice.call(t));
}
function Tr(r) {
  var t = {
    width: 0,
    height: 0,
    left: 0,
    right: 0,
    top: 0,
    bottom: 0
  };
  return C(r) && ht(t, r), t;
}
var ne = -1;
function qe(r) {
  return dt(window.requestAnimationFrame) ? window.requestAnimationFrame(r) : window.setTimeout(r, 20);
}
function gr(r) {
  dt(window.cancelAnimationFrame) ? window.cancelAnimationFrame(r) : window.clearTimeout(r);
}
var mr = (
  /** @class */
  (function() {
    function r(t) {
      this._options = { duration: 500, iterationCount: 1 }, this._currentIterationCount = 0, this._running = !1, this._time = 0, ht(this._options, t);
    }
    return r.prototype._loop = function() {
      var t = this;
      this._running = !0;
      var e = function() {
        var i;
        if (t._running) {
          var a = (/* @__PURE__ */ new Date()).getTime() - t._time;
          a < t._options.duration ? ((i = t._doFrameCallback) === null || i === void 0 || i.call(t, a), qe(e)) : (t.stop(), t._currentIterationCount++, t._currentIterationCount < t._options.iterationCount && t.start());
        }
      };
      qe(e);
    }, r.prototype.doFrame = function(t) {
      return this._doFrameCallback = t, this;
    }, r.prototype.setDuration = function(t) {
      return this._options.duration = t, this;
    }, r.prototype.setIterationCount = function(t) {
      return this._options.iterationCount = t, this;
    }, r.prototype.start = function() {
      this._running || (this._time = (/* @__PURE__ */ new Date()).getTime(), this._loop());
    }, r.prototype.stop = function() {
      var t;
      this._running && ((t = this._doFrameCallback) === null || t === void 0 || t.call(this, this._options.duration)), this._running = !1;
    }, r;
  })()
), or = 1, jr = (/* @__PURE__ */ new Date()).getTime();
function Te(r) {
  var t = (/* @__PURE__ */ new Date()).getTime();
  return t === jr ? ++or : or = 1, jr = t, "".concat(r ?? "").concat(t, "_").concat(or);
}
function te(r, t) {
  var e, i = document.createElement(r), a = t ?? {};
  for (var n in a)
    i.style[n] = (e = a[n]) !== null && e !== void 0 ? e : "";
  return i;
}
function _r(r, t, e) {
  var i = 0, a = 0;
  for (a = r.length - 1; i !== a; ) {
    var n = Math.floor((a + i) / 2), o = a - i, s = r[n][t];
    if (e === r[i][t])
      return i;
    if (e === r[a][t])
      return a;
    if (e === s)
      return n;
    if (e > s ? i = n : a = n, o <= 2)
      break;
  }
  return i;
}
function qa(r) {
  var t = Math.floor(Zt(r)), e = me(t), i = r / e, a = 0;
  return i < 1.5 ? a = 1 : i < 2.5 ? a = 2 : i < 3.5 ? a = 3 : i < 4.5 ? a = 4 : i < 5.5 ? a = 5 : i < 6.5 ? a = 6 : a = 8, r = a * e, +r.toFixed(Math.abs(t));
}
function Zr(r, t) {
  t = Math.max(0, t ?? 0);
  var e = Math.pow(10, t);
  return Math.round(r * e) / e;
}
function Xa(r) {
  var t = r.toString(), e = t.indexOf("e");
  if (e > 0) {
    var i = +t.slice(e + 1);
    return i < 0 ? -i : 0;
  }
  var a = t.indexOf(".");
  return a < 0 ? 0 : t.length - 1 - a;
}
function Ai(r, t, e) {
  for (var i, a, n = [Number.MIN_SAFE_INTEGER, Number.MAX_SAFE_INTEGER], o = r.length, s = 0; s < o; ) {
    var l = r[s];
    n[0] = Math.max((i = l[t]) !== null && i !== void 0 ? i : Number.MIN_SAFE_INTEGER, n[0]), n[1] = Math.min((a = l[e]) !== null && a !== void 0 ? a : Number.MAX_SAFE_INTEGER, n[1]), ++s;
  }
  return n;
}
function Zt(r) {
  return r === 0 ? 0 : Math.log10(r);
}
function me(r) {
  return Math.pow(10, r);
}
function Kr() {
  return { from: 0, to: 0, realFrom: 0, realTo: 0 };
}
var Ha = (
  /** @class */
  (function() {
    function r(t) {
      this._holdingTasks = null, this._running = !1, this._callback = t;
    }
    return r.prototype.add = function(t) {
      this._running ? C(this._holdingTasks) ? this._holdingTasks = M(M({}, this._holdingTasks), t) : this._holdingTasks = t : this._runTask(t);
    }, r.prototype._runTask = function(t) {
      return Ir(this, void 0, void 0, function() {
        var e, i;
        return Sr(this, function(a) {
          switch (a.label) {
            case 0:
              this._running = !0, a.label = 1;
            case 1:
              return a.trys.push([1, , 3, 4]), [4, Promise.all(Object.values(t))];
            case 2:
              return a.sent(), [3, 4];
            case 3:
              return this._running = !1, (i = this._callback) === null || i === void 0 || i.call(this), C(this._holdingTasks) && (e = this._holdingTasks, this._runTask(e), this._holdingTasks = null), [
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
), gt = {
  PRICE: 2,
  VOLUME: 0
}, Ua = (
  /** @class */
  (function() {
    function r() {
      this._callbacks = [];
    }
    return r.prototype.subscribe = function(t) {
      var e = this._callbacks.indexOf(t);
      e < 0 && this._callbacks.push(t);
    }, r.prototype.unsubscribe = function(t) {
      if (dt(t)) {
        var e = this._callbacks.indexOf(t);
        e > -1 && this._callbacks.splice(e, 1);
      } else
        this._callbacks = [];
    }, r.prototype.execute = function(t) {
      this._callbacks.forEach(function(e) {
        e(t);
      });
    }, r.prototype.isEmpty = function() {
      return this._callbacks.length === 0;
    }, r;
  })()
);
function ke(r) {
  return r === "transparent" || r === "none" || /^[rR][gG][Bb][Aa]\(([\s]*(2[0-4][0-9]|25[0-5]|[01]?[0-9][0-9]?)[\s]*,){3}[\s]*0[\s]*\)$/.test(r) || /^[hH][Ss][Ll][Aa]\(([\s]*(360｜3[0-5][0-9]|[012]?[0-9][0-9]?)[\s]*,)([\s]*((100|[0-9][0-9]?)%|0)[\s]*,){2}([\s]*0[\s]*)\)$/.test(r);
}
function se(r, t) {
  var e = r.replace(/^#/, ""), i = parseInt(e, 16), a = i >> 16 & 255, n = i >> 8 & 255, o = i & 255;
  return "rgba(".concat(a, ", ").concat(n, ", ").concat(o, ", ").concat(t ?? 1, ")");
}
var N = {
  RED: "#F92855",
  GREEN: "#2DC08E",
  WHITE: "#FFFFFF",
  GREY: "#76808F",
  BLUE: "#1677FF"
};
function Ga() {
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
function ja() {
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
        color: se(N.BLUE, 0.01)
      }, {
        offset: 1,
        color: se(N.BLUE, 0.2)
      }],
      point: {
        show: !0,
        color: N.BLUE,
        radius: 4,
        rippleColor: se(N.BLUE, 0.3),
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
function Za() {
  var r = se(N.GREEN, 0.7), t = se(N.RED, 0.7);
  return {
    ohlc: {
      compareRule: "current_open",
      upColor: r,
      downColor: t,
      noChangeColor: N.GREY
    },
    bars: [{
      style: "fill",
      borderStyle: "solid",
      borderSize: 1,
      borderDashedValue: [2, 2],
      upColor: r,
      downColor: t,
      noChangeColor: N.GREY
    }],
    lines: ["#FF9600", "#935EBD", N.BLUE, "#E11D74", "#01C5C4"].map(function(e) {
      return {
        style: "solid",
        smooth: !1,
        size: 1,
        dashedValue: [2, 2],
        color: e
      };
    }),
    circles: [{
      style: "fill",
      borderStyle: "solid",
      borderSize: 1,
      borderDashedValue: [2, 2],
      upColor: r,
      downColor: t,
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
function Jr() {
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
function Ka() {
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
function Ja() {
  var r = se(N.BLUE, 0.35), t = se(N.BLUE, 0.25);
  function e() {
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
      color: t,
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
      color: t,
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
    text: e()
  };
}
function Qa() {
  return {
    size: 1,
    color: "#DDDDDD",
    fill: !0,
    activeBackgroundColor: se(N.BLUE, 0.08)
  };
}
function tn() {
  return {
    grid: Ga(),
    candle: ja(),
    indicator: Za(),
    xAxis: Jr(),
    yAxis: Jr(),
    separator: Qa(),
    crosshair: Ka(),
    overlay: Ja()
  };
}
function Ar(r, t, e, i, a) {
  var n = r.result, o = r.figures, s = r.styles, l = vt(s, "texts", i.texts), u = l.length, c = vt(s, "circles", i.circles), d = c.length, h = vt(s, "bars", i.bars), f = h.length, v = vt(s, "lines", i.lines), p = v.length, g = 0, m = 0, x = 0, y = 0, E, _ = 0;
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
          prev: n[t - 1],
          current: n[t],
          next: n[t + 1]
        },
        indicator: r,
        barSpace: e,
        defaultStyles: i
      });
      a(I, M(M({}, E), T), _);
    }
  });
}
var Mi = (
  /** @class */
  (function() {
    function r(t) {
      this.precision = 4, this.calcParams = [], this.shouldOhlc = !1, this.shouldFormatBigNumber = !1, this.visible = !0, this.zLevel = 0, this.series = "normal", this.figures = [], this.minValue = null, this.maxValue = null, this.styles = null, this.shouldUpdate = function(e, i) {
        var a = JSON.stringify(e.calcParams) !== JSON.stringify(i.calcParams) || e.figures !== i.figures || e.calc !== i.calc, n = a || e.shortName !== i.shortName || e.paneId !== i.paneId || e.yAxisId !== i.yAxisId || e.series !== i.series || e.minValue !== i.minValue || e.maxValue !== i.maxValue || e.precision !== i.precision || e.shouldOhlc !== i.shouldOhlc || e.shouldFormatBigNumber !== i.shouldFormatBigNumber || e.visible !== i.visible || e.zLevel !== i.zLevel || e.extendData !== i.extendData || e.regenerateFigures !== i.regenerateFigures || e.createTooltipDataSource !== i.createTooltipDataSource || e.draw !== i.draw;
        return { calc: a, draw: n };
      }, this.calc = function() {
        return [];
      }, this.regenerateFigures = null, this.createTooltipDataSource = null, this.draw = null, this.result = [], this._lockSeriesPrecision = !1, this.override(t), this._lockSeriesPrecision = !1;
    }
    return r.prototype.override = function(t) {
      var e, i, a = this, n = a.result;
      a._prevIndicator;
      var o = ze(a, ["result", "_prevIndicator"]);
      this._prevIndicator = M(M({}, $e(o)), { result: n });
      var s = t.id, l = t.name, u = t.shortName, c = t.precision, d = t.styles, h = t.figures, f = t.calcParams, v = ze(t, ["id", "name", "shortName", "precision", "styles", "figures", "calcParams"]);
      !K(this.id) && K(s) && (this.id = s), K(this.name) || (this.name = l ?? ""), this.shortName = (e = u ?? this.shortName) !== null && e !== void 0 ? e : this.name, O(c) && (this.precision = c, this._lockSeriesPrecision = !0), C(d) && ((i = this.styles) !== null && i !== void 0 || (this.styles = {}), ht(this.styles, d)), ht(this, v), C(f) && (this.calcParams = f, dt(this.regenerateFigures) && (this.figures = this.regenerateFigures(this.calcParams))), this.figures = h ?? this.figures;
    }, r.prototype.setSeriesPrecision = function(t) {
      this._lockSeriesPrecision || (this.precision = t);
    }, r.prototype.shouldUpdateImp = function() {
      var t = this._prevIndicator.zLevel !== this.zLevel, e = this.shouldUpdate(this._prevIndicator, this);
      return Me(e) ? { calc: e, draw: e, sort: t } : M(M({}, e), { sort: t });
    }, r.prototype.calcImp = function(t) {
      return Ir(this, void 0, void 0, function() {
        var e;
        return Sr(this, function(i) {
          switch (i.label) {
            case 0:
              return i.trys.push([0, 2, , 3]), [4, this.calc(t, this)];
            case 1:
              return e = i.sent(), this.result = e, [2, !0];
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
    }, r.extend = function(t) {
      var e = (
        /** @class */
        (function(i) {
          X(a, i);
          function a() {
            return i.call(this, t) || this;
          }
          return a;
        })(r)
      );
      return e;
    }, r;
  })()
), en = {
  name: "AVP",
  shortName: "AVP",
  series: "price",
  precision: 2,
  figures: [
    { key: "avp", title: "AVP: ", type: "line" }
  ],
  calc: function(r) {
    var t = 0, e = 0;
    return r.map(function(i) {
      var a, n, o = {}, s = (a = i.turnover) !== null && a !== void 0 ? a : 0, l = (n = i.volume) !== null && n !== void 0 ? n : 0;
      return t += s, e += l, e !== 0 && (o.avp = t / e), o;
    });
  }
}, rn = {
  name: "AO",
  shortName: "AO",
  calcParams: [5, 34],
  figures: [{
    key: "ao",
    title: "AO: ",
    type: "bar",
    baseValue: 0,
    styles: function(r) {
      var t, e, i = r.data, a = r.indicator, n = r.defaultStyles, o = i.prev, s = i.current, l = (t = o?.ao) !== null && t !== void 0 ? t : Number.MIN_SAFE_INTEGER, u = (e = s?.ao) !== null && e !== void 0 ? e : Number.MIN_SAFE_INTEGER, c = "";
      u > l ? c = vt(a.styles, "bars[0].upColor", n.bars[0].upColor) : c = vt(a.styles, "bars[0].downColor", n.bars[0].downColor);
      var d = u > l ? "stroke" : "fill";
      return { color: c, style: d, borderColor: c };
    }
  }],
  calc: function(r, t) {
    var e = t.calcParams, i = Math.max(e[0], e[1]), a = 0, n = 0, o = 0, s = 0;
    return r.map(function(l, u) {
      var c = {}, d = (l.low + l.high) / 2;
      if (a += d, n += d, u >= e[0] - 1) {
        o = a / e[0];
        var h = r[u - (e[0] - 1)];
        a -= (h.low + h.high) / 2;
      }
      if (u >= e[1] - 1) {
        s = n / e[1];
        var h = r[u - (e[1] - 1)];
        n -= (h.low + h.high) / 2;
      }
      return u >= i - 1 && (c.ao = o - s), c;
    });
  }
}, an = {
  name: "BIAS",
  shortName: "BIAS",
  calcParams: [6, 12, 24],
  figures: [
    { key: "bias1", title: "BIAS6: ", type: "line" },
    { key: "bias2", title: "BIAS12: ", type: "line" },
    { key: "bias3", title: "BIAS24: ", type: "line" }
  ],
  regenerateFigures: function(r) {
    return r.map(function(t, e) {
      return { key: "bias".concat(e + 1), title: "BIAS".concat(t, ": "), type: "line" };
    });
  },
  calc: function(r, t) {
    var e = t.calcParams, i = t.figures, a = [];
    return r.map(function(n, o) {
      var s = {}, l = n.close;
      return e.forEach(function(u, c) {
        var d;
        if (a[c] = ((d = a[c]) !== null && d !== void 0 ? d : 0) + l, o >= u - 1) {
          var h = a[c] / e[c];
          s[i[c].key] = (l - h) / h * 100, a[c] -= r[o - (u - 1)].close;
        }
      }), s;
    });
  }
};
function nn(r, t) {
  var e = r.length, i = 0;
  return r.forEach(function(a) {
    var n = a.close - t;
    i += n * n;
  }), i = Math.abs(i), Math.sqrt(i / e);
}
var on = {
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
  calc: function(r, t) {
    var e = t.calcParams, i = e[0] - 1, a = 0;
    return r.map(function(n, o) {
      var s = n.close, l = {};
      if (a += s, o >= i) {
        l.mid = a / e[0];
        var u = nn(r.slice(o - i, o + 1), l.mid);
        l.up = l.mid + e[1] * u, l.dn = l.mid - e[1] * u, a -= r[o - i].close;
      }
      return l;
    });
  }
}, sn = {
  name: "BRAR",
  shortName: "BRAR",
  calcParams: [26],
  figures: [
    { key: "br", title: "BR: ", type: "line" },
    { key: "ar", title: "AR: ", type: "line" }
  ],
  calc: function(r, t) {
    var e = t.calcParams, i = 0, a = 0, n = 0, o = 0;
    return r.map(function(s, l) {
      var u, c, d = {}, h = s.high, f = s.low, v = s.open, p = ((u = r[l - 1]) !== null && u !== void 0 ? u : s).close;
      if (n += h - v, o += v - f, i += h - p, a += p - f, l >= e[0] - 1) {
        o !== 0 ? d.ar = n / o * 100 : d.ar = 0, a !== 0 ? d.br = i / a * 100 : d.br = 0;
        var g = r[l - (e[0] - 1)], m = g.high, x = g.low, y = g.open, E = ((c = r[l - e[0]]) !== null && c !== void 0 ? c : r[l - (e[0] - 1)]).close;
        i -= m - E, a -= E - x, n -= m - y, o -= y - x;
      }
      return d;
    });
  }
}, ln = {
  name: "BBI",
  shortName: "BBI",
  series: "price",
  precision: 2,
  calcParams: [3, 6, 12, 24],
  shouldOhlc: !0,
  figures: [
    { key: "bbi", title: "BBI: ", type: "line" }
  ],
  calc: function(r, t) {
    var e = t.calcParams, i = Math.max.apply(Math, Pe([], Re(e), !1)), a = [], n = [];
    return r.map(function(o, s) {
      var l = {}, u = o.close;
      if (e.forEach(function(d, h) {
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
}, un = {
  name: "CCI",
  shortName: "CCI",
  calcParams: [20],
  figures: [
    { key: "cci", title: "CCI: ", type: "line" }
  ],
  calc: function(r, t) {
    var e = t.calcParams, i = e[0] - 1, a = 0, n = [];
    return r.map(function(o, s) {
      var l = {}, u = (o.high + o.low + o.close) / 3;
      if (a += u, n.push(u), s >= i) {
        var c = a / e[0], d = n.slice(s - i, s + 1), h = 0;
        d.forEach(function(p) {
          h += Math.abs(p - c);
        });
        var f = h / e[0];
        l.cci = f !== 0 ? (u - c) / f / 0.015 : 0;
        var v = (r[s - i].high + r[s - i].low + r[s - i].close) / 3;
        a -= v;
      }
      return l;
    });
  }
}, cn = {
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
  calc: function(r, t) {
    var e = t.calcParams, i = Math.ceil(e[1] / 2.5 + 1), a = Math.ceil(e[2] / 2.5 + 1), n = Math.ceil(e[3] / 2.5 + 1), o = Math.ceil(e[4] / 2.5 + 1), s = 0, l = [], u = 0, c = [], d = 0, h = [], f = 0, v = [], p = [];
    return r.forEach(function(g, m) {
      var x, y, E, _, I, w = {}, b = (x = r[m - 1]) !== null && x !== void 0 ? x : g, S = (b.high + b.close + b.low + b.open) / 4, T = Math.max(0, g.high - S), A = Math.max(0, S - g.low);
      m >= e[0] - 1 && (A !== 0 ? w.cr = T / A * 100 : w.cr = 0, s += w.cr, u += w.cr, d += w.cr, f += w.cr, m >= e[0] + e[1] - 2 && (l.push(s / e[1]), m >= e[0] + e[1] + i - 3 && (w.ma1 = l[l.length - 1 - i]), s -= (y = p[m - (e[1] - 1)].cr) !== null && y !== void 0 ? y : 0), m >= e[0] + e[2] - 2 && (c.push(u / e[2]), m >= e[0] + e[2] + a - 3 && (w.ma2 = c[c.length - 1 - a]), u -= (E = p[m - (e[2] - 1)].cr) !== null && E !== void 0 ? E : 0), m >= e[0] + e[3] - 2 && (h.push(d / e[3]), m >= e[0] + e[3] + n - 3 && (w.ma3 = h[h.length - 1 - n]), d -= (_ = p[m - (e[3] - 1)].cr) !== null && _ !== void 0 ? _ : 0), m >= e[0] + e[4] - 2 && (v.push(f / e[4]), m >= e[0] + e[4] + o - 3 && (w.ma4 = v[v.length - 1 - o]), f -= (I = p[m - (e[4] - 1)].cr) !== null && I !== void 0 ? I : 0)), p.push(w);
    }), p;
  }
}, dn = {
  name: "DMA",
  shortName: "DMA",
  calcParams: [10, 50, 10],
  figures: [
    { key: "dma", title: "DMA: ", type: "line" },
    { key: "ama", title: "AMA: ", type: "line" }
  ],
  calc: function(r, t) {
    var e = t.calcParams, i = Math.max(e[0], e[1]), a = 0, n = 0, o = 0, s = [];
    return r.forEach(function(l, u) {
      var c, d = {}, h = l.close;
      a += h, n += h;
      var f = 0, v = 0;
      if (u >= e[0] - 1 && (f = a / e[0], a -= r[u - (e[0] - 1)].close), u >= e[1] - 1 && (v = n / e[1], n -= r[u - (e[1] - 1)].close), u >= i - 1) {
        var p = f - v;
        d.dma = p, o += p, u >= i + e[2] - 2 && (d.ama = o / e[2], o -= (c = s[u - (e[2] - 1)].dma) !== null && c !== void 0 ? c : 0);
      }
      s.push(d);
    }), s;
  }
}, hn = {
  name: "DMI",
  shortName: "DMI",
  calcParams: [14, 6],
  figures: [
    { key: "pdi", title: "PDI: ", type: "line" },
    { key: "mdi", title: "MDI: ", type: "line" },
    { key: "adx", title: "ADX: ", type: "line" },
    { key: "adxr", title: "ADXR: ", type: "line" }
  ],
  calc: function(r, t) {
    var e = t.calcParams, i = 0, a = 0, n = 0, o = 0, s = 0, l = 0, u = 0, c = 0, d = [];
    return r.forEach(function(h, f) {
      var v, p, g = {}, m = (v = r[f - 1]) !== null && v !== void 0 ? v : h, x = m.close, y = h.high, E = h.low, _ = y - E, I = Math.abs(y - x), w = Math.abs(x - E), b = y - m.high, S = m.low - E, T = Math.max(Math.max(_, I), w), A = b > 0 && b > S ? b : 0, k = S > 0 && S > b ? S : 0;
      if (i += T, a += A, n += k, f >= e[0] - 1) {
        f > e[0] - 1 ? (o = o - o / e[0] + T, s = s - s / e[0] + A, l = l - l / e[0] + k) : (o = i, s = a, l = n);
        var F = 0, P = 0;
        o !== 0 && (F = s * 100 / o, P = l * 100 / o), g.pdi = F, g.mdi = P;
        var D = 0;
        P + F !== 0 && (D = Math.abs(P - F) / (P + F) * 100), u += D, f >= e[0] * 2 - 2 && (f > e[0] * 2 - 2 ? c = (c * (e[0] - 1) + D) / e[0] : c = u / e[0], g.adx = c, f >= e[0] * 2 + e[1] - 3 && (g.adxr = (((p = d[f - (e[1] - 1)].adx) !== null && p !== void 0 ? p : 0) + c) / 2));
      }
      d.push(g);
    }), d;
  }
}, vn = {
  name: "EMV",
  shortName: "EMV",
  calcParams: [14, 9],
  figures: [
    { key: "emv", title: "EMV: ", type: "line" },
    { key: "maEmv", title: "MAEMV: ", type: "line" }
  ],
  calc: function(r, t) {
    var e = t.calcParams, i = 0, a = [];
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
        i += l.emv, a.push(l.emv), o >= e[0] && (l.maEmv = i / e[0], i -= a[o - e[0]]);
      }
      return l;
    });
  }
}, fn = {
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
    return r.map(function(t, e) {
      return { key: "ema".concat(e + 1), title: "EMA".concat(t, ": "), type: "line" };
    });
  },
  calc: function(r, t) {
    var e = t.calcParams, i = t.figures, a = 0, n = [];
    return r.map(function(o, s) {
      var l = {}, u = o.close;
      return a += u, e.forEach(function(c, d) {
        s >= c - 1 && (s > c - 1 ? n[d] = (2 * u + (c - 1) * n[d]) / (c + 1) : n[d] = a / c, l[i[d].key] = n[d]);
      }), l;
    });
  }
}, pn = {
  name: "MTM",
  shortName: "MTM",
  calcParams: [12, 6],
  figures: [
    { key: "mtm", title: "MTM: ", type: "line" },
    { key: "maMtm", title: "MAMTM: ", type: "line" }
  ],
  calc: function(r, t) {
    var e = t.calcParams, i = 0, a = [];
    return r.forEach(function(n, o) {
      var s, l = {};
      if (o >= e[0]) {
        var u = n.close, c = r[o - e[0]].close;
        l.mtm = u - c, i += l.mtm, o >= e[0] + e[1] - 1 && (l.maMtm = i / e[1], i -= (s = a[o - (e[1] - 1)].mtm) !== null && s !== void 0 ? s : 0);
      }
      a.push(l);
    }), a;
  }
}, gn = {
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
    return r.map(function(t, e) {
      return { key: "ma".concat(e + 1), title: "MA".concat(t, ": "), type: "line" };
    });
  },
  calc: function(r, t) {
    var e = t.calcParams, i = t.figures, a = [];
    return r.map(function(n, o) {
      var s = {}, l = n.close;
      return e.forEach(function(u, c) {
        var d;
        a[c] = ((d = a[c]) !== null && d !== void 0 ? d : 0) + l, o >= u - 1 && (s[i[c].key] = a[c] / u, a[c] -= r[o - (u - 1)].close);
      }), s;
    });
  }
}, mn = {
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
        var t, e, i = r.data, a = r.indicator, n = r.defaultStyles, o = i.prev, s = i.current, l = (t = o?.macd) !== null && t !== void 0 ? t : Number.MIN_SAFE_INTEGER, u = (e = s?.macd) !== null && e !== void 0 ? e : Number.MIN_SAFE_INTEGER, c = "";
        u > 0 ? c = vt(a.styles, "bars[0].upColor", n.bars[0].upColor) : u < 0 ? c = vt(a.styles, "bars[0].downColor", n.bars[0].downColor) : c = vt(a.styles, "bars[0].noChangeColor", n.bars[0].noChangeColor);
        var d = l < u ? "stroke" : "fill";
        return { style: d, color: c, borderColor: c };
      }
    }
  ],
  calc: function(r, t) {
    var e = t.calcParams, i = 0, a = 0, n = 0, o = 0, s = 0, l = 0, u = Math.max(e[0], e[1]);
    return r.map(function(c, d) {
      var h = {}, f = c.close;
      return i += f, d >= e[0] - 1 && (d > e[0] - 1 ? a = (2 * f + (e[0] - 1) * a) / (e[0] + 1) : a = i / e[0]), d >= e[1] - 1 && (d > e[1] - 1 ? n = (2 * f + (e[1] - 1) * n) / (e[1] + 1) : n = i / e[1]), d >= u - 1 && (o = a - n, h.dif = o, s += o, d >= u + e[2] - 2 && (d > u + e[2] - 2 ? l = (o * 2 + l * (e[2] - 1)) / (e[2] + 1) : l = s / e[2], h.macd = (o - l) * 2, h.dea = l)), h;
    });
  }
}, _n = {
  name: "OBV",
  shortName: "OBV",
  calcParams: [30],
  figures: [
    { key: "obv", title: "OBV: ", type: "line" },
    { key: "maObv", title: "MAOBV: ", type: "line" }
  ],
  calc: function(r, t) {
    var e = t.calcParams, i = 0, a = 0, n = [];
    return r.forEach(function(o, s) {
      var l, u, c, d, h = (l = r[s - 1]) !== null && l !== void 0 ? l : o;
      o.close < h.close ? a -= (u = o.volume) !== null && u !== void 0 ? u : 0 : o.close > h.close && (a += (c = o.volume) !== null && c !== void 0 ? c : 0);
      var f = { obv: a };
      i += a, s >= e[0] - 1 && (f.maObv = i / e[0], i -= (d = n[s - (e[0] - 1)].obv) !== null && d !== void 0 ? d : 0), n.push(f);
    }), n;
  }
}, yn = {
  name: "PVT",
  shortName: "PVT",
  figures: [
    { key: "pvt", title: "PVT: ", type: "line" }
  ],
  calc: function(r) {
    var t = 0;
    return r.map(function(e, i) {
      var a, n, o = {}, s = e.close, l = (a = e.volume) !== null && a !== void 0 ? a : 1, u = ((n = r[i - 1]) !== null && n !== void 0 ? n : e).close, c = 0, d = u * l;
      return d !== 0 && (c = (s - u) / d), t += c, o.pvt = t, o;
    });
  }
}, xn = {
  name: "PSY",
  shortName: "PSY",
  calcParams: [12, 6],
  figures: [
    { key: "psy", title: "PSY: ", type: "line" },
    { key: "maPsy", title: "MAPSY: ", type: "line" }
  ],
  calc: function(r, t) {
    var e = t.calcParams, i = 0, a = 0, n = [], o = [];
    return r.forEach(function(s, l) {
      var u, c, d = {}, h = ((u = r[l - 1]) !== null && u !== void 0 ? u : s).close, f = s.close - h > 0 ? 1 : 0;
      n.push(f), i += f, l >= e[0] - 1 && (d.psy = i / e[0] * 100, a += d.psy, l >= e[0] + e[1] - 2 && (d.maPsy = a / e[1], a -= (c = o[l - (e[1] - 1)].psy) !== null && c !== void 0 ? c : 0), i -= n[l - (e[0] - 1)]), o.push(d);
    }), o;
  }
}, wn = {
  name: "ROC",
  shortName: "ROC",
  calcParams: [12, 6],
  figures: [
    { key: "roc", title: "ROC: ", type: "line" },
    { key: "maRoc", title: "MAROC: ", type: "line" }
  ],
  calc: function(r, t) {
    var e = t.calcParams, i = [], a = 0;
    return r.forEach(function(n, o) {
      var s, l, u = {};
      if (o >= e[0] - 1) {
        var c = n.close, d = ((s = r[o - e[0]]) !== null && s !== void 0 ? s : r[o - (e[0] - 1)]).close;
        d !== 0 ? u.roc = (c - d) / d * 100 : u.roc = 0, a += u.roc, o >= e[0] - 1 + e[1] - 1 && (u.maRoc = a / e[1], a -= (l = i[o - (e[1] - 1)].roc) !== null && l !== void 0 ? l : 0);
      }
      i.push(u);
    }), i;
  }
}, bn = {
  name: "RSI",
  shortName: "RSI",
  calcParams: [6, 12, 24],
  figures: [
    { key: "rsi1", title: "RSI1: ", type: "line" },
    { key: "rsi2", title: "RSI2: ", type: "line" },
    { key: "rsi3", title: "RSI3: ", type: "line" }
  ],
  regenerateFigures: function(r) {
    return r.map(function(t, e) {
      var i = e + 1;
      return { key: "rsi".concat(i), title: "RSI".concat(i, ": "), type: "line" };
    });
  },
  calc: function(r, t) {
    var e = t.calcParams, i = t.figures, a = [], n = [], o = [], s = [];
    return r.map(function(l, u) {
      var c = {}, d = u === 0 ? 0 : l.close - r[u - 1].close, h = Math.max(d, 0), f = Math.max(-d, 0);
      return e.forEach(function(v, p) {
        var g, m;
        a[p] = ((g = a[p]) !== null && g !== void 0 ? g : 0) + h, n[p] = ((m = n[p]) !== null && m !== void 0 ? m : 0) + f, !(u < v) && (o[p] === void 0 || s[p] === void 0 ? (o[p] = a[p] / v, s[p] = n[p] / v) : (o[p] = (o[p] * (v - 1) + h) / v, s[p] = (s[p] * (v - 1) + f) / v), s[p] === 0 ? c[i[p].key] = 100 : o[p] === 0 ? c[i[p].key] = 0 : c[i[p].key] = 100 - 100 / (1 + o[p] / s[p]));
      }), c;
    });
  }
}, Cn = {
  name: "SMA",
  shortName: "SMA",
  series: "price",
  calcParams: [12, 2],
  precision: 2,
  figures: [
    { key: "sma", title: "SMA: ", type: "line" }
  ],
  shouldOhlc: !0,
  calc: function(r, t) {
    var e = t.calcParams, i = 0, a = 0;
    return r.map(function(n, o) {
      var s = {}, l = n.close;
      return i += l, o >= e[0] - 1 && (o > e[0] - 1 ? a = (l * e[1] + a * (e[0] - e[1] + 1)) / (e[0] + 1) : a = i / e[0], s.sma = a), s;
    });
  }
}, En = {
  name: "KDJ",
  shortName: "KDJ",
  calcParams: [9, 3, 3],
  figures: [
    { key: "k", title: "K: ", type: "line" },
    { key: "d", title: "D: ", type: "line" },
    { key: "j", title: "J: ", type: "line" }
  ],
  calc: function(r, t) {
    var e = t.calcParams, i = [];
    return r.forEach(function(a, n) {
      var o, s, l, u, c = {}, d = a.close;
      if (n >= e[0] - 1) {
        var h = Ai(r.slice(n - (e[0] - 1), n + 1), "high", "low"), f = h[0], v = h[1], p = f - v, g = (d - v) / (p === 0 ? 1 : p) * 100;
        c.k = ((e[1] - 1) * ((s = (o = i[n - 1]) === null || o === void 0 ? void 0 : o.k) !== null && s !== void 0 ? s : 50) + g) / e[1], c.d = ((e[2] - 1) * ((u = (l = i[n - 1]) === null || l === void 0 ? void 0 : l.d) !== null && u !== void 0 ? u : 50) + c.k) / e[2], c.j = 3 * c.k - 2 * c.d;
      }
      i.push(c);
    }), i;
  }
}, In = {
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
        var t, e, i, a = r.data, n = r.indicator, o = r.defaultStyles, s = a.current, l = (t = s?.sar) !== null && t !== void 0 ? t : Number.MIN_SAFE_INTEGER, u = (((e = s?.high) !== null && e !== void 0 ? e : 0) + ((i = s?.low) !== null && i !== void 0 ? i : 0)) / 2, c = l < u ? vt(n.styles, "circles[0].upColor", o.circles[0].upColor) : vt(n.styles, "circles[0].downColor", o.circles[0].downColor);
        return { color: c };
      }
    }
  ],
  calc: function(r, t) {
    var e = t.calcParams, i = e[0] / 100, a = e[1] / 100, n = e[2] / 100, o = i, s = -100, l = !1, u = 0;
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
}, Sn = {
  name: "TRIX",
  shortName: "TRIX",
  calcParams: [12, 9],
  figures: [
    { key: "trix", title: "TRIX: ", type: "line" },
    { key: "maTrix", title: "MATRIX: ", type: "line" }
  ],
  calc: function(r, t) {
    var e = t.calcParams, i = 0, a = 0, n = 0, o = 0, s = 0, l = 0, u = 0, c = [];
    return r.forEach(function(d, h) {
      var f, v = {}, p = d.close;
      if (i += p, h >= e[0] - 1 && (h > e[0] - 1 ? a = (2 * p + (e[0] - 1) * a) / (e[0] + 1) : a = i / e[0], s += a, h >= e[0] * 2 - 2 && (h > e[0] * 2 - 2 ? n = (2 * a + (e[0] - 1) * n) / (e[0] + 1) : n = s / e[0], l += n, h >= e[0] * 3 - 3))) {
        var g = 0, m = 0;
        h > e[0] * 3 - 3 ? (g = (2 * n + (e[0] - 1) * o) / (e[0] + 1), m = (g - o) / o * 100) : g = l / e[0], o = g, v.trix = m, u += m, h >= e[0] * 3 + e[1] - 4 && (v.maTrix = u / e[1], u -= (f = c[h - (e[1] - 1)].trix) !== null && f !== void 0 ? f : 0);
      }
      c.push(v);
    }), c;
  }
};
function Qr() {
  return {
    key: "volume",
    title: "VOLUME: ",
    type: "bar",
    baseValue: 0,
    styles: function(r) {
      var t = r.data, e = r.indicator, i = r.defaultStyles, a = t.current, n = vt(e.styles, "bars[0].noChangeColor", i.bars[0].noChangeColor);
      return C(a) && (a.close > a.open ? n = vt(e.styles, "bars[0].upColor", i.bars[0].upColor) : a.close < a.open && (n = vt(e.styles, "bars[0].downColor", i.bars[0].downColor))), { color: n };
    }
  };
}
var Tn = {
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
    Qr()
  ],
  regenerateFigures: function(r) {
    var t = r.map(function(e, i) {
      return { key: "ma".concat(i + 1), title: "MA".concat(e, ": "), type: "line" };
    });
    return t.push(Qr()), t;
  },
  calc: function(r, t) {
    var e = t.calcParams, i = t.figures, a = [];
    return r.map(function(n, o) {
      var s, l = (s = n.volume) !== null && s !== void 0 ? s : 0, u = { volume: l, open: n.open, close: n.close };
      return e.forEach(function(c, d) {
        var h, f;
        a[d] = ((h = a[d]) !== null && h !== void 0 ? h : 0) + l, o >= c - 1 && (u[i[d].key] = a[d] / c, a[d] -= (f = r[o - (c - 1)].volume) !== null && f !== void 0 ? f : 0);
      }), u;
    });
  }
}, An = {
  name: "VR",
  shortName: "VR",
  calcParams: [26, 6],
  figures: [
    { key: "vr", title: "VR: ", type: "line" },
    { key: "maVr", title: "MAVR: ", type: "line" }
  ],
  calc: function(r, t) {
    var e = t.calcParams, i = 0, a = 0, n = 0, o = 0, s = [];
    return r.forEach(function(l, u) {
      var c, d, h, f, v, p = {}, g = l.close, m = ((c = r[u - 1]) !== null && c !== void 0 ? c : l).close, x = (d = l.volume) !== null && d !== void 0 ? d : 0;
      if (g > m ? i += x : g < m ? a += x : n += x, u >= e[0] - 1) {
        var y = n / 2;
        a + y === 0 ? p.vr = 0 : p.vr = (i + y) / (a + y) * 100, o += p.vr, u >= e[0] + e[1] - 2 && (p.maVr = o / e[1], o -= (h = s[u - (e[1] - 1)].vr) !== null && h !== void 0 ? h : 0);
        var E = r[u - (e[0] - 1)], _ = (f = r[u - e[0]]) !== null && f !== void 0 ? f : E, I = E.close, w = (v = E.volume) !== null && v !== void 0 ? v : 0;
        I > _.close ? i -= w : I < _.close ? a -= w : n -= w;
      }
      s.push(p);
    }), s;
  }
}, Mn = {
  name: "WR",
  shortName: "WR",
  calcParams: [6, 10, 14],
  figures: [
    { key: "wr1", title: "WR1: ", type: "line" },
    { key: "wr2", title: "WR2: ", type: "line" },
    { key: "wr3", title: "WR3: ", type: "line" }
  ],
  regenerateFigures: function(r) {
    return r.map(function(t, e) {
      return { key: "wr".concat(e + 1), title: "WR".concat(e + 1, ": "), type: "line" };
    });
  },
  calc: function(r, t) {
    var e = t.calcParams, i = t.figures;
    return r.map(function(a, n) {
      var o = {}, s = a.close;
      return e.forEach(function(l, u) {
        var c = l - 1;
        if (n >= c) {
          var d = Ai(r.slice(n - c, n + 1), "high", "low"), h = d[0], f = d[1], v = h - f;
          o[i[u].key] = v === 0 ? 0 : (s - h) / v * 100;
        }
      }), o;
    });
  }
}, Mr = {}, Pn = [
  en,
  rn,
  an,
  on,
  sn,
  ln,
  un,
  cn,
  dn,
  hn,
  vn,
  fn,
  pn,
  gn,
  mn,
  _n,
  yn,
  xn,
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
  Mr[r.name] = Mi.extend(r);
});
function kn(r) {
  Mr[r.name] = Mi.extend(r);
}
function Pi(r) {
  var t;
  return (t = Mr[r]) !== null && t !== void 0 ? t : null;
}
function Ot(r, t) {
  var e, i = (e = t?.ignoreEvent) !== null && e !== void 0 ? e : !1;
  return Me(i) ? !i : !i.includes(r);
}
var ti = 1, Ge = -1, Dn = "overlay_", Ce = "overlay_figure_", Rn = (
  /** @class */
  (function() {
    function r(t) {
      this.groupId = "", this.totalStep = 1, this.currentStep = ti, this.drawingMode = "step", this.lock = !1, this.visible = !0, this.zLevel = 0, this.needDefaultPointFigure = !1, this.needDefaultXAxisFigure = !1, this.needDefaultYAxisFigure = !1, this.mode = "normal", this.modeSensitivity = 8, this.points = [], this.styles = null, this.createPointFigures = null, this.createXAxisFigures = null, this.createYAxisFigures = null, this.performEventPressedMove = null, this.performEventMoveForDrawing = null, this.onDrawStart = null, this.onDrawing = null, this.onDrawEnd = null, this.onClick = null, this.onDoubleClick = null, this.onRightClick = null, this.onPressedMoveStart = null, this.onPressedMoving = null, this.onPressedMoveEnd = null, this.onMouseMove = null, this.onMouseEnter = null, this.onMouseLeave = null, this.onRemoved = null, this.onSelected = null, this.onDeselected = null, this._prevZLevel = 0, this._prevPressedPoint = null, this._prevPressedPoints = [], this.override(t);
    }
    return r.prototype.override = function(t) {
      var e, i;
      this._prevOverlay = $e(M(M({}, this), { _prevOverlay: null }));
      var a = t.id, n = t.name;
      t.currentStep;
      var o = t.points, s = t.styles, l = ze(t, ["id", "name", "currentStep", "points", "styles"]);
      if (ht(this, l), K(this.name) || (this.name = n ?? ""), !K(this.id) && K(a) && (this.id = a), C(s) && ((e = this.styles) !== null && e !== void 0 || (this.styles = {}), ht(this.styles, s)), $t(o) && o.length > 0) {
        this.points = Pe([], Re(o), !1), this.currentStep = Ge;
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
    }, r.prototype.setPrevZLevel = function(t) {
      this._prevZLevel = t;
    }, r.prototype.shouldUpdate = function() {
      var t = this._prevOverlay.zLevel !== this.zLevel, e = t || JSON.stringify(this._prevOverlay.points) !== JSON.stringify(this.points) || this._prevOverlay.visible !== this.visible || this._prevOverlay.extendData !== this.extendData || this._prevOverlay.styles !== this.styles;
      return { sort: t, draw: e };
    }, r.prototype.nextStep = function() {
      this.currentStep === this.totalStep - 1 ? this.currentStep = Ge : this.currentStep++;
    }, r.prototype.forceComplete = function() {
      this.currentStep = Ge;
    }, r.prototype.isDrawing = function() {
      return this.currentStep !== Ge;
    }, r.prototype.isStart = function() {
      return this.currentStep === ti;
    }, r.prototype.isContinuousDrawingMode = function() {
      return this.drawingMode === "continuous";
    }, r.prototype.startContinuousDrawing = function(t) {
      this.points = [], this.continuousDrawingModeEventMoveForDrawing(t), this.currentStep = 2;
    }, r.prototype.continuousDrawingModeEventMoveForDrawing = function(t) {
      var e = {};
      return O(t.timestamp) && (e.timestamp = t.timestamp), O(t.dataIndex) && (e.dataIndex = t.dataIndex), O(t.value) && (e.value = t.value), this.points.push(e), !0;
    }, r.prototype.stepDrawingModeEventMoveForDrawing = function(t) {
      var e, i = this.currentStep - 1, a = {};
      O(t.timestamp) && (a.timestamp = t.timestamp), O(t.dataIndex) && (a.dataIndex = t.dataIndex), O(t.value) && (a.value = t.value), this.points[i] = a, (e = this.performEventMoveForDrawing) === null || e === void 0 || e.call(this, {
        currentStep: this.currentStep,
        mode: this.mode,
        points: this.points,
        performPointIndex: i,
        performPoint: a
      });
    }, r.prototype.eventPressedPointMove = function(t, e) {
      var i;
      this.points[e].timestamp = t.timestamp, O(t.dataIndex) && (this.points[e].dataIndex = t.dataIndex), O(t.value) && (this.points[e].value = t.value), (i = this.performEventPressedMove) === null || i === void 0 || i.call(this, {
        currentStep: this.currentStep,
        points: this.points,
        mode: this.mode,
        performPointIndex: e,
        performPoint: this.points[e]
      });
    }, r.prototype.startPressedMove = function(t) {
      this._prevPressedPoint = M({}, t), this._prevPressedPoints = $e(this.points);
    }, r.prototype.eventPressedOtherMove = function(t, e) {
      var i = this;
      if (this._prevPressedPoint !== null) {
        var a = null;
        O(t.dataIndex) && O(this._prevPressedPoint.dataIndex) && (a = t.dataIndex - this._prevPressedPoint.dataIndex);
        var n = null;
        O(t.value) && O(this._prevPressedPoint.value) && (n = t.value - this._prevPressedPoint.value), this.points = this._prevPressedPoints.map(function(o) {
          var s, l, u = M({}, o);
          if (O(a) && (O(o.dataIndex) || O(o.timestamp))) {
            var c = O(o.timestamp) ? i.isContinuousDrawingMode() ? e.timestampToFloatIndex(o.timestamp) : e.timestampToDataIndex(o.timestamp) : o.dataIndex;
            u.dataIndex = c + a, u.timestamp = i.isContinuousDrawingMode() ? (s = e.floatIndexToTimestamp(u.dataIndex)) !== null && s !== void 0 ? s : void 0 : (l = e.dataIndexToTimestamp(u.dataIndex)) !== null && l !== void 0 ? l : void 0;
          }
          return O(n) && O(o.value) && (u.value = o.value + n), u;
        });
      }
    }, r.extend = function(t) {
      var e = (
        /** @class */
        (function(i) {
          X(a, i);
          function a() {
            return i.call(this, t) || this;
          }
          return a;
        })(r)
      );
      return e;
    }, r;
  })()
), Fn = {
  name: "fibonacciLine",
  totalStep: 3,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(r) {
    var t, e, i, a = r.chart, n = r.coordinates, o = r.bounding, s = r.overlay, l = r.yAxis, u = s.points;
    if (n.length > 0) {
      var c = 0;
      if (!((t = l?.isInCandle()) !== null && t !== void 0) || t)
        c = (i = (e = a.getSymbol()) === null || e === void 0 ? void 0 : e.pricePrecision) !== null && i !== void 0 ? i : gt.PRICE;
      else {
        var d = a.getIndicators({ paneId: s.paneId });
        d.forEach(function(y) {
          c = Math.max(c, y.precision);
        });
      }
      var h = [], f = [], v = 0, p = o.width;
      if (n.length > 1 && O(u[0].value) && O(u[1].value)) {
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
}, Bn = {
  name: "horizontalRayLine",
  totalStep: 3,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(r) {
    var t = r.coordinates, e = r.bounding, i = { x: 0, y: t[0].y };
    return C(t[1]) && t[0].x < t[1].x && (i.x = e.width), [
      {
        type: "line",
        attrs: { coordinates: [t[0], i] }
      }
    ];
  },
  performEventPressedMove: function(r) {
    var t = r.points, e = r.performPoint;
    t[0].value = e.value, t[1].value = e.value;
  },
  performEventMoveForDrawing: function(r) {
    var t = r.currentStep, e = r.points, i = r.performPoint;
    t === 2 && (e[0].value = i.value);
  }
}, On = {
  name: "horizontalSegment",
  totalStep: 3,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(r) {
    var t = r.coordinates, e = [];
    return t.length === 2 && e.push({ coordinates: t }), [
      {
        type: "line",
        attrs: e
      }
    ];
  },
  performEventPressedMove: function(r) {
    var t = r.points, e = r.performPoint;
    t[0].value = e.value, t[1].value = e.value;
  },
  performEventMoveForDrawing: function(r) {
    var t = r.currentStep, e = r.points, i = r.performPoint;
    t === 2 && (e[0].value = i.value);
  }
}, Ln = {
  name: "horizontalStraightLine",
  totalStep: 2,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(r) {
    var t = r.coordinates, e = r.bounding;
    return [{
      type: "line",
      attrs: {
        coordinates: [
          {
            x: 0,
            y: t[0].y
          },
          {
            x: e.width,
            y: t[0].y
          }
        ]
      }
    }];
  }
}, Pr = (
  /** @class */
  (function() {
    function r() {
      this._children = [], this._callbacks = /* @__PURE__ */ new Map();
    }
    return r.prototype.registerEvent = function(t, e) {
      return this._callbacks.set(t, e), this;
    }, r.prototype.onEvent = function(t, e) {
      var i = this._callbacks.get(t);
      return C(i) && this.checkEventOn(e) ? i(e) : !1;
    }, r.prototype.dispatchEventToChildren = function(t, e) {
      var i = this._children.length - 1;
      if (i > -1) {
        for (var a = i; a > -1; a--)
          if (this._children[a].dispatchEvent(t, e))
            return !0;
      }
      return !1;
    }, r.prototype.dispatchEvent = function(t, e) {
      return this.dispatchEventToChildren(t, e) ? !0 : this.onEvent(t, e);
    }, r.prototype.addChild = function(t) {
      return this._children.push(t), this;
    }, r.prototype.clear = function() {
      this._children = [];
    }, r;
  })()
), ft = 2, Vn = (
  /** @class */
  (function(r) {
    X(t, r);
    function t(e) {
      var i = r.call(this) || this;
      return i.attrs = e.attrs, i.styles = e.styles, i;
    }
    return t.prototype.checkEventOn = function(e) {
      return this.checkEventOnImp(e, this.attrs, this.styles);
    }, t.prototype.setAttrs = function(e) {
      return this.attrs = e, this;
    }, t.prototype.setStyles = function(e) {
      return this.styles = e, this;
    }, t.prototype.draw = function(e) {
      this.drawImp(e, this.attrs, this.styles);
    }, t.extend = function(e) {
      var i = (
        /** @class */
        (function(a) {
          X(n, a);
          function n() {
            return a !== null && a.apply(this, arguments) || this;
          }
          return n.prototype.checkEventOnImp = function(o, s, l) {
            return e.checkEventOn(o, s, l);
          }, n.prototype.drawImp = function(o, s, l) {
            e.draw(o, s, l);
          }, n;
        })(t)
      );
      return i;
    }, t;
  })(Pr)
);
function Nn(r, t) {
  var e, i, a = [];
  a = a.concat(t);
  try {
    for (var n = Ct(a), o = n.next(); !o.done; o = n.next()) {
      var s = o.value, l = s.coordinates;
      if (l.length > 1)
        for (var u = 1; u < l.length; u++) {
          var c = l[u - 1], d = l[u];
          if (c.x === d.x) {
            if (Math.abs(c.y - r.y) + Math.abs(d.y - r.y) - Math.abs(c.y - d.y) < ft + ft && Math.abs(r.x - c.x) < ft)
              return !0;
          } else {
            var h = kr(c, d), f = ki(h, r), v = Math.abs(f - r.y);
            if (Math.abs(c.x - r.x) + Math.abs(d.x - r.x) - Math.abs(c.x - d.x) < ft + ft && v * v / (h[0] * h[0] + 1) < ft * ft)
              return !0;
          }
        }
    }
  } catch (p) {
    e = { error: p };
  } finally {
    try {
      o && !o.done && (i = n.return) && i.call(n);
    } finally {
      if (e) throw e.error;
    }
  }
  return !1;
}
function ki(r, t) {
  return r !== null ? t.x * r[0] + r[1] : t.y;
}
function tr(r, t, e) {
  var i = kr(r, t);
  return ki(i, e);
}
function kr(r, t) {
  var e = r.x - t.x;
  if (e !== 0) {
    var i = (r.y - t.y) / e, a = r.y - i * r.x;
    return [i, a];
  }
  return null;
}
function Di(r, t, e) {
  var i = t.length, a = O(e) ? e > 0 && e < 1 ? e : 0 : e ? 0.5 : 0;
  if (a > 0 && i > 2) {
    for (var n = t[0].x, o = t[0].y, s = 1; s < i - 1; s++) {
      var l = t[s - 1], u = t[s], c = t[s + 1], d = u.x - l.x, h = u.y - l.y, f = c.x - u.x, v = c.y - u.y, p = c.x - l.x, g = c.y - l.y, m = Math.sqrt(d * d + h * h), x = Math.sqrt(f * f + v * v), y = x / (x + m), E = u.x + p * a * y, _ = u.y + g * a * y;
      E = Math.min(E, Math.max(c.x, u.x)), _ = Math.min(_, Math.max(c.y, u.y)), E = Math.max(E, Math.min(c.x, u.x)), _ = Math.max(_, Math.min(c.y, u.y)), p = E - u.x, g = _ - u.y;
      var I = u.x - p * m / x, w = u.y - g * m / x;
      I = Math.min(I, Math.max(l.x, u.x)), w = Math.min(w, Math.max(l.y, u.y)), I = Math.max(I, Math.min(l.x, u.x)), w = Math.max(w, Math.min(l.y, u.y)), p = u.x - I, g = u.y - w, E = u.x + p * x / m, _ = u.y + g * x / m, r.bezierCurveTo(n, o, I, w, u.x, u.y), n = E, o = _;
    }
    var b = t[i - 1];
    r.bezierCurveTo(n, o, b.x, b.y, b.x, b.y);
  } else
    for (var s = 1; s < i; s++)
      r.lineTo(t[s].x, t[s].y);
}
function Yn(r, t, e) {
  var i = [];
  i = i.concat(t);
  var a = e.style, n = a === void 0 ? "solid" : a, o = e.smooth, s = o === void 0 ? !1 : o, l = e.size, u = l === void 0 ? 1 : l, c = e.color, d = c === void 0 ? "currentColor" : c, h = e.dashedValue, f = h === void 0 ? [2, 2] : h, v = e.lineCap, p = e.lineJoin, g = O(s) ? s > 0 : s;
  r.lineWidth = u, r.strokeStyle = d, K(v) ? r.lineCap = v : g ? r.lineCap = "round" : r.lineCap = "butt", K(p) ? r.lineJoin = p : g ? r.lineJoin = "round" : r.lineJoin = "miter", n === "dashed" ? r.setLineDash(f) : r.setLineDash([]);
  var m = u % 2 === 1 ? 0.5 : 0;
  i.forEach(function(x) {
    var y = x.coordinates;
    y.length > 1 && (y.length === 2 && (y[0].x === y[1].x || y[0].y === y[1].y) ? (r.beginPath(), y[0].x === y[1].x ? (r.moveTo(y[0].x + m, y[0].y), r.lineTo(y[1].x + m, y[1].y)) : (r.moveTo(y[0].x, y[0].y + m), r.lineTo(y[1].x, y[1].y + m)), r.stroke(), r.closePath()) : (r.save(), u % 2 === 1 && r.translate(0.5, 0.5), r.beginPath(), r.moveTo(y[0].x, y[0].y), Di(r, y, s), r.stroke(), r.closePath(), r.restore()));
  });
}
var Wn = {
  name: "line",
  checkEventOn: Nn,
  draw: function(r, t, e) {
    Yn(r, t, e);
  }
};
function Ri(r, t, e) {
  var i = e ?? 0, a = [];
  if (r.length > 1)
    if (r[0].x === r[1].x) {
      var n = 0, o = t.height;
      if (a.push({ coordinates: [{ x: r[0].x, y: n }, { x: r[0].x, y: o }] }), r.length > 2) {
        a.push({ coordinates: [{ x: r[2].x, y: n }, { x: r[2].x, y: o }] });
        for (var s = r[0].x - r[2].x, l = 0; l < i; l++) {
          var u = s * (l + 1);
          a.push({ coordinates: [{ x: r[0].x + u, y: n }, { x: r[0].x + u, y: o }] });
        }
      }
    } else {
      var c = 0, d = t.width, h = kr(r[0], r[1]), f = h[0], v = h[1];
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
var $n = {
  name: "parallelStraightLine",
  totalStep: 4,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(r) {
    var t = r.coordinates, e = r.bounding;
    return [
      {
        type: "line",
        attrs: Ri(t, e)
      }
    ];
  }
}, zn = {
  name: "priceChannelLine",
  totalStep: 4,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(r) {
    var t = r.coordinates, e = r.bounding;
    return [
      {
        type: "line",
        attrs: Ri(t, e, 1)
      }
    ];
  }
}, qn = {
  name: "priceLine",
  totalStep: 2,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(r) {
    var t, e, i, a = r.chart, n = r.coordinates, o = r.bounding, s = r.overlay, l = r.yAxis, u = 0;
    if (!((t = l?.isInCandle()) !== null && t !== void 0) || t)
      u = (i = (e = a.getSymbol()) === null || e === void 0 ? void 0 : e.pricePrecision) !== null && i !== void 0 ? i : gt.PRICE;
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
function Xn(r, t) {
  if (r.length > 1) {
    var e = { x: 0, y: 0 };
    return r[0].x === r[1].x && r[0].y !== r[1].y ? r[0].y < r[1].y ? e = {
      x: r[0].x,
      y: t.height
    } : e = {
      x: r[0].x,
      y: 0
    } : r[0].x > r[1].x ? e = {
      x: 0,
      y: tr(r[0], r[1], { x: 0, y: r[0].y })
    } : e = {
      x: t.width,
      y: tr(r[0], r[1], { x: t.width, y: r[0].y })
    }, { coordinates: [r[0], e] };
  }
  return [];
}
var Hn = {
  name: "rayLine",
  totalStep: 3,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(r) {
    var t = r.coordinates, e = r.bounding;
    return [
      {
        type: "line",
        attrs: Xn(t, e)
      }
    ];
  }
}, Un = {
  name: "segment",
  totalStep: 3,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(r) {
    var t = r.coordinates;
    return t.length === 2 ? [
      {
        type: "line",
        attrs: { coordinates: t }
      }
    ] : [];
  }
}, Gn = {
  name: "straightLine",
  totalStep: 3,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(r) {
    var t = r.coordinates, e = r.bounding;
    return t.length === 2 ? t[0].x === t[1].x ? [
      {
        type: "line",
        attrs: {
          coordinates: [
            {
              x: t[0].x,
              y: 0
            },
            {
              x: t[0].x,
              y: e.height
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
              y: tr(t[0], t[1], { x: 0, y: t[0].y })
            },
            {
              x: e.width,
              y: tr(t[0], t[1], { x: e.width, y: t[0].y })
            }
          ]
        }
      }
    ] : [];
  }
}, jn = {
  name: "verticalRayLine",
  totalStep: 3,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(r) {
    var t = r.coordinates, e = r.bounding;
    if (t.length === 2) {
      var i = { x: t[0].x, y: 0 };
      return t[0].y < t[1].y && (i.y = e.height), [
        {
          type: "line",
          attrs: { coordinates: [t[0], i] }
        }
      ];
    }
    return [];
  },
  performEventPressedMove: function(r) {
    var t = r.points, e = r.performPoint;
    t[0].timestamp = e.timestamp, t[0].dataIndex = e.dataIndex, t[1].timestamp = e.timestamp, t[1].dataIndex = e.dataIndex;
  },
  performEventMoveForDrawing: function(r) {
    var t = r.currentStep, e = r.points, i = r.performPoint;
    t === 2 && (e[0].timestamp = i.timestamp, e[0].dataIndex = i.dataIndex);
  }
}, Zn = {
  name: "verticalSegment",
  totalStep: 3,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(r) {
    var t = r.coordinates;
    return t.length === 2 ? [
      {
        type: "line",
        attrs: { coordinates: t }
      }
    ] : [];
  },
  performEventPressedMove: function(r) {
    var t = r.points, e = r.performPoint;
    t[0].timestamp = e.timestamp, t[0].dataIndex = e.dataIndex, t[1].timestamp = e.timestamp, t[1].dataIndex = e.dataIndex;
  },
  performEventMoveForDrawing: function(r) {
    var t = r.currentStep, e = r.points, i = r.performPoint;
    t === 2 && (e[0].timestamp = i.timestamp, e[0].dataIndex = i.dataIndex);
  }
}, Kn = {
  name: "verticalStraightLine",
  totalStep: 2,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(r) {
    var t = r.coordinates, e = r.bounding;
    return [
      {
        type: "line",
        attrs: {
          coordinates: [
            {
              x: t[0].x,
              y: 0
            },
            {
              x: t[0].x,
              y: e.height
            }
          ]
        }
      }
    ];
  }
}, Jn = {
  name: "simpleAnnotation",
  totalStep: 2,
  styles: {
    line: { style: "dashed" }
  },
  createPointFigures: function(r) {
    var t, e = r.overlay, i = r.coordinates, a = "";
    C(e.extendData) && (dt(e.extendData) ? a = e.extendData(e) : a = (t = e.extendData) !== null && t !== void 0 ? t : "");
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
}, Qn = {
  name: "simpleTag",
  totalStep: 2,
  styles: {
    line: { style: "dashed" }
  },
  createPointFigures: function(r) {
    var t = r.bounding, e = r.coordinates;
    return {
      type: "line",
      attrs: {
        coordinates: [
          { x: 0, y: e[0].y },
          { x: t.width, y: e[0].y }
        ]
      },
      ignoreEvent: !0
    };
  },
  createYAxisFigures: function(r) {
    var t, e, i, a, n = r.chart, o = r.overlay, s = r.coordinates, l = r.bounding, u = r.yAxis, c = (t = u?.isFromZero()) !== null && t !== void 0 ? t : !1, d = "left", h = 0;
    c ? (d = "left", h = 0) : (d = "right", h = l.width);
    var f = "";
    return C(o.extendData) && (dt(o.extendData) ? f = o.extendData(o) : f = (e = o.extendData) !== null && e !== void 0 ? e : ""), !C(f) && O(o.points[0].value) && (f = bt(o.points[0].value, (a = (i = n.getSymbol()) === null || i === void 0 ? void 0 : i.pricePrecision) !== null && a !== void 0 ? a : gt.PRICE)), { type: "text", attrs: { x: h, y: s[0].y, text: f, align: d, baseline: "middle" } };
  }
}, to = {
  name: "brush",
  totalStep: 2,
  drawingMode: "continuous",
  needDefaultPointFigure: !1,
  needDefaultXAxisFigure: !1,
  needDefaultYAxisFigure: !1,
  createPointFigures: function(r) {
    var t = r.coordinates;
    return t.length < 2 ? [] : [
      {
        type: "line",
        attrs: { coordinates: t },
        styles: {
          smooth: !1,
          lineCap: "round",
          lineJoin: "round"
        }
      }
    ];
  }
}, Fi = {}, eo = [
  Fn,
  Bn,
  On,
  Ln,
  $n,
  zn,
  qn,
  Hn,
  Un,
  Gn,
  jn,
  Zn,
  Kn,
  Jn,
  Qn,
  to
];
eo.forEach(function(r) {
  Fi[r.name] = Rn.extend(r);
});
function ro(r) {
  var t;
  return (t = Fi[r]) !== null && t !== void 0 ? t : null;
}
var io = {
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
}, ao = {
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
}, no = {
  light: io,
  dark: ao
};
function oo(r) {
  var t;
  return (t = no[r]) !== null && t !== void 0 ? t : null;
}
var j = {
  CANDLE: "candle_pane",
  INDICATOR: "indicator_pane_",
  X_AXIS: "x_axis_pane"
}, so = 10, lo = 80, uo = 0.2, yr = 10, co = (
  /** @class */
  (function() {
    function r(t, e) {
      var i = this;
      this._styles = tn(), this._formatter = {
        formatDate: function(v) {
          var p = v.dateTimeFormat, g = v.timestamp, m = v.template;
          return Ya(p, g, m);
        },
        formatBigNumber: Wa,
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
          return $a(v, i._thousandsSeparator.sign);
        }
      }, this._decimalFold = {
        threshold: 3,
        format: function(v) {
          return za(v, i._decimalFold.threshold);
        }
      }, this._hotKey = {
        enabled: !0,
        exclude: []
      }, this._symbol = null, this._period = null, this._dataList = [], this._dataLoader = null, this._loading = !1, this._dataLoadMore = { forward: !1, backward: !1 }, this._zoomEnabled = !0, this._zoomAnchor = {
        main: "cursor",
        xAxis: "cursor"
      }, this._scrollEnabled = !0, this._totalBarSpace = 0, this._barSpace = so, this._offsetRightDistance = lo, this._startLastBarRightSideDiffBarCount = 0, this._scrollLimitRole = "bar_count", this._minVisibleBarCount = { left: 2, right: 2 }, this._maxOffsetDistance = { left: 50, right: 50 }, this._visibleRange = Kr(), this._visibleRangeDataList = [], this._visibleRangeHighLowPrice = [
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
      }, this._chart = t;
      var a = e ?? {}, n = a.styles, o = a.locale, s = a.timezone, l = a.formatter, u = a.thousandsSeparator, c = a.decimalFold, d = a.zoomAnchor, h = a.hotkey, f = a.layout;
      C(f) && ht(this._layoutOptions, f), this._calcOptimalBarSpace(), this._lastBarRightSideDiffBarCount = this._offsetRightDistance / this._barSpace, C(n) && this.setStyles(n), K(o) && this.setLocale(o), this.setTimezone(s ?? ""), C(l) && this.setFormatter(l), C(u) && this.setThousandsSeparator(u), C(c) && this.setDecimalFold(c), C(d) && this.setZoomAnchor(d), C(h) && this.setHotkey(h), this._taskScheduler = new Ha(function() {
        i._chart.layout({
          measureWidth: !0,
          update: !0,
          buildYAxisTick: !0
        });
      });
    }
    return r.prototype.setStyles = function(t) {
      var e = this, i, a, n, o, s, l, u = null;
      if (K(t) ? u = oo(t) : u = t, ht(this._styles, u), $t((n = (a = (i = u?.candle) === null || i === void 0 ? void 0 : i.tooltip) === null || a === void 0 ? void 0 : a.legend) === null || n === void 0 ? void 0 : n.template) && (this._styles.candle.tooltip.legend.template = u.candle.tooltip.legend.template), C((l = (s = (o = u?.candle) === null || o === void 0 ? void 0 : o.priceMark) === null || s === void 0 ? void 0 : s.last) === null || l === void 0 ? void 0 : l.extendTexts)) {
        this._clearLastPriceMarkExtendTextUpdateTimer();
        var c = [];
        this._styles.candle.priceMark.last.extendTexts.forEach(function(d) {
          var h = d.updateInterval;
          if (d.show && h > 0 && !c.includes(h)) {
            c.push(h);
            var f = setInterval(function() {
              e._chart.updatePane(0, j.CANDLE);
            }, h);
            e._lastPriceMarkExtendTextUpdateTimers.push(f);
          }
        });
      }
    }, r.prototype.getStyles = function() {
      return this._styles;
    }, r.prototype.setFormatter = function(t) {
      ht(this._formatter, t);
    }, r.prototype.getFormatter = function() {
      return this._formatter;
    }, r.prototype.getInnerFormatter = function() {
      return this._innerFormatter;
    }, r.prototype.setLocale = function(t) {
      this._locale = t;
    }, r.prototype.getLocale = function() {
      return this._locale;
    }, r.prototype.setTimezone = function(t) {
      if (!C(this._dateTimeFormat) || this.getTimezone() !== t) {
        var e = {
          hour12: !1,
          year: "numeric",
          month: "2-digit",
          day: "2-digit",
          hour: "2-digit",
          minute: "2-digit",
          second: "2-digit"
        };
        t.length > 0 && (e.timeZone = t);
        var i = null;
        try {
          i = new Intl.DateTimeFormat("en", e);
        } catch {
        }
        i !== null && (this._dateTimeFormat = i);
      }
    }, r.prototype.getTimezone = function() {
      return this._dateTimeFormat.resolvedOptions().timeZone;
    }, r.prototype.getDateTimeFormat = function() {
      return this._dateTimeFormat;
    }, r.prototype.setThousandsSeparator = function(t) {
      ht(this._thousandsSeparator, t);
    }, r.prototype.getThousandsSeparator = function() {
      return this._thousandsSeparator;
    }, r.prototype.setDecimalFold = function(t) {
      ht(this._decimalFold, t);
    }, r.prototype.getDecimalFold = function() {
      return this._decimalFold;
    }, r.prototype.setHotkey = function(t) {
      ht(this._hotKey, t);
    }, r.prototype.getHotkey = function() {
      return this._hotKey;
    }, r.prototype.getHotKey = function() {
      return this._hotKey;
    }, r.prototype.setSymbol = function(t) {
      var e = this;
      this.resetData(function() {
        e._symbol = M(M({ pricePrecision: gt.PRICE, volumePrecision: gt.VOLUME }, e._symbol), t), e._synchronizeIndicatorSeriesPrecision();
      });
    }, r.prototype.getSymbol = function() {
      return this._symbol;
    }, r.prototype.setPeriod = function(t) {
      var e = this;
      this.resetData(function() {
        e._period = t;
      });
    }, r.prototype.getPeriod = function() {
      return this._period;
    }, r.prototype.getDataList = function() {
      return this._dataList;
    }, r.prototype.getVisibleRangeDataList = function() {
      return this._visibleRangeDataList;
    }, r.prototype.getVisibleRangeHighLowPrice = function() {
      return this._visibleRangeHighLowPrice;
    }, r.prototype._addData = function(t, e, i) {
      var a, n, o = !1, s = !1;
      if ($t(t)) {
        var l = { backward: !1, forward: !1 };
        switch (Me(i) ? (l.backward = i, l.forward = i) : (l.backward = (a = i?.backward) !== null && a !== void 0 ? a : !1, l.forward = (n = i?.forward) !== null && n !== void 0 ? n : !1), e) {
          case "init": {
            this._clearData(), this._dataList = t, this._dataLoadMore.backward = l.backward, this._dataLoadMore.forward = l.forward, this.setOffsetRightDistance(this._offsetRightDistance), s = !0;
            break;
          }
          case "backward": {
            this._dataList = this._dataList.concat(t), this._dataLoadMore.backward = l.backward, this._lastBarRightSideDiffBarCount -= t.length, this._startLastBarRightSideDiffBarCount -= t.length, s = t.length > 0;
            break;
          }
          case "forward": {
            this._dataList = t.concat(this._dataList), this._dataLoadMore.forward = l.forward, s = t.length > 0;
            break;
          }
        }
        o = !0;
      } else {
        var u = this._dataList.length, c = t.timestamp, d = vt(this._dataList[u - 1], "timestamp", 0);
        if (c > d) {
          this._dataList.push(t);
          var h = this.getLastBarRightSideDiffBarCount();
          h < 0 && this.setLastBarRightSideDiffBarCount(--h), o = !0, s = !0;
        } else c === d && (this._dataList[u - 1] = t, o = !0, s = !0);
      }
      if (o && s) {
        this._adjustVisibleRange(), this.setCrosshair(this._crosshair, { notInvalidate: !0 });
        var f = this.getIndicatorsByFilter({});
        f.length > 0 ? this._calcIndicator(f) : this._chart.layout({
          measureWidth: !0,
          update: !0,
          buildYAxisTick: !0,
          cacheYAxisWidth: e !== "init"
        });
      }
    }, r.prototype.setDataLoader = function(t) {
      var e = this;
      this.resetData(function() {
        e._dataLoader = t;
      });
    }, r.prototype._calcOptimalBarSpace = function() {
      var t = 4, e = 1 - uo * Math.atan(Math.max(t, this._barSpace) - t) / (Math.PI * 0.5), i = Math.min(Math.floor(this._barSpace * e), Math.floor(this._barSpace));
      i % 2 === 0 && i + 2 >= this._barSpace && --i, this._gapBarSpace = Math.max(1, i);
    }, r.prototype._adjustVisibleRange = function() {
      var t, e, i = this._dataList.length, a = this._totalBarSpace / this._barSpace, n = 0, o = 0;
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
            prev: (t = this._dataList[f - 1]) !== null && t !== void 0 ? t : v,
            current: v,
            next: (e = this._dataList[f + 1]) !== null && e !== void 0 ? e : v
          }
        }), C(v) && (this._visibleRangeHighLowPrice[0].price < v.high && (this._visibleRangeHighLowPrice[0].price = v.high, this._visibleRangeHighLowPrice[0].x = p), this._visibleRangeHighLowPrice[1].price > v.low && (this._visibleRangeHighLowPrice[1].price = v.low, this._visibleRangeHighLowPrice[1].x = p));
      }
      d === 0 ? this._dataLoadMore.forward && this._processDataLoad("forward") : u === i && this._dataLoadMore.backward && this._processDataLoad("backward");
    }, r.prototype._processDataLoad = function(t) {
      var e = this, i, a, n, o;
      if (!this._loading && C(this._dataLoader) && C(this._symbol) && C(this._period)) {
        this._loading = !0;
        var s = {
          type: t,
          symbol: this._symbol,
          period: this._period,
          timestamp: null,
          callback: function(l, u) {
            var c, d;
            e._loading = !1, e._addData(l, t, u), t === "init" && ((d = (c = e._dataLoader) === null || c === void 0 ? void 0 : c.subscribeBar) === null || d === void 0 || d.call(c, {
              symbol: e._symbol,
              period: e._period,
              callback: function(h) {
                e._addData(h, "update");
              }
            }));
          }
        };
        switch (t) {
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
      var t, e;
      C(this._dataLoader) && C(this._symbol) && C(this._period) && ((e = (t = this._dataLoader).unsubscribeBar) === null || e === void 0 || e.call(t, {
        symbol: this._symbol,
        period: this._period
      }));
    }, r.prototype.resetData = function(t) {
      this._processDataUnsubscribe(), t?.(), this._loading = !1, this._processDataLoad("init");
    }, r.prototype.getBarSpace = function() {
      return {
        bar: this._barSpace,
        halfBar: this._barSpace / 2,
        gapBar: this._gapBarSpace,
        halfGapBar: Math.floor(this._gapBarSpace / 2)
      };
    }, r.prototype.setBarSpace = function(t, e) {
      t < this._layoutOptions.barSpaceLimit.min || t > this._layoutOptions.barSpaceLimit.max || this._barSpace === t || (this._barSpace = t, this._calcOptimalBarSpace(), e?.(), this._adjustVisibleRange(), this.setCrosshair(this._crosshair, { notInvalidate: !0 }), this._chart.layout({
        measureWidth: !0,
        update: !0,
        buildYAxisTick: !0,
        cacheYAxisWidth: !0
      }));
    }, r.prototype.getLayoutOptions = function() {
      return this._layoutOptions;
    }, r.prototype.setTotalBarSpace = function(t) {
      this._totalBarSpace !== t && (this._totalBarSpace = t, this._adjustVisibleRange(), this.setCrosshair(this._crosshair, { notInvalidate: !0 }));
    }, r.prototype.setOffsetRightDistance = function(t, e) {
      return this._offsetRightDistance = this._scrollLimitRole === "distance" ? Math.min(this._maxOffsetDistance.right, t) : t, this._lastBarRightSideDiffBarCount = this._offsetRightDistance / this._barSpace, (e ?? !1) && (this._adjustVisibleRange(), this.setCrosshair(this._crosshair, { notInvalidate: !0 }), this._chart.layout({
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
    }, r.prototype.setLastBarRightSideDiffBarCount = function(t) {
      this._lastBarRightSideDiffBarCount = t;
    }, r.prototype.setMaxOffsetLeftDistance = function(t) {
      this._scrollLimitRole = "distance", this._maxOffsetDistance.left = t;
    }, r.prototype.setMaxOffsetRightDistance = function(t) {
      this._scrollLimitRole = "distance", this._maxOffsetDistance.right = t;
    }, r.prototype.setLeftMinVisibleBarCount = function(t) {
      this._scrollLimitRole = "bar_count", this._minVisibleBarCount.left = t;
    }, r.prototype.setRightMinVisibleBarCount = function(t) {
      this._scrollLimitRole = "bar_count", this._minVisibleBarCount.right = t;
    }, r.prototype.getVisibleRange = function() {
      return this._visibleRange;
    }, r.prototype.startScroll = function() {
      this._startLastBarRightSideDiffBarCount = this._lastBarRightSideDiffBarCount;
    }, r.prototype.scroll = function(t) {
      if (this._scrollEnabled) {
        var e = t / this._barSpace, i = this._lastBarRightSideDiffBarCount * this._barSpace;
        this._lastBarRightSideDiffBarCount = this._startLastBarRightSideDiffBarCount - e, this._adjustVisibleRange(), this.setCrosshair(this._crosshair, { notInvalidate: !0 }), this._chart.layout({
          measureWidth: !0,
          update: !0,
          buildYAxisTick: !0,
          cacheYAxisWidth: !0
        });
        var a = Math.round(i - this._lastBarRightSideDiffBarCount * this._barSpace);
        a !== 0 && this.executeAction("onScroll", { distance: a });
      }
    }, r.prototype.getDataByDataIndex = function(t) {
      var e;
      return (e = this._dataList[t]) !== null && e !== void 0 ? e : null;
    }, r.prototype.coordinateToFloatIndex = function(t) {
      var e = this._dataList.length, i = (this._totalBarSpace - t) / this._barSpace, a = e + this._lastBarRightSideDiffBarCount - i;
      return Math.round(a * 1e6) / 1e6;
    }, r.prototype.dataIndexToTimestamp = function(t) {
      var e = this._dataList.length;
      if (e === 0)
        return null;
      var i = this.getDataByDataIndex(t);
      if (C(i))
        return i.timestamp;
      if (C(this._period)) {
        var a = e - 1, n = null, o = 0;
        if (t > a ? (n = this._dataList[a].timestamp, o = t - a) : t < 0 && (n = this._dataList[0].timestamp, o = t), O(n)) {
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
    }, r.prototype.timestampToDataIndex = function(t) {
      var e = this._dataList.length;
      if (e === 0)
        return 0;
      if (C(this._period)) {
        var i = null, a = 0, n = e - 1, o = this._dataList[n].timestamp;
        t > o && (i = o, a = n);
        var s = this._dataList[0].timestamp;
        if (t < s && (i = s, a = 0), O(i)) {
          var l = this._period, u = l.type, c = l.span;
          switch (u) {
            case "second":
              return a + Math.floor((t - i) / (c * 1e3));
            case "minute":
              return a + Math.floor((t - i) / (c * 60 * 1e3));
            case "hour":
              return a + Math.floor((t - i) / (c * 60 * 60 * 1e3));
            case "day":
              return a + Math.floor((t - i) / (c * 24 * 60 * 60 * 1e3));
            case "week":
              return a + Math.floor((t - i) / (c * 7 * 24 * 60 * 60 * 1e3));
            case "month": {
              var d = new Date(i), h = new Date(t), f = d.getFullYear(), v = h.getFullYear(), p = d.getMonth(), g = h.getMonth();
              return a + Math.floor(((v - f) * 12 + (g - p)) / c);
            }
            case "year": {
              var f = new Date(i).getFullYear(), v = new Date(t).getFullYear();
              return a + Math.floor((v - f) / c);
            }
          }
        }
      }
      return _r(this._dataList, "timestamp", t);
    }, r.prototype.dataIndexToCoordinate = function(t) {
      var e = this._dataList.length, i = e + this._lastBarRightSideDiffBarCount - t;
      return Math.floor(this._totalBarSpace - (i - 0.5) * this._barSpace + 0.5);
    }, r.prototype.coordinateToDataIndex = function(t) {
      return Math.ceil(this.coordinateToFloatIndex(t)) - 1;
    }, r.prototype.floatIndexToTimestamp = function(t) {
      var e = this._dataList.length;
      if (e === 0)
        return null;
      var i = e - 1;
      if (t > i && e >= 2) {
        var a = this._dataList[i].timestamp, n = this._dataList[i - 1].timestamp, o = a - n;
        if (o > 0) {
          var s = t - i;
          return Math.round(a + s * o);
        }
      }
      if (t < 0 && e >= 2) {
        var l = this._dataList[0].timestamp, u = this._dataList[1].timestamp, o = u - l;
        if (o > 0)
          return Math.round(l + t * o);
      }
      var c = Math.floor(t), d = t - c, h = this.dataIndexToTimestamp(c);
      if (d === 0 || !O(h))
        return h;
      var f = this.dataIndexToTimestamp(c + 1);
      return O(f) ? Math.round(h + (f - h) * d) : h;
    }, r.prototype.timestampToFloatIndex = function(t) {
      var e = this._dataList.length;
      if (e === 0)
        return 0;
      var i = this._dataList[0].timestamp, a = this._dataList[e - 1].timestamp;
      if (t > a && e >= 2) {
        var n = this._dataList[e - 2].timestamp, o = a - n;
        if (o > 0) {
          var s = t - a, l = s / o;
          return e - 1 + l;
        }
      }
      if (t < i && e >= 2) {
        var u = this._dataList[1].timestamp, o = u - i;
        if (o > 0) {
          var c = i - t, d = c / o;
          return -d;
        }
      }
      for (var h = 0, f = e - 1, v = 0; h <= f; ) {
        var p = Math.floor((h + f) / 2), g = this._dataList[p].timestamp;
        g <= t ? (v = p, h = p + 1) : f = p - 1;
      }
      var m = this._dataList[v], x = v + 1 < e ? this._dataList[v + 1] : null;
      if (C(m) && C(x)) {
        var y = m.timestamp, E = x.timestamp;
        if (t >= y && E > y) {
          var _ = (t - y) / (E - y);
          return v + Math.min(_, 1);
        }
      }
      return v;
    }, r.prototype.zoom = function(t, e, i) {
      var a = this, n;
      if (this._zoomEnabled) {
        var o = e ?? { x: (n = this._crosshair.x) !== null && n !== void 0 ? n : this._totalBarSpace / 2 };
        i === "xAxis" ? this._zoomAnchor.xAxis === "last_bar" && (o.x = this.dataIndexToCoordinate(this._dataList.length - 1)) : this._zoomAnchor.main === "last_bar" && (o.x = this.dataIndexToCoordinate(this._dataList.length - 1));
        var s = o.x, l = this.coordinateToFloatIndex(s), u = this._barSpace, c = this._barSpace + t * (this._barSpace / yr);
        this.setBarSpace(c, function() {
          a._lastBarRightSideDiffBarCount += l - a.coordinateToFloatIndex(s);
        });
        var d = this._barSpace / u;
        d !== 1 && this.executeAction("onZoom", { scale: d });
      }
    }, r.prototype.setZoomEnabled = function(t) {
      this._zoomEnabled = t;
    }, r.prototype.isZoomEnabled = function() {
      return this._zoomEnabled;
    }, r.prototype.setZoomAnchor = function(t) {
      K(t) ? (this._zoomAnchor.main = t, this._zoomAnchor.xAxis = t) : (K(t.main) && (this._zoomAnchor.main = t.main), K(t.xAxis) && (this._zoomAnchor.xAxis = t.xAxis));
    }, r.prototype.getZoomAnchor = function() {
      return M({}, this._zoomAnchor);
    }, r.prototype.setScrollEnabled = function(t) {
      this._scrollEnabled = t;
    }, r.prototype.isScrollEnabled = function() {
      return this._scrollEnabled;
    }, r.prototype.setCrosshair = function(t, e) {
      var i, a = e ?? {}, n = a.notInvalidate, o = a.notExecuteAction, s = a.forceInvalidate, l = t ?? {}, u = 0, c = 0;
      O(l.x) ? (u = this.coordinateToDataIndex(l.x), u < 0 ? c = 0 : u > this._dataList.length - 1 ? c = this._dataList.length - 1 : c = u) : (u = this._dataList.length - 1, c = u);
      var d = this._dataList[c], h = this.dataIndexToCoordinate(u), f = { x: this._crosshair.x, y: this._crosshair.y, paneId: this._crosshair.paneId };
      this._crosshair = M(M({}, l), { realX: h, kLineData: d, realDataIndex: u, dataIndex: c, timestamp: (i = this.dataIndexToTimestamp(u)) !== null && i !== void 0 ? i : void 0 }), (f.x !== l.x || f.y !== l.y || f.paneId !== l.paneId || (s ?? !1)) && (C(d) && !(o ?? !1) && this.hasAction("onCrosshairChange") && K(this._crosshair.paneId) && this.executeAction("onCrosshairChange", t), (n ?? !1) || this._chart.updatePane(
        1
        /* UpdateLevel.Overlay */
      ));
    }, r.prototype.getCrosshair = function() {
      return this._crosshair;
    }, r.prototype.executeAction = function(t, e) {
      var i;
      (i = this._actions.get(t)) === null || i === void 0 || i.execute(e);
    }, r.prototype.subscribeAction = function(t, e) {
      var i;
      this._actions.has(t) || this._actions.set(t, new Ua()), (i = this._actions.get(t)) === null || i === void 0 || i.subscribe(e);
    }, r.prototype.unsubscribeAction = function(t, e) {
      var i = this._actions.get(t);
      C(i) && (i.unsubscribe(e), i.isEmpty() && this._actions.delete(t));
    }, r.prototype.hasAction = function(t) {
      var e = this._actions.get(t);
      return C(e) && !e.isEmpty();
    }, r.prototype._sortIndicators = function(t) {
      var e;
      K(t) ? (e = this._indicators.get(t)) === null || e === void 0 || e.sort(function(i, a) {
        return i.zLevel - a.zLevel;
      }) : this._indicators.forEach(function(i) {
        i.sort(function(a, n) {
          return a.zLevel - n.zLevel;
        });
      });
    }, r.prototype._calcIndicator = function(t) {
      var e = this, i = [];
      if (i = i.concat(t), i.length > 0) {
        var a = {};
        i.forEach(function(n) {
          a[n.id] = n.calcImp(e._dataList);
        }), this._taskScheduler.add(a);
      }
    }, r.prototype.addIndicator = function(t, e) {
      var i = t.name, a = this.getIndicatorsByFilter(t);
      if (a.length > 0)
        return !1;
      var n = t.paneId, o = this.getIndicatorsByPaneId(n), s = Pi(i), l = new s();
      return this._synchronizeIndicatorSeriesPrecision(l), l.override(t), e || (this.removeIndicator({ paneId: n }), o = []), o.push(l), this._indicators.set(n, o), this._sortIndicators(n), this._calcIndicator(l), !0;
    }, r.prototype.getIndicatorsByPaneId = function(t) {
      var e;
      return (e = this._indicators.get(t)) !== null && e !== void 0 ? e : [];
    }, r.prototype.getIndicatorsByFilter = function(t) {
      var e = t.paneId, i = t.name, a = t.id, n = function(s) {
        return C(a) ? s.id === a : !C(i) || s.name === i;
      }, o = [];
      return C(e) ? o = o.concat(this.getIndicatorsByPaneId(e).filter(n)) : this._indicators.forEach(function(s) {
        o = o.concat(s.filter(n));
      }), o;
    }, r.prototype.removeIndicator = function(t) {
      var e = this, i = !1, a = this.getIndicatorsByFilter(t);
      return a.forEach(function(n) {
        var o = e.getIndicatorsByPaneId(n.paneId), s = o.findIndex(function(l) {
          return l.id === n.id;
        });
        s > -1 && (o.splice(s, 1), i = !0), o.length === 0 && e._indicators.delete(n.paneId);
      }), i;
    }, r.prototype.hasIndicators = function(t) {
      return this._indicators.has(t);
    }, r.prototype._synchronizeIndicatorSeriesPrecision = function(t) {
      if (C(this._symbol)) {
        var e = this._symbol, i = e.pricePrecision, a = i === void 0 ? gt.PRICE : i, n = e.volumePrecision, o = n === void 0 ? gt.VOLUME : n, s = function(l) {
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
        C(t) ? s(t) : this._indicators.forEach(function(l) {
          l.forEach(function(u) {
            s(u);
          });
        });
      }
    }, r.prototype.overrideIndicator = function(t) {
      var e = this, i = !1, a = !1, n = this.getIndicatorsByFilter(t);
      return n.forEach(function(o) {
        var s = o.paneId;
        o.override(t);
        var l = o.paneId;
        if (s !== l) {
          var u = e.getIndicatorsByPaneId(s), c = u.findIndex(function(g) {
            return g.id === o.id;
          });
          c > -1 && u.splice(c, 1), u.length === 0 && e._indicators.delete(s);
          var d = e.getIndicatorsByPaneId(l);
          d.some(function(g) {
            return g.id === o.id;
          }) || (d.push(o), e._indicators.set(l, d)), a = !0;
        }
        var h = o.shouldUpdateImp(), f = h.calc, v = h.draw, p = h.sort;
        p && (a = !0), f ? e._calcIndicator(o) : v && (i = !0);
      }), a && this._sortIndicators(), i || a;
    }, r.prototype.getOverlaysByFilter = function(t) {
      var e, i = t.id, a = t.groupId, n = t.paneId, o = t.name, s = function(c) {
        return C(i) ? c.id === i : C(a) ? c.groupId === a && (!C(o) || c.name === o) : !C(o) || c.name === o;
      }, l = [];
      C(n) ? l = l.concat(this.getOverlaysByPaneId(n).filter(s)) : this._overlays.forEach(function(c) {
        l = l.concat(c.filter(s));
      });
      var u = (e = this._progressOverlayInfo) === null || e === void 0 ? void 0 : e.overlay;
      return C(u) && s(u) && l.push(u), l;
    }, r.prototype.getOverlaysByPaneId = function(t) {
      var e;
      if (!K(t)) {
        var i = [];
        return this._overlays.forEach(function(a) {
          i = i.concat(a);
        }), i;
      }
      return (e = this._overlays.get(t)) !== null && e !== void 0 ? e : [];
    }, r.prototype._sortOverlays = function(t) {
      var e;
      K(t) ? (e = this._overlays.get(t)) === null || e === void 0 || e.sort(function(i, a) {
        return i.zLevel - a.zLevel;
      }) : this._overlays.forEach(function(i) {
        i.sort(function(a, n) {
          return a.zLevel - n.zLevel;
        });
      });
    }, r.prototype.addOverlays = function(t, e) {
      var i = this, a = [], n = t.map(function(o, s) {
        var l, u, c, d, h, f, v, p;
        if (C(o.id)) {
          var g = null;
          try {
            for (var m = Ct(i._overlays), x = m.next(); !x.done; x = m.next()) {
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
        var I = ro(o.name);
        if (C(I)) {
          var w = (c = o.id) !== null && c !== void 0 ? c : Te(Dn), _ = new I(), b = (d = o.paneId) !== null && d !== void 0 ? d : j.CANDLE;
          o.id = w, (h = o.groupId) !== null && h !== void 0 || (o.groupId = w);
          var S = i.getOverlaysByPaneId(b).length;
          return (f = o.zLevel) !== null && f !== void 0 || (o.zLevel = S), _.override(o), a.includes(b) || a.push(b), _.isDrawing() ? i._progressOverlayInfo = { paneId: b, overlay: _, appointPaneFlag: e[s] } : (i._overlays.has(b) || i._overlays.set(b, []), (v = i._overlays.get(b)) === null || v === void 0 || v.push(_)), _.isStart() && ((p = _.onDrawStart) === null || p === void 0 || p.call(_, { overlay: _, chart: i._chart })), w;
        }
        return null;
      });
      return a.length > 0 && (this._sortOverlays(), a.forEach(function(o) {
        i._chart.updatePane(1, o);
      }), this._chart.updatePane(1, j.X_AXIS)), n;
    }, r.prototype.getProgressOverlayInfo = function() {
      return this._progressOverlayInfo;
    }, r.prototype.progressOverlayComplete = function() {
      var t;
      if (this._progressOverlayInfo !== null) {
        var e = this._progressOverlayInfo, i = e.overlay, a = e.paneId;
        i.isDrawing() || (this._overlays.has(a) || this._overlays.set(a, []), (t = this._overlays.get(a)) === null || t === void 0 || t.push(i), this._sortOverlays(a), this._progressOverlayInfo = null);
      }
    }, r.prototype.updateProgressOverlayInfo = function(t, e) {
      this._progressOverlayInfo !== null && (Me(e) && e && (this._progressOverlayInfo.appointPaneFlag = e), this._progressOverlayInfo.paneId = t, this._progressOverlayInfo.overlay.override({ paneId: t }));
    }, r.prototype.overrideOverlay = function(t) {
      var e = this, i = !1, a = [], n = this.getOverlaysByFilter(t);
      return n.forEach(function(o) {
        o.override(t);
        var s = o.shouldUpdate(), l = s.sort, u = s.draw;
        l && (i = !0), (l || u) && (a.includes(o.paneId) || a.push(o.paneId));
      }), i && this._sortOverlays(), a.length > 0 ? (a.forEach(function(o) {
        e._chart.updatePane(1, o);
      }), this._chart.updatePane(1, j.X_AXIS), !0) : !1;
    }, r.prototype.removeOverlay = function(t) {
      var e = this, i = [], a = this.getOverlaysByFilter(t);
      return a.forEach(function(n) {
        var o, s = n.paneId, l = e.getOverlaysByPaneId(n.paneId);
        if ((o = n.onRemoved) === null || o === void 0 || o.call(n, { overlay: n, chart: e._chart }), i.includes(s) || i.push(s), n.isDrawing())
          e._progressOverlayInfo = null;
        else {
          var u = l.findIndex(function(c) {
            return c.id === n.id;
          });
          u > -1 && l.splice(u, 1);
        }
        l.length === 0 && e._overlays.delete(s);
      }), i.length > 0 ? (i.forEach(function(n) {
        e._chart.updatePane(1, n);
      }), this._chart.updatePane(1, j.X_AXIS), !0) : !1;
    }, r.prototype.setPressedOverlayInfo = function(t) {
      this._pressedOverlayInfo = t;
    }, r.prototype.getPressedOverlayInfo = function() {
      return this._pressedOverlayInfo;
    }, r.prototype.setHoverOverlayInfo = function(t, e, i) {
      var a = this._hoverOverlayInfo, n = a.overlay, o = a.figureType, s = a.figureIndex, l = a.figure, u = t.overlay;
      if ((n?.id !== u?.id || o !== t.figureType || s !== t.figureIndex) && (this._hoverOverlayInfo = t, n?.id !== u?.id)) {
        var c = !1, d = !1;
        n !== null && (n.override({ zLevel: n.getPrevZLevel() }), d = !0, i(n, l) && (c = !0)), u !== null && (u.setPrevZLevel(u.zLevel), u.override({ zLevel: Number.MAX_SAFE_INTEGER }), d = !0, e(u, t.figure) && (c = !0)), d && this._sortOverlays(), c || this._chart.updatePane(
          1
          /* UpdateLevel.Overlay */
        );
      }
    }, r.prototype.getHoverOverlayInfo = function() {
      return this._hoverOverlayInfo;
    }, r.prototype.setClickOverlayInfo = function(t, e, i) {
      var a = this._clickOverlayInfo, n = a.paneId, o = a.overlay, s = a.figureType, l = a.figure, u = a.figureIndex, c = t.overlay;
      (o?.id !== c?.id || s !== t.figureType || u !== t.figureIndex) && (this._clickOverlayInfo = t, o?.id !== c?.id && (C(o) && i(o, l), C(c) && e(c, t.figure), this._chart.updatePane(1, t.paneId), n !== t.paneId && this._chart.updatePane(1, n), this._chart.updatePane(1, j.X_AXIS)));
    }, r.prototype.getClickOverlayInfo = function() {
      return this._clickOverlayInfo;
    }, r.prototype.isOverlayEmpty = function() {
      return this._overlays.size === 0 && this._progressOverlayInfo === null;
    }, r.prototype.isOverlayDrawing = function() {
      var t, e;
      return (e = (t = this._progressOverlayInfo) === null || t === void 0 ? void 0 : t.overlay.isDrawing()) !== null && e !== void 0 ? e : !1;
    }, r.prototype._clearLastPriceMarkExtendTextUpdateTimer = function() {
      this._lastPriceMarkExtendTextUpdateTimers.forEach(function(t) {
        clearInterval(t);
      }), this._lastPriceMarkExtendTextUpdateTimers = [];
    }, r.prototype._clearData = function() {
      this._dataLoadMore.backward = !1, this._dataLoadMore.forward = !1, this._loading = !1, this._dataList = [], this._visibleRangeDataList = [], this._visibleRangeHighLowPrice = [
        { x: 0, price: Number.MIN_SAFE_INTEGER },
        { x: 0, price: Number.MAX_SAFE_INTEGER }
      ], this._visibleRange = Kr(), this._crosshair = {};
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
}, Ye = 7;
function ho() {
  return Ir(this, void 0, void 0, function() {
    return Sr(this, function(r) {
      switch (r.label) {
        case 0:
          return [4, new Promise(function(t) {
            var e = new ResizeObserver(function(i) {
              t(i.every(function(a) {
                return "devicePixelContentBoxSize" in a;
              })), e.disconnect();
            });
            e.observe(document.body, { box: "device-pixel-content-box" });
          }).catch(function() {
            return !1;
          })];
        case 1:
          return [2, r.sent()];
      }
    });
  });
}
var ei = (
  /** @class */
  (function() {
    function r(t, e) {
      var i = this;
      this._supportedDevicePixelContentBox = !1, this._width = 0, this._height = 0, this._pixelWidth = 0, this._pixelHeight = 0, this._nextPixelWidth = 0, this._nextPixelHeight = 0, this._requestAnimationId = ne, this._mediaQueryListener = function() {
        var a = oe(i._element);
        i._nextPixelWidth = Math.round(i._element.clientWidth * a), i._nextPixelHeight = Math.round(i._element.clientHeight * a), i._resetPixelRatio();
      }, this._listener = e, this._element = te("canvas", t), this._ctx = this._element.getContext("2d"), ho().then(function(a) {
        i._supportedDevicePixelContentBox = a, a ? (i._resizeObserver = new ResizeObserver(function(n) {
          var o = n.find(function(l) {
            return l.target === i._element;
          }), s = o?.devicePixelContentBoxSize[0];
          C(s) && (i._nextPixelWidth = s.inlineSize, i._nextPixelHeight = s.blockSize, (i._pixelWidth !== i._nextPixelWidth || i._pixelHeight !== i._nextPixelHeight) && i._resetPixelRatio());
        }), i._resizeObserver.observe(i._element, { box: "device-pixel-content-box" })) : (i._mediaQueryList = window.matchMedia("(resolution: ".concat(oe(i._element), "dppx)")), i._mediaQueryList.addListener(i._mediaQueryListener));
      }).catch(function(a) {
        return !1;
      });
    }
    return r.prototype._resetPixelRatio = function() {
      var t = this;
      this._executeListener(function() {
        var e = t._element.clientWidth, i = t._element.clientHeight;
        t._width = e, t._height = i, t._pixelWidth = t._nextPixelWidth, t._pixelHeight = t._nextPixelHeight, t._element.width = t._nextPixelWidth, t._element.height = t._nextPixelHeight;
        var a = t._nextPixelWidth / e, n = t._nextPixelHeight / i;
        t._ctx.scale(a, n);
      });
    }, r.prototype._executeListener = function(t) {
      var e = this;
      this._requestAnimationId === ne && (this._requestAnimationId = qe(function() {
        e._ctx.clearRect(0, 0, e._width, e._height), t?.(), e._listener(), e._requestAnimationId = ne;
      }));
    }, r.prototype.update = function(t, e) {
      if (this._width !== t || this._height !== e) {
        if (this._element.style.width = "".concat(t, "px"), this._element.style.height = "".concat(e, "px"), !this._supportedDevicePixelContentBox) {
          var i = oe(this._element);
          this._nextPixelWidth = Math.round(t * i), this._nextPixelHeight = Math.round(e * i), this._resetPixelRatio();
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
), Bi = (
  /** @class */
  (function(r) {
    X(t, r);
    function t(e, i) {
      var a = r.call(this) || this;
      return a._bounding = Tr(), a._cursor = "crosshair", a._forceCursor = null, a._pane = i, a._rootContainer = e, a._container = a.createContainer(), e.appendChild(a._container), a;
    }
    return t.prototype.setBounding = function(e) {
      return ht(this._bounding, e), this;
    }, t.prototype.getContainer = function() {
      return this._container;
    }, t.prototype.getBounding = function() {
      return this._bounding;
    }, t.prototype.getPane = function() {
      return this._pane;
    }, t.prototype.checkEventOn = function(e) {
      return !0;
    }, t.prototype.setCursor = function(e) {
      K(this._forceCursor) || e !== this._cursor && (this._cursor = e, this._container.style.cursor = this._cursor);
    }, t.prototype.setForceCursor = function(e) {
      var i;
      e !== this._forceCursor && (this._forceCursor = e, this._container.style.cursor = (i = this._forceCursor) !== null && i !== void 0 ? i : this._cursor);
    }, t.prototype.getForceCursor = function() {
      return this._forceCursor;
    }, t.prototype.update = function(e) {
      this.updateImp(
        this._container,
        this._bounding,
        e ?? 3
        /* UpdateLevel.Drawer */
      );
    }, t.prototype.destroy = function() {
      this._rootContainer.removeChild(this._container);
    }, t;
  })(Pr)
), Dr = (
  /** @class */
  (function(r) {
    X(t, r);
    function t(e, i) {
      var a = r.call(this, e, i) || this;
      a._mainCanvas = new ei({
        position: "absolute",
        top: "0",
        left: "0",
        zIndex: "2",
        boxSizing: "border-box"
      }, function() {
        a.updateMain(a._mainCanvas.getContext());
      }), a._overlayCanvas = new ei({
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
    return t.prototype.createContainer = function() {
      return te("div", {
        margin: "0",
        padding: "0",
        position: "absolute",
        top: "0",
        overflow: "hidden",
        boxSizing: "border-box",
        zIndex: "1"
      });
    }, t.prototype.updateImp = function(e, i, a) {
      var n = i.width, o = i.height, s = i.left;
      e.style.left = "".concat(s, "px");
      var l = a, u = e.clientWidth, c = e.clientHeight;
      switch ((n !== u || o !== c) && (e.style.width = "".concat(n, "px"), e.style.height = "".concat(o, "px"), l = 3), l) {
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
    }, t.prototype.destroy = function() {
      this._mainCanvas.destroy(), this._overlayCanvas.destroy(), r.prototype.destroy.call(this);
    }, t.prototype.getImage = function(e) {
      var i = this.getBounding(), a = i.width, n = i.height, o = te("canvas", {
        width: "".concat(a, "px"),
        height: "".concat(n, "px"),
        boxSizing: "border-box"
      }), s = o.getContext("2d"), l = oe(o);
      return o.width = a * l, o.height = n * l, s.scale(l, l), s.drawImage(this._mainCanvas.getElement(), 0, 0, a, n), e && s.drawImage(this._overlayCanvas.getElement(), 0, 0, a, n), o;
    }, t;
  })(Bi)
);
function vo(r, t) {
  var e, i, a = [];
  a = a.concat(t);
  try {
    for (var n = Ct(a), o = n.next(); !o.done; o = n.next()) {
      var s = o.value, l = s.x, u = s.y, c = s.r, d = r.x - l, h = r.y - u;
      if (!(d * d + h * h > c * c))
        return !0;
    }
  } catch (f) {
    e = { error: f };
  } finally {
    try {
      o && !o.done && (i = n.return) && i.call(n);
    } finally {
      if (e) throw e.error;
    }
  }
  return !1;
}
function fo(r, t, e) {
  var i = [];
  i = i.concat(t);
  var a = e.style, n = a === void 0 ? "fill" : a, o = e.color, s = o === void 0 ? "currentColor" : o, l = e.borderSize, u = l === void 0 ? 1 : l, c = e.borderColor, d = c === void 0 ? "currentColor" : c, h = e.borderStyle, f = h === void 0 ? "solid" : h, v = e.borderDashedValue, p = v === void 0 ? [2, 2] : v, g = (n === "fill" || e.style === "stroke_fill") && (!K(s) || !ke(s));
  g && (r.fillStyle = s, i.forEach(function(m) {
    var x = m.x, y = m.y, E = m.r;
    r.beginPath(), r.arc(x, y, E, 0, Math.PI * 2), r.closePath(), r.fill();
  })), (n === "stroke" || e.style === "stroke_fill") && u > 0 && !ke(d) && (r.strokeStyle = d, r.lineWidth = u, f === "dashed" ? r.setLineDash(p) : r.setLineDash([]), i.forEach(function(m) {
    var x = m.x, y = m.y, E = m.r;
    (!g || E > u) && (r.beginPath(), r.arc(x, y, E, 0, Math.PI * 2), r.closePath(), r.stroke());
  }));
}
var po = {
  name: "circle",
  checkEventOn: vo,
  draw: function(r, t, e) {
    fo(r, t, e);
  }
};
function go(r, t) {
  var e, i, a = [];
  a = a.concat(t);
  try {
    for (var n = Ct(a), o = n.next(); !o.done; o = n.next()) {
      for (var s = o.value, l = !1, u = s.coordinates, c = 0, d = u.length - 1; c < u.length; d = c++)
        u[c].y > r.y != u[d].y > r.y && r.x < (u[d].x - u[c].x) * (r.y - u[c].y) / (u[d].y - u[c].y) + u[c].x && (l = !l);
      if (l)
        return !0;
    }
  } catch (h) {
    e = { error: h };
  } finally {
    try {
      o && !o.done && (i = n.return) && i.call(n);
    } finally {
      if (e) throw e.error;
    }
  }
  return !1;
}
function mo(r, t, e) {
  var i = [];
  i = i.concat(t);
  var a = e.style, n = a === void 0 ? "fill" : a, o = e.color, s = o === void 0 ? "currentColor" : o, l = e.borderSize, u = l === void 0 ? 1 : l, c = e.borderColor, d = c === void 0 ? "currentColor" : c, h = e.borderStyle, f = h === void 0 ? "solid" : h, v = e.borderDashedValue, p = v === void 0 ? [2, 2] : v;
  (n === "fill" || e.style === "stroke_fill") && (!K(s) || !ke(s)) && (r.fillStyle = s, i.forEach(function(g) {
    var m = g.coordinates;
    r.beginPath(), r.moveTo(m[0].x, m[0].y);
    for (var x = 1; x < m.length; x++)
      r.lineTo(m[x].x, m[x].y);
    r.closePath(), r.fill();
  })), (n === "stroke" || e.style === "stroke_fill") && u > 0 && !ke(d) && (r.strokeStyle = d, r.lineWidth = u, f === "dashed" ? r.setLineDash(p) : r.setLineDash([]), i.forEach(function(g) {
    var m = g.coordinates;
    r.beginPath(), r.moveTo(m[0].x, m[0].y);
    for (var x = 1; x < m.length; x++)
      r.lineTo(m[x].x, m[x].y);
    r.closePath(), r.stroke();
  }));
}
var _o = {
  name: "polygon",
  checkEventOn: go,
  draw: function(r, t, e) {
    mo(r, t, e);
  }
};
function Oi(r, t) {
  var e, i, a = [];
  a = a.concat(t);
  try {
    for (var n = Ct(a), o = n.next(); !o.done; o = n.next()) {
      var s = o.value, l = s.x, u = s.width;
      u < ft * 2 && (l -= ft, u = ft * 2);
      var c = s.y, d = s.height;
      if (d < ft * 2 && (c -= ft, d = ft * 2), r.x >= l && r.x <= l + u && r.y >= c && r.y <= c + d)
        return !0;
    }
  } catch (h) {
    e = { error: h };
  } finally {
    try {
      o && !o.done && (i = n.return) && i.call(n);
    } finally {
      if (e) throw e.error;
    }
  }
  return !1;
}
function Li(r, t, e) {
  var i, a = [];
  a = a.concat(t);
  var n = e.style, o = n === void 0 ? "fill" : n, s = e.color, l = s === void 0 ? "transparent" : s, u = e.borderSize, c = u === void 0 ? 1 : u, d = e.borderColor, h = d === void 0 ? "transparent" : d, f = e.borderStyle, v = f === void 0 ? "solid" : f, p = e.borderRadius, g = p === void 0 ? 0 : p, m = e.borderDashedValue, x = m === void 0 ? [2, 2] : m, y = (i = r.roundRect) !== null && i !== void 0 ? i : r.rect, E = (o === "fill" || e.style === "stroke_fill") && (!K(l) || !ke(l));
  if (E && (r.fillStyle = l, a.forEach(function(w) {
    var b = w.x, S = w.y, T = w.width, A = w.height;
    r.beginPath(), y.call(r, b, S, T, A, g), r.closePath(), r.fill();
  })), (o === "stroke" || e.style === "stroke_fill") && c > 0 && !ke(h)) {
    r.strokeStyle = h, r.fillStyle = h, r.lineWidth = c, v === "dashed" ? r.setLineDash(x) : r.setLineDash([]);
    var _ = c % 2 === 1 ? 0.5 : 0, I = Math.round(_ * 2);
    a.forEach(function(w) {
      var b = w.x, S = w.y, T = w.width, A = w.height;
      T > c * 2 && A > c * 2 ? (r.beginPath(), y.call(r, b + _, S + _, T - I, A - I, g), r.closePath(), r.stroke()) : E || r.fillRect(b, S, T, A);
    });
  }
}
var yo = {
  name: "rect",
  checkEventOn: Oi,
  draw: function(r, t, e) {
    Li(r, t, e);
  }
};
function Vi(r, t) {
  var e = t.size, i = e === void 0 ? 12 : e, a = t.paddingLeft, n = a === void 0 ? 0 : a, o = t.paddingTop, s = o === void 0 ? 0 : o, l = t.paddingRight, u = l === void 0 ? 0 : l, c = t.paddingBottom, d = c === void 0 ? 0 : c, h = t.weight, f = h === void 0 ? "normal" : h, v = t.family, p = r.x, g = r.y, m = r.text, x = r.align, y = x === void 0 ? "left" : x, E = r.baseline, _ = E === void 0 ? "top" : E, I = r.width, w = r.height, b = I ?? n + qt(m, i, f, v) + u, S = w ?? s + i + d, T = 0;
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
function xo(r, t, e) {
  var i, a, n = [];
  n = n.concat(t);
  try {
    for (var o = Ct(n), s = o.next(); !s.done; s = o.next()) {
      var l = s.value, u = Vi(l, e), c = u.x, d = u.y, h = u.width, f = u.height;
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
function wo(r, t, e) {
  var i = [];
  i = i.concat(t);
  var a = e.color, n = a === void 0 ? "currentColor" : a, o = e.size, s = o === void 0 ? 12 : o, l = e.family, u = e.weight, c = e.paddingLeft, d = c === void 0 ? 0 : c, h = e.paddingTop, f = h === void 0 ? 0 : h, v = e.paddingRight, p = v === void 0 ? 0 : v, g = i.map(function(m) {
    return Vi(m, e);
  });
  Li(r, g, M(M({}, e), { color: e.backgroundColor })), r.textAlign = "left", r.textBaseline = "top", r.font = ye(s, u, l), r.fillStyle = n, i.forEach(function(m, x) {
    var y = g[x];
    r.fillText(m.text, y.x + d, y.y + f, y.width - d - p);
  });
}
var bo = {
  name: "text",
  checkEventOn: xo,
  draw: function(r, t, e) {
    wo(r, t, e);
  }
};
function Co(r, t) {
  var e = r.x - t.x, i = r.y - t.y;
  return Math.sqrt(e * e + i * i);
}
function Eo(r, t) {
  var e, i, a = [];
  a = a.concat(t);
  try {
    for (var n = Ct(a), o = n.next(); !o.done; o = n.next()) {
      var s = o.value;
      if (Math.abs(Co(r, s) - s.r) < ft) {
        var l = s.r, u = s.startAngle, c = s.endAngle, d = l * Math.cos(u) + s.x, h = l * Math.sin(u) + s.y, f = l * Math.cos(c) + s.x, v = l * Math.sin(c) + s.y;
        if (r.x <= Math.max(d, f) + ft && r.x >= Math.min(d, f) - ft && r.y <= Math.max(h, v) + ft && r.y >= Math.min(h, v) - ft)
          return !0;
      }
    }
  } catch (p) {
    e = { error: p };
  } finally {
    try {
      o && !o.done && (i = n.return) && i.call(n);
    } finally {
      if (e) throw e.error;
    }
  }
  return !1;
}
function Io(r, t, e) {
  var i = [];
  i = i.concat(t);
  var a = e.style, n = a === void 0 ? "solid" : a, o = e.size, s = o === void 0 ? 1 : o, l = e.color, u = l === void 0 ? "currentColor" : l, c = e.dashedValue, d = c === void 0 ? [2, 2] : c;
  r.lineWidth = s, r.strokeStyle = u, n === "dashed" ? r.setLineDash(d) : r.setLineDash([]), i.forEach(function(h) {
    var f = h.x, v = h.y, p = h.r, g = h.startAngle, m = h.endAngle;
    r.beginPath(), r.arc(f, v, p, g, m), r.stroke(), r.closePath();
  });
}
var So = {
  name: "arc",
  checkEventOn: Eo,
  draw: function(r, t, e) {
    Io(r, t, e);
  }
};
function ri(r, t, e, i, a, n, o) {
  var s = Re(i, 7), l = s[0], u = s[1], c = s[2], d = s[3], h = s[4], f = s[5], v = s[6], p = o ? t + f : f + a, g = o ? e + v : v + n, m = To(t, e, l, u, c, d, h, p, g);
  m.forEach(function(x) {
    r.bezierCurveTo(x[0], x[1], x[2], x[3], x[4], x[5]);
  });
}
function To(r, t, e, i, a, n, o, s, l) {
  for (var u = Ao(r, t, e, i, a, n, o, s, l), c = u.cx, d = u.cy, h = u.startAngle, f = u.deltaAngle, v = [], p = Math.ceil(Math.abs(f) / (Math.PI / 2)), g = 0; g < p; g++) {
    var m = h + g * f / p, x = h + (g + 1) * f / p, y = Mo(c, d, e, i, a, m, x);
    v.push(y);
  }
  return v;
}
function Ao(r, t, e, i, a, n, o, s, l) {
  var u = a * Math.PI / 180, c = (r - s) / 2, d = (t - l) / 2, h = Math.cos(u) * c + Math.sin(u) * d, f = -Math.sin(u) * c + Math.cos(u) * d, v = Math.pow(h, 2) / Math.pow(e, 2) + Math.pow(f, 2) / Math.pow(i, 2);
  v > 1 && (e *= Math.sqrt(v), i *= Math.sqrt(v));
  var p = n === o ? -1 : 1, g = Math.pow(e, 2) * Math.pow(i, 2) - Math.pow(e, 2) * Math.pow(f, 2) - Math.pow(i, 2) * Math.pow(h, 2), m = Math.pow(e, 2) * Math.pow(f, 2) + Math.pow(i, 2) * Math.pow(h, 2), x = p * Math.sqrt(Math.abs(g / m)) * (e * f / i), y = p * Math.sqrt(Math.abs(g / m)) * (-i * h / e), E = Math.cos(u) * x - Math.sin(u) * y + (r + s) / 2, _ = Math.sin(u) * x + Math.cos(u) * y + (t + l) / 2, I = Math.atan2((f - y) / i, (h - x) / e), w = Math.atan2((-f - y) / i, (-h - x) / e) - I;
  return w < 0 && o === 1 ? w += 2 * Math.PI : w > 0 && o === 0 && (w -= 2 * Math.PI), { cx: E, cy: _, startAngle: I, deltaAngle: w };
}
function Mo(r, t, e, i, a, n, o) {
  var s = Math.sin(o - n) * (Math.sqrt(4 + 3 * Math.pow(Math.tan((o - n) / 2), 2)) - 1) / 3, l = Math.cos(a), u = Math.sin(a), c = r + e * Math.cos(n) * l - i * Math.sin(n) * u, d = t + e * Math.cos(n) * u + i * Math.sin(n) * l, h = r + e * Math.cos(o) * l - i * Math.sin(o) * u, f = t + e * Math.cos(o) * u + i * Math.sin(o) * l, v = c + s * (-e * Math.sin(n) * l - i * Math.cos(n) * u), p = d + s * (-e * Math.sin(n) * u + i * Math.cos(n) * l), g = h - s * (-e * Math.sin(o) * l - i * Math.cos(o) * u), m = f - s * (-e * Math.sin(o) * u + i * Math.cos(o) * l);
  return [v, p, g, m, h, f];
}
function Po(r, t, e) {
  var i = [];
  i = i.concat(t);
  var a = e.lineWidth, n = a === void 0 ? 1 : a, o = e.color, s = o === void 0 ? "currentColor" : o;
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
            ri(r, g, m, _, f, v, !1), g = _[5] + f, m = _[6] + v;
            break;
          case "a":
            ri(r, g, m, _, f, v, !0), g += _[5], m += _[6];
            break;
          case "Z":
          case "z":
            r.closePath(), g = x, m = y;
            break;
        }
      }), e.style === "fill" ? r.fill() : r.stroke();
    }
  });
}
var ko = {
  name: "path",
  checkEventOn: Oi,
  draw: function(r, t, e) {
    Po(r, t, e);
  }
}, Ni = {}, Do = [po, Wn, _o, yo, bo, So, ko];
Do.forEach(function(r) {
  Ni[r.name] = Vn.extend(r);
});
function Ro(r) {
  var t;
  return (t = Ni[r]) !== null && t !== void 0 ? t : null;
}
var Rt = (
  /** @class */
  (function(r) {
    X(t, r);
    function t(e) {
      var i = r.call(this) || this;
      return i._widget = e, i;
    }
    return t.prototype.getWidget = function() {
      return this._widget;
    }, t.prototype.createFigure = function(e, i) {
      var a = Ro(e.name);
      if (a !== null) {
        var n = new a(e);
        if (C(i)) {
          for (var o in i)
            i.hasOwnProperty(o) && n.registerEvent(o, i[o]);
          this.addChild(n);
        }
        return n;
      }
      return null;
    }, t.prototype.draw = function(e) {
      this.clear(), this.drawImp(e);
    }, t.prototype.checkEventOn = function(e) {
      return !0;
    }, t;
  })(Pr)
), Fo = (
  /** @class */
  (function(r) {
    X(t, r);
    function t() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return t.prototype.drawImp = function(e) {
      var i, a, n = this.getWidget(), o = this.getWidget().getPane(), s = o.getChart(), l = n.getBounding(), u = s.getStyles().grid, c = u.show;
      if (c) {
        e.save(), e.globalCompositeOperation = "destination-over";
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
          })) === null || i === void 0 || i.draw(e);
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
          })) === null || a === void 0 || a.draw(e);
        }
        e.restore();
      }
    }, t;
  })(Rt)
), Yi = (
  /** @class */
  (function(r) {
    X(t, r);
    function t() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return t.prototype.eachChildren = function(e) {
      for (var i = this.getWidget().getPane(), a = i.getChart().getChartStore(), n = a.getVisibleRangeDataList(), o = a.getBarSpace(), s = n.length, l = 0; l < s; )
        e(n[l], o, l), ++l;
    }, t;
  })(Rt)
), Wi = (
  /** @class */
  (function(r) {
    X(t, r);
    function t() {
      var e = r.apply(this, Pe([], Re(arguments), !1)) || this;
      return e._boundCandleBarClickEvent = function(i) {
        return function() {
          return e.getWidget().getPane().getChart().getChartStore().executeAction("onCandleBarClick", i), !1;
        };
      }, e;
    }
    return t.prototype.drawImp = function(e) {
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
            var A = f.convertToPixel(_), k = f.convertToPixel(b), F = [
              A,
              k,
              f.convertToPixel(I),
              f.convertToPixel(w)
            ];
            F.sort(function(V, B) {
              return V - B;
            });
            var P = p.gapBar % 2 === 0 ? 1 : 0, D = [];
            switch (l) {
              case "candle_solid": {
                D = i._createSolidBar(m, F, p, T, P);
                break;
              }
              case "candle_stroke": {
                D = i._createStrokeBar(m, F, p, T, P);
                break;
              }
              case "candle_up_stroke": {
                b > _ ? D = i._createStrokeBar(m, F, p, T, P) : D = i._createSolidBar(m, F, p, T, P);
                break;
              }
              case "candle_down_stroke": {
                _ > b ? D = i._createStrokeBar(m, F, p, T, P) : D = i._createSolidBar(m, F, p, T, P);
                break;
              }
              case "ohlc": {
                D = [
                  {
                    name: "rect",
                    attrs: [
                      {
                        x: m - d,
                        y: F[0],
                        width: c,
                        height: F[3] - F[0]
                      },
                      {
                        x: m - p.halfGapBar,
                        y: A + c > F[3] ? F[3] - c : A,
                        width: p.halfGapBar - d,
                        height: c
                      },
                      {
                        x: m + d,
                        y: k + c > F[3] ? F[3] - c : k,
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
            D.forEach(function(V) {
              var B, L = null;
              n && (L = {
                mouseClickEvent: i._boundCandleBarClickEvent(v)
              }), (B = i.createFigure(V, L ?? void 0)) === null || B === void 0 || B.draw(e);
            });
          }
        });
      }
    }, t.prototype.getCandleBarOptions = function() {
      var e = this.getWidget().getPane(), i = e.getDefaultYAxisId();
      if (!C(i))
        return null;
      var a = e.getChart().getStyles().candle;
      return {
        yAxisId: i,
        type: a.type,
        styles: a.bar
      };
    }, t.prototype._createSolidBar = function(e, i, a, n, o) {
      return [
        {
          name: "rect",
          attrs: {
            x: e,
            y: i[0],
            width: 1,
            height: i[3] - i[0]
          },
          styles: { color: n[2] }
        },
        {
          name: "rect",
          attrs: {
            x: e - a.halfGapBar,
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
    }, t.prototype._createStrokeBar = function(e, i, a, n, o) {
      return [
        {
          name: "rect",
          attrs: [
            {
              x: e,
              y: i[0],
              width: 1,
              height: i[1] - i[0]
            },
            {
              x: e,
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
            x: e - a.halfGapBar,
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
    }, t;
  })(Yi)
), Bo = (
  /** @class */
  (function(r) {
    X(t, r);
    function t() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return t.prototype.getCandleBarOptions = function() {
      var e, i, a = this.getWidget().getPane(), n = a.getChart().getChartStore(), o = n.getIndicatorsByPaneId(a.getId());
      try {
        for (var s = Ct(o), l = s.next(); !l.done; l = s.next()) {
          var u = l.value, c = a.getYAxisComponentById(u.yAxisId);
          if (u.shouldOhlc && u.visible && !c.isInCandle()) {
            var d = u.styles, h = n.getStyles().indicator, f = vt(d, "ohlc.compareRule", h.ohlc.compareRule), v = vt(d, "ohlc.upColor", h.ohlc.upColor), p = vt(d, "ohlc.downColor", h.ohlc.downColor), g = vt(d, "ohlc.noChangeColor", h.ohlc.noChangeColor);
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
        e = { error: m };
      } finally {
        try {
          l && !l.done && (i = s.return) && i.call(s);
        } finally {
          if (e) throw e.error;
        }
      }
      return null;
    }, t.prototype.drawImp = function(e) {
      var i = this;
      r.prototype.drawImp.call(this, e);
      var a = this.getWidget(), n = a.getPane(), o = n.getChart(), s = a.getBounding(), l = o.getXAxisPane().getXAxisComponent(), u = o.getChartStore(), c = u.getIndicatorsByPaneId(n.getId()), d = u.getStyles().indicator;
      e.save(), c.forEach(function(h) {
        var f = n.getYAxisComponentById(h.yAxisId);
        if (h.visible) {
          h.zLevel < 0 ? e.globalCompositeOperation = "destination-over" : e.globalCompositeOperation = "source-over";
          var v = !1;
          if (h.draw !== null && (e.save(), v = h.draw({
            ctx: e,
            chart: o,
            indicator: h,
            bounding: s,
            xAxis: l,
            yAxis: f
          }), e.restore()), !v) {
            var p = h.result, g = [];
            i.eachChildren(function(m, x) {
              var y, E, _, I = x.halfGapBar, w = m.dataIndex, b = m.x, S = l.convertToPixel(w - 1), T = l.convertToPixel(w + 1), A = (y = p[w - 1]) !== null && y !== void 0 ? y : null, k = (E = p[w]) !== null && E !== void 0 ? E : null, F = (_ = p[w + 1]) !== null && _ !== void 0 ? _ : null, P = { x: S }, D = { x: b }, V = { x: T };
              h.figures.forEach(function(B) {
                var L = B.key, W = A?.[L];
                O(W) && (P[L] = f.convertToPixel(W));
                var Q = k?.[L];
                O(Q) && (D[L] = f.convertToPixel(Q));
                var nt = F?.[L];
                O(nt) && (V[L] = f.convertToPixel(nt));
              }), Ar(h, w, x, d, function(B, L, W) {
                var Q, nt, et, at, ot;
                if (C(k?.[B.key])) {
                  var ut = D[B.key], $ = (Q = B.attrs) === null || Q === void 0 ? void 0 : Q.call(B, {
                    data: { prev: A, current: k, next: F },
                    coordinate: { prev: P, current: D, next: V },
                    bounding: s,
                    barSpace: x,
                    xAxis: l,
                    yAxis: f
                  });
                  switch (B.type) {
                    case "text": {
                      $ = M({
                        x: b,
                        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment -- ignore
                        y: ut,
                        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment -- ignore
                        text: k?.[B.key],
                        align: "center",
                        baseline: "middle"
                      }, $);
                      break;
                    }
                    case "circle": {
                      $ = M({ x: b, y: ut, r: Math.max(1, I) }, $);
                      break;
                    }
                    case "rect":
                    case "bar": {
                      var G = (nt = B.baseValue) !== null && nt !== void 0 ? nt : f.getRange().from, z = f.convertToPixel(G), Z = Math.abs(z - ut);
                      G !== k?.[B.key] && (Z = Math.max(1, Z));
                      var H = 0;
                      ut > z ? H = z : H = ut;
                      var lt = (et = $?.width) !== null && et !== void 0 ? et : I * 2;
                      $ = M({ x: b - lt / 2, y: H, width: Math.max(1, lt), height: Z }, $);
                      break;
                    }
                    case "line": {
                      C(g[W]) || (g[W] = []), O(D[B.key]) && O(V[B.key]) && g[W].push({
                        coordinates: (at = $?.coordinates) !== null && at !== void 0 ? at : [
                          // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment -- ignore
                          { x: D.x, y: D[B.key] },
                          // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment -- ignore
                          { x: V.x, y: V[B.key] }
                        ],
                        styles: L
                      });
                      break;
                    }
                  }
                  var J = B.type;
                  C($) && J !== "line" && ((ot = i.createFigure({
                    name: J === "bar" ? "rect" : J,
                    attrs: $,
                    styles: L
                  })) === null || ot === void 0 || ot.draw(e));
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
                  var k, F = A.coordinates, P = A.styles;
                  (k = i.createFigure({
                    name: "line",
                    attrs: { coordinates: F },
                    styles: P
                  })) === null || k === void 0 || k.draw(e);
                });
              }
            });
          }
        }
      }), e.restore();
    }, t;
  })(Wi)
), Oo = (
  /** @class */
  (function(r) {
    X(t, r);
    function t() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return t.prototype.drawImp = function(e) {
      var i = this.getWidget(), a = i.getPane(), n = i.getBounding(), o = i.getPane().getChart().getChartStore(), s = o.getCrosshair(), l = o.getStyles().crosshair;
      if (K(s.paneId) && l.show) {
        if (s.paneId === a.getId()) {
          var u = s.y;
          this._drawLine(e, [
            { x: 0, y: u },
            { x: n.width, y: u }
          ], l.horizontal);
        }
        var c = s.realX;
        this._drawLine(e, [
          { x: c, y: 0 },
          { x: c, y: n.height }
        ], l.vertical);
      }
    }, t.prototype._drawLine = function(e, i, a) {
      var n;
      if (a.show) {
        var o = a.line;
        o.show && ((n = this.createFigure({
          name: "line",
          attrs: { coordinates: i },
          styles: o
        })) === null || n === void 0 || n.draw(e));
      }
    }, t;
  })(Rt)
), $i = (
  /** @class */
  (function(r) {
    X(t, r);
    function t(e) {
      var i = r.call(this, e) || this;
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
    return t.prototype.drawImp = function(e) {
      var i = this.getWidget(), a = i.getPane(), n = a.getChart().getChartStore(), o = n.getCrosshair();
      if (C(o.kLineData)) {
        var s = i.getBounding(), l = n.getStyles().indicator.tooltip, u = l.offsetLeft, c = l.offsetTop, d = l.offsetRight;
        this.drawIndicatorTooltip(e, u, c, s.width - d);
      }
    }, t.prototype.drawIndicatorTooltip = function(e, i, a, n) {
      var o = this, s = this.getWidget().getPane(), l = s.getChart().getChartStore(), u = l.getStyles().indicator, c = u.tooltip;
      if (this.isDrawTooltip(l.getCrosshair(), c)) {
        var d = l.getIndicatorsByPaneId(s.getId()), h = c.title, f = c.legend;
        d.forEach(function(v) {
          var p = 0, g = { x: i, y: a }, m = o.getIndicatorTooltipData(v), x = m.name, y = m.calcParamsText, E = m.legends, _ = m.features, I = x.length > 0, w = E.length > 0;
          if (I || w) {
            var b = o.classifyTooltipFeatures(_);
            if (p = o.drawStandardTooltipFeatures(e, b[0], g, v, i, p, n), I) {
              var S = x;
              y.length > 0 && (S = "".concat(S).concat(y));
              var T = h.color;
              p = o.drawStandardTooltipLegends(e, [
                {
                  title: { text: "", color: T },
                  value: { text: S, color: T }
                }
              ], g, i, p, n, h);
            }
            p = o.drawStandardTooltipFeatures(e, b[1], g, v, i, p, n), w && (p = o.drawStandardTooltipLegends(e, E, g, i, p, n, f)), p = o.drawStandardTooltipFeatures(e, b[2], g, v, i, p, n), a = g.y + p;
          }
        });
      }
      return a;
    }, t.prototype.drawStandardTooltipFeatures = function(e, i, a, n, o, s, l) {
      var u = this;
      if (i.length > 0) {
        var c = 0, d = 0;
        i.forEach(function(v) {
          var p = v.marginLeft, g = p === void 0 ? 0 : p, m = v.marginTop, x = m === void 0 ? 0 : m, y = v.marginRight, E = y === void 0 ? 0 : y, _ = v.marginBottom, I = _ === void 0 ? 0 : _, w = v.paddingLeft, b = w === void 0 ? 0 : w, S = v.paddingTop, T = S === void 0 ? 0 : S, A = v.paddingRight, k = A === void 0 ? 0 : A, F = v.paddingBottom, P = F === void 0 ? 0 : F, D = v.size, V = D === void 0 ? 0 : D, B = v.type, L = v.content, W = 0;
          if (B === "icon_font") {
            var Q = L;
            e.font = ye(V, "normal", Q.family), W = e.measureText(Q.code).width;
          } else
            W = V;
          c += g + b + W + k + E, d = Math.max(d, x + T + V + P + I);
        }), a.x + c > l ? (a.x = o, a.y += s, s = d) : s = Math.max(s, d);
        var h = this.getWidget().getPane(), f = h.getId();
        i.forEach(function(v) {
          var p, g, m, x, y, E = v.marginLeft, _ = E === void 0 ? 0 : E, I = v.marginTop, w = I === void 0 ? 0 : I, b = v.marginRight, S = b === void 0 ? 0 : b, T = v.paddingLeft, A = T === void 0 ? 0 : T, k = v.paddingTop, F = k === void 0 ? 0 : k, P = v.paddingRight, D = P === void 0 ? 0 : P, V = v.paddingBottom, B = V === void 0 ? 0 : V, L = v.backgroundColor, W = v.activeBackgroundColor, Q = v.borderRadius, nt = v.size, et = nt === void 0 ? 0 : nt, at = v.color, ot = v.activeColor, ut = v.type, $ = v.content, G = at, z = L;
          ((p = u._activeFeatureInfo) === null || p === void 0 ? void 0 : p.paneId) === f && ((g = u._activeFeatureInfo.indicator) === null || g === void 0 ? void 0 : g.id) === n?.id && u._activeFeatureInfo.feature.id === v.id && (G = ot ?? at, z = W ?? L);
          var Z = "onCandleTooltipFeatureClick", H = {
            paneId: f,
            feature: v
          };
          C(n) && (Z = "onIndicatorTooltipFeatureClick", H.indicator = n);
          var lt = {
            mouseDownEvent: u._featureClickEvent(Z, H),
            mouseMoveEvent: u._featureMouseMoveEvent(H)
          }, J = 0;
          if (ut === "icon_font") {
            var it = $;
            (m = u.createFigure({
              name: "text",
              attrs: { text: it.code, x: a.x + _, y: a.y + w },
              styles: {
                paddingLeft: A,
                paddingTop: F,
                paddingRight: D,
                paddingBottom: B,
                borderRadius: Q,
                size: et,
                family: it.family,
                color: G,
                backgroundColor: z
              }
            }, lt)) === null || m === void 0 || m.draw(e), J = e.measureText(it.code).width;
          } else {
            (x = u.createFigure({
              name: "rect",
              attrs: { x: a.x + _, y: a.y + w, width: et, height: et },
              styles: {
                paddingLeft: A,
                paddingTop: F,
                paddingRight: D,
                paddingBottom: B,
                color: z
              }
            }, lt)) === null || x === void 0 || x.draw(e);
            var Et = $;
            (y = u.createFigure({
              name: "path",
              attrs: { path: Et.path, x: a.x + _ + A, y: a.y + w + F, width: et, height: et },
              styles: {
                style: Et.style,
                lineWidth: Et.lineWidth,
                color: G
              }
            })) === null || y === void 0 || y.draw(e), J = et;
          }
          a.x += _ + A + J + D + S;
        });
      }
      return s;
    }, t.prototype.drawStandardTooltipLegends = function(e, i, a, n, o, s, l) {
      var u = this;
      if (i.length > 0) {
        var c = l.marginLeft, d = l.marginTop, h = l.marginRight, f = l.marginBottom, v = l.size, p = l.family, g = l.weight;
        e.font = ye(v, g, p), i.forEach(function(m) {
          var x, y, E = m.title, _ = m.value, I = e.measureText(E.text).width, w = e.measureText(_.text).width, b = I + w, S = d + v + f;
          a.x + c + b + h > s ? (a.x = n, a.y += o, o = S) : o = Math.max(o, S), E.text.length > 0 && ((x = u.createFigure({
            name: "text",
            attrs: { x: a.x + c, y: a.y + d, text: E.text },
            styles: { color: E.color, size: v, family: p, weight: g }
          })) === null || x === void 0 || x.draw(e)), (y = u.createFigure({
            name: "text",
            attrs: { x: a.x + c + I, y: a.y + d, text: _.text },
            styles: { color: _.color, size: v, family: p, weight: g }
          })) === null || y === void 0 || y.draw(e), a.x += c + b + h;
        });
      }
      return o;
    }, t.prototype.isDrawTooltip = function(e, i) {
      var a = i.showRule;
      return a === "always" || a === "follow_cross" && K(e.paneId);
    }, t.prototype.getIndicatorTooltipData = function(e) {
      var i, a = this.getWidget().getPane().getChart().getChartStore(), n = a.getStyles().indicator, o = n.tooltip, s = o.title, l = "", u = "";
      if (s.show && (s.showName && (l = e.shortName), s.showParams)) {
        var c = e.calcParams;
        c.length > 0 && (u = "(".concat(c.join(","), ")"));
      }
      var d = { name: l, calcParamsText: u, legends: [], features: o.features }, h = a.getCrosshair().dataIndex, f = e.result, v = a.getInnerFormatter(), p = a.getDecimalFold(), g = a.getThousandsSeparator(), m = [];
      if (e.visible) {
        var x = a.getBarSpace(), y = (i = f[h]) !== null && i !== void 0 ? i : {}, E = o.legend.defaultValue;
        Ar(e, h, x, n, function(D, V) {
          if (K(D.title)) {
            var B = V.color, L = y[D.key];
            O(L) && (L = bt(L, e.precision), e.shouldFormatBigNumber && (L = v.formatBigNumber(L)), L = p.format(g.format(L))), m.push({ title: { text: D.title, color: B }, value: { text: L ?? E, color: B } });
          }
        }), d.legends = m;
      }
      if (dt(e.createTooltipDataSource)) {
        var _ = this.getWidget(), I = _.getPane(), w = I.getChart(), b = e.createTooltipDataSource({
          chart: w,
          indicator: e,
          crosshair: a.getCrosshair(),
          bounding: _.getBounding(),
          xAxis: I.getChart().getXAxisPane().getXAxisComponent(),
          yAxis: I.getYAxisComponentById(e.yAxisId)
        }), S = b.name, T = b.calcParamsText, A = b.legends, k = b.features;
        if (s.show && (K(S) && s.showName && (d.name = S), K(T) && s.showParams && (d.calcParamsText = T)), C(k) && (d.features = k), C(A) && e.visible) {
          var F = [], P = n.tooltip.legend.color;
          A.forEach(function(D) {
            var V = { text: "", color: P };
            zt(D.title) ? V = D.title : V.text = D.title;
            var B = { text: "", color: P };
            zt(D.value) ? B = D.value : B.text = D.value, O(Number(B.text)) && (B.text = p.format(g.format(B.text))), F.push({ title: V, value: B });
          }), d.legends = F;
        }
      }
      return d;
    }, t.prototype.classifyTooltipFeatures = function(e) {
      var i = [], a = [], n = [];
      return e.forEach(function(o) {
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
    }, t;
  })(Rt)
), zi = (
  /** @class */
  (function(r) {
    X(t, r);
    function t(e) {
      var i = r.call(this, e) || this;
      return i._initEvent(), i;
    }
    return t.prototype._initEvent = function() {
      var e = this, i = this.getWidget(), a = i.getPane(), n = a.getId(), o = a.getChart(), s = o.getChartStore();
      this.registerEvent("mouseMoveEvent", function(l) {
        var u, c = s.getProgressOverlayInfo();
        if (c !== null) {
          var d = c.overlay, h = c.paneId;
          d.isStart() && (s.updateProgressOverlayInfo(n), h = n);
          var f = d.points.length - 1;
          return d.isDrawing() && h === n && (d.stepDrawingModeEventMoveForDrawing(e._coordinateToPoint(d, l)), (u = d.onDrawing) === null || u === void 0 || u.call(d, M({ chart: o, overlay: d }, l))), e._figureMouseMoveEvent(d, "point", f, { key: "".concat(Ce, "point_").concat(f), type: "circle", attrs: {} })(l);
        }
        return s.setHoverOverlayInfo({
          paneId: n,
          overlay: null,
          figureType: "none",
          figureIndex: -1,
          figure: null
        }, function(v, p) {
          return e._processOverlayMouseEnterEvent(v, p, l);
        }, function(v, p) {
          return e._processOverlayMouseLeaveEvent(v, p, l);
        }), i.setForceCursor(null), !1;
      }).registerEvent("mouseClickEvent", function(l) {
        var u, c, d = s.getProgressOverlayInfo();
        if (d !== null) {
          var h = d.overlay, f = d.paneId;
          h.isStart() && (s.updateProgressOverlayInfo(n, !0), f = n);
          var v = h.points.length - 1;
          return h.isDrawing() && f === n && (h.stepDrawingModeEventMoveForDrawing(e._coordinateToPoint(h, l)), (u = h.onDrawing) === null || u === void 0 || u.call(h, M({ chart: o, overlay: h }, l)), h.nextStep(), h.isDrawing() || (s.progressOverlayComplete(), (c = h.onDrawEnd) === null || c === void 0 || c.call(h, M({ chart: o, overlay: h }, l)))), e._figureMouseClickEvent(h, "point", v, {
            key: "".concat(Ce, "point_").concat(v),
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
          return e._processOverlaySelectedEvent(p, g, l);
        }, function(p, g) {
          return e._processOverlayDeselectedEvent(p, g, l);
        }), !1;
      }).registerEvent("mouseDoubleClickEvent", function(l) {
        var u, c = s.getProgressOverlayInfo();
        if (c !== null) {
          var d = c.overlay, h = c.paneId;
          d.isDrawing() && h === n && (d.forceComplete(), d.isDrawing() || (s.progressOverlayComplete(), (u = d.onDrawEnd) === null || u === void 0 || u.call(d, M({ chart: o, overlay: d }, l))));
          var f = d.points.length - 1;
          return e._figureMouseClickEvent(d, "point", f, {
            key: "".concat(Ce, "point_").concat(f),
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
            return e._figureMouseRightClickEvent(c, "point", d, {
              key: "".concat(Ce, "point_").concat(d),
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
            var h = e._coordinateToPoint(d, l);
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
        return v !== null && Ot("onPressedMoveEnd", p) && ((c = v.onPressedMoveEnd) === null || c === void 0 || c.call(v, M({ chart: o, overlay: v, figure: p ?? void 0 }, l))), s.setPressedOverlayInfo({
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
            var f = e._coordinateToPoint(h, l);
            return h.continuousDrawingModeEventMoveForDrawing(f), (u = h.onDrawing) === null || u === void 0 || u.call(h, M({ chart: o, overlay: h }, l)), e.getWidget().setForceCursor("pointer"), !0;
          }
        }
        var v = s.getPressedOverlayInfo(), p = v.overlay, g = v.figureType, m = v.figureIndex, x = v.figure;
        if (p !== null && Ot("onPressedMoving", x) && !p.lock) {
          var f = e._coordinateToPoint(p, l);
          g === "point" ? p.eventPressedPointMove(f, m) : p.eventPressedOtherMove(f, e.getWidget().getPane().getChart().getChartStore());
          var y = !1;
          return (c = p.onPressedMoving) === null || c === void 0 || c.call(p, M(M({ chart: o, overlay: p, figure: x ?? void 0 }, l), { preventDefault: function() {
            y = !0;
          } })), y ? e.getWidget().setForceCursor(null) : e.getWidget().setForceCursor("pointer"), !0;
        }
        return e.getWidget().setForceCursor(null), !1;
      });
    }, t.prototype._createFigureEvents = function(e, i, a, n) {
      return e.isDrawing() ? null : {
        mouseMoveEvent: this._figureMouseMoveEvent(e, i, a, n),
        mouseDownEvent: this._figureMouseDownEvent(e, i, a, n),
        mouseClickEvent: this._figureMouseClickEvent(e, i, a, n),
        mouseRightClickEvent: this._figureMouseRightClickEvent(e, i, a, n),
        mouseDoubleClickEvent: this._figureMouseDoubleClickEvent(e, i, a, n)
      };
    }, t.prototype._processOverlayMouseEnterEvent = function(e, i, a) {
      return dt(e.onMouseEnter) && Ot("onMouseEnter", i) ? (e.onMouseEnter(M({ chart: this.getWidget().getPane().getChart(), overlay: e, figure: i ?? void 0 }, a)), !0) : !1;
    }, t.prototype._processOverlayMouseLeaveEvent = function(e, i, a) {
      return dt(e.onMouseLeave) && Ot("onMouseLeave", i) ? (e.onMouseLeave(M({ chart: this.getWidget().getPane().getChart(), overlay: e, figure: i ?? void 0 }, a)), !0) : !1;
    }, t.prototype._processOverlaySelectedEvent = function(e, i, a) {
      var n;
      return Ot("onSelected", i) ? ((n = e.onSelected) === null || n === void 0 || n.call(e, M({ chart: this.getWidget().getPane().getChart(), overlay: e, figure: i ?? void 0 }, a)), !0) : !1;
    }, t.prototype._processOverlayDeselectedEvent = function(e, i, a) {
      var n;
      return Ot("onDeselected", i) ? ((n = e.onDeselected) === null || n === void 0 || n.call(e, M({ chart: this.getWidget().getPane().getChart(), overlay: e, figure: i ?? void 0 }, a)), !0) : !1;
    }, t.prototype._figureMouseMoveEvent = function(e, i, a, n) {
      var o = this;
      return function(s) {
        var l, u = o.getWidget().getPane(), c = !e.isDrawing() && Ot("onMouseMove", n);
        if (c) {
          var d = !1;
          (l = e.onMouseMove) === null || l === void 0 || l.call(e, M(M({ chart: u.getChart(), overlay: e, figure: n }, s), { preventDefault: function() {
            d = !0;
          } })), d ? o.getWidget().setForceCursor(null) : o.getWidget().setForceCursor("pointer");
        }
        return u.getChart().getChartStore().setHoverOverlayInfo({ paneId: u.getId(), overlay: e, figureType: i, figure: n, figureIndex: a }, function(h, f) {
          return o._processOverlayMouseEnterEvent(h, f, s);
        }, function(h, f) {
          return o._processOverlayMouseLeaveEvent(h, f, s);
        }), c;
      };
    }, t.prototype._figureMouseDownEvent = function(e, i, a, n) {
      var o = this;
      return function(s) {
        var l;
        if (e.lock)
          return !1;
        var u = o.getWidget().getPane(), c = u.getId();
        return e.startPressedMove(o._coordinateToPoint(e, s)), Ot("onPressedMoveStart", n) ? ((l = e.onPressedMoveStart) === null || l === void 0 || l.call(e, M({ chart: u.getChart(), overlay: e, figure: n }, s)), u.getChart().getChartStore().setPressedOverlayInfo({ paneId: c, overlay: e, figureType: i, figureIndex: a, figure: n }), !e.isDrawing()) : !1;
      };
    }, t.prototype._figureMouseClickEvent = function(e, i, a, n) {
      var o = this;
      return function(s) {
        var l, u = o.getWidget().getPane(), c = u.getId(), d = !e.isDrawing() && Ot("onClick", n);
        return d && ((l = e.onClick) === null || l === void 0 || l.call(e, M({ chart: o.getWidget().getPane().getChart(), overlay: e, figure: n }, s))), u.getChart().getChartStore().setClickOverlayInfo({ paneId: c, overlay: e, figureType: i, figureIndex: a, figure: n }, function(h, f) {
          return o._processOverlaySelectedEvent(h, f, s);
        }, function(h, f) {
          return o._processOverlayDeselectedEvent(h, f, s);
        }), d;
      };
    }, t.prototype._figureMouseDoubleClickEvent = function(e, i, a, n) {
      var o = this;
      return function(s) {
        var l;
        return Ot("onDoubleClick", n) ? ((l = e.onDoubleClick) === null || l === void 0 || l.call(e, M(M({}, s), { chart: o.getWidget().getPane().getChart(), figure: n, overlay: e })), !e.isDrawing()) : !1;
      };
    }, t.prototype._figureMouseRightClickEvent = function(e, i, a, n) {
      var o = this;
      return function(s) {
        var l;
        if (Ot("onRightClick", n)) {
          var u = !1;
          return (l = e.onRightClick) === null || l === void 0 || l.call(e, M(M({ chart: o.getWidget().getPane().getChart(), overlay: e, figure: n }, s), { preventDefault: function() {
            u = !0;
          } })), u || o.getWidget().getPane().getChart().getChartStore().removeOverlay(e), !e.isDrawing();
        }
        return !1;
      };
    }, t.prototype._coordinateToPoint = function(e, i) {
      var a, n, o = {}, s = this.getWidget().getPane(), l = s.getChart(), u = s.getId(), c = l.getChartStore();
      if (this.coordinateToPointTimestampDataIndexFlag()) {
        var d = e;
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
        if (e.mode !== "normal" && u === j.CANDLE && O(o.dataIndex)) {
          var m = c.getDataByDataIndex(o.dataIndex);
          if (m !== null) {
            var x = e.modeSensitivity;
            if (g > m.high)
              if (e.mode === "weak_magnet") {
                var y = p.convertToPixel(m.high), E = p.reverse ? y + x : y - x, _ = p.convertFromPixel(E);
                g < _ && (g = m.high);
              } else
                g = m.high;
            else if (g < m.low)
              if (e.mode === "weak_magnet") {
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
    }, t.prototype.coordinateToPointValueFlag = function() {
      return !0;
    }, t.prototype.coordinateToPointTimestampDataIndexFlag = function() {
      return !0;
    }, t.prototype.dispatchEvent = function(e, i) {
      var a = this.getWidget().getPane().getChart().getChartStore().isOverlayDrawing();
      return a ? this.onEvent(e, i) : r.prototype.dispatchEvent.call(this, e, i);
    }, t.prototype.drawImp = function(e) {
      var i = this, a = this.getCompleteOverlays();
      a.forEach(function(o) {
        o.visible && i._drawOverlay(e, o);
      });
      var n = this.getProgressOverlay();
      C(n) && n.visible && this._drawOverlay(e, n);
    }, t.prototype._drawOverlay = function(e, i) {
      var a = i.points, n = this.getWidget().getPane(), o = n.getChart(), s = o.getChartStore(), l = n.getYAxisComponentById(), u = i.isContinuousDrawingMode(), c = a.map(function(h) {
        var f, v = null;
        u && O(h.timestamp) ? v = s.timestampToFloatIndex(h.timestamp) : O(h.timestamp) ? v = s.timestampToDataIndex(h.timestamp) : O(h.dataIndex) && (v = h.dataIndex);
        var p = { x: 0, y: 0 };
        return O(v) && (p.x = s.dataIndexToCoordinate(v)), O(h.value) && (p.y = (f = l?.convertToPixel(h.value)) !== null && f !== void 0 ? f : 0), p;
      });
      if (c.length > 0) {
        var d = [].concat(this.getFigures(i, c));
        this.drawFigures(e, i, d);
      }
      this.drawDefaultFigures(e, i, c);
    }, t.prototype.drawFigures = function(e, i, a) {
      var n = this, o = this.getWidget().getPane().getChart().getStyles().overlay;
      a.forEach(function(s, l) {
        var u = s.type, c = s.styles, d = s.attrs, h = [].concat(d);
        h.forEach(function(f) {
          var v, p, g = n._createFigureEvents(i, "other", l, s), m = M(M(M({}, o[u]), (v = i.styles) === null || v === void 0 ? void 0 : v[u]), c);
          (p = n.createFigure({
            name: u,
            attrs: f,
            styles: m
          }, g ?? void 0)) === null || p === void 0 || p.draw(e);
        });
      });
    }, t.prototype.getCompleteOverlays = function() {
      var e = this.getWidget().getPane();
      return e.getChart().getChartStore().getOverlaysByPaneId(e.getId());
    }, t.prototype.getProgressOverlay = function() {
      var e = this.getWidget().getPane(), i = e.getChart().getChartStore().getProgressOverlayInfo();
      return C(i) && i.paneId === e.getId() ? i.overlay : null;
    }, t.prototype.getFigures = function(e, i) {
      var a, n, o = this.getWidget(), s = o.getPane(), l = s.getChart(), u = s.getYAxisComponentById(), c = l.getXAxisPane().getXAxisComponent(), d = o.getBounding();
      return (n = (a = e.createPointFigures) === null || a === void 0 ? void 0 : a.call(e, { chart: l, overlay: e, coordinates: i, bounding: d, xAxis: c, yAxis: u })) !== null && n !== void 0 ? n : [];
    }, t.prototype.drawDefaultFigures = function(e, i, a) {
      var n = this, o, s;
      if (i.needDefaultPointFigure) {
        var l = this.getWidget().getPane().getChart().getChartStore(), u = l.getHoverOverlayInfo(), c = l.getClickOverlayInfo();
        if (((o = u.overlay) === null || o === void 0 ? void 0 : o.id) === i.id && u.figureType !== "none" || ((s = c.overlay) === null || s === void 0 ? void 0 : s.id) === i.id && c.figureType !== "none") {
          var d = l.getStyles().overlay, h = i.styles, f = M(M({}, d.point), h?.point);
          a.forEach(function(v, p) {
            var g, m, x, y, E, _ = v.x, I = v.y, w = f.radius, b = f.color, S = f.borderColor, T = f.borderSize;
            ((g = u.overlay) === null || g === void 0 ? void 0 : g.id) === i.id && u.figureType === "point" && ((m = u.figure) === null || m === void 0 ? void 0 : m.key) === "".concat(Ce, "point_").concat(p) && (w = f.activeRadius, b = f.activeColor, S = f.activeBorderColor, T = f.activeBorderSize), (y = n.createFigure({
              name: "circle",
              attrs: { x: _, y: I, r: w + T },
              styles: { color: S }
            }, (x = n._createFigureEvents(i, "point", p, {
              key: "".concat(Ce, "point_").concat(p),
              type: "circle",
              attrs: { x: _, y: I, r: w + T },
              styles: { color: S }
            })) !== null && x !== void 0 ? x : void 0)) === null || y === void 0 || y.draw(e), (E = n.createFigure({
              name: "circle",
              attrs: { x: _, y: I, r: w },
              styles: { color: b }
            })) === null || E === void 0 || E.draw(e);
          });
        }
      }
    }, t;
  })(Rt)
), qi = (
  /** @class */
  (function(r) {
    X(t, r);
    function t(e, i) {
      var a = r.call(this, e, i) || this;
      return a._gridView = new Fo(a), a._indicatorView = new Bo(a), a._crosshairLineView = new Oo(a), a._tooltipView = a.createTooltipView(), a._overlayView = new zi(a), a.addChild(a._tooltipView), a.addChild(a._overlayView), a;
    }
    return t.prototype.getName = function() {
      return U.MAIN;
    }, t.prototype.updateMain = function(e) {
      this.updateMainContent(e), this._indicatorView.draw(e), this._gridView.draw(e);
    }, t.prototype.createTooltipView = function() {
      return new $i(this);
    }, t.prototype.updateMainContent = function(e) {
    }, t.prototype.updateOverlayContent = function(e) {
    }, t.prototype.updateOverlay = function(e) {
      this._overlayView.draw(e), this._crosshairLineView.draw(e), this.updateOverlayContent(e), this._tooltipView.draw(e);
    }, t;
  })(Dr)
), Lo = (
  /** @class */
  (function(r) {
    X(t, r);
    function t() {
      var e = r.apply(this, Pe([], Re(arguments), !1)) || this;
      return e._ripplePoint = e.createFigure({
        name: "circle",
        attrs: {
          x: 0,
          y: 0,
          r: 0
        },
        styles: {
          style: "fill"
        }
      }), e._animationFrameTime = 0, e._animation = new mr({ iterationCount: 1 / 0 }).doFrame(function(i) {
        e._animationFrameTime = i;
        var a = e.getWidget().getPane();
        a.getChart().updatePane(0, a.getId());
      }), e;
    }
    return t.prototype.drawImp = function(e) {
      var i, a, n, o = this.getWidget(), s = o.getPane(), l = s.getChart(), u = l.getDataList(), c = u.length - 1, d = o.getBounding(), h = s.getYAxisComponentById(), f = l.getStyles().candle.area, v = [], p = Number.MAX_SAFE_INTEGER, g = Number.MIN_SAFE_INTEGER, m = null;
      if (this.eachChildren(function(w) {
        var b = w.x, S = w.data.current, T = S?.[f.value];
        if (O(T)) {
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
        })) === null || i === void 0 || i.draw(e);
        var x = f.backgroundColor, y = "";
        if ($t(x)) {
          var E = e.createLinearGradient(0, d.height, 0, p);
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
        e.fillStyle = y, e.beginPath(), e.moveTo(g, d.height), e.lineTo(v[0].x, v[0].y), Di(e, v, f.smooth), e.lineTo(v[v.length - 1].x, d.height), e.closePath(), e.fill();
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
        })) === null || a === void 0 || a.draw(e);
        var I = _.rippleRadius;
        _.animation && (I = _.radius + this._animationFrameTime / _.animationDuration * (_.rippleRadius - _.radius), this._animation.setDuration(_.animationDuration).start()), (n = this._ripplePoint) === null || n === void 0 || n.setAttrs({
          x: m.x,
          y: m.y,
          r: I
        }).setStyles({ style: "fill", color: _.rippleColor }).draw(e);
      } else
        this.stopAnimation();
    }, t.prototype.stopAnimation = function() {
      this._animation.stop();
    }, t;
  })(Yi)
), Vo = (
  /** @class */
  (function(r) {
    X(t, r);
    function t() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return t.prototype.drawImp = function(e) {
      var i, a, n = this.getWidget(), o = n.getPane(), s = o.getChart().getChartStore(), l = s.getStyles().candle.priceMark, u = l.high, c = l.low;
      if (l.show && (u.show || c.show)) {
        var d = s.getVisibleRangeHighLowPrice(), h = (a = (i = s.getSymbol()) === null || i === void 0 ? void 0 : i.pricePrecision) !== null && a !== void 0 ? a : gt.PRICE, f = o.getYAxisComponentById(), v = d[0], p = v.price, g = v.x, m = d[1], x = m.price, y = m.x, E = f.convertToPixel(p), _ = f.convertToPixel(x), I = s.getDecimalFold(), w = s.getThousandsSeparator();
        u.show && p !== Number.MIN_SAFE_INTEGER && this._drawMark(e, I.format(w.format(bt(p, h))), { x: g, y: E }, E < _ ? [-2, -5] : [2, 5], u), c.show && x !== Number.MAX_SAFE_INTEGER && this._drawMark(e, I.format(w.format(bt(x, h))), { x: y, y: _ }, E < _ ? [2, 5] : [-2, -5], c);
      }
    }, t.prototype._drawMark = function(e, i, a, n, o) {
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
      })) === null || s === void 0 || s.draw(e);
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
      })) === null || l === void 0 || l.draw(e), (u = this.createFigure({
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
      })) === null || u === void 0 || u.draw(e);
    }, t;
  })(Rt)
), No = (
  /** @class */
  (function(r) {
    X(t, r);
    function t() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return t.prototype.drawImp = function(e) {
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
          })) === null || n === void 0 || n.draw(e);
        }
      }
    }, t;
  })(Rt)
), Yo = {
  second: "HH:mm:ss",
  minute: "HH:mm",
  hour: "MM-DD HH:mm",
  day: "YYYY-MM-DD",
  week: "YYYY-MM-DD",
  month: "YYYY-MM",
  year: "YYYY"
}, Xi = {
  second: "HH:mm:ss",
  minute: "YYYY-MM-DD HH:mm",
  hour: "YYYY-MM-DD HH:mm",
  day: "YYYY-MM-DD",
  week: "YYYY-MM-DD",
  month: "YYYY-MM",
  year: "YYYY"
}, Wo = {
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
}, $o = {
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
}, zo = {
  "zh-CN": Wo,
  "en-US": $o
};
function ii(r, t) {
  var e;
  return (e = zo[t][r]) !== null && e !== void 0 ? e : r;
}
var qo = (
  /** @class */
  (function(r) {
    X(t, r);
    function t() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return t.prototype.drawImp = function(e) {
      var i = this.getWidget(), a = i.getPane().getChart().getChartStore(), n = a.getCrosshair();
      if (C(n.kLineData)) {
        var o = i.getBounding(), s = a.getStyles(), l = s.candle, u = s.indicator;
        if (l.tooltip.showType === "rect" && u.tooltip.showType === "rect") {
          var c = this.isDrawTooltip(n, l.tooltip), d = this.isDrawTooltip(n, u.tooltip);
          this._drawRectTooltip(e, c, d, l.tooltip.offsetTop);
        } else if (l.tooltip.showType === "standard" && u.tooltip.showType === "standard") {
          var h = l.tooltip, f = h.offsetLeft, v = h.offsetTop, p = h.offsetRight, g = o.width - p, m = this._drawCandleStandardTooltip(e, f, v, g);
          this.drawIndicatorTooltip(e, f, m, g);
        } else if (l.tooltip.showType === "rect" && u.tooltip.showType === "standard") {
          var x = l.tooltip, f = x.offsetLeft, v = x.offsetTop, p = x.offsetRight, g = o.width - p, y = this.drawIndicatorTooltip(e, f, v, g), c = this.isDrawTooltip(n, l.tooltip);
          this._drawRectTooltip(e, c, !1, y);
        } else {
          var E = l.tooltip, f = E.offsetLeft, v = E.offsetTop, p = E.offsetRight, g = o.width - p, _ = this._drawCandleStandardTooltip(e, f, v, g), d = this.isDrawTooltip(n, u.tooltip);
          this._drawRectTooltip(e, !1, d, _);
        }
      }
    }, t.prototype._drawCandleStandardTooltip = function(e, i, a, n) {
      var o, s = this.getWidget().getPane().getChart().getChartStore(), l = s.getStyles().candle, u = l.tooltip, c = u.legend, d = 0, h = { x: i, y: a }, f = s.getCrosshair();
      if (this.isDrawTooltip(f, u)) {
        var v = u.title;
        if (v.show) {
          var p = (o = s.getPeriod()) !== null && o !== void 0 ? o : {}, g = p.type, m = g === void 0 ? "" : g, x = p.span, y = x === void 0 ? "" : x, E = Gr(v.template, M(M({}, s.getSymbol()), { period: "".concat(y).concat(ii(m, s.getLocale())) })), _ = v.color, I = this.drawStandardTooltipLegends(e, [
            {
              title: { text: "", color: _ },
              value: { text: E, color: _ }
            }
          ], { x: i, y: a }, i, 0, n, v);
          h.y = h.y + I;
        }
        var w = this._getCandleTooltipLegends(), b = this.classifyTooltipFeatures(u.features);
        d = this.drawStandardTooltipFeatures(e, b[0], h, null, i, d, n), d = this.drawStandardTooltipFeatures(e, b[1], h, null, i, d, n), w.length > 0 && (d = this.drawStandardTooltipLegends(e, w, h, i, d, n, c)), d = this.drawStandardTooltipFeatures(e, b[2], h, null, i, d, n);
      }
      return h.y + d;
    }, t.prototype._drawRectTooltip = function(e, i, a, n) {
      var o = this, s, l, u = this.getWidget(), c = u.getPane(), d = c.getChart().getChartStore(), h = d.getStyles(), f = h.candle, v = h.indicator, p = f.tooltip, g = v.tooltip;
      if (i || a) {
        var m = this._getCandleTooltipLegends(), x = p.offsetLeft, y = p.offsetTop, E = p.offsetRight, _ = p.offsetBottom, I = p.legend, w = I.marginLeft, b = I.marginRight, S = I.marginTop, T = I.marginBottom, A = I.size, k = I.weight, F = I.family, P = p.rect, D = P.position, V = P.paddingLeft, B = P.paddingRight, L = P.paddingTop, W = P.paddingBottom, Q = P.offsetLeft, nt = P.offsetRight, et = P.offsetTop, at = P.offsetBottom, ot = P.borderSize, ut = P.borderRadius, $ = P.borderColor, G = P.color, z = 0, Z = 0, H = 0;
        i && (e.font = ye(A, k, F), m.forEach(function(Gt) {
          var Bt = Gt.title, Dt = Gt.value, Yt = "".concat(Bt.text).concat(Dt.text), jt = e.measureText(Yt).width + w + b;
          z = Math.max(z, jt);
        }), H += (T + S + A) * m.length);
        var lt = g.legend, J = lt.marginLeft, it = lt.marginRight, Et = lt.marginTop, At = lt.marginBottom, mt = lt.size, Pt = lt.weight, St = lt.family, he = [];
        if (a) {
          var Nt = d.getIndicatorsByPaneId(c.getId());
          e.font = ye(mt, Pt, St), Nt.forEach(function(Gt) {
            var Bt = o.getIndicatorTooltipData(Gt).legends;
            he.push(Bt), Bt.forEach(function(Dt) {
              var Yt = Dt.title, jt = Dt.value, Ue = "".concat(Yt.text).concat(jt.text), Pa = e.measureText(Ue).width + J + it;
              z = Math.max(z, Pa), H += Et + At + mt;
            });
          });
        }
        if (Z += z, Z !== 0 && H !== 0) {
          var be = d.getCrosshair(), ve = u.getBounding(), kt = c.getYAxisWidget().getBounding();
          Z += ot * 2 + V + B, H += ot * 2 + L + W;
          var _t = ve.width / 2, yt = D === "pointer" && be.paneId === j.CANDLE, It = ((s = be.realX) !== null && s !== void 0 ? s : 0) > _t, Ft = 0;
          if (yt) {
            var Hr = be.realX;
            It ? Ft = Hr - nt - Z : Ft = Hr + Q;
          } else {
            var He = this.getWidget().getPane().getYAxisComponentById();
            It ? (Ft = Q + x, He.inside && He.position === "left" && (Ft += kt.width)) : (Ft = ve.width - nt - Z - E, He.inside && He.position === "right" && (Ft -= kt.width));
          }
          var fe = n + et;
          if (yt) {
            var Ta = be.y;
            fe = Ta - H / 2, fe + H > ve.height - at - _ && (fe = ve.height - at - H - _), fe < n + et && (fe = n + et + y);
          }
          (l = this.createFigure({
            name: "rect",
            attrs: {
              x: Ft,
              y: fe,
              width: Z,
              height: H
            },
            styles: {
              style: "stroke_fill",
              color: G,
              borderColor: $,
              borderSize: ot,
              borderRadius: ut
            }
          })) === null || l === void 0 || l.draw(e);
          var Aa = Ft + ot + V + w, ae = fe + ot + L;
          if (i && m.forEach(function(Gt) {
            var Bt, Dt;
            ae += S;
            var Yt = Gt.title;
            (Bt = o.createFigure({
              name: "text",
              attrs: {
                x: Aa,
                y: ae,
                text: Yt.text
              },
              styles: {
                color: Yt.color,
                size: A,
                family: F,
                weight: k
              }
            })) === null || Bt === void 0 || Bt.draw(e);
            var jt = Gt.value;
            (Dt = o.createFigure({
              name: "text",
              attrs: {
                x: Ft + Z - ot - b - B,
                y: ae,
                text: jt.text,
                align: "right"
              },
              styles: {
                color: jt.color,
                size: A,
                family: F,
                weight: k
              }
            })) === null || Dt === void 0 || Dt.draw(e), ae += A + T;
          }), a) {
            var Ma = Ft + ot + V + J;
            he.forEach(function(Gt) {
              Gt.forEach(function(Bt) {
                var Dt, Yt;
                ae += Et;
                var jt = Bt.title, Ue = Bt.value;
                (Dt = o.createFigure({
                  name: "text",
                  attrs: {
                    x: Ma,
                    y: ae,
                    text: jt.text
                  },
                  styles: {
                    color: jt.color,
                    size: mt,
                    family: St,
                    weight: Pt
                  }
                })) === null || Dt === void 0 || Dt.draw(e), (Yt = o.createFigure({
                  name: "text",
                  attrs: {
                    x: Ft + Z - ot - it - B,
                    y: ae,
                    text: Ue.text,
                    align: "right"
                  },
                  styles: {
                    color: Ue.color,
                    size: mt,
                    family: St,
                    weight: Pt
                  }
                })) === null || Yt === void 0 || Yt.draw(e), ae += mt + At;
              });
            });
          }
        }
      }
    }, t.prototype._getCandleTooltipLegends = function() {
      var e, i, a, n, o, s, l, u, c = this.getWidget().getPane().getChart().getChartStore(), d = c.getStyles().candle, h = c.getDataList(), f = c.getInnerFormatter(), v = c.getDecimalFold(), p = c.getThousandsSeparator(), g = c.getLocale(), m = (e = c.getSymbol()) !== null && e !== void 0 ? e : {}, x = m.pricePrecision, y = x === void 0 ? gt.PRICE : x, E = m.volumePrecision, _ = E === void 0 ? gt.VOLUME : E, I = c.getPeriod(), w = (i = c.getCrosshair().dataIndex) !== null && i !== void 0 ? i : 0, b = d.tooltip, S = b.legend, T = S.color, A = S.defaultValue, k = S.template, F = (a = h[w - 1]) !== null && a !== void 0 ? a : null, P = h[w], D = (n = F?.close) !== null && n !== void 0 ? n : P.close, V = P.close - D, B = M(M({}, P), { time: f.formatDate(P.timestamp, Xi[(o = I?.type) !== null && o !== void 0 ? o : "day"], "tooltip"), open: v.format(p.format(bt(P.open, y))), high: v.format(p.format(bt(P.high, y))), low: v.format(p.format(bt(P.low, y))), close: v.format(p.format(bt(P.close, y))), volume: v.format(p.format(f.formatBigNumber(bt((s = P.volume) !== null && s !== void 0 ? s : A, _)))), turnover: v.format(p.format(bt((l = P.turnover) !== null && l !== void 0 ? l : A, y))), change: D === 0 ? A : "".concat(p.format(bt(V / D * 100)), "%") }), L = dt(k) ? k({ prev: F, current: P, next: (u = h[w + 1]) !== null && u !== void 0 ? u : null }, d) : k;
      return L.map(function(W) {
        var Q = W.title, nt = W.value, et = { text: "", color: T };
        zt(Q) ? et = M({}, Q) : et.text = Q, et.text = ii(et.text, g);
        var at = { text: A, color: T };
        return zt(nt) ? at = M({}, nt) : at.text = nt, C(/{change}/.exec(at.text)) && (at.color = V === 0 ? d.priceMark.last.noChangeColor : V > 0 ? d.priceMark.last.upColor : d.priceMark.last.downColor), at.text = Gr(at.text, B), { title: et, value: at };
      });
    }, t;
  })($i)
), Xo = (
  /** @class */
  (function(r) {
    X(t, r);
    function t(e) {
      var i = r.call(this, e) || this;
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
    return t.prototype.drawImp = function(e) {
      var i = this, a, n, o = this.getWidget(), s = o.getPane(), l = o.getPane().getChart().getChartStore(), u = l.getCrosshair(), c = this.getWidget(), d = c.getPane().getYAxisComponentById();
      if (K(u.paneId) && u.paneId === s.getId() && d.isInCandle()) {
        var h = l.getStyles().crosshair, f = h.horizontal.features;
        if (h.show && h.horizontal.show && f.length > 0) {
          var v = d.position === "right", p = c.getBounding(), g = 0, m = h.horizontal.text;
          if (d.inside && m.show) {
            var x = d.convertFromPixel(u.y), y = d.getRange(), E = d.displayValueToText(d.realValueToDisplayValue(d.valueToRealValue(x, { range: y }), { range: y }), (n = (a = l.getSymbol()) === null || a === void 0 ? void 0 : a.pricePrecision) !== null && n !== void 0 ? n : gt.PRICE);
            E = l.getDecimalFold().format(l.getThousandsSeparator().format(E)), g = m.paddingLeft + qt(E, m.size, m.weight, m.family) + m.paddingRight;
          }
          var _ = g;
          v && (_ = p.width - g);
          var I = u.y;
          f.forEach(function(w) {
            var b, S, T, A, k = w.marginLeft, F = k === void 0 ? 0 : k, P = w.marginTop, D = P === void 0 ? 0 : P, V = w.marginRight, B = V === void 0 ? 0 : V, L = w.paddingLeft, W = L === void 0 ? 0 : L, Q = w.paddingTop, nt = Q === void 0 ? 0 : Q, et = w.paddingRight, at = et === void 0 ? 0 : et, ot = w.paddingBottom, ut = ot === void 0 ? 0 : ot, $ = w.color, G = w.activeColor, z = w.backgroundColor, Z = w.activeBackgroundColor, H = w.borderRadius, lt = w.size, J = lt === void 0 ? 0 : lt, it = w.type, Et = w.content, At = J;
            if (it === "icon_font") {
              var mt = Et;
              At = W + qt(mt.code, J, "normal", mt.family) + at;
            }
            v ? _ -= At + B : _ += F;
            var Pt = $, St = z;
            ((b = i._activeFeatureInfo) === null || b === void 0 ? void 0 : b.feature.id) === w.id && (Pt = G ?? $, St = Z ?? z);
            var he = {
              mouseDownEvent: i._featureClickEvent({ crosshair: u, feature: w }),
              mouseMoveEvent: i._featureMouseMoveEvent({ crosshair: u, feature: w })
            };
            if (it === "icon_font") {
              var mt = Et;
              (S = i.createFigure({
                name: "text",
                attrs: {
                  text: mt.code,
                  x: _,
                  y: I + D,
                  baseline: "middle"
                },
                styles: {
                  paddingLeft: W,
                  paddingTop: nt,
                  paddingRight: at,
                  paddingBottom: ut,
                  borderRadius: H,
                  size: J,
                  family: mt.family,
                  color: Pt,
                  backgroundColor: St
                }
              }, he)) === null || S === void 0 || S.draw(e);
            } else {
              (T = i.createFigure({
                name: "rect",
                attrs: { x: _, y: I + D - J / 2, width: J, height: J },
                styles: {
                  paddingLeft: W,
                  paddingTop: nt,
                  paddingRight: at,
                  paddingBottom: ut,
                  color: St
                }
              }, he)) === null || T === void 0 || T.draw(e);
              var Nt = Et;
              (A = i.createFigure({
                name: "path",
                attrs: { path: Nt.path, x: _, y: I + D + nt - J / 2, width: J, height: J },
                styles: {
                  style: Nt.style,
                  lineWidth: Nt.lineWidth,
                  color: Pt
                }
              })) === null || A === void 0 || A.draw(e);
            }
            v ? _ -= F : _ += At + B;
          });
        }
      }
    }, t;
  })(Rt)
), Ho = (
  /** @class */
  (function(r) {
    X(t, r);
    function t(e, i) {
      var a = r.call(this, e, i) || this;
      return a._candleBarView = new Wi(a), a._candleAreaView = new Lo(a), a._candleHighLowPriceView = new Vo(a), a._candleLastPriceLineView = new No(a), a._crosshairFeatureView = new Xo(a), a.addChild(a._candleBarView), a.addChild(a._crosshairFeatureView), a;
    }
    return t.prototype.updateMainContent = function(e) {
      var i = this.getPane().getChart().getStyles().candle;
      i.type !== "area" ? (this._candleBarView.draw(e), this._candleHighLowPriceView.draw(e), this._candleAreaView.stopAnimation()) : this._candleAreaView.draw(e), this._candleLastPriceLineView.draw(e);
    }, t.prototype.updateOverlayContent = function(e) {
      this._crosshairFeatureView.draw(e);
    }, t.prototype.createTooltipView = function() {
      return new qo(this);
    }, t;
  })(qi)
), Hi = (
  /** @class */
  (function(r) {
    X(t, r);
    function t() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return t.prototype.drawImp = function(e) {
      var i = this, a, n, o = this.getWidget(), s = o.getPane(), l = o.getBounding(), u = this.getAxis(), c = this.getAxisStyles(s.getChart().getStyles());
      if (c.show) {
        c.axisLine.show && ((a = this.createFigure({
          name: "line",
          attrs: this.createAxisLine(l, c),
          styles: c.axisLine
        })) === null || a === void 0 || a.draw(e));
        var d = u.getTicks();
        if (c.tickLine.show) {
          var h = this.createTickLines(d, l, c);
          h.forEach(function(v) {
            var p;
            (p = i.createFigure({
              name: "line",
              attrs: v,
              styles: c.tickLine
            })) === null || p === void 0 || p.draw(e);
          });
        }
        if (c.tickText.show) {
          var f = this.createTickTexts(d, l, c);
          (n = this.createFigure({
            name: "text",
            attrs: f,
            styles: c.tickText
          })) === null || n === void 0 || n.draw(e);
        }
      }
    }, t;
  })(Rt)
), Uo = (
  /** @class */
  (function(r) {
    X(t, r);
    function t() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return t.prototype.getAxis = function() {
      return this.getWidget().getAxisComponent();
    }, t.prototype.getAxisStyles = function(e) {
      return e.yAxis;
    }, t.prototype.createAxisLine = function(e, i) {
      var a = this.getAxis(), n = i.axisLine.size, o = 0;
      return a.isFromZero() ? o = 0 : o = e.width - n, {
        coordinates: [
          { x: o, y: 0 },
          { x: o, y: e.height }
        ]
      };
    }, t.prototype.createTickLines = function(e, i, a) {
      var n = this.getAxis(), o = a.axisLine, s = a.tickLine, l = 0, u = 0;
      return n.isFromZero() ? (l = 0, o.show && (l += o.size), u = l + s.length) : (l = i.width, o.show && (l -= o.size), u = l - s.length), e.map(function(c) {
        return {
          coordinates: [
            { x: l, y: c.coord },
            { x: u, y: c.coord }
          ]
        };
      });
    }, t.prototype.createTickTexts = function(e, i, a) {
      var n = this.getAxis(), o = a.axisLine, s = a.tickLine, l = a.tickText, u = 0;
      n.isFromZero() ? (u = l.marginStart, o.show && (u += o.size), s.show && (u += s.length)) : (u = i.width - l.marginEnd, o.show && (u -= o.size), s.show && (u -= s.length));
      var c = this.getAxis().isFromZero() ? "left" : "right";
      return e.map(function(d) {
        return {
          x: u,
          y: d.coord,
          text: d.text,
          align: c,
          baseline: "middle"
        };
      });
    }, t;
  })(Hi)
), Go = (
  /** @class */
  (function(r) {
    X(t, r);
    function t() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return t.prototype.drawImp = function(e) {
      var i = this, a, n, o, s, l = this.getWidget(), u = l.getPane(), c = l.getBounding(), d = u.getChart().getChartStore(), h = d.getStyles().candle.priceMark, f = h.last, v = f.text;
      if (h.show && f.show && v.show) {
        var p = (n = (a = d.getSymbol()) === null || a === void 0 ? void 0 : a.pricePrecision) !== null && n !== void 0 ? n : gt.PRICE, g = l.getAxisComponent(), m = d.getDataList(), x = m[m.length - 1];
        if (C(x)) {
          var y = x.close, E = x.open, _ = f.compareRule === "current_open" ? E : (s = (o = m[m.length - 2]) === null || o === void 0 ? void 0 : o.close) !== null && s !== void 0 ? s : y, I = g.convertToNicePixel(y), w = "";
          y > _ ? w = f.upColor : y < _ ? w = f.downColor : w = f.noChangeColor;
          var b = 0, S = "left";
          g.isFromZero() ? (b = 0, S = "left") : (b = c.width, S = "right");
          var T = [], A = g.getRange(), k = g.displayValueToText(g.realValueToDisplayValue(g.valueToRealValue(y, { range: A }), { range: A }), p);
          k = d.getDecimalFold().format(d.getThousandsSeparator().format(k));
          var F = v.paddingLeft, P = v.paddingRight, D = v.paddingTop, V = v.paddingBottom, B = v.size, L = v.family, W = v.weight, Q = F + qt(k, B, W, L) + P, nt = D + B + V;
          T.push({
            name: "text",
            attrs: {
              x: b,
              y: I,
              width: Q,
              height: nt,
              text: k,
              align: S,
              baseline: "middle"
            },
            styles: M(M({}, v), { backgroundColor: w })
          });
          var et = d.getInnerFormatter().formatExtendText, at = B / 2, ot = I - at - D, ut = I + at + V;
          f.extendTexts.forEach(function($, G) {
            var z = et({ type: "last_price", data: x, index: G });
            if (z.length > 0 && $.show) {
              var Z = $.size / 2, H = 0;
              $.position === "above_price" ? (ot -= $.paddingBottom + Z, H = ot, ot -= Z + $.paddingTop) : (ut += $.paddingTop + Z, H = ut, ut += Z + $.paddingBottom), Q = Math.max(Q, $.paddingLeft + qt(z, $.size, $.weight, $.family) + $.paddingRight), T.push({
                name: "text",
                attrs: {
                  x: b,
                  y: H,
                  width: Q,
                  height: $.paddingTop + $.size + $.paddingBottom,
                  text: z,
                  align: S,
                  baseline: "middle"
                },
                styles: M(M({}, $), { backgroundColor: w })
              });
            }
          }), T.forEach(function($) {
            var G;
            $.attrs.width = Q, (G = i.createFigure($)) === null || G === void 0 || G.draw(e);
          });
        }
      }
    }, t;
  })(Rt)
), jo = (
  /** @class */
  (function(r) {
    X(t, r);
    function t() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return t.prototype.drawImp = function(e) {
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
            Ar(w, g, p, l, function(k, F) {
              var P, D = T[k.key];
              if (O(D)) {
                var V = h.convertToNicePixel(D), B = h.displayValueToText(h.realValueToDisplayValue(h.valueToRealValue(D, { range: f }), { range: f }), A);
                w.shouldFormatBigNumber && (B = E.formatBigNumber(B)), B = _.format(I.format(B));
                var L = 0, W = "left";
                h.isFromZero() ? (L = 0, W = "left") : (L = o.width, W = "right"), (P = i.createFigure({
                  name: "text",
                  attrs: {
                    x: L,
                    y: V,
                    text: B,
                    align: W,
                    baseline: "middle"
                  },
                  styles: M(M({}, c), { backgroundColor: F.color })
                })) === null || P === void 0 || P.draw(e);
              }
            });
          }
        });
      }
    }, t;
  })(Rt)
), Ui = (
  /** @class */
  (function(r) {
    X(t, r);
    function t() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return t.prototype.coordinateToPointTimestampDataIndexFlag = function() {
      return !1;
    }, t.prototype.drawDefaultFigures = function(e, i, a) {
      this.drawFigures(e, i, this.getDefaultFigures(i, a));
    }, t.prototype.getDefaultFigures = function(e, i) {
      var a, n = this.getWidget(), o = n.getPane(), s = o.getChart().getChartStore(), l = s.getClickOverlayInfo(), u = [];
      if (e.needDefaultYAxisFigure && e.id === ((a = l.overlay) === null || a === void 0 ? void 0 : a.id) && l.paneId === o.getId()) {
        var c = o.getYAxisComponentById(), d = n.getBounding(), h = Number.MAX_SAFE_INTEGER, f = Number.MIN_SAFE_INTEGER, v = c.isFromZero(), p = "left", g = 0;
        v ? (p = "left", g = 0) : (p = "right", g = d.width);
        var m = s.getDecimalFold(), x = s.getThousandsSeparator();
        i.forEach(function(y, E) {
          var _, I, w = e.points[E];
          if (O(w.value)) {
            h = Math.min(h, y.y), f = Math.max(f, y.y);
            var b = m.format(x.format(bt(w.value, (I = (_ = s.getSymbol()) === null || _ === void 0 ? void 0 : _.pricePrecision) !== null && I !== void 0 ? I : gt.PRICE)));
            u.push({ type: "text", attrs: { x: g, y: y.y, text: b, align: p, baseline: "middle" }, ignoreEvent: !0 });
          }
        }), i.length > 1 && u.unshift({ type: "rect", attrs: { x: 0, y: h, width: d.width, height: f - h }, ignoreEvent: !0 });
      }
      return u;
    }, t.prototype.getFigures = function(e, i) {
      var a, n, o = this.getWidget(), s = o.getPane(), l = s.getChart(), u = s.getYAxisComponentById(), c = l.getXAxisPane().getXAxisComponent(), d = o.getBounding();
      return (n = (a = e.createYAxisFigures) === null || a === void 0 ? void 0 : a.call(e, { chart: l, overlay: e, coordinates: i, bounding: d, xAxis: c, yAxis: u })) !== null && n !== void 0 ? n : [];
    }, t;
  })(zi)
), Gi = (
  /** @class */
  (function(r) {
    X(t, r);
    function t() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return t.prototype.drawImp = function(e) {
      var i, a = this.getWidget(), n = a.getPane(), o = a.getPane().getChart().getChartStore(), s = o.getCrosshair();
      if (K(s.paneId) && this.compare(s, n.getId())) {
        var l = o.getStyles().crosshair;
        if (l.show) {
          var u = this.getDirectionStyles(l), c = u.text;
          if (u.show && c.show) {
            var d = a.getBounding(), h = "getAxisComponent" in a ? a.getAxisComponent() : n.getYAxisComponentById(), f = this.getText(s, o, h);
            e.font = ye(c.size, c.weight, c.family), (i = this.createFigure({
              name: "text",
              attrs: this.getTextAttrs(f, e.measureText(f).width, s, d, h, c),
              styles: c
            })) === null || i === void 0 || i.draw(e);
          }
        }
      }
    }, t.prototype.compare = function(e, i) {
      return e.paneId === i;
    }, t.prototype.getDirectionStyles = function(e) {
      return e.horizontal;
    }, t.prototype.getText = function(e, i, a) {
      var n, o, s, l = a, u = a.convertFromPixel(e.y), c = 0, d = !1;
      if (l.isInCandle())
        c = (o = (n = i.getSymbol()) === null || n === void 0 ? void 0 : n.pricePrecision) !== null && o !== void 0 ? o : gt.PRICE;
      else {
        var h = l.id, f = this.getWidget().getPane();
        f.isManualYAxis(h) && (h = (s = f.getDefaultYAxisId()) !== null && s !== void 0 ? s : h);
        var v = i.getIndicatorsByPaneId(e.paneId).filter(function(m) {
          return m.yAxisId === h;
        });
        v.forEach(function(m) {
          c = Math.max(m.precision, c), d || (d = m.shouldFormatBigNumber);
        });
      }
      var p = l.getRange(), g = l.displayValueToText(l.realValueToDisplayValue(l.valueToRealValue(u, { range: p }), { range: p }), c);
      return d && (g = i.getInnerFormatter().formatBigNumber(g)), i.getDecimalFold().format(i.getThousandsSeparator().format(g));
    }, t.prototype.getTextAttrs = function(e, i, a, n, o, s) {
      var l = o, u = 0, c = "left";
      return l.isFromZero() ? (u = 0, c = "left") : (u = n.width, c = "right"), { x: u, y: a.y, text: e, align: c, baseline: "middle" };
    }, t;
  })(Rt)
), Zo = (
  /** @class */
  (function(r) {
    X(t, r);
    function t(e, i, a) {
      var n = r.call(this, e, i) || this;
      return n._yAxisView = new Uo(n), n._candleLastPriceLabelView = new Go(n), n._indicatorLastValueView = new jo(n), n._overlayYAxisView = new Ui(n), n._crosshairHorizontalLabelView = new Gi(n), n._yAxis = a, n.setCursor("ns-resize"), n.addChild(n._overlayYAxisView), n;
    }
    return t.prototype.getAxisComponent = function() {
      return this._yAxis;
    }, t.prototype.getName = function() {
      return U.Y_AXIS;
    }, t.prototype.updateMain = function(e) {
      this._yAxisView.draw(e);
      var i = this.getPane(), a = i.isDefaultYAxis(this._yAxis.id) || i.isManualYAxis(this._yAxis.id);
      a && this.getAxisComponent().isInCandle() && this._candleLastPriceLabelView.draw(e), this._indicatorLastValueView.draw(e);
    }, t.prototype.updateOverlay = function(e) {
      this._overlayYAxisView.draw(e), this._crosshairHorizontalLabelView.draw(e);
    }, t;
  })(Dr)
), Le = 8;
function ai() {
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
var ji = (
  /** @class */
  (function() {
    function r(t) {
      this.scrollZoomEnabled = !0, this._range = ai(), this._prevRange = ai(), this._ticks = [], this._autoCalcTickFlag = !0, this._parent = t;
    }
    return r.prototype.getParent = function() {
      return this._parent;
    }, r.prototype.buildTicks = function(t) {
      return this._autoCalcTickFlag && (this._range = this.createRangeImp()), this._prevRange.from !== this._range.from || this._prevRange.to !== this._range.to || t ? (this._prevRange = this._range, this._ticks = this.createTicksImp(), !0) : !1;
    }, r.prototype.getTicks = function() {
      return this._ticks;
    }, r.prototype.setRange = function(t) {
      this._autoCalcTickFlag = !1, this._range = t;
    }, r.prototype.getRange = function() {
      return this._range;
    }, r.prototype.setAutoCalcTickFlag = function(t) {
      this._autoCalcTickFlag = t;
    }, r.prototype.getAutoCalcTickFlag = function() {
      return this._autoCalcTickFlag;
    }, r;
  })()
), sr = "yAxis_", lr = (
  /** @class */
  (function(r) {
    X(t, r);
    function t(e, i) {
      var a = r.call(this, e) || this;
      a.id = "", a.paneId = "", a.reverse = !1, a.inside = !1, a.position = "right", a.gap = {
        top: 0.2,
        bottom: 0.1
      }, a.createRange = function(h) {
        return h.defaultRange;
      }, a.minSpan = function(h) {
        return me(-h);
      }, a.valueToRealValue = function(h) {
        return h;
      }, a.realValueToDisplayValue = function(h) {
        return h;
      }, a.displayValueToRealValue = function(h) {
        return h;
      }, a.realValueToValue = function(h) {
        return h;
      }, a.displayValueToText = function(h, f) {
        return bt(h, f);
      };
      var n = i.minSpan, o = i.valueToRealValue, s = i.realValueToDisplayValue, l = i.displayValueToRealValue, u = i.realValueToValue, c = i.displayValueToText, d = ze(i, ["minSpan", "valueToRealValue", "realValueToDisplayValue", "displayValueToRealValue", "realValueToValue", "displayValueToText"]);
      return dt(n) && (a.minSpan = n), dt(o) && (a.valueToRealValue = o), dt(s) && (a.realValueToDisplayValue = s), dt(l) && (a.displayValueToRealValue = l), dt(u) && (a.realValueToValue = u), dt(c) && (a.displayValueToText = c), a.override(d), a;
    }
    return t.prototype.override = function(e) {
      var i = e.id, a = e.name, n = e.gap, o = ze(e, ["id", "name", "gap"]);
      C(i) && this.id.length === 0 && (this.id = i), !K(this.name) && K(a) && (this.name = a), ht(this.gap, n), ht(this, o);
    }, t.prototype._getIndicatorsByYAxisIds = function() {
      var e = this.getParent(), i = /* @__PURE__ */ new Set([this.id]);
      if (e.isManualYAxis(this.id)) {
        var a = e.getDefaultYAxisId();
        C(a) && i.add(a);
      }
      return e.getChart().getChartStore().getIndicatorsByPaneId(e.getId()).filter(function(n) {
        return i.has(n.yAxisId);
      });
    }, t.prototype._shouldUseCandleData = function() {
      var e = this.getParent();
      return this.isInCandle() && (e.isDefaultYAxis(this.id) || e.isManualYAxis(this.id));
    }, t.prototype.createRangeImp = function() {
      var e, i, a = this.getParent(), n = a.getChart(), o = n.getChartStore(), s = a.getId(), l = Number.MAX_SAFE_INTEGER, u = Number.MIN_SAFE_INTEGER, c = !1, d = Number.MAX_SAFE_INTEGER, h = Number.MIN_SAFE_INTEGER, f = Number.MAX_SAFE_INTEGER, v = this._getIndicatorsByYAxisIds();
      v.forEach(function(z) {
        c || (c = z.shouldOhlc), f = Math.min(f, z.precision), O(z.minValue) && (d = Math.min(d, z.minValue)), O(z.maxValue) && (h = Math.max(h, z.maxValue));
      });
      var p = 4, g = this.isInCandle();
      if (g) {
        var m = (i = (e = o.getSymbol()) === null || e === void 0 ? void 0 : e.pricePrecision) !== null && i !== void 0 ? i : gt.PRICE;
        f !== Number.MAX_SAFE_INTEGER ? p = Math.min(f, m) : p = m;
      } else
        f !== Number.MAX_SAFE_INTEGER && (p = f);
      var x = o.getVisibleRangeDataList(), y = n.getStyles().candle, E = y.type === "area", _ = y.area.value, I = this._shouldUseCandleData(), w = I && !E || !g && c;
      x.forEach(function(z) {
        var Z = z.dataIndex, H = z.data.current;
        if (C(H) && (w && (l = Math.min(l, H.low), u = Math.max(u, H.high)), I && E)) {
          var lt = H[_];
          O(lt) && (l = Math.min(l, lt), u = Math.max(u, lt));
        }
        v.forEach(function(J) {
          var it, Et = J.result, At = J.figures, mt = (it = Et[Z]) !== null && it !== void 0 ? it : {};
          At.forEach(function(Pt) {
            var St = mt[Pt.key];
            O(St) && (l = Math.min(l, St), u = Math.max(u, St));
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
      }), A = T.realFrom, k = T.realTo, F = T.realRange, P = this.minSpan(p);
      if (A === k || F < P) {
        var D = d === A, V = h === k, B = Le / 2;
        A = D ? A : V ? A - Le * P : A - B * P, k = V ? k : D ? k + Le * P : k + B * P;
      }
      var L = this.getBounding().height, W = this.gap, Q = W.top, nt = W.bottom, et = Q;
      et >= 1 && (et = et / L);
      var at = nt;
      at >= 1 && (at = at / L), F = k - A, A = A - F * at, k = k + F * et;
      var ot = this.realValueToValue(A, { range: T }), ut = this.realValueToValue(k, { range: T }), $ = this.realValueToDisplayValue(A, { range: T }), G = this.realValueToDisplayValue(k, { range: T });
      return {
        from: ot,
        to: ut,
        range: ut - ot,
        realFrom: A,
        realTo: k,
        realRange: k - A,
        displayFrom: $,
        displayTo: G,
        displayRange: G - $
      };
    }, t.prototype.isInCandle = function() {
      return this.getParent().getId() === j.CANDLE;
    }, t.prototype.isFromZero = function() {
      return this.position === "left" && this.inside || this.position === "right" && !this.inside;
    }, t.prototype.createTicksImp = function() {
      var e = this, i, a, n = this.getRange(), o = n.displayFrom, s = n.displayTo, l = n.displayRange, u = [];
      if (l >= 0) {
        var c = qa(l / Le), d = Xa(c), h = Zr(Math.ceil(o / c) * c, d), f = Zr(Math.floor(s / c) * c, d), v = 0, p = h;
        if (c !== 0)
          for (; p <= f; ) {
            var g = p.toFixed(d);
            u[v] = { text: g, coord: 0, value: g }, ++v, p += c;
          }
      }
      var m = this.getParent(), x = this.getBounding().height, y = m.getChart().getChartStore(), E = [], _ = this._getIndicatorsByYAxisIds(), I = y.getStyles(), w = 0, b = !1;
      this._shouldUseCandleData() ? w = (a = (i = y.getSymbol()) === null || i === void 0 ? void 0 : i.pricePrecision) !== null && a !== void 0 ? a : gt.PRICE : _.forEach(function(P) {
        w = Math.max(w, P.precision), b || (b = P.shouldFormatBigNumber);
      });
      var S = y.getInnerFormatter(), T = y.getThousandsSeparator(), A = y.getDecimalFold(), k = I.xAxis.tickText.size, F = NaN;
      return u.forEach(function(P) {
        var D = P.value, V = e.displayValueToText(+D, w), B = e.convertToPixel(e.realValueToValue(e.displayValueToRealValue(+D, { range: n }), { range: n }));
        b && (V = S.formatBigNumber(D)), V = A.format(T.format(V));
        var L = O(F);
        B > k && B < x - k && (L && Math.abs(F - B) > k * 2 || !L) && (E.push({ text: V, coord: B, value: D }), F = B);
      }), dt(this.createTicks) ? this.createTicks({
        range: this.getRange(),
        bounding: this.getBounding(),
        defaultTicks: E
      }) : E;
    }, t.prototype.getAutoSize = function() {
      var e, i, a = this.getParent(), n = a.getChart(), o = n.getChartStore(), s = o.getStyles(), l = s.yAxis, u = l.size;
      if (u !== "auto")
        return u;
      var c = 0;
      if (l.show && (l.axisLine.show && (c += l.axisLine.size), l.tickLine.show && (c += l.tickLine.length), l.tickText.show)) {
        var d = 0;
        this.getTicks().forEach(function(W) {
          d = Math.max(d, qt(W.text, l.tickText.size, l.tickText.weight, l.tickText.family));
        }), c += l.tickText.marginStart + l.tickText.marginEnd + d;
      }
      var h = s.candle.priceMark, f = h.show && h.last.show && h.last.text.show, v = 0, p = s.crosshair, g = p.show && p.horizontal.show && p.horizontal.text.show, m = 0;
      if (f || g) {
        var x = (i = (e = o.getSymbol()) === null || e === void 0 ? void 0 : e.pricePrecision) !== null && i !== void 0 ? i : gt.PRICE, y = this.getRange().displayTo;
        if (f) {
          var E = o.getDataList(), _ = E[E.length - 1];
          if (C(_)) {
            var I = h.last.text, w = I.paddingLeft, b = I.paddingRight, S = I.size, T = I.family, A = I.weight;
            v = w + qt(bt(_.close, x), S, A, T) + b;
            var k = o.getInnerFormatter().formatExtendText;
            h.last.extendTexts.forEach(function(W, Q) {
              var nt = k({ type: "last_price", data: _, index: Q });
              nt.length > 0 && W.show && (v = Math.max(v, W.paddingLeft + qt(nt, W.size, W.weight, W.family) + W.paddingRight));
            });
          }
        }
        if (g) {
          var F = this._getIndicatorsByYAxisIds(), P = 0, D = !1;
          F.forEach(function(W) {
            P = Math.max(W.precision, P), D || (D = W.shouldFormatBigNumber);
          });
          var V = 2;
          if (this._shouldUseCandleData()) {
            var B = s.indicator.lastValueMark;
            B.show && B.text.show ? V = Math.max(P, x) : V = x;
          } else
            V = P;
          var L = bt(y, V);
          D && (L = o.getInnerFormatter().formatBigNumber(L)), L = o.getDecimalFold().format(L), m += p.horizontal.text.paddingLeft + p.horizontal.text.paddingRight + p.horizontal.text.borderSize * 2 + qt(L, p.horizontal.text.size, p.horizontal.text.weight, p.horizontal.text.family);
        }
      }
      return Math.max(c, v, m);
    }, t.prototype.getBounding = function() {
      var e, i;
      return (i = (e = this.getParent().getYAxisWidgetById(this.id)) === null || e === void 0 ? void 0 : e.getBounding()) !== null && i !== void 0 ? i : this.getParent().getMainWidget().getBounding();
    }, t.prototype.convertFromPixel = function(e) {
      var i = this.getBounding().height, a = this.getRange(), n = a.realFrom, o = a.realRange, s = this.reverse ? e / i : 1 - e / i, l = s * o + n;
      return this.realValueToValue(l, { range: a });
    }, t.prototype.convertToPixel = function(e) {
      var i = this.getRange(), a = this.valueToRealValue(e, { range: i }), n = this.getBounding().height, o = i.realFrom, s = i.realRange, l = (a - o) / s;
      return this.reverse ? Math.round(l * n) : Math.round((1 - l) * n);
    }, t.prototype.convertToNicePixel = function(e) {
      var i = this.getBounding().height, a = this.convertToPixel(e);
      return Math.round(Math.max(i * 0.05, Math.min(a, i * 0.98)));
    }, t.extend = function(e) {
      var i = (
        /** @class */
        (function(a) {
          X(n, a);
          function n(o) {
            return a.call(this, o, e) || this;
          }
          return n;
        })(t)
      );
      return i;
    }, t;
  })(ji)
), Ko = {
  name: "normal"
}, Jo = {
  name: "percentage",
  minSpan: function() {
    return Math.pow(10, -2);
  },
  displayValueToText: function(r) {
    return "".concat(bt(r, 2), "%");
  },
  valueToRealValue: function(r, t) {
    var e = t.range;
    return (r - e.from) / e.range * e.realRange + e.realFrom;
  },
  realValueToValue: function(r, t) {
    var e = t.range;
    return (r - e.realFrom) / e.realRange * e.range + e.from;
  },
  createRange: function(r) {
    var t = r.chart, e = r.defaultRange, i = t.getDataList(), a = t.getVisibleRange(), n = i[a.from];
    if (C(n)) {
      var o = e.from, s = e.to, l = e.range, u = (e.from - n.close) / n.close * 100, c = (e.to - n.close) / n.close * 100, d = c - u;
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
    return e;
  }
}, Qo = {
  name: "logarithm",
  minSpan: function(r) {
    return 0.05 * me(-r);
  },
  valueToRealValue: function(r) {
    return r < 0 ? -Zt(Math.abs(r)) : Zt(r);
  },
  realValueToDisplayValue: function(r) {
    return r < 0 ? -me(Math.abs(r)) : me(r);
  },
  displayValueToRealValue: function(r) {
    return r < 0 ? -Zt(Math.abs(r)) : Zt(r);
  },
  realValueToValue: function(r) {
    return r < 0 ? -me(Math.abs(r)) : me(r);
  },
  createRange: function(r) {
    var t = r.defaultRange, e = t.from, i = t.to, a = t.range, n = e < 0 ? -Zt(Math.abs(e)) : Zt(e), o = i < 0 ? -Zt(Math.abs(i)) : Zt(i);
    return {
      from: e,
      to: i,
      range: a,
      realFrom: n,
      realTo: o,
      realRange: o - n,
      displayFrom: e,
      displayTo: i,
      displayRange: a
    };
  }
}, ni = {
  normal: lr.extend(Ko),
  percentage: lr.extend(Jo),
  logarithm: lr.extend(Qo)
};
function ts(r) {
  var t;
  return (t = ni[r]) !== null && t !== void 0 ? t : ni.normal;
}
var Zi = (
  /** @class */
  (function() {
    function r(t, e) {
      this._bounding = Tr(), this._chart = t, this._id = e, this._container = te("div", {
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
    }, r.prototype.update = function(t) {
      this._bounding.height !== this._container.clientHeight && (this._container.style.height = "".concat(this._bounding.height, "px")), this.updateImp(t ?? 3, this._container, this._bounding);
    }, r;
  })()
), Ki = (
  /** @class */
  (function(r) {
    X(t, r);
    function t(e, i) {
      var a = r.call(this, e, i.id) || this;
      a._yAxisWidgets = /* @__PURE__ */ new Map(), a._yAxisComponents = /* @__PURE__ */ new Map(), a._manualYAxisIds = /* @__PURE__ */ new Set(), a._defaultYAxisId = null, a._yAxesBounding = {};
      var n = a.getContainer();
      return a._mainWidget = a.createMainWidget(n), a._options = i, a;
    }
    return t.prototype.setOptions = function(e) {
      return ht(this._options, e), O(e.height) && e.height > 0 && this.setBounding({ height: this._options.height }), this;
    }, t.prototype.setAxisCursor = function(e, i) {
      var a, n, o = null, s = "default";
      this.getId() === j.X_AXIS ? (o = this.getMainWidget().getContainer(), s = "ew-resize") : (o = (n = (a = this.getYAxisWidgetById(i)) === null || a === void 0 ? void 0 : a.getContainer()) !== null && n !== void 0 ? n : null, s = "ns-resize"), !(!C(o) || !Me(e)) && (e ? o.style.cursor = s : o.style.cursor = "default");
    }, t.prototype.createOrOverrideYAxis = function(e) {
      var i, a, n, o, s, l = M(M({}, e), { paneId: this.getId() }), u = l.id, c = (i = l.name) !== null && i !== void 0 ? i : "normal", d = (a = l.needWidget) !== null && a !== void 0 ? a : !0, h = this._yAxisComponents.get(u), f = !C(h) || C(l.name) && h.name !== l.name;
      if (f) {
        if ((n = this._yAxisWidgets.get(u)) === null || n === void 0 || n.destroy(), this._yAxisWidgets.delete(u), h = this.createYAxisComponent(c), h.id = u, h.paneId = this.getId(), this._yAxisComponents.set(u, h), (o = this._defaultYAxisId) !== null && o !== void 0 || (this._defaultYAxisId = u), d) {
          var v = this.createYAxisWidget(this.getContainer(), h);
          C(v) && this._yAxisWidgets.set(u, v);
        }
      } else if (Me(l.needWidget) && C(h)) {
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
    }, t.prototype.getOptions = function() {
      return this._options;
    }, t.prototype.getYAxisComponents = function() {
      return Array.from(this._yAxisComponents.values());
    }, t.prototype.getWidgetYAxisComponents = function() {
      var e = this;
      return Array.from(this._yAxisWidgets.keys()).map(function(i) {
        return e._yAxisComponents.get(i);
      });
    }, t.prototype.hasYAxisComponent = function(e) {
      return this._yAxisComponents.has(e);
    }, t.prototype.setManualYAxis = function(e, i) {
      i ? this._manualYAxisIds.add(e) : this._manualYAxisIds.delete(e);
    }, t.prototype.isManualYAxis = function(e) {
      return this._manualYAxisIds.has(e);
    }, t.prototype.removeYAxis = function(e) {
      var i = this, a, n = this._yAxisComponents.get(e);
      if (!C(n))
        return !1;
      this._yAxisComponents.delete(e), this._manualYAxisIds.delete(e), this._defaultYAxisId === e && (this._defaultYAxisId = (a = this._yAxisComponents.keys().next().value) !== null && a !== void 0 ? a : null);
      var o = this._yAxisWidgets.get(e);
      return C(o) && (o.destroy(), this._yAxisWidgets.delete(e)), this._yAxesBounding = Object.keys(this._yAxesBounding).reduce(function(s, l) {
        return l !== e && (s[l] = i._yAxesBounding[l]), s;
      }, {}), !0;
    }, t.prototype.getDefaultYAxisId = function() {
      return this._defaultYAxisId;
    }, t.prototype.isDefaultYAxis = function(e) {
      return this._defaultYAxisId === e;
    }, t.prototype.getYAxisComponentById = function(e) {
      var i = e ?? this.getDefaultYAxisId();
      return this._yAxisComponents.get(i);
    }, t.prototype.getYAxisWidgetById = function(e) {
      var i, a = e ?? this.getDefaultYAxisId();
      return C(a) && (i = this._yAxisWidgets.get(a)) !== null && i !== void 0 ? i : null;
    }, t.prototype.setYAxesBounding = function(e) {
      this._yAxesBounding = e;
    }, t.prototype.setBounding = function(e, i, a, n) {
      var o = this;
      ht(this.getBounding(), e);
      var s = {};
      C(e.height) && (s.height = e.height), C(e.top) && (s.top = e.top), this._mainWidget.setBounding(s);
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
    }, t.prototype.getMainWidget = function() {
      return this._mainWidget;
    }, t.prototype.getYAxisWidget = function() {
      return this.getYAxisWidgetById();
    }, t.prototype.getYAxisWidgets = function() {
      return Array.from(this._yAxisWidgets.values());
    }, t.prototype.updateImp = function(e) {
      this._mainWidget.update(e), this._yAxisWidgets.forEach(function(i) {
        i.update(e);
      });
    }, t.prototype.destroy = function() {
      this._mainWidget.destroy(), this._yAxisWidgets.forEach(function(e) {
        e.destroy();
      });
    }, t.prototype.getImage = function(e) {
      var i = this.getBounding(), a = i.width, n = i.height, o = te("canvas", {
        width: "".concat(a, "px"),
        height: "".concat(n, "px"),
        boxSizing: "border-box"
      }), s = o.getContext("2d"), l = oe(o);
      o.width = a * l, o.height = n * l, s.scale(l, l);
      var u = this._mainWidget.getBounding();
      return s.drawImage(this._mainWidget.getImage(e), u.left, 0, u.width, u.height), this._yAxisWidgets.forEach(function(c) {
        var d = c.getBounding();
        s.drawImage(c.getImage(e), d.left, 0, d.width, d.height);
      }), o;
    }, t.prototype.createYAxisComponent = function(e) {
      throw new Error("createYAxisComponent is not implemented.");
    }, t.prototype.createYAxisWidget = function(e, i) {
      return null;
    }, t;
  })(Zi)
), Ji = (
  /** @class */
  (function(r) {
    X(t, r);
    function t() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return t.prototype.createYAxisComponent = function(e) {
      var i = ts(e ?? "default");
      return new i(this);
    }, t.prototype.createMainWidget = function(e) {
      return new qi(e, this);
    }, t.prototype.createYAxisWidget = function(e, i) {
      return new Zo(e, this, i);
    }, t;
  })(Ki)
), es = (
  /** @class */
  (function(r) {
    X(t, r);
    function t() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return t.prototype.createMainWidget = function(e) {
      return new Ho(e, this);
    }, t;
  })(Ji)
), rs = (
  /** @class */
  (function(r) {
    X(t, r);
    function t() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return t.prototype.getAxis = function() {
      return this.getWidget().getPane().getXAxisComponent();
    }, t.prototype.getAxisStyles = function(e) {
      return e.xAxis;
    }, t.prototype.createAxisLine = function(e) {
      return {
        coordinates: [
          { x: 0, y: 0 },
          { x: e.width, y: 0 }
        ]
      };
    }, t.prototype.createTickLines = function(e, i, a) {
      var n = a.tickLine, o = a.axisLine.size;
      return e.map(function(s) {
        return {
          coordinates: [
            { x: s.coord, y: 0 },
            { x: s.coord, y: o + n.length }
          ]
        };
      });
    }, t.prototype.createTickTexts = function(e, i, a) {
      var n = a.tickText, o = a.axisLine.size, s = a.tickLine.length;
      return e.map(function(l) {
        return {
          x: l.coord,
          y: o + s + n.marginStart,
          text: l.text,
          align: "center",
          baseline: "top"
        };
      });
    }, t;
  })(Hi)
), is = (
  /** @class */
  (function(r) {
    X(t, r);
    function t() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return t.prototype.coordinateToPointTimestampDataIndexFlag = function() {
      return !0;
    }, t.prototype.coordinateToPointValueFlag = function() {
      return !1;
    }, t.prototype.getCompleteOverlays = function() {
      return this.getWidget().getPane().getChart().getChartStore().getOverlaysByPaneId();
    }, t.prototype.getProgressOverlay = function() {
      var e, i;
      return (i = (e = this.getWidget().getPane().getChart().getChartStore().getProgressOverlayInfo()) === null || e === void 0 ? void 0 : e.overlay) !== null && i !== void 0 ? i : null;
    }, t.prototype.getDefaultFigures = function(e, i) {
      var a, n = [], o = this.getWidget(), s = o.getPane(), l = s.getChart().getChartStore(), u = l.getClickOverlayInfo();
      if (e.needDefaultXAxisFigure && e.id === ((a = u.overlay) === null || a === void 0 ? void 0 : a.id)) {
        var c = Number.MAX_SAFE_INTEGER, d = Number.MIN_SAFE_INTEGER;
        i.forEach(function(h, f) {
          c = Math.min(c, h.x), d = Math.max(d, h.x);
          var v = e.points[f];
          if (O(v.timestamp)) {
            var p = l.getInnerFormatter().formatDate(v.timestamp, "YYYY-MM-DD HH:mm", "crosshair");
            n.push({ type: "text", attrs: { x: h.x, y: 0, text: p, align: "center" }, ignoreEvent: !0 });
          }
        }), i.length > 1 && n.unshift({ type: "rect", attrs: { x: c, y: 0, width: d - c, height: o.getBounding().height }, ignoreEvent: !0 });
      }
      return n;
    }, t.prototype.getFigures = function(e, i) {
      var a, n, o = this.getWidget(), s = o.getPane(), l = s.getChart(), u = s.getYAxisComponentById(), c = l.getXAxisPane().getXAxisComponent(), d = o.getBounding();
      return (n = (a = e.createXAxisFigures) === null || a === void 0 ? void 0 : a.call(e, { chart: l, overlay: e, coordinates: i, bounding: d, xAxis: c, yAxis: u })) !== null && n !== void 0 ? n : [];
    }, t;
  })(Ui)
), as = (
  /** @class */
  (function(r) {
    X(t, r);
    function t() {
      return r !== null && r.apply(this, arguments) || this;
    }
    return t.prototype.compare = function(e) {
      return C(e.timestamp);
    }, t.prototype.getDirectionStyles = function(e) {
      return e.vertical;
    }, t.prototype.getText = function(e, i) {
      var a, n, o = e.timestamp;
      return i.getInnerFormatter().formatDate(o, Xi[(n = (a = i.getPeriod()) === null || a === void 0 ? void 0 : a.type) !== null && n !== void 0 ? n : "day"], "crosshair");
    }, t.prototype.getTextAttrs = function(e, i, a, n, o, s) {
      var l = a.realX, u = 0, c = "center";
      return l - i / 2 - s.paddingLeft < 0 ? (u = 0, c = "left") : l + i / 2 + s.paddingRight > n.width ? (u = n.width, c = "right") : u = l, { x: u, y: 0, text: e, align: c, baseline: "top" };
    }, t;
  })(Gi)
), ns = (
  /** @class */
  (function(r) {
    X(t, r);
    function t(e, i) {
      var a = r.call(this, e, i) || this;
      return a._xAxisView = new rs(a), a._overlayXAxisView = new is(a), a._crosshairVerticalLabelView = new as(a), a.setCursor("ew-resize"), a.addChild(a._overlayXAxisView), a;
    }
    return t.prototype.getName = function() {
      return U.X_AXIS;
    }, t.prototype.updateMain = function(e) {
      this._xAxisView.draw(e);
    }, t.prototype.updateOverlay = function(e) {
      this._overlayXAxisView.draw(e), this._crosshairVerticalLabelView.draw(e);
    }, t;
  })(Dr)
), os = (
  /** @class */
  (function(r) {
    X(t, r);
    function t(e, i) {
      var a = r.call(this, e) || this;
      return a.override(i), a;
    }
    return t.prototype.override = function(e) {
      var i = e.name, a = e.scrollZoomEnabled, n = e.createTicks;
      !K(this.name) && K(i) && (this.name = i), this.scrollZoomEnabled = a ?? this.scrollZoomEnabled, this.createTicks = n ?? this.createTicks;
    }, t.prototype.createRangeImp = function() {
      var e = this.getParent().getChart().getChartStore(), i = e.getVisibleRange(), a = i.realFrom, n = i.realTo, o = a, s = n, l = n - a + 1, u = {
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
    }, t.prototype.createTicksImp = function() {
      var e, i = this.getRange(), a = i.realFrom, n = i.realTo, o = i.from, s = this.getParent().getChart().getChartStore(), l = s.getInnerFormatter().formatDate, u = s.getPeriod(), c = [], d = s.getBarSpace().bar, h = s.getStyles().xAxis.tickText, f = Math.max(qt("YYYY-MM-DD HH:mm:ss", h.size, h.weight, h.family), this.getBounding().width / Le), v = Math.ceil(f / d);
      v % 2 !== 0 && (v += 1);
      for (var p = Math.max(0, Math.floor(a / v) * v), g = p; g < n; g += v)
        if (g >= o) {
          var m = s.dataIndexToTimestamp(g);
          O(m) && c.push({
            coord: this.convertToPixel(g),
            value: m,
            text: l(m, Yo[(e = u?.type) !== null && e !== void 0 ? e : "day"], "xAxis")
          });
        }
      return dt(this.createTicks) ? this.createTicks({
        range: this.getRange(),
        bounding: this.getBounding(),
        defaultTicks: c
      }) : c;
    }, t.prototype.getAutoSize = function() {
      var e = this.getParent().getChart().getStyles(), i = e.xAxis, a = i.size;
      if (a !== "auto")
        return a;
      var n = e.crosshair, o = 0;
      i.show && (i.axisLine.show && (o += i.axisLine.size), i.tickLine.show && (o += i.tickLine.length), i.tickText.show && (o += i.tickText.marginStart + i.tickText.marginEnd + i.tickText.size));
      var s = 0;
      return n.show && n.vertical.show && n.vertical.text.show && (s += n.vertical.text.paddingTop + n.vertical.text.paddingBottom + n.vertical.text.borderSize * 2 + n.vertical.text.size), Math.max(o, s);
    }, t.prototype.getBounding = function() {
      return this.getParent().getMainWidget().getBounding();
    }, t.prototype.convertTimestampFromPixel = function(e) {
      var i = this.getParent().getChart().getChartStore(), a = i.coordinateToDataIndex(e);
      return i.dataIndexToTimestamp(a);
    }, t.prototype.convertTimestampToPixel = function(e) {
      var i = this.getParent().getChart().getChartStore(), a = i.timestampToDataIndex(e);
      return i.dataIndexToCoordinate(a);
    }, t.prototype.convertFromPixel = function(e) {
      return this.getParent().getChart().getChartStore().coordinateToDataIndex(e);
    }, t.prototype.convertToPixel = function(e) {
      return this.getParent().getChart().getChartStore().dataIndexToCoordinate(e);
    }, t.extend = function(e) {
      var i = (
        /** @class */
        (function(a) {
          X(n, a);
          function n(o) {
            return a.call(this, o, e) || this;
          }
          return n;
        })(t)
      );
      return i;
    }, t;
  })(ji)
), ss = {
  name: "normal"
}, oi = {
  normal: os.extend(ss)
};
function ls(r) {
  var t;
  return (t = oi[r]) !== null && t !== void 0 ? t : oi.normal;
}
var us = (
  /** @class */
  (function(r) {
    X(t, r);
    function t(e, i) {
      var a = r.call(this, e, i) || this;
      return a.overrideXAxis({ name: "normal", scrollZoomEnabled: !0 }), a;
    }
    return t.prototype.setOptions = function(e) {
      return r.prototype.setOptions.call(this, e);
    }, t.prototype.overrideXAxis = function(e) {
      var i = e.name;
      return (!C(this._xAxis) || C(i) && this._xAxis.name !== i) && (this._xAxis = this.createXAxisComponent(i ?? "normal")), this._xAxis.override(e), this.setAxisCursor(this._xAxis.scrollZoomEnabled), this;
    }, t.prototype.getXAxisComponent = function() {
      return this._xAxis;
    }, t.prototype.createXAxisComponent = function(e) {
      var i = ls(e);
      return new i(this);
    }, t.prototype.createMainWidget = function(e) {
      return new ns(e, this);
    }, t;
  })(Ki)
);
function cs(r, t) {
  var e = 0;
  return function() {
    var i = Date.now();
    i - e > t && (r.apply(this, arguments), e = i);
  };
}
var ds = (
  /** @class */
  (function(r) {
    X(t, r);
    function t(e, i) {
      var a = r.call(this, e, i) || this;
      return a._dragFlag = !1, a._dragStartY = 0, a._topPaneHeight = 0, a._bottomPaneHeight = 0, a._topPane = null, a._bottomPane = null, a._pressedMouseMoveEvent = cs(a._pressedTouchMouseMoveEvent, 20), a.registerEvent("touchStartEvent", a._mouseDownEvent.bind(a)).registerEvent("touchMoveEvent", a._pressedMouseMoveEvent.bind(a)).registerEvent("touchEndEvent", a._mouseUpEvent.bind(a)).registerEvent("mouseDownEvent", a._mouseDownEvent.bind(a)).registerEvent("mouseUpEvent", a._mouseUpEvent.bind(a)).registerEvent("pressedMouseMoveEvent", a._pressedMouseMoveEvent.bind(a)).registerEvent("mouseEnterEvent", a._mouseEnterEvent.bind(a)).registerEvent("mouseLeaveEvent", a._mouseLeaveEvent.bind(a)), a;
    }
    return t.prototype.getName = function() {
      return U.SEPARATOR;
    }, t.prototype._dragEnabled = function(e, i) {
      return e.getOptions().state === "normal" && i.getOptions().state === "normal" && i.getOptions().dragEnabled;
    }, t.prototype._findAdjustablePane = function(e, i) {
      for (var a = this.getPane().getChart().getDrawPanes(), n = e; n >= 0 && n < a.length; n += i) {
        var o = a[n];
        if (o.getId() !== j.X_AXIS && o.getOptions().state === "normal")
          return o;
      }
      return null;
    }, t.prototype._findDragPanes = function() {
      var e = this.getPane(), i = e.getChart().getDrawPanes(), a = i.indexOf(e.getTopPane()), n = i.indexOf(e.getBottomPane());
      if (a === -1 || n === -1)
        return null;
      var o = this._findAdjustablePane(a, -1), s = this._findAdjustablePane(n, 1);
      return C(o) && C(s) && this._dragEnabled(o, s) ? { topPane: o, bottomPane: s } : null;
    }, t.prototype._mouseDownEvent = function(e) {
      var i = this._findDragPanes();
      return C(i) ? (this._topPane = i.topPane, this._bottomPane = i.bottomPane, this._dragFlag = !0, this._dragStartY = e.pageY, this._topPaneHeight = this._topPane.getBounding().height, this._bottomPaneHeight = this._bottomPane.getBounding().height, !0) : (this._topPane = null, this._bottomPane = null, !1);
    }, t.prototype._mouseUpEvent = function() {
      return this._dragFlag = !1, this._topPane = null, this._bottomPane = null, this._topPaneHeight = 0, this._bottomPaneHeight = 0, this._mouseLeaveEvent();
    }, t.prototype._pressedTouchMouseMoveEvent = function(e) {
      var i = e.pageY - this._dragStartY, a = i < 0;
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
    }, t.prototype._mouseEnterEvent = function() {
      var e = this._findDragPanes();
      if (C(e)) {
        var i = this.getPane().getChart(), a = i.getStyles().separator;
        return this.getContainer().style.background = a.activeBackgroundColor, !0;
      }
      return !1;
    }, t.prototype._mouseLeaveEvent = function() {
      return this._dragFlag ? !1 : (this.getContainer().style.background = "transparent", !0);
    }, t.prototype.createContainer = function() {
      return te("div", {
        width: "100%",
        height: "".concat(Ye, "px"),
        margin: "0",
        padding: "0",
        position: "absolute",
        top: "-3px",
        zIndex: "20",
        boxSizing: "border-box",
        cursor: "ns-resize"
      });
    }, t.prototype.updateImp = function(e, i, a) {
      if (a === 4 || a === 2) {
        var n = this.getPane().getChart().getStyles().separator;
        e.style.top = "".concat(-Math.floor((Ye - n.size) / 2), "px"), e.style.height = "".concat(Ye, "px");
      }
    }, t;
  })(Bi)
), hs = (
  /** @class */
  (function(r) {
    X(t, r);
    function t(e, i, a, n) {
      var o = r.call(this, e, i) || this;
      return o.getContainer().style.overflow = "", o._topPane = a, o._bottomPane = n, o._separatorWidget = new ds(o.getContainer(), o), o;
    }
    return t.prototype.setBounding = function(e) {
      return ht(this.getBounding(), e), this;
    }, t.prototype.getTopPane = function() {
      return this._topPane;
    }, t.prototype.setTopPane = function(e) {
      return this._topPane = e, this;
    }, t.prototype.getBottomPane = function() {
      return this._bottomPane;
    }, t.prototype.setBottomPane = function(e) {
      return this._bottomPane = e, this;
    }, t.prototype.getWidget = function() {
      return this._separatorWidget;
    }, t.prototype.getImage = function(e) {
      var i = this.getBounding(), a = i.width, n = i.height, o = this.getChart().getStyles().separator, s = te("canvas", {
        width: "".concat(a, "px"),
        height: "".concat(n, "px"),
        boxSizing: "border-box"
      }), l = s.getContext("2d"), u = oe(s);
      return s.width = a * u, s.height = n * u, l.scale(u, u), l.fillStyle = o.color, l.fillRect(0, 0, a, n), s;
    }, t.prototype.updateImp = function(e, i, a) {
      if (e === 4 || e === 2) {
        var n = this.getChart().getStyles().separator;
        i.style.backgroundColor = n.color, i.style.height = "".concat(a.height, "px"), i.style.marginLeft = "".concat(a.left, "px"), i.style.width = "".concat(a.width, "px"), this._separatorWidget.update(e);
      }
    }, t;
  })(Zi)
);
function si() {
  return typeof window > "u" ? !1 : window.navigator.userAgent.toLowerCase().includes("firefox");
}
function ur() {
  return typeof window > "u" ? !1 : /iPhone|iPad|iPod|iOS/.test(window.navigator.userAgent);
}
function vs() {
  return /Mac|iPhone|iPad|iPod|iOS/.test(window.navigator.userAgent);
}
var je = {
  ResetClick: 500,
  LongTap: 500,
  PreventFiresTouchEvents: 500
}, Ee = {
  CancelClick: 5,
  CancelTap: 5,
  DoubleClick: 5,
  DoubleTap: 30
}, Be = {
  Left: 0,
  Middle: 1,
  Right: 2
}, fs = 10, ps = (
  /** @class */
  (function() {
    function r(t, e, i) {
      var a = this;
      this._clickCount = 0, this._clickTimeoutId = null, this._clickCoordinate = { x: Number.NEGATIVE_INFINITY, y: Number.POSITIVE_INFINITY }, this._tapCount = 0, this._tapTimeoutId = null, this._tapCoordinate = { x: Number.NEGATIVE_INFINITY, y: Number.POSITIVE_INFINITY }, this._longTapTimeoutId = null, this._longTapActive = !1, this._mouseMoveStartCoordinate = null, this._touchMoveStartCoordinate = null, this._touchMoveExceededManhattanDistance = !1, this._cancelClick = !1, this._cancelTap = !1, this._unsubscribeOutsideMouseEvents = null, this._unsubscribeOutsideTouchEvents = null, this._unsubscribeMobileSafariEvents = null, this._unsubscribeMousemove = null, this._unsubscribeMouseWheel = null, this._unsubscribeContextMenu = null, this._unsubscribeRootMouseEvents = null, this._unsubscribeRootTouchEvents = null, this._startPinchMiddleCoordinate = null, this._startPinchDistance = 0, this._pinchPrevented = !1, this._preventTouchDragProcess = !1, this._mousePressed = !1, this._lastTouchEventTimeStamp = 0, this._activeTouchId = null, this._acceptMouseLeave = !ur(), this._onFirefoxOutsideMouseUp = function(n) {
        a._mouseUpHandler(n);
      }, this._onMobileSafariDoubleClick = function(n) {
        if (a._firesTouchEvents(n)) {
          if (++a._tapCount, a._tapTimeoutId !== null && a._tapCount > 1) {
            var o = a._mouseTouchMoveWithDownInfo(a._getCoordinate(n), a._tapCoordinate).manhattanDistance;
            o < Ee.DoubleTap && !a._cancelTap && a._processEvent(a._makeCompatEvent(n), a._handler.doubleTapEvent), a._resetTapTimeout();
          }
        } else if (++a._clickCount, a._clickTimeoutId !== null && a._clickCount > 1) {
          var o = a._mouseTouchMoveWithDownInfo(a._getCoordinate(n), a._clickCoordinate).manhattanDistance;
          o < Ee.DoubleClick && !a._cancelClick && a._processEvent(a._makeCompatEvent(n), a._handler.mouseDoubleClickEvent), a._resetClickTimeout();
        }
      }, this._target = t, this._handler = e, this._options = i, this._init();
    }
    return r.prototype.destroy = function() {
      this._unsubscribeOutsideMouseEvents !== null && (this._unsubscribeOutsideMouseEvents(), this._unsubscribeOutsideMouseEvents = null), this._unsubscribeOutsideTouchEvents !== null && (this._unsubscribeOutsideTouchEvents(), this._unsubscribeOutsideTouchEvents = null), this._unsubscribeMousemove !== null && (this._unsubscribeMousemove(), this._unsubscribeMousemove = null), this._unsubscribeMouseWheel !== null && (this._unsubscribeMouseWheel(), this._unsubscribeMouseWheel = null), this._unsubscribeContextMenu !== null && (this._unsubscribeContextMenu(), this._unsubscribeContextMenu = null), this._unsubscribeRootMouseEvents !== null && (this._unsubscribeRootMouseEvents(), this._unsubscribeRootMouseEvents = null), this._unsubscribeRootTouchEvents !== null && (this._unsubscribeRootTouchEvents(), this._unsubscribeRootTouchEvents = null), this._unsubscribeMobileSafariEvents !== null && (this._unsubscribeMobileSafariEvents(), this._unsubscribeMobileSafariEvents = null), this._clearLongTapTimeout(), this._resetClickTimeout();
    }, r.prototype._mouseEnterHandler = function(t) {
      var e = this, i, a, n;
      (i = this._unsubscribeMousemove) === null || i === void 0 || i.call(this), (a = this._unsubscribeMouseWheel) === null || a === void 0 || a.call(this), (n = this._unsubscribeContextMenu) === null || n === void 0 || n.call(this);
      var o = this._mouseMoveHandler.bind(this);
      this._unsubscribeMousemove = function() {
        e._target.removeEventListener("mousemove", o);
      }, this._target.addEventListener("mousemove", o);
      var s = this._mouseWheelHandler.bind(this);
      this._unsubscribeMouseWheel = function() {
        e._target.removeEventListener("wheel", s);
      }, this._target.addEventListener("wheel", s, { passive: !1 });
      var l = this._contextMenuHandler.bind(this);
      this._unsubscribeContextMenu = function() {
        e._target.removeEventListener("contextmenu", l);
      }, this._target.addEventListener("contextmenu", l, { passive: !1 }), !this._firesTouchEvents(t) && (this._processEvent(this._makeCompatEvent(t), this._handler.mouseEnterEvent), this._acceptMouseLeave = !0);
    }, r.prototype._resetClickTimeout = function() {
      this._clickTimeoutId !== null && clearTimeout(this._clickTimeoutId), this._clickCount = 0, this._clickTimeoutId = null, this._clickCoordinate = { x: Number.NEGATIVE_INFINITY, y: Number.POSITIVE_INFINITY };
    }, r.prototype._resetTapTimeout = function() {
      this._tapTimeoutId !== null && clearTimeout(this._tapTimeoutId), this._tapCount = 0, this._tapTimeoutId = null, this._tapCoordinate = { x: Number.NEGATIVE_INFINITY, y: Number.POSITIVE_INFINITY };
    }, r.prototype._mouseMoveHandler = function(t) {
      this._mousePressed || this._touchMoveStartCoordinate !== null || this._firesTouchEvents(t) || (this._processEvent(this._makeCompatEvent(t), this._handler.mouseMoveEvent), this._acceptMouseLeave = !0);
    }, r.prototype._mouseWheelHandler = function(t) {
      if (Math.abs(t.deltaX) > Math.abs(t.deltaY)) {
        if (!C(this._handler.mouseWheelHortEvent) || (this._preventDefault(t), Math.abs(t.deltaX) === 0))
          return;
        this._handler.mouseWheelHortEvent(this._makeCompatEvent(t), -t.deltaX);
      } else {
        if (!C(this._handler.mouseWheelVertEvent))
          return;
        var e = -(t.deltaY / 100);
        if (e === 0)
          return;
        switch (this._preventDefault(t), t.deltaMode) {
          case t.DOM_DELTA_PAGE: {
            e *= 120;
            break;
          }
          case t.DOM_DELTA_LINE: {
            e *= 32;
            break;
          }
        }
        if (e !== 0) {
          var i = Math.sign(e) * Math.min(1, Math.abs(e));
          this._handler.mouseWheelVertEvent(this._makeCompatEvent(t), i);
        }
      }
    }, r.prototype._contextMenuHandler = function(t) {
      this._preventDefault(t);
    }, r.prototype._touchMoveHandler = function(t) {
      var e = this._touchWithId(t.changedTouches, this._activeTouchId);
      if (e !== null && (this._lastTouchEventTimeStamp = this._eventTimeStamp(t), this._startPinchMiddleCoordinate === null && !this._preventTouchDragProcess)) {
        this._pinchPrevented = !0;
        var i = this._mouseTouchMoveWithDownInfo(this._getCoordinate(e), this._touchMoveStartCoordinate), a = i.xOffset, n = i.yOffset, o = i.manhattanDistance;
        if (!(!this._touchMoveExceededManhattanDistance && o < Ee.CancelTap)) {
          if (!this._touchMoveExceededManhattanDistance) {
            var s = a * 0.5, l = n >= s && !this._options.treatVertDragAsPageScroll(), u = s > n && !this._options.treatHorzDragAsPageScroll();
            !l && !u && (this._preventTouchDragProcess = !0), this._touchMoveExceededManhattanDistance = !0, this._cancelTap = !0, this._clearLongTapTimeout(), this._resetTapTimeout();
          }
          this._preventTouchDragProcess || this._processEvent(this._makeCompatEvent(t, e), this._handler.touchMoveEvent);
        }
      }
    }, r.prototype._mouseMoveWithDownHandler = function(t) {
      if (t.button === Be.Left) {
        var e = this._mouseTouchMoveWithDownInfo(this._getCoordinate(t), this._mouseMoveStartCoordinate), i = e.manhattanDistance;
        i >= Ee.CancelClick && (this._cancelClick = !0, this._resetClickTimeout()), this._cancelClick && this._processEvent(this._makeCompatEvent(t), this._handler.pressedMouseMoveEvent);
      }
    }, r.prototype._mouseTouchMoveWithDownInfo = function(t, e) {
      var i = Math.abs(e.x - t.x), a = Math.abs(e.y - t.y), n = i + a;
      return { xOffset: i, yOffset: a, manhattanDistance: n };
    }, r.prototype._touchEndHandler = function(t) {
      var e = this._touchWithId(t.changedTouches, this._activeTouchId);
      if (e === null && t.touches.length === 0 && (e = t.changedTouches[0]), e !== null) {
        this._activeTouchId = null, this._lastTouchEventTimeStamp = this._eventTimeStamp(t), this._clearLongTapTimeout(), this._touchMoveStartCoordinate = null, this._unsubscribeRootTouchEvents !== null && (this._unsubscribeRootTouchEvents(), this._unsubscribeRootTouchEvents = null);
        var i = this._makeCompatEvent(t, e);
        if (this._processEvent(i, this._handler.touchEndEvent), ++this._tapCount, this._tapTimeoutId !== null && this._tapCount > 1) {
          var a = this._mouseTouchMoveWithDownInfo(this._getCoordinate(e), this._tapCoordinate).manhattanDistance;
          a < Ee.DoubleTap && !this._cancelTap && this._processEvent(i, this._handler.doubleTapEvent), this._resetTapTimeout();
        } else
          this._cancelTap || (this._processEvent(i, this._handler.tapEvent), C(this._handler.tapEvent) && this._preventDefault(t));
        this._tapCount === 0 && this._preventDefault(t), t.touches.length === 0 && this._longTapActive && (this._longTapActive = !1, this._preventDefault(t));
      }
    }, r.prototype._mouseUpHandler = function(t) {
      if (t.button === Be.Left) {
        var e = this._makeCompatEvent(t);
        if (this._mouseMoveStartCoordinate = null, this._mousePressed = !1, this._unsubscribeRootMouseEvents !== null && (this._unsubscribeRootMouseEvents(), this._unsubscribeRootMouseEvents = null), si()) {
          var i = this._target.ownerDocument.documentElement;
          i.removeEventListener("mouseleave", this._onFirefoxOutsideMouseUp);
        }
        if (!this._firesTouchEvents(t))
          if (this._processEvent(e, this._handler.mouseUpEvent), ++this._clickCount, this._clickTimeoutId !== null && this._clickCount > 1) {
            var a = this._mouseTouchMoveWithDownInfo(this._getCoordinate(t), this._clickCoordinate).manhattanDistance;
            a < Ee.DoubleClick && !this._cancelClick && this._processEvent(e, this._handler.mouseDoubleClickEvent), this._resetClickTimeout();
          } else
            this._cancelClick || this._processEvent(e, this._handler.mouseClickEvent);
      }
    }, r.prototype._clearLongTapTimeout = function() {
      this._longTapTimeoutId !== null && (clearTimeout(this._longTapTimeoutId), this._longTapTimeoutId = null);
    }, r.prototype._touchStartHandler = function(t) {
      if (this._activeTouchId === null) {
        var e = t.changedTouches[0];
        this._activeTouchId = e.identifier, this._lastTouchEventTimeStamp = this._eventTimeStamp(t);
        var i = this._target.ownerDocument.documentElement;
        this._cancelTap = !1, this._touchMoveExceededManhattanDistance = !1, this._preventTouchDragProcess = !1, this._touchMoveStartCoordinate = this._getCoordinate(e), this._unsubscribeRootTouchEvents !== null && (this._unsubscribeRootTouchEvents(), this._unsubscribeRootTouchEvents = null);
        {
          var a = this._touchMoveHandler.bind(this), n = this._touchEndHandler.bind(this);
          this._unsubscribeRootTouchEvents = function() {
            i.removeEventListener("touchmove", a), i.removeEventListener("touchend", n);
          }, i.addEventListener("touchmove", a, { passive: !1 }), i.addEventListener("touchend", n, { passive: !1 }), this._clearLongTapTimeout(), this._longTapTimeoutId = setTimeout(this._longTapHandler.bind(this, t), je.LongTap);
        }
        this._processEvent(this._makeCompatEvent(t, e), this._handler.touchStartEvent), this._tapTimeoutId === null && (this._tapCount = 0, this._tapTimeoutId = setTimeout(this._resetTapTimeout.bind(this), je.ResetClick), this._tapCoordinate = this._getCoordinate(e));
      }
    }, r.prototype._mouseDownHandler = function(t) {
      if (t.button === Be.Right) {
        this._preventDefault(t), this._processEvent(this._makeCompatEvent(t), this._handler.mouseRightClickEvent);
        return;
      }
      if (t.button === Be.Left) {
        var e = this._target.ownerDocument.documentElement;
        si() && e.addEventListener("mouseleave", this._onFirefoxOutsideMouseUp), this._cancelClick = !1, this._mouseMoveStartCoordinate = this._getCoordinate(t), this._unsubscribeRootMouseEvents !== null && (this._unsubscribeRootMouseEvents(), this._unsubscribeRootMouseEvents = null);
        {
          var i = this._mouseMoveWithDownHandler.bind(this), a = this._mouseUpHandler.bind(this);
          this._unsubscribeRootMouseEvents = function() {
            e.removeEventListener("mousemove", i), e.removeEventListener("mouseup", a);
          }, e.addEventListener("mousemove", i), e.addEventListener("mouseup", a);
        }
        this._mousePressed = !0, !this._firesTouchEvents(t) && (this._processEvent(this._makeCompatEvent(t), this._handler.mouseDownEvent), this._clickTimeoutId === null && (this._clickCount = 0, this._clickTimeoutId = setTimeout(this._resetClickTimeout.bind(this), je.ResetClick), this._clickCoordinate = this._getCoordinate(t)));
      }
    }, r.prototype._init = function() {
      var t = this;
      this._target.addEventListener("mouseenter", this._mouseEnterHandler.bind(this)), this._target.addEventListener("touchcancel", this._clearLongTapTimeout.bind(this));
      {
        var e = this._target.ownerDocument, i = function(a) {
          t._handler.mouseDownOutsideEvent != null && (a.composed && t._target.contains(a.composedPath()[0]) || a.target !== null && t._target.contains(a.target) || t._handler.mouseDownOutsideEvent({ x: 0, y: 0, pageX: 0, pageY: 0 }));
        };
        this._unsubscribeOutsideTouchEvents = function() {
          e.removeEventListener("touchstart", i);
        }, this._unsubscribeOutsideMouseEvents = function() {
          e.removeEventListener("mousedown", i);
        }, e.addEventListener("mousedown", i), e.addEventListener("touchstart", i, { passive: !0 });
      }
      ur() && (this._unsubscribeMobileSafariEvents = function() {
        t._target.removeEventListener("dblclick", t._onMobileSafariDoubleClick);
      }, this._target.addEventListener("dblclick", this._onMobileSafariDoubleClick)), this._target.addEventListener("mouseleave", this._mouseLeaveHandler.bind(this)), this._target.addEventListener("touchstart", this._touchStartHandler.bind(this), { passive: !0 }), this._target.addEventListener("mousedown", function(a) {
        if (a.button === Be.Middle)
          return a.preventDefault(), !1;
      }), this._target.addEventListener("mousedown", this._mouseDownHandler.bind(this)), this._initPinch(), this._target.addEventListener("touchmove", function() {
      }, { passive: !1 });
    }, r.prototype._initPinch = function() {
      var t = this;
      !C(this._handler.pinchStartEvent) && !C(this._handler.pinchEvent) && !C(this._handler.pinchEndEvent) || (this._target.addEventListener("touchstart", function(e) {
        t._checkPinchState(e.touches);
      }, { passive: !0 }), this._target.addEventListener("touchmove", function(e) {
        if (!(e.touches.length !== 2 || t._startPinchMiddleCoordinate === null) && C(t._handler.pinchEvent)) {
          var i = t._getTouchDistance(e.touches[0], e.touches[1]), a = i / t._startPinchDistance;
          t._handler.pinchEvent(M(M({}, t._startPinchMiddleCoordinate), { pageX: 0, pageY: 0 }), a), t._preventDefault(e);
        }
      }, { passive: !1 }), this._target.addEventListener("touchend", function(e) {
        t._checkPinchState(e.touches);
      }));
    }, r.prototype._checkPinchState = function(t) {
      t.length === 1 && (this._pinchPrevented = !1), t.length !== 2 || this._pinchPrevented || this._longTapActive ? this._stopPinch() : this._startPinch(t);
    }, r.prototype._startPinch = function(t) {
      var e = this._target.getBoundingClientRect();
      this._startPinchMiddleCoordinate = {
        x: (t[0].clientX - e.left + (t[1].clientX - e.left)) / 2,
        y: (t[0].clientY - e.top + (t[1].clientY - e.top)) / 2
      }, this._startPinchDistance = this._getTouchDistance(t[0], t[1]), C(this._handler.pinchStartEvent) && this._handler.pinchStartEvent({ x: 0, y: 0, pageX: 0, pageY: 0 }), this._clearLongTapTimeout();
    }, r.prototype._stopPinch = function() {
      this._startPinchMiddleCoordinate !== null && (this._startPinchMiddleCoordinate = null, C(this._handler.pinchEndEvent) && this._handler.pinchEndEvent({ x: 0, y: 0, pageX: 0, pageY: 0 }));
    }, r.prototype._mouseLeaveHandler = function(t) {
      var e, i, a;
      (e = this._unsubscribeMousemove) === null || e === void 0 || e.call(this), (i = this._unsubscribeMouseWheel) === null || i === void 0 || i.call(this), (a = this._unsubscribeContextMenu) === null || a === void 0 || a.call(this), !this._firesTouchEvents(t) && this._acceptMouseLeave && (this._processEvent(this._makeCompatEvent(t), this._handler.mouseLeaveEvent), this._acceptMouseLeave = !ur());
    }, r.prototype._longTapHandler = function(t) {
      var e = this._touchWithId(t.touches, this._activeTouchId);
      e !== null && (this._processEvent(this._makeCompatEvent(t, e), this._handler.longTapEvent), this._cancelTap = !0, this._longTapActive = !0);
    }, r.prototype._firesTouchEvents = function(t) {
      var e;
      return C((e = t.sourceCapabilities) === null || e === void 0 ? void 0 : e.firesTouchEvents) ? t.sourceCapabilities.firesTouchEvents : this._eventTimeStamp(t) < this._lastTouchEventTimeStamp + je.PreventFiresTouchEvents;
    }, r.prototype._processEvent = function(t, e) {
      e?.call(this._handler, t);
    }, r.prototype._makeCompatEvent = function(t, e) {
      var i = this, a = e ?? t, n = this._target.getBoundingClientRect();
      return {
        x: a.clientX - n.left,
        y: a.clientY - n.top,
        pageX: a.pageX,
        pageY: a.pageY,
        isTouch: !t.type.startsWith("mouse") && t.type !== "contextmenu" && t.type !== "click" && t.type !== "wheel",
        preventDefault: function() {
          t.type !== "touchstart" && i._preventDefault(t);
        }
      };
    }, r.prototype._getTouchDistance = function(t, e) {
      var i = t.clientX - e.clientX, a = t.clientY - e.clientY;
      return Math.sqrt(i * i + a * a);
    }, r.prototype._preventDefault = function(t) {
      t.cancelable && t.preventDefault();
    }, r.prototype._getCoordinate = function(t) {
      return {
        x: t.pageX,
        y: t.pageY
      };
    }, r.prototype._eventTimeStamp = function(t) {
      var e;
      return (e = t.timeStamp) !== null && e !== void 0 ? e : performance.now();
    }, r.prototype._touchWithId = function(t, e) {
      for (var i = 0; i < t.length; ++i)
        if (t[i].identifier === e)
          return t[i];
      return null;
    }, r;
  })()
), li = {
  name: "scrollLeft",
  keys: "Shift+ArrowLeft",
  action: function(r) {
    var t = r.chart;
    t.scrollByDistance(-3 * t.getBarSpace().bar);
  }
}, ui = {
  name: "scrollRight",
  keys: "Shift+ArrowRight",
  action: function(r) {
    var t = r.chart;
    t.scrollByDistance(3 * t.getBarSpace().bar);
  }
}, ci = {
  name: "zoomIn",
  keys: ["Shift+Equal", "Shift+NumpadAdd"],
  action: function(r) {
    var t = r.chart;
    t.zoomAtCoordinate(1.05);
  }
}, di = {
  name: "zoomOut",
  keys: ["Shift+Minus", "Shift+NumpadSubtract"],
  action: function(r) {
    var t = r.chart;
    t.zoomAtCoordinate(0.95);
  }
}, Ie, Qi = (Ie = {}, Ie[li.name] = li, Ie[ui.name] = ui, Ie[ci.name] = ci, Ie[di.name] = di, Ie);
function gs(r) {
  var t;
  return (t = Qi[r]) !== null && t !== void 0 ? t : null;
}
function ms() {
  return Object.keys(Qi);
}
var _s = {
  command: "meta",
  cmd: "meta",
  control: "ctrl",
  option: "alt",
  mod: vs() ? "meta" : "ctrl"
}, hi = {
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
}, cr = ["ctrl", "alt", "shift", "meta"], ys = (
  /** @class */
  (function() {
    function r(t, e) {
      var i = this;
      this._flingStartTime = (/* @__PURE__ */ new Date()).getTime(), this._flingScrollRequestId = null, this._startScrollCoordinate = null, this._touchCoordinate = null, this._touchCancelCrosshair = !1, this._touchZoomed = !1, this._pinchScale = 1, this._mouseDownWidget = null, this._prevYAxisRanges = /* @__PURE__ */ new Map(), this._xAxisStartScaleCoordinate = null, this._xAxisStartScaleDistance = 0, this._xAxisScale = 1, this._yAxisStartScaleDistance = 0, this._mouseMoveTriggerWidgetInfo = { pane: null, widget: null }, this._boundKeyBoardDownEvent = function(a) {
        var n, o, s, l = a.target, u = l?.tagName.toLowerCase();
        if (!(u === "input" || u === "textarea" || l?.isContentEditable === !0)) {
          var c = i._chart.getHotKey(), d = c.enabled, h = c.exclude;
          if (d) {
            var f = [];
            a.ctrlKey && f.push("ctrl"), a.altKey && f.push("alt"), a.shiftKey && f.push("shift"), a.metaKey && f.push("meta");
            var v = a.code.trim().toLowerCase();
            /^key[a-z]$/.test(v) ? f.push(v.slice(3)) : /^digit[0-9]$/.test(v) ? f.push(v.slice(5)) : f.push((n = hi[v]) !== null && n !== void 0 ? n : v);
            for (var p = f.join("+"), g = ms(), m = g.length - 1; m >= 0; m--) {
              var x = g[m], y = gs(x);
              if (!h.includes(x) && C(y)) {
                var E = $t(y.keys) ? y.keys : [y.keys], _ = E.some(function(w) {
                  var b = [], S = "";
                  return w.replace(/\+\+$/, "+Plus").replace(/\+=$/, "+Equal").split("+").forEach(function(T) {
                    var A, k, F = (A = _s[T.trim().toLowerCase()]) !== null && A !== void 0 ? A : T, P = F.trim().toLowerCase(), D = "";
                    /^key[a-z]$/.test(P) ? D = P.slice(3) : /^digit[0-9]$/.test(P) ? D = P.slice(5) : D = (k = hi[P]) !== null && k !== void 0 ? k : P, cr.includes(D) ? b.includes(D) || b.push(D) : D.length > 0 && (S = D);
                  }), b.sort(function(T, A) {
                    return cr.indexOf(T) - cr.indexOf(A);
                  }), Pe(Pe([], Re(b), !1), [S], !1).filter(function(T) {
                    return T.length > 0;
                  }).join("+") === p;
                });
                if (_) {
                  var I = { chart: i._chart, event: a, key: p, hotkey: y };
                  if (!dt(y.check) || y.check(I)) {
                    (!((o = y.preventDefault) !== null && o !== void 0) || o) && a.preventDefault(), (s = y.stopPropagation) !== null && s !== void 0 && s && a.stopPropagation(), y.action(I);
                    return;
                  }
                }
              }
            }
          }
        }
      }, this._chart = e, this._event = new ps(t, this, {
        treatVertDragAsPageScroll: function() {
          return !1;
        },
        treatHorzDragAsPageScroll: function() {
          return !1;
        }
      }), document.addEventListener("keydown", this._boundKeyBoardDownEvent);
    }
    return r.prototype._getYAxisByWidget = function(t) {
      return t.getName() === U.Y_AXIS ? t.getAxisComponent() : t.getPane().getYAxisComponentById();
    }, r.prototype._getYAxisScaleTargetByWidget = function(t) {
      var e = this._getYAxisByWidget(t), i = t.getPane();
      return i.isManualYAxis(e.id) ? i.getYAxisComponentById() : e;
    }, r.prototype._syncYAxisValueRange = function(t, e) {
      var i = t.getRange(), a = e.from, n = e.to, o = t.valueToRealValue(a, { range: i }), s = t.valueToRealValue(n, { range: i }), l = t.realValueToDisplayValue(o, { range: i }), u = t.realValueToDisplayValue(s, { range: i });
      t.setRange({
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
    }, r.prototype._syncManualYAxesValueRange = function(t, e) {
      var i = this, a = e.getRange();
      t.getPane().getYAxisComponents().forEach(function(n) {
        var o = n;
        o !== e && t.getPane().isManualYAxis(o.id) && i._syncYAxisValueRange(o, a);
      });
    }, r.prototype._resetYAxisAndManualYAxes = function(t, e) {
      e.setAutoCalcTickFlag(!0), t.getPane().getYAxisComponents().forEach(function(i) {
        var a = i;
        t.getPane().isManualYAxis(a.id) && a.setAutoCalcTickFlag(!0);
      }), this._chart.layout({
        measureWidth: !0,
        update: !0,
        buildYAxisTick: !0
      });
    }, r.prototype.pinchStartEvent = function() {
      return this._touchZoomed = !0, this._pinchScale = 1, !0;
    }, r.prototype.pinchEvent = function(t, e) {
      var i = this._findWidgetByEvent(t), a = i.pane, n = i.widget;
      if (a?.getId() !== j.X_AXIS && n?.getName() === U.MAIN) {
        var o = this._makeWidgetEvent(t, n), s = (e - this._pinchScale) * 5;
        return this._pinchScale = e, this._chart.getChartStore().zoom(s, { x: o.x, y: o.y }, "main"), !0;
      }
      return !1;
    }, r.prototype.mouseWheelHortEvent = function(t, e) {
      var i = this._chart.getChartStore();
      return i.startScroll(), i.scroll(e), !0;
    }, r.prototype.mouseWheelVertEvent = function(t, e) {
      var i = this._findWidgetByEvent(t).widget, a = this._makeWidgetEvent(t, i), n = i?.getName();
      if (n === U.MAIN)
        return this._chart.getChartStore().zoom(e, { x: a.x, y: a.y }, "main"), !0;
      if (n === U.Y_AXIS) {
        var o = i, s = this._getYAxisByWidget(o);
        if (s.scrollZoomEnabled) {
          var l = 1 + e * 0.05, u = this._getYAxisScaleTargetByWidget(o);
          return this._zoomYAxis(u, l), this._syncManualYAxesValueRange(o, u), !0;
        }
      }
      return !1;
    }, r.prototype.mouseDownEvent = function(t) {
      var e, i, a = this._findWidgetByEvent(t), n = a.pane, o = a.widget;
      if (this._mouseDownWidget = o, o !== null) {
        var s = this._makeWidgetEvent(t, o), l = o.getName();
        switch (l) {
          case U.SEPARATOR:
            return o.dispatchEvent("mouseDownEvent", s);
          case U.MAIN: {
            var u = o.dispatchEvent("mouseDownEvent", s);
            if (!u) {
              var c = n.getYAxisComponents();
              try {
                for (var d = Ct(c), h = d.next(); !h.done; h = d.next()) {
                  var f = h.value, v = f;
                  if (!v.getAutoCalcTickFlag()) {
                    var p = v.getRange();
                    this._prevYAxisRanges.set(v, M({}, p));
                  }
                }
              } catch (g) {
                e = { error: g };
              } finally {
                try {
                  h && !h.done && (i = d.return) && i.call(d);
                } finally {
                  if (e) throw e.error;
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
    }, r.prototype.mouseMoveEvent = function(t) {
      var e, i, a, n = this._findWidgetByEvent(t), o = n.pane, s = n.widget, l = this._makeWidgetEvent(t, s);
      if ((((e = this._mouseMoveTriggerWidgetInfo.pane) === null || e === void 0 ? void 0 : e.getId()) !== o?.getId() || ((i = this._mouseMoveTriggerWidgetInfo.widget) === null || i === void 0 ? void 0 : i.getName()) !== s?.getName()) && (s?.dispatchEvent("mouseEnterEvent", l), (a = this._mouseMoveTriggerWidgetInfo.widget) === null || a === void 0 || a.dispatchEvent("mouseLeaveEvent", l), this._mouseMoveTriggerWidgetInfo = { pane: o, widget: s }), s !== null) {
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
    }, r.prototype.pressedMouseMoveEvent = function(t) {
      var e, i;
      if (this._mouseDownWidget !== null && this._mouseDownWidget.getName() === U.SEPARATOR)
        return this._mouseDownWidget.dispatchEvent("pressedMouseMoveEvent", t);
      var a = this._findWidgetByEvent(t), n = a.pane, o = a.widget;
      if (o !== null && ((e = this._mouseDownWidget) === null || e === void 0 ? void 0 : e.getPane().getId()) === n?.getId() && ((i = this._mouseDownWidget) === null || i === void 0 ? void 0 : i.getName()) === o.getName()) {
        var s = this._makeWidgetEvent(t, o), l = o.getName();
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
    }, r.prototype.mouseUpEvent = function(t) {
      var e = this._findWidgetByEvent(t).widget, i = !1;
      if (e !== null) {
        var a = this._makeWidgetEvent(t, e), n = e.getName();
        switch (n) {
          case U.MAIN:
          case U.SEPARATOR:
          case U.X_AXIS:
          case U.Y_AXIS: {
            i = e.dispatchEvent("mouseUpEvent", a);
            break;
          }
        }
        i && this._chart.updatePane(
          1
          /* UpdateLevel.Overlay */
        );
      }
      return this._mouseDownWidget = null, this._startScrollCoordinate = null, this._prevYAxisRanges.clear(), this._xAxisStartScaleCoordinate = null, this._xAxisStartScaleDistance = 0, this._xAxisScale = 1, this._yAxisStartScaleDistance = 0, i;
    }, r.prototype.mouseClickEvent = function(t) {
      var e = this._findWidgetByEvent(t).widget;
      if (e !== null) {
        var i = this._makeWidgetEvent(t, e);
        return e.dispatchEvent("mouseClickEvent", i);
      }
      return !1;
    }, r.prototype.mouseRightClickEvent = function(t) {
      var e = this._findWidgetByEvent(t).widget, i = !1;
      if (e !== null) {
        var a = this._makeWidgetEvent(t, e), n = e.getName();
        switch (n) {
          case U.MAIN:
          case U.X_AXIS:
          case U.Y_AXIS: {
            i = e.dispatchEvent("mouseRightClickEvent", a);
            break;
          }
        }
        i && this._chart.updatePane(
          1
          /* UpdateLevel.Overlay */
        );
      }
      return !1;
    }, r.prototype.mouseDoubleClickEvent = function(t) {
      var e = this._findWidgetByEvent(t).widget;
      if (e !== null) {
        var i = e.getName();
        switch (i) {
          case U.MAIN: {
            var a = this._makeWidgetEvent(t, e);
            return e.dispatchEvent("mouseDoubleClickEvent", a);
          }
          case U.Y_AXIS: {
            var n = e, o = this._getYAxisByWidget(n), s = this._getYAxisScaleTargetByWidget(n);
            if (!s.getAutoCalcTickFlag() || !o.getAutoCalcTickFlag())
              return this._resetYAxisAndManualYAxes(n, s), !0;
            break;
          }
        }
      }
      return !1;
    }, r.prototype.mouseLeaveEvent = function() {
      return this._chart.getChartStore().setCrosshair(), !0;
    }, r.prototype.touchStartEvent = function(t) {
      var e, i, a, n = this._findWidgetByEvent(t), o = n.pane, s = n.widget;
      if (s !== null) {
        var l = this._makeWidgetEvent(t, s);
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
            this._flingScrollRequestId !== null && (gr(this._flingScrollRequestId), this._flingScrollRequestId = null), this._flingStartTime = (/* @__PURE__ */ new Date()).getTime();
            var d = o.getYAxisComponents();
            try {
              for (var h = Ct(d), f = h.next(); !f.done; f = h.next()) {
                var v = f.value, p = v;
                if (!p.getAutoCalcTickFlag()) {
                  var g = p.getRange();
                  this._prevYAxisRanges.set(p, M({}, g));
                }
              }
            } catch (E) {
              e = { error: E };
            } finally {
              try {
                f && !f.done && (i = h.return) && i.call(h);
              } finally {
                if (e) throw e.error;
              }
            }
            if (this._startScrollCoordinate = { x: l.x, y: l.y }, c.startScroll(), this._touchZoomed = !1, this._touchCoordinate !== null) {
              var m = l.x - this._touchCoordinate.x, x = l.y - this._touchCoordinate.y, y = Math.sqrt(m * m + x * x);
              y < fs ? (this._touchCoordinate = { x: l.x, y: l.y }, c.setCrosshair({ x: l.x, y: l.y, paneId: o?.getId() })) : (this._touchCoordinate = null, this._touchCancelCrosshair = !0, c.setCrosshair());
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
    }, r.prototype.touchMoveEvent = function(t) {
      var e, i, a, n = this._findWidgetByEvent(t), o = n.pane, s = n.widget;
      if (s !== null) {
        var l = this._makeWidgetEvent(t, s), u = s.getName(), c = this._chart.getChartStore();
        switch (u) {
          case U.MAIN:
            return s.dispatchEvent("pressedMouseMoveEvent", l) ? ((e = l.preventDefault) === null || e === void 0 || e.call(l), c.setCrosshair(void 0, { notInvalidate: !0 }), this._chart.updatePane(
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
    }, r.prototype.touchEndEvent = function(t) {
      var e = this, i = this._findWidgetByEvent(t).widget;
      if (i !== null) {
        var a = this._makeWidgetEvent(t, i), n = i.getName();
        switch (n) {
          case U.MAIN: {
            if (i.dispatchEvent("mouseUpEvent", a), this._startScrollCoordinate !== null) {
              var o = (/* @__PURE__ */ new Date()).getTime() - this._flingStartTime, s = a.x - this._startScrollCoordinate.x, l = s / (o > 0 ? o : 1) * 20;
              if (o < 200 && Math.abs(l) > 0) {
                var u = this._chart.getChartStore(), c = function() {
                  e._flingScrollRequestId = qe(function() {
                    u.startScroll(), u.scroll(l), l = l * (1 - 0.025), Math.abs(l) < 1 ? e._flingScrollRequestId !== null && (gr(e._flingScrollRequestId), e._flingScrollRequestId = null) : c();
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
    }, r.prototype.tapEvent = function(t) {
      var e = this._findWidgetByEvent(t), i = e.pane, a = e.widget, n = !1;
      if (a !== null) {
        var o = this._makeWidgetEvent(t, a), s = a.dispatchEvent("mouseClickEvent", o);
        if (a.getName() === U.MAIN) {
          var l = this._makeWidgetEvent(t, a), u = this._chart.getChartStore();
          s ? (this._touchCancelCrosshair = !0, this._touchCoordinate = null, u.setCrosshair(void 0, { notInvalidate: !0 }), n = !0) : (!this._touchCancelCrosshair && !this._touchZoomed && (this._touchCoordinate = { x: l.x, y: l.y }, u.setCrosshair({ x: l.x, y: l.y, paneId: i?.getId() }, { notInvalidate: !0 }), n = !0), this._touchCancelCrosshair = !1);
        }
        (n || s) && this._chart.updatePane(
          1
          /* UpdateLevel.Overlay */
        );
      }
      return n;
    }, r.prototype.doubleTapEvent = function(t) {
      return this.mouseDoubleClickEvent(t);
    }, r.prototype.longTapEvent = function(t) {
      var e = this._findWidgetByEvent(t), i = e.pane, a = e.widget;
      if (a !== null && a.getName() === U.MAIN) {
        var n = this._makeWidgetEvent(t, a);
        return this._touchCoordinate = { x: n.x, y: n.y }, this._chart.getChartStore().setCrosshair({ x: n.x, y: n.y, paneId: i?.getId() }), !0;
      }
      return !1;
    }, r.prototype._processMainScrollingEvent = function(t, e) {
      var i, a, n;
      if (this._startScrollCoordinate !== null) {
        var o = t.getPane().getYAxisComponents();
        try {
          for (var s = Ct(o), l = s.next(); !l.done; l = s.next()) {
            var u = l.value, c = u, d = this._prevYAxisRanges.get(c);
            if (C(d) && !c.getAutoCalcTickFlag() && c.scrollZoomEnabled) {
              (n = e.preventDefault) === null || n === void 0 || n.call(e);
              var h = d.from, f = d.to, v = d.range, p = 0;
              c.reverse ? p = this._startScrollCoordinate.y - e.y : p = e.y - this._startScrollCoordinate.y;
              var g = t.getBounding(), m = p / g.height, x = v * m, y = h + x, E = f + x, _ = c.valueToRealValue(y, { range: d }), I = c.valueToRealValue(E, { range: d }), w = c.realValueToDisplayValue(_, { range: d }), b = c.realValueToDisplayValue(I, { range: d });
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
        var S = e.x - this._startScrollCoordinate.x;
        this._chart.getChartStore().scroll(S);
      }
    }, r.prototype._processXAxisScrollStartEvent = function(t, e) {
      var i = t.dispatchEvent("mouseDownEvent", e);
      return i && this._chart.updatePane(
        1
        /* UpdateLevel.Overlay */
      ), this._xAxisStartScaleCoordinate = { x: e.x, y: e.y }, this._xAxisStartScaleDistance = e.pageX, i;
    }, r.prototype._processXAxisScrollingEvent = function(t, e) {
      var i = t.dispatchEvent("pressedMouseMoveEvent", e);
      if (i)
        this._chart.updatePane(
          1
          /* UpdateLevel.Overlay */
        );
      else {
        var a = t.getPane().getXAxisComponent();
        if (a.scrollZoomEnabled && this._xAxisStartScaleDistance !== 0) {
          var n = this._xAxisStartScaleDistance / e.pageX;
          if (Number.isFinite(n)) {
            var o = (n - this._xAxisScale) * 10;
            this._xAxisScale = n, this._chart.getChartStore().zoom(o, this._xAxisStartScaleCoordinate, "xAxis");
          }
        }
      }
      return i;
    }, r.prototype._processYAxisScaleStartEvent = function(t, e) {
      var i = t.dispatchEvent("mouseDownEvent", e);
      i && this._chart.updatePane(
        1
        /* UpdateLevel.Overlay */
      );
      var a = this._getYAxisScaleTargetByWidget(t), n = a.getRange();
      return this._prevYAxisRanges.set(a, M({}, n)), this._yAxisStartScaleDistance = e.pageY, i;
    }, r.prototype._processYAxisScalingEvent = function(t, e) {
      var i, a = t.dispatchEvent("pressedMouseMoveEvent", e);
      if (a)
        this._chart.updatePane(
          1
          /* UpdateLevel.Overlay */
        );
      else {
        var n = this._getYAxisByWidget(t), o = this._getYAxisScaleTargetByWidget(t), s = this._prevYAxisRanges.get(o);
        if (C(s) && n.scrollZoomEnabled && this._yAxisStartScaleDistance !== 0) {
          (i = e.preventDefault) === null || i === void 0 || i.call(e);
          var l = e.pageY / this._yAxisStartScaleDistance;
          this._zoomYAxis(o, l, s), this._syncManualYAxesValueRange(t, o);
        }
      }
      return a;
    }, r.prototype._zoomYAxis = function(t, e, i) {
      var a = i ?? t.getRange(), n = a.from, o = a.to, s = a.range, l = s * e, u = (l - s) / 2, c = n - u, d = o + u, h = t.valueToRealValue(c, { range: a }), f = t.valueToRealValue(d, { range: a }), v = t.realValueToDisplayValue(h, { range: a }), p = t.realValueToDisplayValue(f, { range: a });
      t.setRange({
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
    }, r.prototype._findWidgetByEvent = function(t) {
      var e, i, a, n, o, s, l = t.x, u = t.y, c = this._chart.getSeparatorPanes(), d = this._chart.getStyles().separator.size;
      try {
        for (var h = Ct(c), f = h.next(); !f.done; f = h.next()) {
          var v = f.value, p = v[1], g = p.getBounding(), m = g.top - Math.round((Ye - d) / 2);
          if (l >= g.left && l <= g.left + g.width && u >= m && u <= m + Ye)
            return { pane: p, widget: p.getWidget() };
        }
      } catch (P) {
        e = { error: P };
      } finally {
        try {
          f && !f.done && (i = h.return) && i.call(h);
        } finally {
          if (e) throw e.error;
        }
      }
      var x = this._chart.getDrawPanes(), y = null;
      try {
        for (var E = Ct(x), _ = E.next(); !_.done; _ = E.next()) {
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
            for (var T = Ct(y.getYAxisWidgets()), A = T.next(); !A.done; A = T.next()) {
              var k = A.value, F = k.getBounding();
              if (l >= F.left && l <= F.left + F.width && u >= F.top && u <= F.top + F.height) {
                w = k;
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
    }, r.prototype._makeWidgetEvent = function(t, e) {
      var i, a, n, o = (i = e?.getBounding()) !== null && i !== void 0 ? i : null;
      return M(M({}, t), { x: t.x - ((a = o?.left) !== null && a !== void 0 ? a : 0), y: t.y - ((n = o?.top) !== null && n !== void 0 ? n : 0) });
    }, r.prototype.destroy = function() {
      document.removeEventListener("keydown", this._boundKeyBoardDownEvent), this._event.destroy();
    }, r;
  })()
), ta = (
  /** @class */
  (function() {
    function r(t, e) {
      var i = this;
      this._chartBounding = Tr(), this._drawPanes = [], this._separatorPanes = /* @__PURE__ */ new Map(), this._layoutUpdateOptions = {
        sort: !0,
        measureHeight: !0,
        measureWidth: !0,
        secondMeasureWidth: !1,
        update: !0,
        buildYAxisTick: !1,
        cacheYAxisWidth: !1,
        forceBuildYAxisTick: !1
      }, this._layoutPending = !1, this._resizeObserver = null, this._resizeRequestAnimationId = ne, this._scheduleResize = function() {
        i._resizeRequestAnimationId === ne && (i._resizeRequestAnimationId = qe(function() {
          i._resizeRequestAnimationId = ne, (i._chartBounding.width !== Math.floor(i._chartContainer.clientWidth) || i._chartBounding.height !== Math.floor(i._chartContainer.clientHeight)) && i.resize();
        }));
      }, this._cacheYAxisWidth = { left: 0, right: 0 }, this._initContainer(t), this._chartEvent = new ys(this._chartContainer, this), this._chartStore = new co(this, e);
      var a = this._chartStore.getLayoutOptions(), n = a.pane;
      this._candlePane = this._createPane(es, M(M({}, n), { id: j.CANDLE })), this._candlePane.createOrOverrideYAxis(M(M({}, a.yAxis), { id: Te(sr) })), this._xAxisPane = this._createPane(us, M(M({}, n), { id: j.X_AXIS, order: Number.MAX_SAFE_INTEGER })), this._layout(), this._initResizeListener();
    }
    return r.prototype._initContainer = function(t) {
      this._container = t, this._chartContainer = te("div", {
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
      }), this._chartContainer.tabIndex = 1, t.appendChild(this._chartContainer), this._cacheChartBounding();
    }, r.prototype._cacheChartBounding = function() {
      this._chartBounding.width = Math.floor(this._chartContainer.clientWidth), this._chartBounding.height = Math.floor(this._chartContainer.clientHeight);
    }, r.prototype._initResizeListener = function() {
      var t = this;
      C(ResizeObserver) ? (this._resizeObserver = new ResizeObserver(function() {
        t._scheduleResize();
      }), this._resizeObserver.observe(this._chartContainer)) : window.addEventListener("resize", this._scheduleResize);
    }, r.prototype._createPane = function(t, e) {
      var i = new t(this, e);
      return this._drawPanes.push(i), i;
    }, r.prototype.getDrawPaneById = function(t) {
      if (t === j.CANDLE)
        return this._candlePane;
      if (t === j.X_AXIS)
        return this._xAxisPane;
      var e = this._drawPanes.find(function(i) {
        return i.getId() === t;
      });
      return e ?? null;
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
    }, r.prototype.layout = function(t) {
      var e = this, i, a, n, o, s, l, u, c;
      (i = t.sort) !== null && i !== void 0 && i && (this._layoutUpdateOptions.sort = t.sort), (a = t.measureHeight) !== null && a !== void 0 && a && (this._layoutUpdateOptions.measureHeight = t.measureHeight), (n = t.measureWidth) !== null && n !== void 0 && n && (this._layoutUpdateOptions.measureWidth = t.measureWidth), (o = t.secondMeasureWidth) !== null && o !== void 0 && o && (this._layoutUpdateOptions.secondMeasureWidth = t.secondMeasureWidth), (s = t.update) !== null && s !== void 0 && s && (this._layoutUpdateOptions.update = t.update), (l = t.buildYAxisTick) !== null && l !== void 0 && l && (this._layoutUpdateOptions.buildYAxisTick = t.buildYAxisTick), (u = t.cacheYAxisWidth) !== null && u !== void 0 && u && (this._layoutUpdateOptions.cacheYAxisWidth = t.cacheYAxisWidth), (c = t.forceBuildYAxisTick) !== null && c !== void 0 && c && (this._layoutUpdateOptions.forceBuildYAxisTick = t.forceBuildYAxisTick), this._layoutPending || (this._layoutPending = !0, Promise.resolve().then(function(d) {
        e._layout(), e._layoutPending = !1;
      }).catch(function(d) {
      }));
    }, r.prototype._layout = function() {
      var t = this, e, i = this._layoutUpdateOptions, a = i.sort, n = i.measureHeight, o = i.measureWidth, s = i.secondMeasureWidth, l = i.update, u = i.buildYAxisTick, c = i.cacheYAxisWidth, d = i.forceBuildYAxisTick;
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
              var S = new hs(t, "", h, b);
              t._chartContainer.appendChild(S.getContainer()), t._separatorPanes.set(b, S);
            }
            h = b;
          }
          t._chartContainer.appendChild(b.getContainer());
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
          var _ = (e = g.find(function(b) {
            return b.getId() === j.CANDLE && b.getOptions().state === "normal";
          })) !== null && e !== void 0 ? e : g.find(function(b) {
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
          var S = t._separatorPanes.get(b);
          C(S) && (S.setBounding({ height: E, top: I }), I += E), b.setBounding({ top: I }), I += b.getBounding().height;
        });
      }
      var w = function() {
        var b = o;
        if ((u || d) && t._drawPanes.forEach(function(G) {
          G.getYAxisComponents().forEach(function(z) {
            var Z = z.buildTicks(d);
            b || (b = Z);
          });
        }), b) {
          var S = t._chartBounding.width, T = t.getStyles(), A = [], k = [], F = [], P = [], D = function(G, z, Z) {
            var H;
            G[z] = Math.max((H = G[z]) !== null && H !== void 0 ? H : 0, Z);
          };
          t._drawPanes.forEach(function(G) {
            var z = [], Z = [], H = [], lt = [];
            G.getId() !== j.X_AXIS && G.getWidgetYAxisComponents().forEach(function(J) {
              var it = J;
              it.position === "left" ? it.inside ? Z.push(it) : z.push(it) : it.inside ? H.push(it) : lt.push(it);
            }), z.forEach(function(J, it) {
              D(A, it, J.getAutoSize());
            }), Z.forEach(function(J, it) {
              D(k, it, J.getAutoSize());
            }), H.forEach(function(J, it) {
              D(F, it, J.getAutoSize());
            }), lt.forEach(function(J, it) {
              D(P, it, J.getAutoSize());
            });
          });
          var V = A.reduce(function(G, z) {
            return G + z;
          }, 0), B = P.reduce(function(G, z) {
            return G + z;
          }, 0);
          c && (V = Math.max(t._cacheYAxisWidth.left, V), B = Math.max(t._cacheYAxisWidth.right, B)), t._cacheYAxisWidth.left = V, t._cacheYAxisWidth.right = B;
          var L = S, W = 0, Q = 0;
          L -= V, W = V, L -= B, Q = B, t._chartStore.setTotalBarSpace(L);
          var nt = { width: S }, et = { width: L, left: W, right: Q }, at = { width: V }, ot = { width: B }, ut = T.separator.fill, $ = {};
          ut ? $ = nt : $ = et, t._drawPanes.forEach(function(G) {
            var z, Z;
            (z = t._separatorPanes.get(G)) === null || z === void 0 || z.setBounding($);
            var H = {}, lt = 0, J = 0, it = 0, Et = 0, At = [], mt = [], Pt = [], St = [];
            G.getId() !== j.X_AXIS && G.getWidgetYAxisComponents().forEach(function(kt) {
              var _t = kt;
              _t.position === "left" ? _t.inside ? mt.push(_t) : At.push(_t) : _t.inside ? Pt.push(_t) : St.push(_t);
            });
            var he = At.reduce(function(kt, _t, yt) {
              var It;
              return kt + ((It = A[yt]) !== null && It !== void 0 ? It : 0);
            }, 0);
            lt = V - he;
            for (var Nt = At.length - 1; Nt >= 0; Nt--) {
              var be = At[Nt], ve = (Z = A[Nt]) !== null && Z !== void 0 ? Z : 0;
              H[be.id] = { width: ve, left: lt }, lt += ve;
            }
            mt.forEach(function(kt, _t) {
              var yt, It = (yt = k[_t]) !== null && yt !== void 0 ? yt : 0;
              H[kt.id] = { width: It, left: W + J }, J += It;
            }), Pt.forEach(function(kt, _t) {
              var yt, It = (yt = F[_t]) !== null && yt !== void 0 ? yt : 0;
              it += It, H[kt.id] = { width: It, left: W + L - it };
            }), St.forEach(function(kt, _t) {
              var yt, It = (yt = P[_t]) !== null && yt !== void 0 ? yt : 0;
              H[kt.id] = { width: It, left: W + L + Et }, Et += It;
            }), G.setYAxesBounding(H), G.setBounding(nt, et, at, ot);
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
    }, r.prototype.updatePane = function(t, e) {
      var i = this;
      if (C(e)) {
        var a = this.getDrawPaneById(e);
        a?.update(t);
      } else
        this._drawPanes.forEach(function(n) {
          var o;
          n.update(t), (o = i._separatorPanes.get(n)) === null || o === void 0 || o.update(t);
        });
    }, r.prototype.getDom = function(t, e) {
      var i, a;
      if (C(t)) {
        var n = this.getDrawPaneById(t);
        if (C(n)) {
          var o = e ?? "root";
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
    }, r.prototype.getSize = function(t, e) {
      var i, a;
      if (C(t)) {
        var n = this.getDrawPaneById(t);
        if (C(n)) {
          var o = e ?? "root";
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
      this._drawPanes.forEach(function(t) {
        t.getYAxisComponents().forEach(function(e) {
          e.setAutoCalcTickFlag(!0);
        });
      });
    }, r.prototype.setSymbol = function(t) {
      t !== this.getSymbol() && (this._resetYAxisAutoCalcTickFlag(), this._chartStore.setSymbol(t));
    }, r.prototype.getSymbol = function() {
      return this._chartStore.getSymbol();
    }, r.prototype.setPeriod = function(t) {
      t !== this.getPeriod() && (this._resetYAxisAutoCalcTickFlag(), this._chartStore.setPeriod(t));
    }, r.prototype.getPeriod = function() {
      return this._chartStore.getPeriod();
    }, r.prototype.setStyles = function(t) {
      var e = this;
      this._setOptions(function() {
        e._chartStore.setStyles(t);
      });
    }, r.prototype.getStyles = function() {
      return this._chartStore.getStyles();
    }, r.prototype.setFormatter = function(t) {
      var e = this;
      this._setOptions(function() {
        e._chartStore.setFormatter(t);
      });
    }, r.prototype.getFormatter = function() {
      return this._chartStore.getFormatter();
    }, r.prototype.setLocale = function(t) {
      var e = this;
      this._setOptions(function() {
        e._chartStore.setLocale(t);
      });
    }, r.prototype.getLocale = function() {
      return this._chartStore.getLocale();
    }, r.prototype.setTimezone = function(t) {
      var e = this;
      this._setOptions(function() {
        e._chartStore.setTimezone(t);
      });
    }, r.prototype.getTimezone = function() {
      return this._chartStore.getTimezone();
    }, r.prototype.setThousandsSeparator = function(t) {
      var e = this;
      this._setOptions(function() {
        e._chartStore.setThousandsSeparator(t);
      });
    }, r.prototype.getThousandsSeparator = function() {
      return this._chartStore.getThousandsSeparator();
    }, r.prototype.setDecimalFold = function(t) {
      var e = this;
      this._setOptions(function() {
        e._chartStore.setDecimalFold(t);
      });
    }, r.prototype.getDecimalFold = function() {
      return this._chartStore.getDecimalFold();
    }, r.prototype.setHotkey = function(t) {
      this._chartStore.setHotkey(t);
    }, r.prototype.getHotkey = function() {
      return this._chartStore.getHotkey();
    }, r.prototype.getHotKey = function() {
      return this._chartStore.getHotKey();
    }, r.prototype._setOptions = function(t) {
      t(), this.layout({
        measureHeight: !0,
        measureWidth: !0,
        update: !0,
        buildYAxisTick: !0,
        forceBuildYAxisTick: !0
      });
    }, r.prototype.setOffsetRightDistance = function(t) {
      this._chartStore.setOffsetRightDistance(t, !0);
    }, r.prototype.getOffsetRightDistance = function() {
      return this._chartStore.getOffsetRightDistance();
    }, r.prototype.setMaxOffsetLeftDistance = function(t) {
      t < 0 || this._chartStore.setMaxOffsetLeftDistance(t);
    }, r.prototype.setMaxOffsetRightDistance = function(t) {
      t < 0 || this._chartStore.setMaxOffsetRightDistance(t);
    }, r.prototype.setLeftMinVisibleBarCount = function(t) {
      t < 0 || this._chartStore.setLeftMinVisibleBarCount(Math.ceil(t));
    }, r.prototype.setRightMinVisibleBarCount = function(t) {
      t < 0 || this._chartStore.setRightMinVisibleBarCount(Math.ceil(t));
    }, r.prototype.setBarSpace = function(t) {
      this._chartStore.setBarSpace(t);
    }, r.prototype.getBarSpace = function() {
      return this._chartStore.getBarSpace();
    }, r.prototype.getVisibleRange = function() {
      return this._chartStore.getVisibleRange();
    }, r.prototype._removeOrphanYAxes = function() {
      var t = this, e = !1;
      return this._drawPanes.forEach(function(i) {
        var a = i.getId();
        if (a !== j.X_AXIS) {
          var n = /* @__PURE__ */ new Set(), o = i.getDefaultYAxisId();
          C(o) && n.add(o), t._chartStore.getIndicatorsByPaneId(a).forEach(function(s) {
            n.add(s.yAxisId);
          }), i.getYAxisComponents().forEach(function(s) {
            !n.has(s.id) && !i.isManualYAxis(s.id) && (e = i.removeYAxis(s.id) || e);
          });
        }
      }), e;
    }, r.prototype._createOrUseIndicatorYAxis = function(t, e) {
      var i = !1;
      return t.hasYAxisComponent(e) || (t.createOrOverrideYAxis(M(M({}, this._chartStore.getLayoutOptions().yAxis), { id: e })), i = !0), t.isManualYAxis(e) && (t.setManualYAxis(e, !1), i = !0), i;
    }, r.prototype.resetData = function() {
      this._chartStore.resetData();
    }, r.prototype.getDataList = function() {
      return this._chartStore.getDataList();
    }, r.prototype.setDataLoader = function(t) {
      this._resetYAxisAutoCalcTickFlag(), this._chartStore.setDataLoader(t);
    }, r.prototype.createIndicator = function(t, e) {
      var i, a, n, o, s = K(t) ? { name: t } : t;
      if (Pi(s.name) === null)
        return null;
      (i = s.id) !== null && i !== void 0 || (s.id = Te("".concat(s.name, "_"))), (a = s.paneId) !== null && a !== void 0 || (s.paneId = Te(j.INDICATOR));
      var l = this.getDrawPaneById(s.paneId);
      (n = s.yAxisId) !== null && n !== void 0 || (s.yAxisId = (o = l?.getDefaultYAxisId()) !== null && o !== void 0 ? o : Te(sr));
      var u = this._chartStore.addIndicator(s, e ?? !1);
      if (u) {
        var c = !1, d = this.getDrawPaneById(s.paneId);
        return C(d) || (d = this._createPane(Ji, M(M({}, this._chartStore.getLayoutOptions().pane), { id: s.paneId })), c = !0), this._createOrUseIndicatorYAxis(d, s.yAxisId), this._removeOrphanYAxes(), this.layout({
          sort: c,
          measureHeight: !0,
          measureWidth: !0,
          update: !0,
          buildYAxisTick: !0,
          forceBuildYAxisTick: !0
        }), s.id;
      }
      return null;
    }, r.prototype.overrideIndicator = function(t) {
      var e = this, i = this._chartStore.getIndicatorsByFilter(t);
      if (i.length === 0)
        return !1;
      var a = this._chartStore.overrideIndicator(t);
      return i.forEach(function(n) {
        var o = e.getDrawPaneById(n.paneId);
        C(o) && (a = e._createOrUseIndicatorYAxis(o, n.yAxisId) || a);
      }), a && (this._removeOrphanYAxes(), this.layout({
        measureWidth: !0,
        update: !0,
        buildYAxisTick: !0,
        forceBuildYAxisTick: !0
      })), a;
    }, r.prototype.getIndicators = function(t) {
      return this._chartStore.getIndicatorsByFilter(t ?? {});
    }, r.prototype.removeIndicator = function(t) {
      var e = this, i = this._chartStore.removeIndicator(t ?? {});
      if (i) {
        this._removeOrphanYAxes();
        var a = !1, n = [];
        this._drawPanes.forEach(function(o) {
          var s = o.getId();
          if (s !== j.X_AXIS && s !== j.CANDLE) {
            var l = e._chartStore.getIndicatorsByPaneId(s);
            l.length === 0 && n.push(s);
          }
        }), n.forEach(function(o) {
          var s = e._drawPanes.findIndex(function(u) {
            return u.getId() === o;
          }), l = e._drawPanes[s];
          C(l) && (e._drawPanes.splice(s, 1), l.destroy(), a = !0);
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
    }, r.prototype.createOverlay = function(t) {
      var e = this, i = [], a = [], n = function(s) {
        !C(s.paneId) || e.getDrawPaneById(s.paneId) === null ? (s.paneId = j.CANDLE, a.push(!1)) : a.push(!0), i.push(s);
      };
      K(t) ? n({ name: t }) : $t(t) ? t.forEach(function(s) {
        var l = null;
        K(s) ? l = { name: s } : l = s, n(l);
      }) : n(t);
      var o = this._chartStore.addOverlays(i, a);
      return $t(t) ? o : o[0];
    }, r.prototype.getOverlays = function(t) {
      return this._chartStore.getOverlaysByFilter(t ?? {});
    }, r.prototype.overrideOverlay = function(t) {
      return this._chartStore.overrideOverlay(t);
    }, r.prototype.removeOverlay = function(t) {
      return this._chartStore.removeOverlay(t ?? {});
    }, r.prototype.setPaneOptions = function(t) {
      var e, i, a, n = !1, o = !1, s = !1, l = C(t.id);
      try {
        for (var u = Ct(this._drawPanes), c = u.next(); !c.done; c = u.next()) {
          var d = c.value, h = d.getId();
          if (l && t.id === h || !l) {
            if (h !== j.X_AXIS) {
              var f = d.getOptions(), v = f.state;
              if (O(t.height) && t.height > 0) {
                var p = Math.max((a = t.minHeight) !== null && a !== void 0 ? a : f.minHeight, 0), g = Math.max(p, t.height);
                o = !0, n = !0, d.setBounding({ height: g });
              }
              C(t.state) && (o = !0, n = !0, v === "normal" && t.state !== "normal" ? d.setOptions({ height: d.getBounding().height }) : v !== "normal" && t.state === "normal" && !O(t.height) && d.setBounding({
                height: Math.max(f.minHeight, f.height)
              }));
            }
            if (O(t.order) && (o = !0, s = !0), d.setOptions(t), h === t.id)
              break;
          }
        }
      } catch (m) {
        e = { error: m };
      } finally {
        try {
          c && !c.done && (i = u.return) && i.call(u);
        } finally {
          if (e) throw e.error;
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
    }, r.prototype.createYAxis = function(t) {
      var e, i, a = (e = t.paneId) !== null && e !== void 0 ? e : j.CANDLE, n = this.getDrawPaneById(a);
      if (!C(n) || a === j.X_AXIS)
        return null;
      var o = (i = t.id) !== null && i !== void 0 ? i : Te(sr);
      return n.hasYAxisComponent(o) || (n.createOrOverrideYAxis(M(M(M({}, this._chartStore.getLayoutOptions().yAxis), t), { id: o, paneId: a })), n.setManualYAxis(o, !0), this.layout({
        measureWidth: !0,
        update: !0,
        buildYAxisTick: !0,
        forceBuildYAxisTick: !0
      })), o;
    }, r.prototype.removeYAxis = function(t) {
      var e, i, a = t.id, n = t.name;
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
        for (var u = Ct(this.getYAxes(t)), c = u.next(); !c.done; c = u.next()) {
          var d = c.value;
          s(d);
        }
      } catch (h) {
        e = { error: h };
      } finally {
        try {
          c && !c.done && (i = u.return) && i.call(u);
        } finally {
          if (e) throw e.error;
        }
      }
      return o && this.layout({
        measureWidth: !0,
        update: !0,
        buildYAxisTick: !0,
        forceBuildYAxisTick: !0
      }), o;
    }, r.prototype.getYAxes = function(t) {
      var e, i, a = t.paneId, n = t.id, o = t.name, s = function(u) {
        return C(n) ? u.id === n : !C(o) || u.name === o;
      }, l = [];
      return C(a) ? l = l.concat((i = (e = this.getDrawPaneById(a)) === null || e === void 0 ? void 0 : e.getYAxisComponents().filter(s)) !== null && i !== void 0 ? i : []) : this._drawPanes.forEach(function(u) {
        u.getId() !== j.X_AXIS && (l = l.concat(u.getYAxisComponents().filter(s)));
      }), l;
    }, r.prototype.overrideYAxis = function(t) {
      var e = this, i = this.getYAxes({ paneId: t.paneId, id: t.id });
      i.length !== 0 && (i.forEach(function(a) {
        var n;
        (n = e.getDrawPaneById(a.paneId)) === null || n === void 0 || n.createOrOverrideYAxis(M(M({}, t), { id: a.id }));
      }), this.layout({
        measureWidth: !0,
        update: !0,
        buildYAxisTick: !0,
        forceBuildYAxisTick: !0
      }));
    }, r.prototype.overrideXAxis = function(t) {
      this._xAxisPane.overrideXAxis(t), this.layout({
        measureHeight: !0,
        update: !0,
        buildYAxisTick: !0,
        forceBuildYAxisTick: !0
      });
    }, r.prototype.getPaneOptions = function(t) {
      var e;
      if (C(t)) {
        var i = this.getDrawPaneById(t);
        return (e = i?.getOptions()) !== null && e !== void 0 ? e : null;
      }
      return this._drawPanes.map(function(a) {
        return a.getOptions();
      });
    }, r.prototype.setZoomEnabled = function(t) {
      this._chartStore.setZoomEnabled(t);
    }, r.prototype.isZoomEnabled = function() {
      return this._chartStore.isZoomEnabled();
    }, r.prototype.setZoomAnchor = function(t) {
      this._chartStore.setZoomAnchor(t);
    }, r.prototype.getZoomAnchor = function() {
      return this._chartStore.getZoomAnchor();
    }, r.prototype.setScrollEnabled = function(t) {
      this._chartStore.setScrollEnabled(t);
    }, r.prototype.isScrollEnabled = function() {
      return this._chartStore.isScrollEnabled();
    }, r.prototype.scrollByDistance = function(t, e) {
      var i = this, a = O(e) && e > 0 ? e : 0;
      if (this._chartStore.startScroll(), a > 0) {
        var n = new mr({ duration: a });
        n.doFrame(function(o) {
          var s = t * (o / a);
          i._chartStore.scroll(s);
        }), n.start();
      } else
        this._chartStore.scroll(t);
    }, r.prototype.scrollToRealTime = function(t) {
      var e = this._chartStore.getBarSpace().bar, i = this._chartStore.getLastBarRightSideDiffBarCount() - this._chartStore.getInitialOffsetRightDistance() / e, a = i * e;
      this.scrollByDistance(a, t);
    }, r.prototype.scrollToDataIndex = function(t, e) {
      var i = (this._chartStore.getLastBarRightSideDiffBarCount() + (this.getDataList().length - 1 - t)) * this._chartStore.getBarSpace().bar;
      this.scrollByDistance(i, e);
    }, r.prototype.scrollToTimestamp = function(t, e) {
      var i = _r(this.getDataList(), "timestamp", t);
      this.scrollToDataIndex(i, e);
    }, r.prototype.zoomAtCoordinate = function(t, e, i) {
      var a = this, n = O(i) && i > 0 ? i : 0, o = this._chartStore.getBarSpace().bar, s = o * t, l = s - o;
      if (n > 0) {
        var u = 0, c = new mr({ duration: n });
        c.doFrame(function(d) {
          var h = l * (d / n), f = (h - u) / a._chartStore.getBarSpace().bar * yr;
          a._chartStore.zoom(f, e ?? null, "main"), u = h;
        }), c.start();
      } else
        this._chartStore.zoom(l / o * yr, e ?? null, "main");
    }, r.prototype.zoomAtDataIndex = function(t, e, i) {
      var a = this._chartStore.dataIndexToCoordinate(e);
      this.zoomAtCoordinate(t, { x: a, y: 0 }, i);
    }, r.prototype.zoomAtTimestamp = function(t, e, i) {
      var a = _r(this.getDataList(), "timestamp", e);
      this.zoomAtDataIndex(t, a, i);
    }, r.prototype.convertToPixel = function(t, e) {
      var i = this, a, n = e ?? {}, o = n.paneId, s = o === void 0 ? j.CANDLE : o, l = n.yAxisId, u = n.absolute, c = u === void 0 ? !1 : u, d = [];
      if (s !== j.X_AXIS) {
        var h = this.getDrawPaneById(s);
        if (h !== null) {
          var f = h.getBounding(), v = [].concat(t), p = this._xAxisPane.getXAxisComponent(), g = h.getYAxisComponentById(l);
          d = v.map(function(m) {
            var x = {}, y = m.dataIndex;
            if (O(m.timestamp) && (y = i._chartStore.timestampToDataIndex(m.timestamp)), O(y) && (x.x = p.convertToPixel(y)), O(m.value)) {
              var E = g.convertToPixel(m.value);
              x.y = c ? f.top + E : E;
            }
            return x;
          });
        }
      }
      return $t(t) ? d : (a = d[0]) !== null && a !== void 0 ? a : {};
    }, r.prototype.convertFromPixel = function(t, e) {
      var i = this, a, n = e ?? {}, o = n.paneId, s = o === void 0 ? j.CANDLE : o, l = n.yAxisId, u = n.absolute, c = u === void 0 ? !1 : u, d = [];
      if (s !== j.X_AXIS) {
        var h = this.getDrawPaneById(s);
        if (h !== null) {
          var f = h.getBounding(), v = [].concat(t), p = this._xAxisPane.getXAxisComponent(), g = h.getYAxisComponentById(l);
          d = v.map(function(m) {
            var x, y = {};
            if (O(m.x)) {
              var E = p.convertFromPixel(m.x);
              y.dataIndex = E, y.timestamp = (x = i._chartStore.dataIndexToTimestamp(E)) !== null && x !== void 0 ? x : void 0;
            }
            if (O(m.y)) {
              var _ = c ? m.y - f.top : m.y;
              y.value = g.convertFromPixel(_);
            }
            return y;
          });
        }
      }
      return $t(t) ? d : (a = d[0]) !== null && a !== void 0 ? a : {};
    }, r.prototype.executeAction = function(t, e) {
      var i;
      if (t === "onCrosshairChange") {
        var a = null;
        C(e) && (a = M({}, e), (i = a.paneId) !== null && i !== void 0 || (a.paneId = j.CANDLE)), this._chartStore.setCrosshair(a, { notExecuteAction: !0 });
      }
    }, r.prototype.subscribeAction = function(t, e) {
      this._chartStore.subscribeAction(t, e);
    }, r.prototype.unsubscribeAction = function(t, e) {
      this._chartStore.unsubscribeAction(t, e);
    }, r.prototype.getConvertPictureUrl = function(t, e, i) {
      var a = this, n = this._chartBounding, o = n.width, s = n.height, l = te("canvas", {
        width: "".concat(o, "px"),
        height: "".concat(s, "px"),
        boxSizing: "border-box"
      }), u = l.getContext("2d"), c = oe(l);
      l.width = o * c, l.height = s * c, u.scale(c, c), u.fillStyle = i ?? "#FFFFFF", u.fillRect(0, 0, o, s);
      var d = t ?? !1;
      return this._drawPanes.forEach(function(h) {
        var f = a._separatorPanes.get(h);
        if (C(f)) {
          var v = f.getBounding();
          u.drawImage(f.getImage(d), v.left, v.top, v.width, v.height);
        }
        var p = h.getBounding();
        u.drawImage(h.getImage(d), 0, p.top, o, p.height);
      }), l.toDataURL("image/".concat(e ?? "jpeg"));
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
      this._resizeRequestAnimationId !== ne && (gr(this._resizeRequestAnimationId), this._resizeRequestAnimationId = ne), C(this._resizeObserver) ? (this._resizeObserver.disconnect(), this._resizeObserver = null) : window.removeEventListener("resize", this._scheduleResize), this._chartEvent.destroy(), this._drawPanes.forEach(function(t) {
        t.destroy();
      }), this._drawPanes = [], this._separatorPanes.clear(), this._chartStore.destroy(), this._container.removeChild(this._chartContainer);
    }, r;
  })()
), er = /* @__PURE__ */ new Map(), xs = 1;
function ws(r, t) {
  var e = null;
  if (K(r) ? e = document.getElementById(r) : e = r, e === null)
    return null;
  var i = er.get(e.id);
  if (C(i))
    return i;
  var a = "k_line_chart_".concat(xs++);
  return i = new ta(e, t), i.id = a, e.setAttribute("k-line-chart-id", a), er.set(a, i), i;
}
function bs(r) {
  var t, e, i = null;
  if (r instanceof ta)
    i = r.id;
  else {
    var a = null;
    K(r) ? a = document.getElementById(r) : a = r, i = (t = a?.getAttribute("k-line-chart-id")) !== null && t !== void 0 ? t : null;
  }
  i !== null && ((e = er.get(i)) === null || e === void 0 || e.destroy(), er.delete(i));
}
const ea = "STOCK_EVA_API_MA", ra = "STOCK_EVA_API_VOLUME", ia = "STOCK_EVA_API_MACD", aa = "STOCK_EVA_API_RSI", vi = ["ma5", "ma10", "ma20", "ma60", "ma120", "ma250"], Cs = ["macd", "macd_signal", "macd_hist"], fi = /* @__PURE__ */ new WeakSet();
function Es(r) {
  return Date.parse(`${r}T00:00:00+08:00`);
}
function Is(r) {
  return r.map((t) => ({
    timestamp: Es(t.trade_date),
    open: t.open,
    high: t.high,
    low: t.low,
    close: t.close,
    volume: t.volume,
    turnover: t.amount,
    amount: t.amount,
    ma5: t.ma5,
    ma10: t.ma10,
    ma20: t.ma20,
    ma60: t.ma60,
    ma120: t.ma120,
    ma250: t.ma250,
    macd: t.macd,
    macd_signal: t.macd_signal,
    macd_hist: t.macd_hist,
    rsi14: t.rsi14
  }));
}
function dr(r, t) {
  return r.map(
    (e) => Object.fromEntries(t.map((i) => [i, e[i] ?? null]))
  );
}
function Ss(r) {
  fi.has(r) || (r({
    name: ea,
    shortName: "API MA",
    series: "price",
    figures: vi.map((t) => ({
      key: t,
      title: `${t.toUpperCase()}: `,
      type: "line"
    })),
    calc: (t) => dr(t, vi)
  }), r({
    name: ra,
    shortName: "成交量",
    series: "volume",
    shouldFormatBigNumber: !0,
    figures: [{ key: "volume", title: "VOL: ", type: "bar", baseValue: 0 }],
    calc: (t) => t.map((e) => ({
      volume: typeof e.volume == "number" && Number.isFinite(e.volume) ? e.volume : null
    }))
  }), r({
    name: ia,
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
    calc: (t) => dr(t, Cs)
  }), r({
    name: aa,
    shortName: "API RSI14",
    figures: [{ key: "rsi14", title: "RSI14: ", type: "line" }],
    calc: (t) => dr(t, ["rsi14"])
  }), fi.add(r));
}
const Ts = {
  init: (r) => {
    const t = ws(r, {
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
    if (!t) throw new Error("KLineChart initialization failed");
    return t;
  },
  dispose: (r) => {
    bs(r);
  },
  registerIndicator: kn
};
function As(r, t, e = Ts) {
  const i = Is(t.series);
  Ss(e.registerIndicator);
  const a = e.init(r);
  return a.setSymbol({
    ticker: t.symbol,
    pricePrecision: 4,
    volumePrecision: 0
  }), a.setPeriod({ span: 1, type: "day" }), a.setDataLoader({
    getBars: ({ callback: n }) => {
      n(i, { forward: !1, backward: !1 });
    }
  }), a.createIndicator({ name: ea, paneId: "candle_pane" }, !0), a.createIndicator(ra), a.createIndicator(ia), a.createIndicator(aa), () => e.dispose(r);
}
const Ms = "http://127.0.0.1:8000/api/v1", Ps = /^\d{4}-\d{2}-\d{2}$/, ks = /^(sh|sz)\.[0-9]{6}$/;
class xr extends Error {
  status;
  detail;
  constructor(t, e) {
    super(e), this.name = "ApiError", this.status = t, this.detail = e;
  }
}
function pt(r, t) {
  if (typeof r != "object" || r === null || Array.isArray(r))
    throw new TypeError(`${t} must be an object`);
  return r;
}
function Y(r, t, e) {
  if (typeof r != "string" || e && !e.includes(r))
    throw new TypeError(`${t} must be a valid string`);
  return r;
}
function ee(r, t) {
  const e = Y(r, t);
  if (!Ps.test(e))
    throw new TypeError(`${t} must be an ISO date`);
  return e;
}
function Rr(r, t) {
  return r === null ? null : ee(r, t);
}
function ie(r, t) {
  if (typeof r != "number" || !Number.isFinite(r))
    throw new TypeError(`${t} must be a finite number`);
  return r;
}
function Kt(r, t) {
  return r === null ? null : ie(r, t);
}
function Ds(r, t) {
  return r == null ? null : ie(r, t);
}
function Ht(r, t) {
  const e = ie(r, t);
  if (!Number.isInteger(e) || e < 0)
    throw new TypeError(`${t} must be a non-negative integer`);
  return e;
}
function pi(r, t) {
  return r == null ? null : Ht(r, t);
}
function Xe(r, t) {
  if (typeof r != "boolean") throw new TypeError(`${t} must be boolean`);
  return r;
}
function st(r, t) {
  if (!Array.isArray(r) || !r.every((e) => typeof e == "string"))
    throw new TypeError(`${t} must be a string array`);
  return [...r];
}
function xe(r, t, e) {
  if (!Array.isArray(r)) throw new TypeError(`${t} must be an array`);
  return r.map(e);
}
function gi(r, t) {
  const e = pt(r, t);
  return Object.fromEntries(
    Object.entries(e).map(([i, a]) => [
      i,
      ie(a, `${t}.${i}`)
    ])
  );
}
function De(r, t) {
  return Y(r, t, [
    "empty",
    "ready",
    "degraded"
  ]);
}
function ar(r, t) {
  return Y(r, t, [
    "ready",
    "degraded",
    "missing"
  ]);
}
function Fr(r, t) {
  const e = pt(r, t);
  return {
    value: ie(e.value, `${t}.value`),
    level: Y(e.level, `${t}.level`, [
      "low",
      "medium",
      "high"
    ]),
    reasons: st(e.reasons, `${t}.reasons`)
  };
}
function Rs(r, t) {
  const e = pt(r, t), i = {
    code: Y(e.code, `${t}.code`),
    detail: Y(e.detail, `${t}.detail`)
  };
  for (const a of ["component", "metric", "source"])
    e[a] !== void 0 && (i[a] = Y(e[a], `${t}.${a}`));
  for (const a of ["value", "raw_value", "score"])
    e[a] !== void 0 && (i[a] = Kt(e[a], `${t}.${a}`));
  return e.as_of !== void 0 && (i.as_of = ee(e.as_of, `${t}.as_of`)), i;
}
function le(r, t) {
  return xe(
    r,
    t,
    (e, i) => Rs(e, `${t}[${i}]`)
  );
}
function Br(r, t) {
  const e = pt(r, t);
  return {
    source: Y(e.source, `${t}.source`),
    source_version: Y(
      e.source_version,
      `${t}.source_version`
    ),
    earliest_input_date: ee(
      e.earliest_input_date,
      `${t}.earliest_input_date`
    ),
    latest_input_date: ee(
      e.latest_input_date,
      `${t}.latest_input_date`
    ),
    record_count: Ht(e.record_count, `${t}.record_count`),
    content_hash: Y(e.content_hash, `${t}.content_hash`)
  };
}
function na(r, t) {
  return xe(
    r,
    t,
    (e, i) => Br(e, `${t}[${i}]`)
  );
}
function Fs(r, t) {
  const e = pt(r, t);
  return {
    name: Y(e.name, `${t}.name`),
    score: Kt(e.score, `${t}.score`),
    weight: ie(e.weight, `${t}.weight`),
    weighted_score: Kt(
      e.weighted_score,
      `${t}.weighted_score`
    ),
    formula_version: Y(
      e.formula_version,
      `${t}.formula_version`
    ),
    quality_status: ar(
      e.quality_status,
      `${t}.quality_status`
    ),
    missing_inputs: st(e.missing_inputs, `${t}.missing_inputs`),
    quality_issues: st(e.quality_issues, `${t}.quality_issues`),
    supporting_evidence: le(
      e.supporting_evidence,
      `${t}.supporting_evidence`
    ),
    contrary_evidence: le(
      e.contrary_evidence,
      `${t}.contrary_evidence`
    ),
    source_lineage: na(
      e.source_lineage,
      `${t}.source_lineage`
    )
  };
}
function Bs(r, t) {
  const e = pt(r, t);
  return {
    expected_boards: st(e.expected_boards, `${t}.expected_boards`),
    observed_boards: st(e.observed_boards, `${t}.observed_boards`),
    missing_boards: st(e.missing_boards, `${t}.missing_boards`),
    expected_index_series: st(
      e.expected_index_series,
      `${t}.expected_index_series`
    ),
    observed_index_series: st(
      e.observed_index_series,
      `${t}.observed_index_series`
    ),
    missing_index_series: st(
      e.missing_index_series,
      `${t}.missing_index_series`
    ),
    coverage_basis: Y(
      e.coverage_basis,
      `${t}.coverage_basis`
    ),
    coverage_evidence_status: Y(
      e.coverage_evidence_status,
      `${t}.coverage_evidence_status`
    ),
    observed_universe_count: Ht(
      e.observed_universe_count,
      `${t}.observed_universe_count`
    ),
    index_coverage_ratio: ie(
      e.index_coverage_ratio,
      `${t}.index_coverage_ratio`
    ),
    scope_status: Y(e.scope_status, `${t}.scope_status`),
    can_support_full_a_share_conclusion: Xe(
      e.can_support_full_a_share_conclusion,
      `${t}.can_support_full_a_share_conclusion`
    ),
    conclusion_disclaimer: Y(
      e.conclusion_disclaimer,
      `${t}.conclusion_disclaimer`
    )
  };
}
function Os(r) {
  const t = pt(r, "market regime");
  return {
    result_id: Y(t.result_id, "result_id"),
    formula_version: Y(t.formula_version, "formula_version"),
    as_of: ee(t.as_of, "as_of"),
    data_as_of: Rr(t.data_as_of, "data_as_of"),
    status: De(t.status, "status"),
    quality_status: De(t.quality_status, "quality_status"),
    strategic_state: Y(t.strategic_state, "strategic_state", [
      "bull",
      "range",
      "bear"
    ]),
    tactical_state: Y(t.tactical_state, "tactical_state", [
      "risk_on",
      "neutral",
      "risk_off"
    ]),
    total_score: Kt(t.total_score, "total_score"),
    component_scores: xe(
      t.component_scores,
      "component_scores",
      (e, i) => Fs(e, `component_scores[${i}]`)
    ),
    weights: gi(t.weights, "weights"),
    thresholds: gi(t.thresholds, "thresholds"),
    confidence: Fr(t.confidence, "confidence"),
    missing_inputs: st(t.missing_inputs, "missing_inputs"),
    supporting_evidence: le(
      t.supporting_evidence,
      "supporting_evidence"
    ),
    contrary_evidence: le(
      t.contrary_evidence,
      "contrary_evidence"
    ),
    quality_issues: st(t.quality_issues, "quality_issues"),
    actual_market_scope: Bs(
      t.actual_market_scope,
      "actual_market_scope"
    ),
    source_lineage: na(t.source_lineage, "source_lineage")
  };
}
function Ls(r, t) {
  const e = pt(r, t);
  return {
    metric: Y(e.metric, `${t}.metric`),
    raw_value: Kt(e.raw_value, `${t}.raw_value`),
    unit: Y(e.unit, `${t}.unit`),
    score: Kt(e.score, `${t}.score`),
    weight: ie(e.weight, `${t}.weight`),
    weighted_score: Kt(
      e.weighted_score,
      `${t}.weighted_score`
    ),
    formula_version: Y(
      e.formula_version,
      `${t}.formula_version`
    ),
    quality_status: ar(
      e.quality_status,
      `${t}.quality_status`
    ),
    missing_inputs: st(e.missing_inputs, `${t}.missing_inputs`),
    quality_issues: st(e.quality_issues, `${t}.quality_issues`),
    effective_count: pi(
      e.effective_count,
      `${t}.effective_count`
    ),
    target_count: pi(
      e.target_count,
      `${t}.target_count`
    ),
    coverage_ratio: Ds(
      e.coverage_ratio,
      `${t}.coverage_ratio`
    )
  };
}
function oa(r, t) {
  return xe(
    r,
    t,
    (e, i) => Ls(e, `${t}[${i}]`)
  );
}
function sa(r, t) {
  const e = pt(r, t), i = {
    generation_id: Y(e.generation_id, `${t}.generation_id`),
    schema_version: Y(e.schema_version, `${t}.schema_version`),
    source: Y(e.source, `${t}.source`),
    source_version: Y(
      e.source_version,
      `${t}.source_version`
    ),
    source_snapshot_date: ee(
      e.source_snapshot_date,
      `${t}.source_snapshot_date`
    ),
    taxonomy_id: Y(e.taxonomy_id, `${t}.taxonomy_id`),
    coverage_ratio: ie(
      e.coverage_ratio,
      `${t}.coverage_ratio`
    )
  };
  return e.source_date_semantics !== void 0 && (i.source_date_semantics = Y(
    e.source_date_semantics,
    `${t}.source_date_semantics`
  )), i;
}
function la(r, t) {
  const e = pt(r, t);
  return {
    scope_status: Y(e.scope_status, `${t}.scope_status`),
    included_markets: st(
      e.included_markets,
      `${t}.included_markets`
    ),
    included_boards: st(
      e.included_boards,
      `${t}.included_boards`
    ),
    excluded_classification_boards: st(
      e.excluded_classification_boards,
      `${t}.excluded_classification_boards`
    ),
    classification_eligible_symbols: Ht(
      e.classification_eligible_symbols,
      `${t}.classification_eligible_symbols`
    ),
    observed_market_symbols: Ht(
      e.observed_market_symbols,
      `${t}.observed_market_symbols`
    ),
    priced_classified_symbols: Ht(
      e.priced_classified_symbols,
      `${t}.priced_classified_symbols`
    ),
    coverage_basis: Y(
      e.coverage_basis,
      `${t}.coverage_basis`
    ),
    can_support_full_a_share_conclusion: Xe(
      e.can_support_full_a_share_conclusion,
      `${t}.can_support_full_a_share_conclusion`
    ),
    can_support_all_industry_conclusion: Xe(
      e.can_support_all_industry_conclusion,
      `${t}.can_support_all_industry_conclusion`
    ),
    conclusion_disclaimer: Y(
      e.conclusion_disclaimer,
      `${t}.conclusion_disclaimer`
    )
  };
}
function ua(r, t) {
  const e = pt(r, t);
  if (e.evidence_tier !== null)
    throw new TypeError(`${t}.evidence_tier must be null`);
  return {
    status: Y(e.status, `${t}.status`, [
      "missing"
    ]),
    evidence_tier: null,
    reason: Y(e.reason, `${t}.reason`)
  };
}
function Vs(r, t) {
  const e = pt(r, t);
  return {
    rank: Ht(e.rank, `${t}.rank`),
    ranking_id: Y(e.ranking_id, `${t}.ranking_id`),
    sector_id: Y(e.sector_id, `${t}.sector_id`),
    sector_name: Y(e.sector_name, `${t}.sector_name`),
    member_count: Ht(e.member_count, `${t}.member_count`),
    priced_member_count: Ht(
      e.priced_member_count,
      `${t}.priced_member_count`
    ),
    ranking_eligible: Xe(
      e.ranking_eligible,
      `${t}.ranking_eligible`
    ),
    ranking_exclusion_reasons: st(
      e.ranking_exclusion_reasons,
      `${t}.ranking_exclusion_reasons`
    ),
    total_score: Kt(e.total_score, `${t}.total_score`),
    confidence: Fr(e.confidence, `${t}.confidence`),
    metric_scores: oa(e.metric_scores, `${t}.metric_scores`),
    supporting_evidence: le(
      e.supporting_evidence,
      `${t}.supporting_evidence`
    ),
    contrary_evidence: le(
      e.contrary_evidence,
      `${t}.contrary_evidence`
    ),
    quality_status: ar(
      e.quality_status,
      `${t}.quality_status`
    ),
    missing_inputs: st(e.missing_inputs, `${t}.missing_inputs`),
    quality_issues: st(e.quality_issues, `${t}.quality_issues`)
  };
}
function Ns(r) {
  const t = pt(r, "sector rotation");
  return {
    result_id: Y(t.result_id, "result_id"),
    formula_version: Y(t.formula_version, "formula_version"),
    status: De(t.status, "status"),
    quality_status: De(t.quality_status, "quality_status"),
    as_of: ee(t.as_of, "as_of"),
    data_as_of: Rr(t.data_as_of, "data_as_of"),
    taxonomy_id: Y(t.taxonomy_id, "taxonomy_id"),
    classification_lineage: t.classification_lineage === null ? null : sa(
      t.classification_lineage,
      "classification_lineage"
    ),
    market_lineage: t.market_lineage === null ? null : Br(t.market_lineage, "market_lineage"),
    actual_scope: la(t.actual_scope, "actual_scope"),
    rankings: xe(
      t.rankings,
      "rankings",
      (e, i) => Vs(e, `rankings[${i}]`)
    ),
    fund_flow_evidence: ua(
      t.fund_flow_evidence,
      "fund_flow_evidence"
    ),
    missing_inputs: st(t.missing_inputs, "missing_inputs"),
    quality_issues: st(t.quality_issues, "quality_issues")
  };
}
function Ys(r, t) {
  const e = pt(r, t);
  if (e.actionable_primary !== !1)
    throw new TypeError(`${t}.actionable_primary must be false`);
  const i = Y(e.symbol, `${t}.symbol`);
  if (!ks.test(i))
    throw new TypeError(`${t}.symbol must be an A-share symbol`);
  return {
    rank: Ht(e.rank, `${t}.rank`),
    candidate_id: Y(e.candidate_id, `${t}.candidate_id`),
    symbol: i,
    name: Y(e.name, `${t}.name`),
    total_score: Kt(e.total_score, `${t}.total_score`),
    confidence: Fr(e.confidence, `${t}.confidence`),
    actionable_primary: !1,
    actionability_status: Y(
      e.actionability_status,
      `${t}.actionability_status`
    ),
    limit_lock_status: Y(
      e.limit_lock_status,
      `${t}.limit_lock_status`
    ),
    leader_qualified: Xe(
      e.leader_qualified,
      `${t}.leader_qualified`
    ),
    qualification_version: Y(
      e.qualification_version,
      `${t}.qualification_version`
    ),
    qualification_reasons: st(
      e.qualification_reasons,
      `${t}.qualification_reasons`
    ),
    disqualification_reasons: st(
      e.disqualification_reasons,
      `${t}.disqualification_reasons`
    ),
    metric_scores: oa(e.metric_scores, `${t}.metric_scores`),
    supporting_evidence: le(
      e.supporting_evidence,
      `${t}.supporting_evidence`
    ),
    contrary_evidence: le(
      e.contrary_evidence,
      `${t}.contrary_evidence`
    ),
    quality_status: ar(
      e.quality_status,
      `${t}.quality_status`
    ),
    missing_inputs: st(e.missing_inputs, `${t}.missing_inputs`),
    quality_issues: st(e.quality_issues, `${t}.quality_issues`)
  };
}
function Ws(r, t) {
  const e = pt(r, t);
  return {
    symbol: Y(e.symbol, `${t}.symbol`),
    reasons: st(e.reasons, `${t}.reasons`)
  };
}
function $s(r) {
  const t = pt(r, "sector leaders");
  return {
    result_id: Y(t.result_id, "result_id"),
    formula_version: Y(t.formula_version, "formula_version"),
    status: De(t.status, "status"),
    quality_status: De(t.quality_status, "quality_status"),
    as_of: ee(t.as_of, "as_of"),
    data_as_of: Rr(t.data_as_of, "data_as_of"),
    taxonomy_id: Y(t.taxonomy_id, "taxonomy_id"),
    sector_id: Y(t.sector_id, "sector_id"),
    sector_name: t.sector_name === null ? null : Y(t.sector_name, "sector_name"),
    classification_lineage: t.classification_lineage === null ? null : sa(
      t.classification_lineage,
      "classification_lineage"
    ),
    market_lineage: t.market_lineage === null ? null : Br(t.market_lineage, "market_lineage"),
    actual_scope: la(t.actual_scope, "actual_scope"),
    candidates: xe(
      t.candidates,
      "candidates",
      (e, i) => Ys(e, `candidates[${i}]`)
    ),
    exclusions: xe(
      t.exclusions,
      "exclusions",
      (e, i) => Ws(e, `exclusions[${i}]`)
    ),
    fund_flow_evidence: ua(
      t.fund_flow_evidence,
      "fund_flow_evidence"
    ),
    missing_inputs: st(t.missing_inputs, "missing_inputs"),
    quality_issues: st(t.quality_issues, "quality_issues")
  };
}
async function zs(r) {
  try {
    const t = pt(await r.json(), "error response");
    if (typeof t.detail == "string") return t.detail;
    if (t.detail !== void 0) {
      const e = pt(t.detail, "error detail");
      if (typeof e.code == "string") return e.code;
    }
  } catch {
  }
  return `HTTP ${r.status}`;
}
async function Or(r, t, e) {
  const i = await t(`${Ms}${r}`, { signal: e });
  if (!i.ok)
    throw new xr(i.status, await zs(i));
  return i.json();
}
function Lr(r, t) {
  const e = new URLSearchParams({ as_of: ee(r, "as_of") });
  return t !== void 0 && e.set("taxonomy_id", t), e.toString();
}
async function ca(r, t = fetch, e = new AbortController().signal) {
  return Os(
    await Or(`/analysis/market-regime?${Lr(r)}`, t, e)
  );
}
async function da(r, t, e = fetch, i = new AbortController().signal) {
  return Ns(
    await Or(
      `/analysis/sector-rotation?${Lr(r, t)}`,
      e,
      i
    )
  );
}
async function ha(r, t, e, i = fetch, a = new AbortController().signal) {
  return $s(
    await Or(
      `/analysis/sectors/${encodeURIComponent(e)}/leaders?${Lr(
        r,
        t
      )}`,
      i,
      a
    )
  );
}
class qs {
  constructor(t = fetch) {
    this.fetcher = t;
  }
  fetcher;
  overviewController = null;
  leaderController = null;
  async loadOverview(t) {
    this.overviewController?.abort(), this.leaderController?.abort();
    const e = new AbortController();
    this.overviewController = e;
    try {
      const [i, a] = await Promise.all([
        ca(t.asOf, this.fetcher, e.signal),
        da(
          t.asOf,
          t.taxonomyId,
          this.fetcher,
          e.signal
        )
      ]);
      return e.signal.aborted || this.overviewController !== e ? null : { market: i, sectors: a };
    } catch (i) {
      if (e.signal.aborted || this.overviewController !== e)
        return null;
      throw i;
    } finally {
      this.overviewController === e && (this.overviewController = null);
    }
  }
  async loadLeaders(t) {
    this.leaderController?.abort();
    const e = new AbortController();
    this.leaderController = e;
    try {
      const i = await ha(
        t.asOf,
        t.taxonomyId,
        t.sectorId,
        this.fetcher,
        e.signal
      );
      return e.signal.aborted || this.leaderController !== e ? null : i;
    } catch (i) {
      if (e.signal.aborted || this.leaderController !== e)
        return null;
      throw i;
    } finally {
      this.leaderController === e && (this.leaderController = null);
    }
  }
  dispose() {
    this.overviewController?.abort(), this.leaderController?.abort(), this.overviewController = null, this.leaderController = null;
  }
}
const Vr = /^(sh|sz)\.[0-9]{6}$/, Nr = /^\d{4}-\d{2}-\d{2}$/;
function va(r) {
  return r === "portfolio" || r === "watchlists" || r === "sectors";
}
function Xs(r) {
  return r === "overview" || r === "sectors";
}
function Yr(r) {
  return typeof r.asOf == "string" && Nr.test(r.asOf) && typeof r.taxonomyId == "string" && r.taxonomyId.length > 0 && typeof r.sectorId == "string" && r.sectorId.length > 0 && (r.returnView === void 0 || r.returnView === "overview" || r.returnView === "sectors");
}
function fa(r, t) {
  if (!Nr.test(t.asOf) || !t.taxonomyId || t.sectorId === "")
    throw new TypeError("invalid decision route");
  const e = new URLSearchParams({
    as_of: t.asOf,
    taxonomy_id: t.taxonomyId
  });
  return t.sectorId !== null && e.set("sector_id", t.sectorId), `#${r}?${e}`;
}
function pa(r, t) {
  const e = new RegExp(`^#${r}(?:\\?(.*))?$`).exec(t);
  if (!e) return null;
  const i = new URLSearchParams(e[1] ?? ""), a = i.get("as_of"), n = i.get("taxonomy_id"), o = i.get("sector_id");
  return a === null || !Nr.test(a) || n === null || n.length === 0 || o === "" ? null : { asOf: a, taxonomyId: n, sectorId: o };
}
function Wr(r) {
  return fa("overview", r);
}
function ga(r) {
  return pa("overview", r);
}
function $r(r) {
  return fa("sectors", r);
}
function ma(r) {
  return pa("sectors", r);
}
function Hs(r, t, e) {
  if (!Vr.test(r)) throw new TypeError("invalid security symbol");
  if (e !== void 0 && !Yr(e))
    throw new TypeError("invalid decision context");
  const i = new URLSearchParams({ from: t });
  return e && (i.set("as_of", e.asOf), i.set("taxonomy_id", e.taxonomyId), i.set("sector_id", e.sectorId), e.returnView && i.set("return_view", e.returnView)), `#security/${encodeURIComponent(r)}?${i}`;
}
function Us(r) {
  const t = /^#security\/([^?]+)(?:\?(.*))?$/.exec(r);
  if (!t) return null;
  let e;
  try {
    e = decodeURIComponent(t[1]).toLowerCase();
  } catch (c) {
    if (c instanceof URIError) return null;
    throw c;
  }
  const i = new URLSearchParams(t[2] ?? ""), a = i.get("from");
  if (!Vr.test(e) || !va(a)) return null;
  const n = i.get("as_of"), o = i.get("taxonomy_id"), s = i.get("sector_id"), l = i.get("return_view");
  return n !== null || o !== null || s !== null ? !Yr({
    asOf: n ?? void 0,
    taxonomyId: o ?? void 0,
    sectorId: s ?? void 0
  }) || l !== null && !Xs(l) ? null : {
    symbol: e,
    sourceView: a,
    asOf: n,
    taxonomyId: o,
    sectorId: s,
    ...l ? { returnView: l } : {}
  } : l === null ? { symbol: e, sourceView: a } : null;
}
function Gs(r, t) {
  const e = (o) => {
    if (!(o instanceof Element)) return null;
    const s = o.closest("[data-security-symbol]");
    if (!s) return null;
    const l = s.dataset.securitySymbol?.toLowerCase() ?? "", u = s.dataset.securitySource ?? "";
    if (!Vr.test(l) || !va(u)) return null;
    const c = {
      asOf: s.dataset.securityAsOf,
      taxonomyId: s.dataset.securityTaxonomy,
      sectorId: s.dataset.securitySector
    };
    return Object.values(c).some((d) => d !== void 0) ? Yr(c) ? { symbol: l, sourceView: u, decisionContext: c } : null : { symbol: l, sourceView: u };
  }, i = (o) => {
    o.decisionContext ? t(o.symbol, o.sourceView, o.decisionContext) : t(o.symbol, o.sourceView);
  }, a = (o) => {
    const s = e(o.target);
    s && (o.preventDefault(), i(s));
  }, n = (o) => {
    if (o.repeat || o.key !== "Enter" && o.key !== " ") return;
    const s = e(o.target);
    s && (o.preventDefault(), i(s));
  };
  return r.addEventListener("click", a), r.addEventListener("keydown", n), () => {
    r.removeEventListener("click", a), r.removeEventListener("keydown", n);
  };
}
const wr = { phase: "idle" };
function js(r) {
  if (r.phase === "idle")
    throw new Error("cockpit response received before load");
  return {
    symbol: r.symbol,
    sourceView: r.sourceView,
    ...r.decisionContext ? { decisionContext: r.decisionContext } : {}
  };
}
function Ze(r, t) {
  if (t.type === "load")
    return {
      phase: "loading",
      symbol: t.symbol,
      sourceView: t.sourceView,
      ...t.decisionContext ? { decisionContext: t.decisionContext } : {}
    };
  const e = js(r);
  if (t.type === "success") {
    if (t.response.status === "empty") {
      const i = t.response.quality_issues.includes(
        "no_effective_trading_data"
      ) ? "no_effective_trading_data" : "no_market_data";
      return {
        ...e,
        phase: "empty",
        reason: i,
        response: t.response
      };
    }
    return { ...e, phase: "ready", response: t.response };
  }
  return t.type === "empty" ? { ...e, phase: "empty", reason: t.reason } : t.status === 409 ? { ...e, phase: "quality-error", message: t.message } : { ...e, phase: "connection-error", message: t.message };
}
function R(r, t, e) {
  const i = document.createElement(r);
  return t !== void 0 && (i.textContent = t), e && (i.className = e), i;
}
function Tt(r, t = 2) {
  return r == null ? "—" : new Intl.NumberFormat("zh-CN", {
    maximumFractionDigits: t
  }).format(r);
}
function we(r) {
  return r == null ? "—" : `${Math.round(r * 100)}%`;
}
function Oe(r, t, e) {
  const i = R("li", void 0, "decision-stage panel");
  i.dataset.decisionStage = r, i.append(R("p", t, "panel-kicker"), R("h3", e));
  const a = R("div", void 0, "decision-stage-body");
  return i.append(a), { item: i, body: a };
}
function re(r, t = "degraded") {
  const e = R("p", r, `decision-status decision-status-${t}`);
  return e.setAttribute("role", "status"), e;
}
function Vt(r) {
  const t = R("dl", void 0, "decision-facts");
  for (const [e, i] of r) {
    const a = R("div");
    a.append(
      R("dt", e),
      document.createTextNode(" "),
      R("dd", i, "mono")
    ), t.append(a);
  }
  return t;
}
function tt(r, t) {
  const e = R("div", void 0, "decision-token-row");
  return e.append(R("strong", r)), e.append(
    R("span", t.length ? t.join(" · ") : "无", "mono")
  ), e;
}
function ue(r) {
  return r.length ? r.map((t) => `${t.code}${t.detail ? `：${t.detail}` : ""}`).join(" · ") : "无";
}
function _a(r) {
  const t = R("div", void 0, "decision-metric-list");
  for (const e of r) {
    const i = R("details", void 0, "decision-detail"), a = R(
      "summary",
      `${e.metric} · 得分 ${Tt(e.score)}`
    ), n = e.effective_count !== null && e.target_count !== null ? `${e.effective_count} / ${e.target_count}` : "—";
    i.append(
      a,
      Vt([
        ["原始值", `${Tt(e.raw_value, 4)} ${e.unit}`],
        ["覆盖样本", n],
        ["覆盖率", we(e.coverage_ratio)],
        ["权重", Tt(e.weight, 4)],
        ["加权分", Tt(e.weighted_score, 4)],
        ["公式版本", e.formula_version],
        ["质量", e.quality_status]
      ]),
      tt("缺失输入", e.missing_inputs),
      tt("质量问题", e.quality_issues)
    ), t.append(i);
  }
  return t;
}
function mi(r) {
  const t = R("details", void 0, "decision-detail");
  if (t.append(R("summary", "查看市场分项、支持证据与反例")), !r.length)
    return t.append(R("p", "没有可展示的市场分项。", "state-message")), t;
  for (const e of r) {
    const i = R("section", void 0, "decision-component");
    i.append(
      R(
        "h4",
        `${e.name} · ${Tt(e.score)} · ${e.quality_status}`
      ),
      Vt([
        ["权重", Tt(e.weight, 4)],
        ["加权分", Tt(e.weighted_score, 4)],
        ["公式版本", e.formula_version]
      ]),
      tt("支持", [ue(e.supporting_evidence)]),
      tt("反例", [ue(e.contrary_evidence)]),
      tt("缺失输入", e.missing_inputs),
      tt("质量问题", e.quality_issues)
    ), t.append(i);
  }
  return t;
}
function nr() {
  return re(
    "仅沪深主板价格样本 / 不能代表全 A 股；以下状态是受限样本的后端分析结果。",
    "warning"
  );
}
function ya(r, t) {
  if (r.append(nr()), t.status === "empty") {
    r.append(
      R("p", "市场状态证据不足，未形成可用结论。", "decision-empty"),
      tt("缺失输入", t.missing_inputs),
      tt("质量问题", t.quality_issues),
      mi(t.component_scores)
    );
    return;
  }
  r.append(
    Vt([
      ["战略样本状态", t.strategic_state],
      ["战术样本状态", t.tactical_state],
      ["后端总分", Tt(t.total_score)],
      ["置信度", `${we(t.confidence.value)} / ${t.confidence.level}`],
      ["请求时点", t.as_of],
      ["实际数据日", t.data_as_of ?? "—"],
      ["公式版本", t.formula_version],
      ["样本范围", t.actual_market_scope.scope_status],
      ["覆盖证据", t.actual_market_scope.coverage_evidence_status]
    ]),
    mi(t.component_scores),
    tt("支持证据", [ue(t.supporting_evidence)]),
    tt("反例", [ue(t.contrary_evidence)]),
    tt("缺失输入", t.missing_inputs),
    tt("质量问题", t.quality_issues)
  );
}
function Zs(r) {
  r.append(
    re("资金证据尚不可用", "missing"),
    R(
      "p",
      "Release 1 未发布 L1/L2/L3 资金证据；成交额仅属于量价证据，不等于净流入，也不用于推断主力方向。",
      "state-message"
    ),
    Vt([
      ["发布阶段", "Release 2"],
      ["当前证据层级", "missing / unavailable"],
      ["可发布趋势", "否"]
    ])
  );
}
function br(r) {
  const t = R("details", void 0, "decision-detail");
  t.append(R("summary", "查看分类、行情血缘与实际覆盖"));
  const e = r.classification_lineage, i = r.market_lineage;
  return t.append(
    Vt([
      ["分类代际", e?.generation_id ?? "—"],
      ["分类版本", e?.schema_version ?? "—"],
      ["分类源日期", e?.source_snapshot_date ?? "—"],
      ["日期语义", e?.source_date_semantics ?? "—"],
      ["分类覆盖率", we(e?.coverage_ratio)],
      ["行情版本", i?.source_version ?? "—"],
      ["行情最新输入", i?.latest_input_date ?? "—"],
      ["行情内容哈希", i?.content_hash ?? "—"],
      ["实际范围", r.actual_scope.scope_status],
      ["定价/分类", `${r.actual_scope.priced_classified_symbols} / ${r.actual_scope.classification_eligible_symbols}`]
    ])
  ), t;
}
function xa(r, t, e) {
  const i = (d) => r.metric_scores.find((h) => h.metric === d)?.raw_value, a = i("leader_count"), n = i("leader_diffusion"), o = i("leader_persistence_days"), s = R(
    "article",
    void 0,
    `decision-ranking${t ? " is-selected" : ""}`
  ), l = R("div", void 0, "decision-ranking-heading"), u = R("div");
  u.append(
    R("span", `后端顺序 ${r.rank}`, "quiet-tag"),
    R("h4", r.sector_name),
    R(
      "p",
      `${r.sector_id} · 得分 ${Tt(r.total_score)} · 置信度 ${we(r.confidence.value)}`,
      "meta-line mono"
    )
  );
  const c = R("button", `查看 ${r.sector_name} 龙头`);
  return c.type = "button", c.dataset.decisionSector = r.sector_id, c.dataset.decisionAsOf = e.asOf, c.dataset.decisionTaxonomy = e.taxonomyId, c.setAttribute("aria-pressed", t ? "true" : "false"), t && c.setAttribute("aria-current", "true"), l.append(u, c), s.append(
    l,
    Vt([
      ["成员/有价", `${r.priced_member_count} / ${r.member_count}`],
      ["质量", r.quality_status],
      ["排名资格", r.ranking_eligible ? "eligible" : "excluded"],
      ["龙头数量", a == null ? "—" : Tt(a, 0)],
      ["扩散度", we(n)],
      ["持续", o == null ? "—" : `${Tt(o)} 日`]
    ]),
    _a(r.metric_scores),
    tt("支持证据", [ue(r.supporting_evidence)]),
    tt("反例", [ue(r.contrary_evidence)]),
    tt("缺失输入", r.missing_inputs),
    tt("排名排除原因", r.ranking_exclusion_reasons),
    tt("质量问题", r.quality_issues)
  ), s;
}
function Ks(r, t, e, i) {
  if (r.append(nr()), !t.rankings.length) {
    r.append(
      R(
        "p",
        "板块数据为空不代表市场没有热点；当前没有足够、已发布的分类与行情证据。",
        "decision-empty"
      ),
      tt("缺失输入", t.missing_inputs),
      tt("质量问题", t.quality_issues),
      br(t)
    );
    return;
  }
  r.append(
    re(`后端返回 ${t.rankings.length} 个板块；保持原始顺序`, t.quality_status)
  );
  const a = R("div", void 0, "decision-rankings");
  for (const n of t.rankings)
    a.append(
      xa(n, n.sector_id === e, i)
    );
  r.append(
    a,
    br(t),
    Vt([
      ["响应公式", t.formula_version],
      ["分类体系", t.taxonomy_id],
      ["实际数据日", t.data_as_of ?? "—"]
    ]),
    tt("缺失输入", t.missing_inputs),
    tt("质量问题", t.quality_issues)
  );
}
function Js(r) {
  r.append(
    re("尚无本地组合风险输入", "missing"),
    R(
      "p",
      "组合风险与目标暴露属于 Release 2。没有现金、净值和风险预算时，不生成仓位区间，也不猜测个人仓位。",
      "state-message"
    )
  );
}
function Qe(r, t, e, i) {
  if (r.append(
    re(
      "龙头仅为后端受限样本候选；不可作为操作首选，不构成买卖建议。",
      "warning"
    )
  ), t.phase === "idle") {
    r.append(R("p", "先选择一个有证据的板块。", "decision-empty"));
    return;
  }
  if (t.phase === "loading") {
    r.append(R("p", "正在读取后端龙头原因与风险…", "state-message"));
    return;
  }
  if (t.phase === "error") {
    r.append(
      R("p", `龙头证据读取失败：${t.message}`, "decision-empty")
    );
    return;
  }
  const a = t.response;
  a.candidates.length || r.append(
    R("p", "当前没有可展示候选；这不等于板块没有龙头。", "decision-empty"),
    tt("缺失输入", a.missing_inputs)
  );
  const n = R("div", void 0, "decision-candidates");
  for (const o of a.candidates) {
    const s = R("article", void 0, "decision-candidate"), l = R("div", void 0, "decision-ranking-heading"), u = R("div");
    u.append(
      R("span", `后端顺序 ${o.rank}`, "quiet-tag"),
      R("h4", `${o.name} · ${o.symbol}`),
      R(
        "p",
        `得分 ${Tt(o.total_score)} · 置信度 ${we(o.confidence.value)}`,
        "meta-line mono"
      )
    );
    const c = R(
      "button",
      `打开 ${o.symbol} 技术驾驶舱`
    );
    c.type = "button", c.dataset.securitySymbol = o.symbol, c.dataset.securitySource = "sectors", c.dataset.securityAsOf = e.asOf, c.dataset.securityTaxonomy = e.taxonomyId, c.dataset.securitySector = i ?? a.sector_id, l.append(u, c), s.append(
      l,
      Vt([
        ["可操作性", `${o.actionable_primary ? "可" : "不可"}作为操作首选`],
        ["研究龙头资格", o.leader_qualified ? "qualified" : "not qualified"],
        ["资格版本", o.qualification_version],
        ["操作状态", o.actionability_status],
        ["涨跌停锁定", o.limit_lock_status === "unavailable" ? "涨跌停锁定状态不可用" : o.limit_lock_status],
        ["质量", o.quality_status]
      ]),
      _a(o.metric_scores),
      tt("支持证据", [ue(o.supporting_evidence)]),
      tt("反例", [ue(o.contrary_evidence)]),
      tt("缺失输入", o.missing_inputs),
      tt("资格原因", o.qualification_reasons),
      tt("不合格原因", o.disqualification_reasons),
      tt("风险/质量", o.quality_issues)
    ), n.append(s);
  }
  if (r.append(n), a.exclusions.length) {
    const o = R("details", void 0, "decision-detail");
    o.append(R("summary", "查看被排除证券与原因"));
    for (const s of a.exclusions)
      o.append(
        R(
          "p",
          `${s.symbol} · ${s.reasons.join(" · ")}`,
          "mono state-message"
        )
      );
    r.append(o);
  }
  r.append(
    Vt([
      ["响应公式", a.formula_version],
      ["板块", a.sector_name ?? a.sector_id],
      ["资金证据", `${a.fund_flow_evidence.status} / Release 2`],
      ["实际数据日", a.data_as_of ?? "—"]
    ]),
    tt("缺失输入", a.missing_inputs),
    tt("质量问题", a.quality_issues)
  );
}
function hr(r, t) {
  const e = R("article", void 0, "panel");
  return e.append(
    R("p", r, "panel-kicker"),
    R("h3", t)
  ), e;
}
function Qs(r, t, e, i) {
  if (!t.rankings.length) {
    r.append(
      R(
        "p",
        "板块数据为空不代表市场没有热点；当前没有足够、已发布的分类与行情证据。",
        "decision-empty"
      ),
      tt("缺失输入", t.missing_inputs),
      tt("质量问题", t.quality_issues)
    );
    return;
  }
  r.append(
    re(`后端返回 ${t.rankings.length} 个板块；保持原始顺序`, t.quality_status)
  );
  const a = R("div", void 0, "decision-rankings");
  a.dataset.sectorRankingList = "true";
  for (const n of t.rankings) {
    const o = R(
      "article",
      void 0,
      `decision-ranking${n.sector_id === e ? " is-selected" : ""}`
    ), s = R("div", void 0, "decision-ranking-heading"), l = R("div");
    l.append(
      R("span", `后端顺序 ${n.rank}`, "quiet-tag"),
      R("h4", n.sector_name),
      R(
        "p",
        `${n.sector_id} · 得分 ${Tt(n.total_score)} · 置信度 ${we(n.confidence.value)}`,
        "meta-line mono"
      )
    );
    const u = R("button", `查看 ${n.sector_name} 龙头`);
    u.type = "button", u.dataset.decisionSector = n.sector_id, u.dataset.decisionAsOf = i.asOf, u.dataset.decisionTaxonomy = i.taxonomyId, u.setAttribute(
      "aria-pressed",
      n.sector_id === e ? "true" : "false"
    ), n.sector_id === e && u.setAttribute("aria-current", "true"), s.append(l, u), o.append(
      s,
      Vt([
        ["后端总分", Tt(n.total_score)],
        ["质量", n.quality_status],
        ["排名资格", n.ranking_eligible ? "eligible" : "excluded"]
      ])
    ), a.append(o);
  }
  r.append(
    a,
    br(t),
    Vt([
      ["响应公式", t.formula_version],
      ["分类体系", t.taxonomy_id],
      ["实际数据日", t.data_as_of ?? "—"]
    ]),
    tt("缺失输入", t.missing_inputs),
    tt("质量问题", t.quality_issues)
  );
}
function wa(r, t) {
  return r === 422 ? `日期不在可读取范围：${t}` : r === 503 ? `本地行情存储暂不可用：${t}` : `本地分析接口不可用：${t}`;
}
function tl(r, t) {
  r.replaceChildren();
  const e = R("header", void 0, "decision-header"), i = R("div");
  i.append(
    R("p", "RELEASE 1 / EVIDENCE FIRST", "panel-kicker"),
    R("h2", "市场 → 板块 → 龙头 → 个股"),
    R(
      "p",
      `请求时点 ${t.query.asOf} · 分类 ${t.query.taxonomyId} · 后端结论只读展示`,
      "meta-line mono"
    )
  ), e.append(i), r.append(e);
  const a = R("ol", void 0, "decision-flow-list");
  a.setAttribute("aria-label", "收盘后证据决策顺序");
  const n = Oe("market", "01 / REGIME", "市场状态"), o = Oe("fund-flow", "02 / FUND EVIDENCE", "资金证据"), s = Oe("sectors", "03 / ROTATION", "板块轮动"), l = Oe(
    "portfolio-risk",
    "04 / PORTFOLIO",
    "组合风险"
  ), u = Oe("leaders", "05 / LEADERS", "龙头候选 → 个股");
  if (a.append(
    n.item,
    o.item,
    s.item,
    l.item,
    u.item
  ), r.append(a), Zs(o.body), Js(l.body), t.phase === "loading") {
    n.body.append(re("正在读取市场状态与板块轮动…", "loading")), s.body.append(re("等待后端板块证据…", "loading")), Qe(u.body, { phase: "idle" }, t.query, null);
    return;
  }
  if (t.phase === "error") {
    const c = R("p", wa(t.status, t.message), "decision-empty");
    c.setAttribute("role", "alert"), n.body.append(c, nr()), s.body.append(
      R("p", "板块数据未读取；不解释为无热点。", "decision-empty")
    ), Qe(u.body, { phase: "idle" }, t.query, null);
    return;
  }
  ya(n.body, t.overview.market), Ks(
    s.body,
    t.overview.sectors,
    t.selectedSectorId,
    t.query
  ), Qe(
    u.body,
    t.leaders,
    t.query,
    t.selectedSectorId
  );
}
function el(r, t) {
  r.replaceChildren();
  const e = R("header", void 0, "decision-header"), i = R("div");
  if (i.append(
    R("p", "SECTORS / EVIDENCE FIRST", "panel-kicker"),
    R("h2", "板块证据工作区"),
    R(
      "p",
      `请求时点 ${t.query.asOf} · 分类 ${t.query.taxonomyId} · 后端结论只读展示`,
      "meta-line mono"
    )
  ), e.append(i), r.append(e), t.phase === "loading") {
    r.append(re("正在读取市场状态与板块轮动…", "loading"));
    return;
  }
  if (t.phase === "error") {
    const u = R("p", wa(t.status, t.message), "decision-empty");
    u.setAttribute("role", "alert"), r.append(u, nr());
    return;
  }
  const a = hr("01 / REGIME", "市场状态（同一复盘上下文）");
  ya(a, t.overview.market);
  const n = R("div", void 0, "sector-workspace sector-evidence-workspace"), o = hr("02 / RANKING", "后端板块排名");
  Qs(
    o,
    t.overview.sectors,
    t.selectedSectorId,
    t.query
  );
  const s = hr("03 / DETAIL", "板块证据与龙头候选"), l = t.overview.sectors.rankings.find(
    (u) => u.sector_id === t.selectedSectorId
  );
  l ? (s.append(xa(l, !0, t.query)), Qe(s, t.leaders, t.query, l.sector_id)) : s.append(
    R("p", "尚无可选择的后端板块；不使用补充数据或虚构图表。", "decision-empty")
  ), n.append(o, s), r.append(a, n);
}
function q(r, t, e) {
  const i = document.createElement(r);
  return t !== void 0 && (i.textContent = t), e && (i.className = e), i;
}
function wt(r, t = 4) {
  return r === null ? "—" : new Intl.NumberFormat("zh-CN", {
    maximumFractionDigits: t
  }).format(r);
}
function rl(r) {
  return r === "portfolio" ? "持仓" : r === "watchlists" ? "自选预警" : "盘后决策流";
}
function _i(r) {
  const t = q("dl", void 0, "security-metadata"), e = [
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
  for (const [i, a] of e) {
    const n = q("div");
    n.append(q("dt", i), q("dd", a, "mono")), t.append(n);
  }
  return t;
}
function yi(r, t) {
  const e = q("article", void 0, "panel indicator-panel");
  e.append(q("h3", r));
  const i = q("dl", void 0, "indicator-values");
  for (const [a, n] of t) {
    const o = q("div");
    o.append(q("dt", a), q("dd", wt(n), "mono")), i.append(o);
  }
  return e.append(i), e;
}
function xt(r) {
  return q("td", r);
}
function il(r) {
  const t = q("div", void 0, "table-wrap security-table"), e = q("table");
  e.setAttribute("aria-label", "技术指标数值替代");
  const i = q(
    "caption",
    `最近 ${Math.min(r.length, 20)} 个有效交易日；完整图表共 ${r.length} 条`
  ), a = q("thead"), n = q("tr");
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
    const l = q("th", s);
    l.scope = "col", n.append(l);
  }
  a.append(n);
  const o = q("tbody");
  for (const s of r.slice(-20)) {
    const l = q("tr");
    l.append(
      xt(s.trade_date),
      xt(wt(s.open)),
      xt(wt(s.high)),
      xt(wt(s.low)),
      xt(wt(s.close)),
      xt(wt(s.volume, 0)),
      xt(wt(s.ma5)),
      xt(wt(s.ma10)),
      xt(wt(s.ma20)),
      xt(wt(s.ma60)),
      xt(wt(s.ma120)),
      xt(wt(s.ma250)),
      xt(wt(s.macd)),
      xt(wt(s.macd_signal)),
      xt(wt(s.macd_hist)),
      xt(wt(s.rsi14))
    ), o.append(l);
  }
  return e.append(i, a, o), t.append(e), t;
}
function al(r, t) {
  const e = t === "no_effective_trading_data" ? "该窗口没有有效交易数据（记录可能全部为停牌占位）。" : "该窗口没有可用行情。", i = q("div", void 0, "panel security-state");
  i.append(
    q("p", "EMPTY / 真实空态", "panel-kicker"),
    q(
      "h2",
      t === "no_effective_trading_data" ? "没有有效交易数据" : "没有可用行情"
    ),
    q("p", e, "state-message")
  ), r.append(i);
}
function nl(r, t, e) {
  const i = r.querySelector("#security-back"), a = r.querySelector("#security-status"), n = r.querySelector("#security-content");
  if (!i || !a || !n)
    throw new Error("security cockpit root is incomplete");
  if (n.replaceChildren(), t.phase === "idle")
    return a.textContent = "尚未选择证券", null;
  if (t.decisionContext) {
    const v = t.decisionContext;
    i.href = v.returnView === "sectors" ? $r(v) : Wr(v), i.dataset.decisionReturn = "true", delete i.dataset.viewTarget;
  } else
    i.href = `#${t.sourceView}`, i.dataset.viewTarget = t.sourceView, delete i.dataset.decisionReturn;
  if (i.textContent = `← 返回${rl(t.sourceView)}`, t.phase === "loading") {
    a.textContent = `正在读取 ${t.symbol} 的交易日和后端分析…`;
    const v = q("div", void 0, "panel security-state");
    return v.append(
      q("p", "LOADING / 后端分析", "panel-kicker"),
      q("h2", t.symbol),
      q("p", "先读取有效交易日，再请求最多 260 日分析。")
    ), n.append(v), null;
  }
  if (t.phase === "empty")
    return a.textContent = `${t.symbol} 无可绘制数据`, t.response && n.append(_i(t.response)), al(n, t.reason), null;
  if (t.phase === "quality-error") {
    a.textContent = `${t.symbol} 未通过数据质量门禁`;
    const v = q("div", void 0, "panel security-state");
    return v.append(
      q("p", "HTTP 409 / FAIL CLOSED", "panel-kicker"),
      q("h2", "数据质量门禁"),
      q("p", t.message, "state-message"),
      q("p", "未使用未复权数据降级，也未绘制蜡烛。", "state-message")
    ), n.append(v), null;
  }
  if (t.phase === "connection-error") {
    a.textContent = `${t.symbol} 连接失败`;
    const v = q("div", void 0, "panel security-state");
    return v.append(
      q("p", "CONNECTION ERROR / 本地服务", "panel-kicker"),
      q("h2", "无法连接本地 API"),
      q("p", t.message, "state-message"),
      q("p", "请确认 Stock EVA API 仅在本机运行。", "state-message")
    ), n.append(v), null;
  }
  const o = t.response;
  a.textContent = `${o.symbol} 已加载 ${o.series.length} 个有效交易日`, n.append(_i(o));
  const s = q("article", void 0, "panel security-chart-panel"), l = q("div", void 0, "panel-heading"), u = q("div");
  u.append(
    q("p", "QFQ DAILY / API INDICATORS", "panel-kicker"),
    q("h2", `${o.symbol} 技术驾驶舱`)
  ), l.append(u, q("span", `截至 ${o.as_of ?? "—"}`, "quiet-tag"));
  const c = q(
    "p",
    `前复权日 K、真实成交量和后端 MA，共 ${o.series.length} 个有效交易日。`,
    "state-message"
  ), d = q("div", void 0, "security-chart");
  d.id = "security-chart", d.dataset.testid = "security-chart", d.setAttribute("role", "img"), d.setAttribute(
    "aria-label",
    `${o.symbol} 前复权日 K、成交量、MA5、10、20、60、120、250、MACD、RSI14 图表`
  ), s.append(l, c, d), n.append(s);
  const h = o.series[o.series.length - 1], f = q("div", void 0, "indicator-grid");
  return f.append(
    yi("MACD（12, 26, 9）", [
      ["MACD", h.macd],
      ["Signal", h.macd_signal],
      ["Histogram", h.macd_hist]
    ]),
    yi("RSI（14）", [["RSI14", h.rsi14]])
  ), n.append(f, il(o.series)), e(d, o);
}
function ct(r, t, e) {
  const i = document.createElement(r);
  return t !== void 0 && (i.textContent = t), e && (i.className = e), i;
}
function zr(r) {
  const t = ct("dl", void 0, "decision-facts");
  for (const [e, i] of r) {
    const a = ct("div");
    a.append(
      ct("dt", e),
      document.createTextNode(" "),
      ct("dd", i, "mono")
    ), t.append(a);
  }
  return t;
}
function Ke(r, t) {
  const e = ct("article", void 0, "panel security-evidence-panel");
  return e.append(ct("p", t, "panel-kicker"), ct("h3", r)), e;
}
function qr(r, t, e) {
  const i = e.status === null ? "连接错误" : `HTTP ${e.status}`, a = ct(
    "p",
    `${t}暂不可用 · ${i} · ${e.message}`,
    "decision-status decision-status-degraded"
  );
  a.setAttribute("role", "status"), r.append(a);
}
function ol(r, t) {
  if (t.phase === "error") {
    qr(r, "市场证据", t);
    return;
  }
  const e = t.response;
  r.append(
    zr([
      ["战略样本状态", e.strategic_state],
      ["战术样本状态", e.tactical_state],
      ["质量", e.quality_status],
      ["样本范围", e.actual_market_scope.scope_status],
      ["实际数据日", e.data_as_of ?? "—"]
    ]),
    ct(
      "p",
      `受限样本：${e.actual_market_scope.conclusion_disclaimer}`,
      "state-message"
    )
  );
}
function sl(r, t, e) {
  if (e.phase === "error") {
    qr(r, "板块证据", e);
    return;
  }
  const i = e.response, a = i.rankings.find(
    (n) => n.sector_id === t.sectorId
  );
  if (!a) {
    r.append(
      ct(
        "p",
        "该板块未出现在后端排名中；不解释为市场没有热点。",
        "decision-status decision-status-degraded"
      )
    );
    return;
  }
  r.append(
    zr([
      ["板块", `${a.sector_name} · ${a.sector_id}`],
      ["后端顺序", String(a.rank)],
      ["后端总分", a.total_score === null ? "—" : String(a.total_score)],
      ["排名资格", a.ranking_eligible ? "eligible" : "excluded"],
      ["质量", a.quality_status],
      ["实际数据日", i.data_as_of ?? "—"]
    ])
  );
}
function ll(r, t, e) {
  if (e.phase === "error") {
    qr(r, "龙头证据", e);
    return;
  }
  const i = e.response, a = i.candidates.find((o) => o.symbol === t);
  if (a) {
    r.append(
      zr([
        ["证券", `${a.name} · ${a.symbol}`],
        ["后端顺序", String(a.rank)],
        ["研究龙头资格", a.leader_qualified ? "qualified" : "not qualified"],
        ["资格原因", a.qualification_reasons.join(" · ") || "无"],
        ["不合格原因", a.disqualification_reasons.join(" · ") || "无"],
        ["操作状态", a.actionability_status],
        ["涨跌停锁定状态", a.limit_lock_status],
        ["质量", a.quality_status]
      ]),
      ct("p", "后端候选仅供复盘研究，不构成操作结论。", "state-message")
    );
    return;
  }
  const n = i.exclusions.find((o) => o.symbol === t);
  if (n) {
    r.append(
      ct(
        "p",
        `该证券被后端排除：${n.reasons.join(" · ")}`,
        "decision-status decision-status-degraded"
      )
    );
    return;
  }
  r.append(
    ct(
      "p",
      "该证券未出现在候选或排除列表；不解释为没有板块关联。",
      "decision-status decision-status-degraded"
    )
  );
}
function ul(r) {
  const t = ct(
    "p",
    "资金证据 missing / unavailable",
    "decision-status decision-status-missing"
  );
  t.setAttribute("role", "status"), r.append(
    t,
    ct(
      "p",
      "Release 1 尚无可发布资金趋势；成交额不等于净流入，不能据此推断主力方向。",
      "state-message"
    )
  );
}
function cl(r, t) {
  if (r.replaceChildren(), t.phase === "idle") return;
  const e = ct("header", void 0, "security-evidence-header");
  if (e.append(
    ct("p", "DECISION CONTEXT / BACKEND EVIDENCE", "panel-kicker"),
    ct("h2", "同一复盘上下文"),
    ct(
      "p",
      `${t.symbol} · 请求时点 ${t.context.asOf} · 分类 ${t.context.taxonomyId} · 板块 ${t.context.sectorId}`,
      "meta-line mono"
    )
  ), r.append(e), t.phase === "loading") {
    const l = ct(
      "p",
      "正在读取同一时点的市场、板块与龙头证据…",
      "decision-status decision-status-loading"
    );
    l.setAttribute("role", "status"), r.append(l);
    return;
  }
  const i = ct("div", void 0, "security-evidence-grid"), a = Ke("市场状态", "01 / REGIME"), n = Ke("板块上下文", "02 / SECTOR"), o = Ke("个股龙头证据", "03 / LEADER"), s = Ke("资金证据边界", "04 / FUND FLOW");
  ol(a, t.market), sl(n, t.context, t.sectors), ll(o, t.symbol, t.leaders), ul(s), i.append(a, n, o, s), r.append(i);
}
const dl = "baostock.industry_classification", xi = "stock-eva-route-change";
let Lt = wr, rt = null, Jt = { phase: "idle" }, Xt = null, Mt = null, Qt = null, Ut = "overview", ce = null, de = null, Ve = null, rr = !1;
function ba() {
  const r = document.querySelector("#security-analysis");
  if (!r) throw new Error("security cockpit section is missing");
  return r;
}
function _e(r = Ut) {
  return document.querySelector(
    r === "sectors" ? "#sector-decision-flow" : "#decision-flow"
  );
}
function hl() {
  return ba().querySelector("#security-evidence");
}
function wi() {
  ce?.(), ce = nl(
    ba(),
    Lt,
    As
  );
}
function We() {
  const r = _e();
  !r || !rt || (Ut === "sectors" ? el(r, rt) : tl(r, rt));
}
function Ae() {
  const r = hl();
  r && cl(r, Jt);
}
function bi(r, t = Ut) {
  Array.from(
    _e(t)?.querySelectorAll("[data-decision-sector]") ?? []
  ).find((i) => i.dataset.decisionSector === r)?.focus();
}
function vl() {
  const r = new Intl.DateTimeFormat("en-US", {
    timeZone: "Asia/Shanghai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  }).formatToParts(/* @__PURE__ */ new Date()), t = Object.fromEntries(r.map((e) => [e.type, e.value]));
  return `${t.year}-${t.month}-${t.day}`;
}
function Ci() {
  return { asOf: vl(), taxonomyId: dl };
}
function Cr(r) {
  return { asOf: r.asOf, taxonomyId: r.taxonomyId };
}
function Xr(r) {
  return {
    status: r instanceof xr ? r.status : null,
    message: r instanceof xr ? r.detail : r instanceof Error ? r.message : "未知连接错误"
  };
}
function vr(r) {
  return r.status === "fulfilled" ? { phase: "ready", response: r.value } : { phase: "error", ...Xr(r.reason) };
}
function fl(r, t) {
  return rt?.phase !== "ready" || rt.query.asOf !== t.asOf || rt.query.taxonomyId !== t.taxonomyId || rt.selectedSectorId !== t.sectorId || rt.leaders.phase !== "ready" || rt.leaders.response.sector_id !== t.sectorId ? null : {
    phase: "ready",
    symbol: r,
    context: t,
    market: { phase: "ready", response: rt.overview.market },
    sectors: { phase: "ready", response: rt.overview.sectors },
    leaders: { phase: "ready", response: rt.leaders.response }
  };
}
async function pl(r, t, e) {
  if (Mt?.abort(), Mt = null, e) {
    Jt = e, Ae();
    return;
  }
  const i = new AbortController();
  Mt = i, Jt = { phase: "loading", symbol: r, context: t }, Ae();
  const a = await Promise.allSettled([
    ca(t.asOf, fetch, i.signal),
    da(
      t.asOf,
      t.taxonomyId,
      fetch,
      i.signal
    ),
    ha(
      t.asOf,
      t.taxonomyId,
      t.sectorId,
      fetch,
      i.signal
    )
  ]);
  Mt !== i || i.signal.aborted || (Jt = {
    phase: "ready",
    symbol: r,
    context: t,
    market: vr(a[0]),
    sectors: vr(a[1]),
    leaders: vr(a[2])
  }, Mt = null, Ae());
}
function gl(r, t, e) {
  const i = e === "sectors" ? $r({ ...r, sectorId: t }) : Wr({ ...r, sectorId: t });
  de = i, window.history.replaceState(null, "", i);
}
async function Ca(r, t, e, i = !1) {
  if (!Qt || rt?.phase !== "ready") return;
  const a = rt.overview;
  rt = {
    phase: "ready",
    query: r,
    overview: a,
    selectedSectorId: t,
    leaders: { phase: "loading" }
  }, We(), i && bi(t, e);
  try {
    const n = await Qt.loadLeaders({
      ...r,
      sectorId: t
    });
    if (n === null || rt?.phase !== "ready" || rt.query.asOf !== r.asOf || rt.query.taxonomyId !== r.taxonomyId || rt.selectedSectorId !== t || Ut !== e)
      return;
    rt = {
      ...rt,
      leaders: { phase: "ready", response: n }
    };
  } catch (n) {
    if (rt?.phase !== "ready" || rt.selectedSectorId !== t || Ut !== e)
      return;
    rt = {
      ...rt,
      leaders: { phase: "error", ...Xr(n) }
    };
  }
  We(), i && bi(t, e);
}
async function Ne(r, t, e = "overview") {
  if (!(!_e(e) || !Qt)) {
    Ut = e, Xt?.abort(), Xt = null, Mt?.abort(), Mt = null, Jt = { phase: "idle" }, Ae(), ce?.(), ce = null, window.stockEvaActivateView?.(e, !1), rt = { phase: "loading", query: r }, We();
    try {
      const a = await Qt.loadOverview(r);
      if (a === null) return;
      const o = a.sectors.rankings.some(
        (s) => s.sector_id === t
      ) ? t : a.sectors.rankings[0]?.sector_id ?? null;
      rt = {
        phase: "ready",
        query: r,
        overview: a,
        selectedSectorId: o,
        leaders: { phase: "idle" }
      }, gl(r, o, e), We(), o && await Ca(r, o, e);
    } catch (a) {
      rt = {
        phase: "error",
        query: r,
        ...Xr(a)
      }, We();
    }
  }
}
async function Ea(r, t, e) {
  Xt?.abort();
  const i = new AbortController();
  Xt = i;
  const a = e ? fl(r, e) : null;
  Qt?.dispose(), window.stockEvaActivateView?.("security", !1), Lt = Ze(Lt, {
    type: "load",
    symbol: r,
    sourceView: t,
    ...e ? { decisionContext: e } : {}
  }), wi(), e ? pl(r, e, a) : (Mt?.abort(), Mt = null, Jt = { phase: "idle" }, Ae());
  try {
    const n = await Oa(
      r,
      fetch,
      i.signal,
      e?.asOf
    );
    if (Xt !== i || i.signal.aborted) return;
    Lt = Ze(Lt, {
      type: "success",
      response: n
    });
  } catch (n) {
    if (Xt !== i || i.signal.aborted) return;
    n instanceof Si ? Lt = Ze(Lt, {
      type: "empty",
      reason: "no_market_data"
    }) : Lt = Ze(Lt, {
      type: "failure",
      status: n instanceof fr ? n.status : null,
      message: n instanceof fr ? n.detail : n instanceof Error ? n.message : "未知连接错误"
    });
  }
  wi();
}
function ml(r, t, e) {
  const i = e && t === "sectors" && Ut === "sectors" ? { ...e, returnView: "sectors" } : e, a = Hs(r, t, i);
  de = a, window.history.pushState(null, "", a), Ea(r, t, i);
}
function pe() {
  const r = window.location.hash;
  if (r === de) return;
  de = r;
  const t = Us(r);
  if (t) {
    const a = t.asOf && t.taxonomyId && t.sectorId ? {
      asOf: t.asOf,
      taxonomyId: t.taxonomyId,
      sectorId: t.sectorId,
      ...t.returnView ? { returnView: t.returnView } : {}
    } : void 0;
    Ea(
      t.symbol,
      t.sourceView,
      a
    );
    return;
  }
  const e = ga(r);
  if (e) {
    Ne(Cr(e), e.sectorId);
    return;
  }
  const i = ma(r);
  if (i) {
    Ne(
      Cr(i),
      i.sectorId,
      "sectors"
    );
    return;
  }
  if (r === "" || r === "#overview") {
    Ne(Ci(), null);
    return;
  }
  if (r === "#sectors" || r.startsWith("#sectors?")) {
    Ne(Ci(), null, "sectors");
    return;
  }
  Qt?.dispose(), rt = null, Xt?.abort(), Xt = null, Mt?.abort(), Mt = null, Jt = { phase: "idle" }, Ae(), ce?.(), ce = null;
}
function Je(r) {
  if (!(r.target instanceof Element)) return;
  const e = r.target.closest("[data-decision-sector]")?.dataset.decisionSector;
  if (!e || rt?.phase !== "ready") return;
  r.preventDefault();
  const i = rt.query, a = Ut === "sectors" ? $r({ ...i, sectorId: e }) : Wr({ ...i, sectorId: e });
  de = a, window.history.pushState(null, "", a), Ca(i, e, Ut, !0);
}
function Ei(r) {
  if (!(r.target instanceof Element)) return;
  const t = r.target.closest(
    "#security-back[data-decision-return]"
  );
  if (!t) return;
  r.preventDefault(), r.stopImmediatePropagation();
  const e = ga(t.hash), i = ma(t.hash), a = e ?? i;
  a && (de = t.hash, window.history.pushState(null, "", t.hash), Ne(
    Cr(a),
    a.sectorId,
    i ? "sectors" : "overview"
  ));
}
function Ia() {
  if (Ve) return;
  Lt = wr, rt = null, Jt = { phase: "idle" }, de = null, Qt = new qs(fetch);
  const r = Gs(document, ml);
  _e("overview")?.addEventListener("click", Je), _e("sectors")?.addEventListener("click", Je), document.addEventListener("click", Ei, !0), window.addEventListener("popstate", pe), window.addEventListener("hashchange", pe), window.addEventListener(xi, pe);
  const t = () => {
    Ve === t && (r(), _e("overview")?.removeEventListener("click", Je), _e("sectors")?.removeEventListener("click", Je), document.removeEventListener("click", Ei, !0), window.removeEventListener("popstate", pe), window.removeEventListener("hashchange", pe), window.removeEventListener(xi, pe), Xt?.abort(), Xt = null, Mt?.abort(), Mt = null, Qt?.dispose(), Qt = null, ce?.(), ce = null, Lt = wr, rt = null, Ut = "overview", Jt = { phase: "idle" }, de = null, Ve = null);
  };
  Ve = t, pe();
}
function Sa() {
  rr = !1, Ia();
}
function yl() {
  rr && (document.removeEventListener("DOMContentLoaded", Sa), rr = !1), Ve?.();
}
document.readyState === "loading" ? (rr = !0, document.addEventListener("DOMContentLoaded", Sa, { once: !0 })) : Ia();
export {
  yl as disposeSecurityCockpit,
  Ia as initializeSecurityCockpit
};
