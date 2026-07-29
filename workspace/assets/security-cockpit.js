const ai = "http://127.0.0.1:8000/api/v1", yr = /^(sh|sz)\.[0-9]{6}$/, Fe = /^\d{4}-\d{2}-\d{2}$/;
class Te extends Error {
  status;
  detail;
  constructor(e, t) {
    super(t), this.name = "ApiError", this.status = e, this.detail = t;
  }
}
class _r extends Error {
  constructor() {
    super("no backend-provided trading dates"), this.name = "NoTradingDatesError";
  }
}
function Be(i) {
  return typeof i == "object" && i !== null && !Array.isArray(i);
}
function Qt(i, e) {
  const t = i[e];
  if (typeof t != "string") throw new TypeError(`${e} must be a string`);
  return t;
}
function Gt(i, e) {
  const t = i[e];
  if (typeof t != "number" || !Number.isFinite(t))
    throw new TypeError(`${e} must be a finite number`);
  return t;
}
function Pt(i, e) {
  return i[e] === null ? null : Gt(i, e);
}
function ni(i) {
  if (!Be(i)) throw new TypeError("series point must be an object");
  const e = Qt(i, "trade_date");
  if (!Fe.test(e))
    throw new TypeError("trade_date must be an ISO date");
  return {
    trade_date: e,
    open: Gt(i, "open"),
    high: Gt(i, "high"),
    low: Gt(i, "low"),
    close: Gt(i, "close"),
    volume: Gt(i, "volume"),
    amount: Gt(i, "amount"),
    ma5: Pt(i, "ma5"),
    ma10: Pt(i, "ma10"),
    ma20: Pt(i, "ma20"),
    ma60: Pt(i, "ma60"),
    ma120: Pt(i, "ma120"),
    ma250: Pt(i, "ma250"),
    macd: Pt(i, "macd"),
    macd_signal: Pt(i, "macd_signal"),
    macd_hist: Pt(i, "macd_hist"),
    rsi14: Pt(i, "rsi14")
  };
}
function oi(i) {
  if (!Be(i)) throw new TypeError("analysis response must be an object");
  const e = Qt(i, "symbol"), t = Qt(i, "status"), r = Qt(i, "source"), a = Qt(i, "price_adjustment"), n = Qt(i, "formula_version");
  if (!yr.test(e)) throw new TypeError("invalid response symbol");
  if (t !== "empty" && t !== "ready")
    throw new TypeError("invalid analysis status");
  if (r !== "baostock") throw new TypeError("invalid analysis source");
  if (a !== "qfq")
    throw new TypeError("invalid price adjustment");
  if (i.as_of !== null && typeof i.as_of != "string")
    throw new TypeError("as_of must be an ISO date or null");
  if (typeof i.as_of == "string" && !Fe.test(i.as_of))
    throw new TypeError("as_of must be an ISO date or null");
  if (!Array.isArray(i.quality_issues) || !i.quality_issues.every((o) => typeof o == "string"))
    throw new TypeError("quality_issues must be a string array");
  if (!Array.isArray(i.series))
    throw new TypeError("series must be an array");
  return {
    symbol: e,
    status: t,
    as_of: i.as_of,
    source: r,
    price_adjustment: a,
    formula_version: n,
    quality_issues: i.quality_issues,
    series: i.series.map(ni)
  };
}
function xr(i) {
  if (!Array.isArray(i) || !i.every(
    (e) => typeof e == "string" && Fe.test(e)
  ))
    throw new TypeError("trading dates must be an ISO date array");
  for (let e = 1; e < i.length; e += 1)
    if (i[e] <= i[e - 1])
      throw new TypeError("trading dates must be strictly ascending");
  return i;
}
function si(i) {
  const e = xr(i);
  if (e.length === 0) throw new _r();
  const t = e.slice(-260);
  return { start: t[0], end: t[t.length - 1] };
}
async function li(i) {
  try {
    const e = await i.json();
    if (Be(e) && typeof e.detail == "string")
      return e.detail;
  } catch {
  }
  return `HTTP ${i.status}`;
}
async function Ge(i, e, t) {
  const r = await e(`${ai}${i}`, { signal: t });
  if (!r.ok)
    throw new Te(r.status, await li(r));
  return r.json();
}
async function ui(i, e = fetch, t = new AbortController().signal) {
  if (!yr.test(i)) throw new TypeError("invalid security symbol");
  const r = xr(
    await Ge("/market/history/dates", e, t)
  ), { start: a, end: n } = si(r), o = new URLSearchParams({ start: a, end: n }), s = await Ge(
    `/securities/${encodeURIComponent(i)}/analysis?${o}`,
    e,
    t
  );
  return oi(s);
}
function ot(i, e) {
  if (!(!kt(i) && !kt(e))) {
    for (var t in e)
      if (Object.prototype.hasOwnProperty.call(e, t)) {
        var r = i[t], a = e[t];
        kt(a) && kt(r) ? ot(r, a) : i[t] = ue(a);
      }
  }
}
function ue(i) {
  if (!kt(i))
    return i;
  var e = null;
  Dt(i) ? e = [] : e = {};
  for (var t in i)
    if (Object.prototype.hasOwnProperty.call(i, t)) {
      var r = i[t];
      kt(r) ? e[t] = ue(r) : e[t] = r;
    }
  return e;
}
function Dt(i) {
  return Object.prototype.toString.call(i) === "[object Array]";
}
function nt(i) {
  return typeof i == "function";
}
function kt(i) {
  return typeof i == "object" && C(i);
}
function B(i) {
  return typeof i == "number" && Number.isFinite(i);
}
function C(i) {
  return i != null;
}
function ee(i) {
  return typeof i == "boolean";
}
function $(i) {
  return typeof i == "string";
}
var hi = /\\(\\)?/g, ci = RegExp(`[^.[\\]]+|\\[(?:([^"'][^[]*)|(["'])((?:(?!\\2)[^\\\\]|\\\\.)*?)\\2)\\]|(?=(?:\\.|\\[\\])(?:\\.|\\[\\]|$))`, "g");
function st(i, e, t) {
  if (C(i)) {
    var r = [];
    e.replace(ci, function(s) {
      for (var l = [], u = 1; u < arguments.length; u++)
        l[u - 1] = arguments[u];
      var h = s;
      return C(l[1]) ? h = l[2].replace(hi, "$1") : C(l[0]) && (h = l[0].trim()), r.push(h), "";
    });
    for (var a = i, n = 0, o = r.length; C(a) && n < o; )
      a = a?.[r[n++]];
    return C(a) ? a : t ?? "--";
  }
  return t ?? "--";
}
function di(i, e) {
  var t = {};
  return i.formatToParts(new Date(e)).forEach(function(r) {
    var a = r.type, n = r.value;
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
function vi(i, e, t) {
  var r = di(i, e);
  return t.replace(/YYYY|MM|DD|HH|mm|ss/g, function(a) {
    return r[a];
  });
}
function pt(i, e) {
  var t = +i;
  return B(t) ? t.toFixed(e ?? 2) : "".concat(i);
}
function fi(i) {
  var e = +i;
  if (B(e)) {
    if (e > 1e9)
      return "".concat(+(e / 1e9).toFixed(3), "B");
    if (e > 1e6)
      return "".concat(+(e / 1e6).toFixed(3), "M");
    if (e > 1e3)
      return "".concat(+(e / 1e3).toFixed(3), "K");
  }
  return "".concat(i);
}
function pi(i, e) {
  var t = "".concat(i);
  if (e.length === 0)
    return t;
  if (t.includes(".")) {
    var r = t.split(".");
    return "".concat(r[0].replace(/(\d)(?=(\d{3})+$)/g, function(a) {
      return "".concat(a).concat(e);
    }), ".").concat(r[1]);
  }
  return t.replace(/(\d)(?=(\d{3})+$)/g, function(a) {
    return "".concat(a).concat(e);
  });
}
function gi(i, e) {
  var t = "".concat(i), r = new RegExp("\\.0{" + e + ",}[1-9][0-9]*$");
  if (r.test(t)) {
    var a = t.split("."), n = a.length - 1, o = a[n], s = /0*/.exec(o);
    if (C(s)) {
      var l = s[0].length;
      return a[n] = o.replace(/0*/, "0{".concat(l, "}")), a.join(".");
    }
  }
  return t;
}
function qe(i, e) {
  return i.replace(/\{(\w+)\}/g, function(t, r) {
    var a = e[r];
    return C(a) ? a : "{".concat(r, "}");
  });
}
var ne = null;
function Wt(i) {
  var e, t;
  return (t = (e = i.ownerDocument.defaultView) === null || e === void 0 ? void 0 : e.devicePixelRatio) !== null && t !== void 0 ? t : 1;
}
function Zt(i, e, t) {
  return "".concat(e ?? "normal", " ").concat(i ?? 12, "px ").concat(t ?? "Helvetica Neue");
}
function Rt(i, e, t, r) {
  if (!C(ne)) {
    var a = document.createElement("canvas"), n = Wt(a);
    ne = a.getContext("2d"), ne.scale(n, n);
  }
  return ne.font = Zt(e, t, r), Math.round(ne.measureText(i).width);
}
var Ae = function(i, e) {
  return Ae = Object.setPrototypeOf || { __proto__: [] } instanceof Array && function(t, r) {
    t.__proto__ = r;
  } || function(t, r) {
    for (var a in r) Object.prototype.hasOwnProperty.call(r, a) && (t[a] = r[a]);
  }, Ae(i, e);
};
function X(i, e) {
  if (typeof e != "function" && e !== null)
    throw new TypeError("Class extends value " + String(e) + " is not a constructor or null");
  Ae(i, e);
  function t() {
    this.constructor = i;
  }
  i.prototype = e === null ? Object.create(e) : (t.prototype = e.prototype, new t());
}
var M = function() {
  return M = Object.assign || function(e) {
    for (var t, r = 1, a = arguments.length; r < a; r++) {
      t = arguments[r];
      for (var n in t) Object.prototype.hasOwnProperty.call(t, n) && (e[n] = t[n]);
    }
    return e;
  }, M.apply(this, arguments);
};
function he(i, e) {
  var t = {};
  for (var r in i) Object.prototype.hasOwnProperty.call(i, r) && e.indexOf(r) < 0 && (t[r] = i[r]);
  if (i != null && typeof Object.getOwnPropertySymbols == "function")
    for (var a = 0, r = Object.getOwnPropertySymbols(i); a < r.length; a++)
      e.indexOf(r[a]) < 0 && Object.prototype.propertyIsEnumerable.call(i, r[a]) && (t[r[a]] = i[r[a]]);
  return t;
}
function Oe(i, e, t, r) {
  function a(n) {
    return n instanceof t ? n : new t(function(o) {
      o(n);
    });
  }
  return new (t || (t = Promise))(function(n, o) {
    function s(h) {
      try {
        u(r.next(h));
      } catch (c) {
        o(c);
      }
    }
    function l(h) {
      try {
        u(r.throw(h));
      } catch (c) {
        o(c);
      }
    }
    function u(h) {
      h.done ? n(h.value) : a(h.value).then(s, l);
    }
    u((r = r.apply(i, [])).next());
  });
}
function Le(i, e) {
  var t = { label: 0, sent: function() {
    if (n[0] & 1) throw n[1];
    return n[1];
  }, trys: [], ops: [] }, r, a, n, o = Object.create((typeof Iterator == "function" ? Iterator : Object).prototype);
  return o.next = s(0), o.throw = s(1), o.return = s(2), typeof Symbol == "function" && (o[Symbol.iterator] = function() {
    return this;
  }), o;
  function s(u) {
    return function(h) {
      return l([u, h]);
    };
  }
  function l(u) {
    if (r) throw new TypeError("Generator is already executing.");
    for (; o && (o = 0, u[0] && (t = 0)), t; ) try {
      if (r = 1, a && (n = u[0] & 2 ? a.return : u[0] ? a.throw || ((n = a.return) && n.call(a), 0) : a.next) && !(n = n.call(a, u[1])).done) return n;
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
      u = e.call(i, t);
    } catch (h) {
      u = [6, h], a = 0;
    } finally {
      r = n = 0;
    }
    if (u[0] & 5) throw u[1];
    return { value: u[0] ? u[1] : void 0, done: !0 };
  }
}
function gt(i) {
  var e = typeof Symbol == "function" && Symbol.iterator, t = e && i[e], r = 0;
  if (t) return t.call(i);
  if (i && typeof i.length == "number") return {
    next: function() {
      return i && r >= i.length && (i = void 0), { value: i && i[r++], done: !i };
    }
  };
  throw new TypeError(e ? "Object is not iterable." : "Symbol.iterator is not defined.");
}
function ae(i, e) {
  var t = typeof Symbol == "function" && i[Symbol.iterator];
  if (!t) return i;
  var r = t.call(i), a, n = [], o;
  try {
    for (; (e === void 0 || e-- > 0) && !(a = r.next()).done; ) n.push(a.value);
  } catch (s) {
    o = { error: s };
  } finally {
    try {
      a && !a.done && (t = r.return) && t.call(r);
    } finally {
      if (o) throw o.error;
    }
  }
  return n;
}
function re(i, e, t) {
  if (arguments.length === 2) for (var r = 0, a = e.length, n; r < a; r++)
    (n || !(r in e)) && (n || (n = Array.prototype.slice.call(e, 0, r)), n[r] = e[r]);
  return i.concat(n || Array.prototype.slice.call(e));
}
function Ve(i) {
  var e = {
    width: 0,
    height: 0,
    left: 0,
    right: 0,
    top: 0,
    bottom: 0
  };
  return C(i) && ot(e, i), e;
}
var Yt = -1;
function ce(i) {
  return nt(window.requestAnimationFrame) ? window.requestAnimationFrame(i) : window.setTimeout(i, 20);
}
function Me(i) {
  nt(window.cancelAnimationFrame) ? window.cancelAnimationFrame(i) : window.clearTimeout(i);
}
var Pe = (
  /** @class */
  (function() {
    function i(e) {
      this._options = { duration: 500, iterationCount: 1 }, this._currentIterationCount = 0, this._running = !1, this._time = 0, ot(this._options, e);
    }
    return i.prototype._loop = function() {
      var e = this;
      this._running = !0;
      var t = function() {
        var r;
        if (e._running) {
          var a = (/* @__PURE__ */ new Date()).getTime() - e._time;
          a < e._options.duration ? ((r = e._doFrameCallback) === null || r === void 0 || r.call(e, a), ce(t)) : (e.stop(), e._currentIterationCount++, e._currentIterationCount < e._options.iterationCount && e.start());
        }
      };
      ce(t);
    }, i.prototype.doFrame = function(e) {
      return this._doFrameCallback = e, this;
    }, i.prototype.setDuration = function(e) {
      return this._options.duration = e, this;
    }, i.prototype.setIterationCount = function(e) {
      return this._options.iterationCount = e, this;
    }, i.prototype.start = function() {
      this._running || (this._time = (/* @__PURE__ */ new Date()).getTime(), this._loop());
    }, i.prototype.stop = function() {
      var e;
      this._running && ((e = this._doFrameCallback) === null || e === void 0 || e.call(this, this._options.duration)), this._running = !1;
    }, i;
  })()
), xe = 1, Ze = (/* @__PURE__ */ new Date()).getTime();
function te(i) {
  var e = (/* @__PURE__ */ new Date()).getTime();
  return e === Ze ? ++xe : xe = 1, Ze = e, "".concat(i ?? "").concat(e, "_").concat(xe);
}
function Vt(i, e) {
  var t, r = document.createElement(i), a = e ?? {};
  for (var n in a)
    r.style[n] = (t = a[n]) !== null && t !== void 0 ? t : "";
  return r;
}
function De(i, e, t) {
  var r = 0, a = 0;
  for (a = i.length - 1; r !== a; ) {
    var n = Math.floor((a + r) / 2), o = a - r, s = i[n][e];
    if (t === i[r][e])
      return r;
    if (t === i[a][e])
      return a;
    if (t === s)
      return n;
    if (t > s ? r = n : a = n, o <= 2)
      break;
  }
  return r;
}
function mi(i) {
  var e = Math.floor(Ot(i)), t = qt(e), r = i / t, a = 0;
  return r < 1.5 ? a = 1 : r < 2.5 ? a = 2 : r < 3.5 ? a = 3 : r < 4.5 ? a = 4 : r < 5.5 ? a = 5 : r < 6.5 ? a = 6 : a = 8, i = a * t, +i.toFixed(Math.abs(e));
}
function $e(i, e) {
  e = Math.max(0, e ?? 0);
  var t = Math.pow(10, e);
  return Math.round(i * t) / t;
}
function yi(i) {
  var e = i.toString(), t = e.indexOf("e");
  if (t > 0) {
    var r = +e.slice(t + 1);
    return r < 0 ? -r : 0;
  }
  var a = e.indexOf(".");
  return a < 0 ? 0 : e.length - 1 - a;
}
function br(i, e, t) {
  for (var r, a, n = [Number.MIN_SAFE_INTEGER, Number.MAX_SAFE_INTEGER], o = i.length, s = 0; s < o; ) {
    var l = i[s];
    n[0] = Math.max((r = l[e]) !== null && r !== void 0 ? r : Number.MIN_SAFE_INTEGER, n[0]), n[1] = Math.min((a = l[t]) !== null && a !== void 0 ? a : Number.MAX_SAFE_INTEGER, n[1]), ++s;
  }
  return n;
}
function Ot(i) {
  return i === 0 ? 0 : Math.log10(i);
}
function qt(i) {
  return Math.pow(10, i);
}
function je() {
  return { from: 0, to: 0, realFrom: 0, realTo: 0 };
}
var _i = (
  /** @class */
  (function() {
    function i(e) {
      this._holdingTasks = null, this._running = !1, this._callback = e;
    }
    return i.prototype.add = function(e) {
      this._running ? C(this._holdingTasks) ? this._holdingTasks = M(M({}, this._holdingTasks), e) : this._holdingTasks = e : this._runTask(e);
    }, i.prototype._runTask = function(e) {
      return Oe(this, void 0, void 0, function() {
        var t, r;
        return Le(this, function(a) {
          switch (a.label) {
            case 0:
              this._running = !0, a.label = 1;
            case 1:
              return a.trys.push([1, , 3, 4]), [4, Promise.all(Object.values(e))];
            case 2:
              return a.sent(), [3, 4];
            case 3:
              return this._running = !1, (r = this._callback) === null || r === void 0 || r.call(this), C(this._holdingTasks) && (t = this._holdingTasks, this._runTask(t), this._holdingTasks = null), [
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
    }, i.prototype.clear = function() {
      this._holdingTasks = null;
    }, i;
  })()
), ut = {
  PRICE: 2,
  VOLUME: 0
}, xi = (
  /** @class */
  (function() {
    function i() {
      this._callbacks = [];
    }
    return i.prototype.subscribe = function(e) {
      var t = this._callbacks.indexOf(e);
      t < 0 && this._callbacks.push(e);
    }, i.prototype.unsubscribe = function(e) {
      if (nt(e)) {
        var t = this._callbacks.indexOf(e);
        t > -1 && this._callbacks.splice(t, 1);
      } else
        this._callbacks = [];
    }, i.prototype.execute = function(e) {
      this._callbacks.forEach(function(t) {
        t(e);
      });
    }, i.prototype.isEmpty = function() {
      return this._callbacks.length === 0;
    }, i;
  })()
);
function ie(i) {
  return i === "transparent" || i === "none" || /^[rR][gG][Bb][Aa]\(([\s]*(2[0-4][0-9]|25[0-5]|[01]?[0-9][0-9]?)[\s]*,){3}[\s]*0[\s]*\)$/.test(i) || /^[hH][Ss][Ll][Aa]\(([\s]*(360｜3[0-5][0-9]|[012]?[0-9][0-9]?)[\s]*,)([\s]*((100|[0-9][0-9]?)%|0)[\s]*,){2}([\s]*0[\s]*)\)$/.test(i);
}
function zt(i, e) {
  var t = i.replace(/^#/, ""), r = parseInt(t, 16), a = r >> 16 & 255, n = r >> 8 & 255, o = r & 255;
  return "rgba(".concat(a, ", ").concat(n, ", ").concat(o, ", ").concat(e ?? 1, ")");
}
var V = {
  RED: "#F92855",
  GREEN: "#2DC08E",
  WHITE: "#FFFFFF",
  GREY: "#76808F",
  BLUE: "#1677FF"
};
function bi() {
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
function wi() {
  var i = {
    show: !0,
    color: V.GREY,
    textOffset: 5,
    textSize: 10,
    textFamily: "Helvetica Neue",
    textWeight: "normal"
  };
  return {
    type: "candle_solid",
    bar: {
      compareRule: "current_open",
      upColor: V.GREEN,
      downColor: V.RED,
      noChangeColor: V.GREY,
      upBorderColor: V.GREEN,
      downBorderColor: V.RED,
      noChangeBorderColor: V.GREY,
      upWickColor: V.GREEN,
      downWickColor: V.RED,
      noChangeWickColor: V.GREY
    },
    area: {
      lineSize: 2,
      lineColor: V.BLUE,
      smooth: !1,
      value: "close",
      backgroundColor: [{
        offset: 0,
        color: zt(V.BLUE, 0.01)
      }, {
        offset: 1,
        color: zt(V.BLUE, 0.2)
      }],
      point: {
        show: !0,
        color: V.BLUE,
        radius: 4,
        rippleColor: zt(V.BLUE, 0.3),
        rippleRadius: 8,
        animation: !0,
        animationDuration: 1e3
      }
    },
    priceMark: {
      show: !0,
      high: M({}, i),
      low: M({}, i),
      last: {
        show: !0,
        compareRule: "current_open",
        upColor: V.GREEN,
        downColor: V.RED,
        noChangeColor: V.GREY,
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
          color: V.WHITE,
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
        color: V.GREY,
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
        color: V.GREY,
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
function Ci() {
  var i = zt(V.GREEN, 0.7), e = zt(V.RED, 0.7);
  return {
    ohlc: {
      compareRule: "current_open",
      upColor: i,
      downColor: e,
      noChangeColor: V.GREY
    },
    bars: [{
      style: "fill",
      borderStyle: "solid",
      borderSize: 1,
      borderDashedValue: [2, 2],
      upColor: i,
      downColor: e,
      noChangeColor: V.GREY
    }],
    lines: ["#FF9600", "#935EBD", V.BLUE, "#E11D74", "#01C5C4"].map(function(t) {
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
      upColor: i,
      downColor: e,
      noChangeColor: V.GREY
    }],
    texts: [{
      paddingLeft: 0,
      paddingTop: 0,
      paddingRight: 0,
      paddingBottom: 0,
      style: "fill",
      size: 12,
      color: V.BLUE,
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
        color: V.WHITE,
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
        color: V.GREY,
        marginLeft: 8,
        marginTop: 4,
        marginRight: 8,
        marginBottom: 4
      },
      legend: {
        size: 12,
        family: "Helvetica Neue",
        weight: "normal",
        color: V.GREY,
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
function Ke() {
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
      color: V.GREY,
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
function Ei() {
  return {
    show: !0,
    horizontal: {
      show: !0,
      line: {
        show: !0,
        style: "dashed",
        dashedValue: [4, 2],
        size: 1,
        color: V.GREY
      },
      text: {
        show: !0,
        style: "fill",
        color: V.WHITE,
        size: 12,
        family: "Helvetica Neue",
        weight: "normal",
        borderStyle: "solid",
        borderDashedValue: [2, 2],
        borderSize: 1,
        borderColor: V.GREY,
        borderRadius: 2,
        paddingLeft: 4,
        paddingRight: 4,
        paddingTop: 4,
        paddingBottom: 4,
        backgroundColor: V.GREY
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
        color: V.GREY
      },
      text: {
        show: !0,
        style: "fill",
        color: V.WHITE,
        size: 12,
        family: "Helvetica Neue",
        weight: "normal",
        borderStyle: "solid",
        borderDashedValue: [2, 2],
        borderSize: 1,
        borderColor: V.GREY,
        borderRadius: 2,
        paddingLeft: 4,
        paddingRight: 4,
        paddingTop: 4,
        paddingBottom: 4,
        backgroundColor: V.GREY
      }
    }
  };
}
function Ii() {
  var i = zt(V.BLUE, 0.35), e = zt(V.BLUE, 0.25);
  function t() {
    return {
      style: "fill",
      color: V.WHITE,
      size: 12,
      family: "Helvetica Neue",
      weight: "normal",
      borderStyle: "solid",
      borderDashedValue: [2, 2],
      borderSize: 1,
      borderRadius: 2,
      borderColor: V.BLUE,
      paddingLeft: 4,
      paddingRight: 4,
      paddingTop: 4,
      paddingBottom: 4,
      backgroundColor: V.BLUE
    };
  }
  return {
    point: {
      color: V.BLUE,
      borderColor: i,
      borderSize: 1,
      radius: 5,
      activeColor: V.BLUE,
      activeBorderColor: i,
      activeBorderSize: 3,
      activeRadius: 5
    },
    line: {
      style: "solid",
      smooth: !1,
      color: V.BLUE,
      size: 1,
      dashedValue: [2, 2]
    },
    rect: {
      style: "fill",
      color: e,
      borderColor: V.BLUE,
      borderSize: 1,
      borderRadius: 0,
      borderStyle: "solid",
      borderDashedValue: [2, 2]
    },
    polygon: {
      style: "fill",
      color: V.BLUE,
      borderColor: V.BLUE,
      borderSize: 1,
      borderStyle: "solid",
      borderDashedValue: [2, 2]
    },
    circle: {
      style: "fill",
      color: e,
      borderColor: V.BLUE,
      borderSize: 1,
      borderStyle: "solid",
      borderDashedValue: [2, 2]
    },
    arc: {
      style: "solid",
      color: V.BLUE,
      size: 1,
      dashedValue: [2, 2]
    },
    text: t()
  };
}
function Si() {
  return {
    size: 1,
    color: "#DDDDDD",
    fill: !0,
    activeBackgroundColor: zt(V.BLUE, 0.08)
  };
}
function Ti() {
  return {
    grid: bi(),
    candle: wi(),
    indicator: Ci(),
    xAxis: Ke(),
    yAxis: Ke(),
    separator: Si(),
    crosshair: Ei(),
    overlay: Ii()
  };
}
function Ne(i, e, t, r, a) {
  var n = i.result, o = i.figures, s = i.styles, l = st(s, "texts", r.texts), u = l.length, h = st(s, "circles", r.circles), c = h.length, d = st(s, "bars", r.bars), f = d.length, v = st(s, "lines", r.lines), p = v.length, g = 0, m = 0, x = 0, _ = 0, E, y = 0;
  o.forEach(function(I) {
    var b;
    switch (I.type) {
      case "text": {
        y = g, E = l[g % u], g++;
        break;
      }
      case "circle": {
        y = m;
        var w = h[m % c];
        E = M(M({}, w), { color: w.noChangeColor }), m++;
        break;
      }
      case "bar": {
        y = x;
        var S = d[x % f];
        E = M(M({}, S), { color: S.noChangeColor }), x++;
        break;
      }
      case "line": {
        y = _, E = v[_ % p], _++;
        break;
      }
    }
    if (C(I.type)) {
      var T = (b = I.styles) === null || b === void 0 ? void 0 : b.call(I, {
        data: {
          prev: n[e - 1],
          current: n[e],
          next: n[e + 1]
        },
        indicator: i,
        barSpace: t,
        defaultStyles: r
      });
      a(I, M(M({}, E), T), y);
    }
  });
}
var wr = (
  /** @class */
  (function() {
    function i(e) {
      this.precision = 4, this.calcParams = [], this.shouldOhlc = !1, this.shouldFormatBigNumber = !1, this.visible = !0, this.zLevel = 0, this.series = "normal", this.figures = [], this.minValue = null, this.maxValue = null, this.styles = null, this.shouldUpdate = function(t, r) {
        var a = JSON.stringify(t.calcParams) !== JSON.stringify(r.calcParams) || t.figures !== r.figures || t.calc !== r.calc, n = a || t.shortName !== r.shortName || t.paneId !== r.paneId || t.yAxisId !== r.yAxisId || t.series !== r.series || t.minValue !== r.minValue || t.maxValue !== r.maxValue || t.precision !== r.precision || t.shouldOhlc !== r.shouldOhlc || t.shouldFormatBigNumber !== r.shouldFormatBigNumber || t.visible !== r.visible || t.zLevel !== r.zLevel || t.extendData !== r.extendData || t.regenerateFigures !== r.regenerateFigures || t.createTooltipDataSource !== r.createTooltipDataSource || t.draw !== r.draw;
        return { calc: a, draw: n };
      }, this.calc = function() {
        return [];
      }, this.regenerateFigures = null, this.createTooltipDataSource = null, this.draw = null, this.result = [], this._lockSeriesPrecision = !1, this.override(e), this._lockSeriesPrecision = !1;
    }
    return i.prototype.override = function(e) {
      var t, r, a = this, n = a.result;
      a._prevIndicator;
      var o = he(a, ["result", "_prevIndicator"]);
      this._prevIndicator = M(M({}, ue(o)), { result: n });
      var s = e.id, l = e.name, u = e.shortName, h = e.precision, c = e.styles, d = e.figures, f = e.calcParams, v = he(e, ["id", "name", "shortName", "precision", "styles", "figures", "calcParams"]);
      !$(this.id) && $(s) && (this.id = s), $(this.name) || (this.name = l ?? ""), this.shortName = (t = u ?? this.shortName) !== null && t !== void 0 ? t : this.name, B(h) && (this.precision = h, this._lockSeriesPrecision = !0), C(c) && ((r = this.styles) !== null && r !== void 0 || (this.styles = {}), ot(this.styles, c)), ot(this, v), C(f) && (this.calcParams = f, nt(this.regenerateFigures) && (this.figures = this.regenerateFigures(this.calcParams))), this.figures = d ?? this.figures;
    }, i.prototype.setSeriesPrecision = function(e) {
      this._lockSeriesPrecision || (this.precision = e);
    }, i.prototype.shouldUpdateImp = function() {
      var e = this._prevIndicator.zLevel !== this.zLevel, t = this.shouldUpdate(this._prevIndicator, this);
      return ee(t) ? { calc: t, draw: t, sort: e } : M(M({}, t), { sort: e });
    }, i.prototype.calcImp = function(e) {
      return Oe(this, void 0, void 0, function() {
        var t;
        return Le(this, function(r) {
          switch (r.label) {
            case 0:
              return r.trys.push([0, 2, , 3]), [4, this.calc(e, this)];
            case 1:
              return t = r.sent(), this.result = t, [2, !0];
            case 2:
              return r.sent(), [2, !1];
            case 3:
              return [
                2
                /*return*/
              ];
          }
        });
      });
    }, i.extend = function(e) {
      var t = (
        /** @class */
        (function(r) {
          X(a, r);
          function a() {
            return r.call(this, e) || this;
          }
          return a;
        })(i)
      );
      return t;
    }, i;
  })()
), Ai = {
  name: "AVP",
  shortName: "AVP",
  series: "price",
  precision: 2,
  figures: [
    { key: "avp", title: "AVP: ", type: "line" }
  ],
  calc: function(i) {
    var e = 0, t = 0;
    return i.map(function(r) {
      var a, n, o = {}, s = (a = r.turnover) !== null && a !== void 0 ? a : 0, l = (n = r.volume) !== null && n !== void 0 ? n : 0;
      return e += s, t += l, t !== 0 && (o.avp = e / t), o;
    });
  }
}, Mi = {
  name: "AO",
  shortName: "AO",
  calcParams: [5, 34],
  figures: [{
    key: "ao",
    title: "AO: ",
    type: "bar",
    baseValue: 0,
    styles: function(i) {
      var e, t, r = i.data, a = i.indicator, n = i.defaultStyles, o = r.prev, s = r.current, l = (e = o?.ao) !== null && e !== void 0 ? e : Number.MIN_SAFE_INTEGER, u = (t = s?.ao) !== null && t !== void 0 ? t : Number.MIN_SAFE_INTEGER, h = "";
      u > l ? h = st(a.styles, "bars[0].upColor", n.bars[0].upColor) : h = st(a.styles, "bars[0].downColor", n.bars[0].downColor);
      var c = u > l ? "stroke" : "fill";
      return { color: h, style: c, borderColor: h };
    }
  }],
  calc: function(i, e) {
    var t = e.calcParams, r = Math.max(t[0], t[1]), a = 0, n = 0, o = 0, s = 0;
    return i.map(function(l, u) {
      var h = {}, c = (l.low + l.high) / 2;
      if (a += c, n += c, u >= t[0] - 1) {
        o = a / t[0];
        var d = i[u - (t[0] - 1)];
        a -= (d.low + d.high) / 2;
      }
      if (u >= t[1] - 1) {
        s = n / t[1];
        var d = i[u - (t[1] - 1)];
        n -= (d.low + d.high) / 2;
      }
      return u >= r - 1 && (h.ao = o - s), h;
    });
  }
}, Pi = {
  name: "BIAS",
  shortName: "BIAS",
  calcParams: [6, 12, 24],
  figures: [
    { key: "bias1", title: "BIAS6: ", type: "line" },
    { key: "bias2", title: "BIAS12: ", type: "line" },
    { key: "bias3", title: "BIAS24: ", type: "line" }
  ],
  regenerateFigures: function(i) {
    return i.map(function(e, t) {
      return { key: "bias".concat(t + 1), title: "BIAS".concat(e, ": "), type: "line" };
    });
  },
  calc: function(i, e) {
    var t = e.calcParams, r = e.figures, a = [];
    return i.map(function(n, o) {
      var s = {}, l = n.close;
      return t.forEach(function(u, h) {
        var c;
        if (a[h] = ((c = a[h]) !== null && c !== void 0 ? c : 0) + l, o >= u - 1) {
          var d = a[h] / t[h];
          s[r[h].key] = (l - d) / d * 100, a[h] -= i[o - (u - 1)].close;
        }
      }), s;
    });
  }
};
function Di(i, e) {
  var t = i.length, r = 0;
  return i.forEach(function(a) {
    var n = a.close - e;
    r += n * n;
  }), r = Math.abs(r), Math.sqrt(r / t);
}
var ki = {
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
  calc: function(i, e) {
    var t = e.calcParams, r = t[0] - 1, a = 0;
    return i.map(function(n, o) {
      var s = n.close, l = {};
      if (a += s, o >= r) {
        l.mid = a / t[0];
        var u = Di(i.slice(o - r, o + 1), l.mid);
        l.up = l.mid + t[1] * u, l.dn = l.mid - t[1] * u, a -= i[o - r].close;
      }
      return l;
    });
  }
}, Ri = {
  name: "BRAR",
  shortName: "BRAR",
  calcParams: [26],
  figures: [
    { key: "br", title: "BR: ", type: "line" },
    { key: "ar", title: "AR: ", type: "line" }
  ],
  calc: function(i, e) {
    var t = e.calcParams, r = 0, a = 0, n = 0, o = 0;
    return i.map(function(s, l) {
      var u, h, c = {}, d = s.high, f = s.low, v = s.open, p = ((u = i[l - 1]) !== null && u !== void 0 ? u : s).close;
      if (n += d - v, o += v - f, r += d - p, a += p - f, l >= t[0] - 1) {
        o !== 0 ? c.ar = n / o * 100 : c.ar = 0, a !== 0 ? c.br = r / a * 100 : c.br = 0;
        var g = i[l - (t[0] - 1)], m = g.high, x = g.low, _ = g.open, E = ((h = i[l - t[0]]) !== null && h !== void 0 ? h : i[l - (t[0] - 1)]).close;
        r -= m - E, a -= E - x, n -= m - _, o -= _ - x;
      }
      return c;
    });
  }
}, Fi = {
  name: "BBI",
  shortName: "BBI",
  series: "price",
  precision: 2,
  calcParams: [3, 6, 12, 24],
  shouldOhlc: !0,
  figures: [
    { key: "bbi", title: "BBI: ", type: "line" }
  ],
  calc: function(i, e) {
    var t = e.calcParams, r = Math.max.apply(Math, re([], ae(t), !1)), a = [], n = [];
    return i.map(function(o, s) {
      var l = {}, u = o.close;
      if (t.forEach(function(c, d) {
        var f;
        a[d] = ((f = a[d]) !== null && f !== void 0 ? f : 0) + u, s >= c - 1 && (n[d] = a[d] / c, a[d] -= i[s - (c - 1)].close);
      }), s >= r - 1) {
        var h = 0;
        n.forEach(function(c) {
          h += c;
        }), l.bbi = h / 4;
      }
      return l;
    });
  }
}, Bi = {
  name: "CCI",
  shortName: "CCI",
  calcParams: [20],
  figures: [
    { key: "cci", title: "CCI: ", type: "line" }
  ],
  calc: function(i, e) {
    var t = e.calcParams, r = t[0] - 1, a = 0, n = [];
    return i.map(function(o, s) {
      var l = {}, u = (o.high + o.low + o.close) / 3;
      if (a += u, n.push(u), s >= r) {
        var h = a / t[0], c = n.slice(s - r, s + 1), d = 0;
        c.forEach(function(p) {
          d += Math.abs(p - h);
        });
        var f = d / t[0];
        l.cci = f !== 0 ? (u - h) / f / 0.015 : 0;
        var v = (i[s - r].high + i[s - r].low + i[s - r].close) / 3;
        a -= v;
      }
      return l;
    });
  }
}, Oi = {
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
  calc: function(i, e) {
    var t = e.calcParams, r = Math.ceil(t[1] / 2.5 + 1), a = Math.ceil(t[2] / 2.5 + 1), n = Math.ceil(t[3] / 2.5 + 1), o = Math.ceil(t[4] / 2.5 + 1), s = 0, l = [], u = 0, h = [], c = 0, d = [], f = 0, v = [], p = [];
    return i.forEach(function(g, m) {
      var x, _, E, y, I, b = {}, w = (x = i[m - 1]) !== null && x !== void 0 ? x : g, S = (w.high + w.close + w.low + w.open) / 4, T = Math.max(0, g.high - S), A = Math.max(0, S - g.low);
      m >= t[0] - 1 && (A !== 0 ? b.cr = T / A * 100 : b.cr = 0, s += b.cr, u += b.cr, c += b.cr, f += b.cr, m >= t[0] + t[1] - 2 && (l.push(s / t[1]), m >= t[0] + t[1] + r - 3 && (b.ma1 = l[l.length - 1 - r]), s -= (_ = p[m - (t[1] - 1)].cr) !== null && _ !== void 0 ? _ : 0), m >= t[0] + t[2] - 2 && (h.push(u / t[2]), m >= t[0] + t[2] + a - 3 && (b.ma2 = h[h.length - 1 - a]), u -= (E = p[m - (t[2] - 1)].cr) !== null && E !== void 0 ? E : 0), m >= t[0] + t[3] - 2 && (d.push(c / t[3]), m >= t[0] + t[3] + n - 3 && (b.ma3 = d[d.length - 1 - n]), c -= (y = p[m - (t[3] - 1)].cr) !== null && y !== void 0 ? y : 0), m >= t[0] + t[4] - 2 && (v.push(f / t[4]), m >= t[0] + t[4] + o - 3 && (b.ma4 = v[v.length - 1 - o]), f -= (I = p[m - (t[4] - 1)].cr) !== null && I !== void 0 ? I : 0)), p.push(b);
    }), p;
  }
}, Li = {
  name: "DMA",
  shortName: "DMA",
  calcParams: [10, 50, 10],
  figures: [
    { key: "dma", title: "DMA: ", type: "line" },
    { key: "ama", title: "AMA: ", type: "line" }
  ],
  calc: function(i, e) {
    var t = e.calcParams, r = Math.max(t[0], t[1]), a = 0, n = 0, o = 0, s = [];
    return i.forEach(function(l, u) {
      var h, c = {}, d = l.close;
      a += d, n += d;
      var f = 0, v = 0;
      if (u >= t[0] - 1 && (f = a / t[0], a -= i[u - (t[0] - 1)].close), u >= t[1] - 1 && (v = n / t[1], n -= i[u - (t[1] - 1)].close), u >= r - 1) {
        var p = f - v;
        c.dma = p, o += p, u >= r + t[2] - 2 && (c.ama = o / t[2], o -= (h = s[u - (t[2] - 1)].dma) !== null && h !== void 0 ? h : 0);
      }
      s.push(c);
    }), s;
  }
}, Vi = {
  name: "DMI",
  shortName: "DMI",
  calcParams: [14, 6],
  figures: [
    { key: "pdi", title: "PDI: ", type: "line" },
    { key: "mdi", title: "MDI: ", type: "line" },
    { key: "adx", title: "ADX: ", type: "line" },
    { key: "adxr", title: "ADXR: ", type: "line" }
  ],
  calc: function(i, e) {
    var t = e.calcParams, r = 0, a = 0, n = 0, o = 0, s = 0, l = 0, u = 0, h = 0, c = [];
    return i.forEach(function(d, f) {
      var v, p, g = {}, m = (v = i[f - 1]) !== null && v !== void 0 ? v : d, x = m.close, _ = d.high, E = d.low, y = _ - E, I = Math.abs(_ - x), b = Math.abs(x - E), w = _ - m.high, S = m.low - E, T = Math.max(Math.max(y, I), b), A = w > 0 && w > S ? w : 0, D = S > 0 && S > w ? S : 0;
      if (r += T, a += A, n += D, f >= t[0] - 1) {
        f > t[0] - 1 ? (o = o - o / t[0] + T, s = s - s / t[0] + A, l = l - l / t[0] + D) : (o = r, s = a, l = n);
        var R = 0, P = 0;
        o !== 0 && (R = s * 100 / o, P = l * 100 / o), g.pdi = R, g.mdi = P;
        var k = 0;
        P + R !== 0 && (k = Math.abs(P - R) / (P + R) * 100), u += k, f >= t[0] * 2 - 2 && (f > t[0] * 2 - 2 ? h = (h * (t[0] - 1) + k) / t[0] : h = u / t[0], g.adx = h, f >= t[0] * 2 + t[1] - 3 && (g.adxr = (((p = c[f - (t[1] - 1)].adx) !== null && p !== void 0 ? p : 0) + h) / 2));
      }
      c.push(g);
    }), c;
  }
}, Ni = {
  name: "EMV",
  shortName: "EMV",
  calcParams: [14, 9],
  figures: [
    { key: "emv", title: "EMV: ", type: "line" },
    { key: "maEmv", title: "MAEMV: ", type: "line" }
  ],
  calc: function(i, e) {
    var t = e.calcParams, r = 0, a = [];
    return i.map(function(n, o) {
      var s, l = {};
      if (o > 0) {
        var u = i[o - 1], h = n.high, c = n.low, d = (s = n.volume) !== null && s !== void 0 ? s : 0, f = (h + c) / 2 - (u.high + u.low) / 2;
        if (d === 0 || h - c === 0)
          l.emv = 0;
        else {
          var v = d / 1e8 / (h - c);
          l.emv = f / v;
        }
        r += l.emv, a.push(l.emv), o >= t[0] && (l.maEmv = r / t[0], r -= a[o - t[0]]);
      }
      return l;
    });
  }
}, Yi = {
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
  regenerateFigures: function(i) {
    return i.map(function(e, t) {
      return { key: "ema".concat(t + 1), title: "EMA".concat(e, ": "), type: "line" };
    });
  },
  calc: function(i, e) {
    var t = e.calcParams, r = e.figures, a = 0, n = [];
    return i.map(function(o, s) {
      var l = {}, u = o.close;
      return a += u, t.forEach(function(h, c) {
        s >= h - 1 && (s > h - 1 ? n[c] = (2 * u + (h - 1) * n[c]) / (h + 1) : n[c] = a / h, l[r[c].key] = n[c]);
      }), l;
    });
  }
}, Wi = {
  name: "MTM",
  shortName: "MTM",
  calcParams: [12, 6],
  figures: [
    { key: "mtm", title: "MTM: ", type: "line" },
    { key: "maMtm", title: "MAMTM: ", type: "line" }
  ],
  calc: function(i, e) {
    var t = e.calcParams, r = 0, a = [];
    return i.forEach(function(n, o) {
      var s, l = {};
      if (o >= t[0]) {
        var u = n.close, h = i[o - t[0]].close;
        l.mtm = u - h, r += l.mtm, o >= t[0] + t[1] - 1 && (l.maMtm = r / t[1], r -= (s = a[o - (t[1] - 1)].mtm) !== null && s !== void 0 ? s : 0);
      }
      a.push(l);
    }), a;
  }
}, zi = {
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
  regenerateFigures: function(i) {
    return i.map(function(e, t) {
      return { key: "ma".concat(t + 1), title: "MA".concat(e, ": "), type: "line" };
    });
  },
  calc: function(i, e) {
    var t = e.calcParams, r = e.figures, a = [];
    return i.map(function(n, o) {
      var s = {}, l = n.close;
      return t.forEach(function(u, h) {
        var c;
        a[h] = ((c = a[h]) !== null && c !== void 0 ? c : 0) + l, o >= u - 1 && (s[r[h].key] = a[h] / u, a[h] -= i[o - (u - 1)].close);
      }), s;
    });
  }
}, Xi = {
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
      styles: function(i) {
        var e, t, r = i.data, a = i.indicator, n = i.defaultStyles, o = r.prev, s = r.current, l = (e = o?.macd) !== null && e !== void 0 ? e : Number.MIN_SAFE_INTEGER, u = (t = s?.macd) !== null && t !== void 0 ? t : Number.MIN_SAFE_INTEGER, h = "";
        u > 0 ? h = st(a.styles, "bars[0].upColor", n.bars[0].upColor) : u < 0 ? h = st(a.styles, "bars[0].downColor", n.bars[0].downColor) : h = st(a.styles, "bars[0].noChangeColor", n.bars[0].noChangeColor);
        var c = l < u ? "stroke" : "fill";
        return { style: c, color: h, borderColor: h };
      }
    }
  ],
  calc: function(i, e) {
    var t = e.calcParams, r = 0, a = 0, n = 0, o = 0, s = 0, l = 0, u = Math.max(t[0], t[1]);
    return i.map(function(h, c) {
      var d = {}, f = h.close;
      return r += f, c >= t[0] - 1 && (c > t[0] - 1 ? a = (2 * f + (t[0] - 1) * a) / (t[0] + 1) : a = r / t[0]), c >= t[1] - 1 && (c > t[1] - 1 ? n = (2 * f + (t[1] - 1) * n) / (t[1] + 1) : n = r / t[1]), c >= u - 1 && (o = a - n, d.dif = o, s += o, c >= u + t[2] - 2 && (c > u + t[2] - 2 ? l = (o * 2 + l * (t[2] - 1)) / (t[2] + 1) : l = s / t[2], d.macd = (o - l) * 2, d.dea = l)), d;
    });
  }
}, Hi = {
  name: "OBV",
  shortName: "OBV",
  calcParams: [30],
  figures: [
    { key: "obv", title: "OBV: ", type: "line" },
    { key: "maObv", title: "MAOBV: ", type: "line" }
  ],
  calc: function(i, e) {
    var t = e.calcParams, r = 0, a = 0, n = [];
    return i.forEach(function(o, s) {
      var l, u, h, c, d = (l = i[s - 1]) !== null && l !== void 0 ? l : o;
      o.close < d.close ? a -= (u = o.volume) !== null && u !== void 0 ? u : 0 : o.close > d.close && (a += (h = o.volume) !== null && h !== void 0 ? h : 0);
      var f = { obv: a };
      r += a, s >= t[0] - 1 && (f.maObv = r / t[0], r -= (c = n[s - (t[0] - 1)].obv) !== null && c !== void 0 ? c : 0), n.push(f);
    }), n;
  }
}, Ui = {
  name: "PVT",
  shortName: "PVT",
  figures: [
    { key: "pvt", title: "PVT: ", type: "line" }
  ],
  calc: function(i) {
    var e = 0;
    return i.map(function(t, r) {
      var a, n, o = {}, s = t.close, l = (a = t.volume) !== null && a !== void 0 ? a : 1, u = ((n = i[r - 1]) !== null && n !== void 0 ? n : t).close, h = 0, c = u * l;
      return c !== 0 && (h = (s - u) / c), e += h, o.pvt = e, o;
    });
  }
}, Gi = {
  name: "PSY",
  shortName: "PSY",
  calcParams: [12, 6],
  figures: [
    { key: "psy", title: "PSY: ", type: "line" },
    { key: "maPsy", title: "MAPSY: ", type: "line" }
  ],
  calc: function(i, e) {
    var t = e.calcParams, r = 0, a = 0, n = [], o = [];
    return i.forEach(function(s, l) {
      var u, h, c = {}, d = ((u = i[l - 1]) !== null && u !== void 0 ? u : s).close, f = s.close - d > 0 ? 1 : 0;
      n.push(f), r += f, l >= t[0] - 1 && (c.psy = r / t[0] * 100, a += c.psy, l >= t[0] + t[1] - 2 && (c.maPsy = a / t[1], a -= (h = o[l - (t[1] - 1)].psy) !== null && h !== void 0 ? h : 0), r -= n[l - (t[0] - 1)]), o.push(c);
    }), o;
  }
}, qi = {
  name: "ROC",
  shortName: "ROC",
  calcParams: [12, 6],
  figures: [
    { key: "roc", title: "ROC: ", type: "line" },
    { key: "maRoc", title: "MAROC: ", type: "line" }
  ],
  calc: function(i, e) {
    var t = e.calcParams, r = [], a = 0;
    return i.forEach(function(n, o) {
      var s, l, u = {};
      if (o >= t[0] - 1) {
        var h = n.close, c = ((s = i[o - t[0]]) !== null && s !== void 0 ? s : i[o - (t[0] - 1)]).close;
        c !== 0 ? u.roc = (h - c) / c * 100 : u.roc = 0, a += u.roc, o >= t[0] - 1 + t[1] - 1 && (u.maRoc = a / t[1], a -= (l = r[o - (t[1] - 1)].roc) !== null && l !== void 0 ? l : 0);
      }
      r.push(u);
    }), r;
  }
}, Zi = {
  name: "RSI",
  shortName: "RSI",
  calcParams: [6, 12, 24],
  figures: [
    { key: "rsi1", title: "RSI1: ", type: "line" },
    { key: "rsi2", title: "RSI2: ", type: "line" },
    { key: "rsi3", title: "RSI3: ", type: "line" }
  ],
  regenerateFigures: function(i) {
    return i.map(function(e, t) {
      var r = t + 1;
      return { key: "rsi".concat(r), title: "RSI".concat(r, ": "), type: "line" };
    });
  },
  calc: function(i, e) {
    var t = e.calcParams, r = e.figures, a = [], n = [], o = [], s = [];
    return i.map(function(l, u) {
      var h = {}, c = u === 0 ? 0 : l.close - i[u - 1].close, d = Math.max(c, 0), f = Math.max(-c, 0);
      return t.forEach(function(v, p) {
        var g, m;
        a[p] = ((g = a[p]) !== null && g !== void 0 ? g : 0) + d, n[p] = ((m = n[p]) !== null && m !== void 0 ? m : 0) + f, !(u < v) && (o[p] === void 0 || s[p] === void 0 ? (o[p] = a[p] / v, s[p] = n[p] / v) : (o[p] = (o[p] * (v - 1) + d) / v, s[p] = (s[p] * (v - 1) + f) / v), s[p] === 0 ? h[r[p].key] = 100 : o[p] === 0 ? h[r[p].key] = 0 : h[r[p].key] = 100 - 100 / (1 + o[p] / s[p]));
      }), h;
    });
  }
}, $i = {
  name: "SMA",
  shortName: "SMA",
  series: "price",
  calcParams: [12, 2],
  precision: 2,
  figures: [
    { key: "sma", title: "SMA: ", type: "line" }
  ],
  shouldOhlc: !0,
  calc: function(i, e) {
    var t = e.calcParams, r = 0, a = 0;
    return i.map(function(n, o) {
      var s = {}, l = n.close;
      return r += l, o >= t[0] - 1 && (o > t[0] - 1 ? a = (l * t[1] + a * (t[0] - t[1] + 1)) / (t[0] + 1) : a = r / t[0], s.sma = a), s;
    });
  }
}, ji = {
  name: "KDJ",
  shortName: "KDJ",
  calcParams: [9, 3, 3],
  figures: [
    { key: "k", title: "K: ", type: "line" },
    { key: "d", title: "D: ", type: "line" },
    { key: "j", title: "J: ", type: "line" }
  ],
  calc: function(i, e) {
    var t = e.calcParams, r = [];
    return i.forEach(function(a, n) {
      var o, s, l, u, h = {}, c = a.close;
      if (n >= t[0] - 1) {
        var d = br(i.slice(n - (t[0] - 1), n + 1), "high", "low"), f = d[0], v = d[1], p = f - v, g = (c - v) / (p === 0 ? 1 : p) * 100;
        h.k = ((t[1] - 1) * ((s = (o = r[n - 1]) === null || o === void 0 ? void 0 : o.k) !== null && s !== void 0 ? s : 50) + g) / t[1], h.d = ((t[2] - 1) * ((u = (l = r[n - 1]) === null || l === void 0 ? void 0 : l.d) !== null && u !== void 0 ? u : 50) + h.k) / t[2], h.j = 3 * h.k - 2 * h.d;
      }
      r.push(h);
    }), r;
  }
}, Ki = {
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
      styles: function(i) {
        var e, t, r, a = i.data, n = i.indicator, o = i.defaultStyles, s = a.current, l = (e = s?.sar) !== null && e !== void 0 ? e : Number.MIN_SAFE_INTEGER, u = (((t = s?.high) !== null && t !== void 0 ? t : 0) + ((r = s?.low) !== null && r !== void 0 ? r : 0)) / 2, h = l < u ? st(n.styles, "circles[0].upColor", o.circles[0].upColor) : st(n.styles, "circles[0].downColor", o.circles[0].downColor);
        return { color: h };
      }
    }
  ],
  calc: function(i, e) {
    var t = e.calcParams, r = t[0] / 100, a = t[1] / 100, n = t[2] / 100, o = r, s = -100, l = !1, u = 0;
    return i.map(function(h, c) {
      var d = u, f = h.high, v = h.low;
      if (l) {
        (s === -100 || s < f) && (s = f, o = Math.min(o + a, n)), u = d + o * (s - d);
        var p = Math.min(i[Math.max(1, c) - 1].low, v);
        u > h.low ? (u = s, o = r, s = -100, l = !l) : u > p && (u = p);
      } else {
        (s === -100 || s > v) && (s = v, o = Math.min(o + a, n)), u = d + o * (s - d);
        var g = Math.max(i[Math.max(1, c) - 1].high, f);
        u < h.high ? (u = s, o = 0, s = -100, l = !l) : u < g && (u = g);
      }
      return { high: f, low: v, sar: u };
    });
  }
}, Ji = {
  name: "TRIX",
  shortName: "TRIX",
  calcParams: [12, 9],
  figures: [
    { key: "trix", title: "TRIX: ", type: "line" },
    { key: "maTrix", title: "MATRIX: ", type: "line" }
  ],
  calc: function(i, e) {
    var t = e.calcParams, r = 0, a = 0, n = 0, o = 0, s = 0, l = 0, u = 0, h = [];
    return i.forEach(function(c, d) {
      var f, v = {}, p = c.close;
      if (r += p, d >= t[0] - 1 && (d > t[0] - 1 ? a = (2 * p + (t[0] - 1) * a) / (t[0] + 1) : a = r / t[0], s += a, d >= t[0] * 2 - 2 && (d > t[0] * 2 - 2 ? n = (2 * a + (t[0] - 1) * n) / (t[0] + 1) : n = s / t[0], l += n, d >= t[0] * 3 - 3))) {
        var g = 0, m = 0;
        d > t[0] * 3 - 3 ? (g = (2 * n + (t[0] - 1) * o) / (t[0] + 1), m = (g - o) / o * 100) : g = l / t[0], o = g, v.trix = m, u += m, d >= t[0] * 3 + t[1] - 4 && (v.maTrix = u / t[1], u -= (f = h[d - (t[1] - 1)].trix) !== null && f !== void 0 ? f : 0);
      }
      h.push(v);
    }), h;
  }
};
function Je() {
  return {
    key: "volume",
    title: "VOLUME: ",
    type: "bar",
    baseValue: 0,
    styles: function(i) {
      var e = i.data, t = i.indicator, r = i.defaultStyles, a = e.current, n = st(t.styles, "bars[0].noChangeColor", r.bars[0].noChangeColor);
      return C(a) && (a.close > a.open ? n = st(t.styles, "bars[0].upColor", r.bars[0].upColor) : a.close < a.open && (n = st(t.styles, "bars[0].downColor", r.bars[0].downColor))), { color: n };
    }
  };
}
var Qi = {
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
    Je()
  ],
  regenerateFigures: function(i) {
    var e = i.map(function(t, r) {
      return { key: "ma".concat(r + 1), title: "MA".concat(t, ": "), type: "line" };
    });
    return e.push(Je()), e;
  },
  calc: function(i, e) {
    var t = e.calcParams, r = e.figures, a = [];
    return i.map(function(n, o) {
      var s, l = (s = n.volume) !== null && s !== void 0 ? s : 0, u = { volume: l, open: n.open, close: n.close };
      return t.forEach(function(h, c) {
        var d, f;
        a[c] = ((d = a[c]) !== null && d !== void 0 ? d : 0) + l, o >= h - 1 && (u[r[c].key] = a[c] / h, a[c] -= (f = i[o - (h - 1)].volume) !== null && f !== void 0 ? f : 0);
      }), u;
    });
  }
}, ta = {
  name: "VR",
  shortName: "VR",
  calcParams: [26, 6],
  figures: [
    { key: "vr", title: "VR: ", type: "line" },
    { key: "maVr", title: "MAVR: ", type: "line" }
  ],
  calc: function(i, e) {
    var t = e.calcParams, r = 0, a = 0, n = 0, o = 0, s = [];
    return i.forEach(function(l, u) {
      var h, c, d, f, v, p = {}, g = l.close, m = ((h = i[u - 1]) !== null && h !== void 0 ? h : l).close, x = (c = l.volume) !== null && c !== void 0 ? c : 0;
      if (g > m ? r += x : g < m ? a += x : n += x, u >= t[0] - 1) {
        var _ = n / 2;
        a + _ === 0 ? p.vr = 0 : p.vr = (r + _) / (a + _) * 100, o += p.vr, u >= t[0] + t[1] - 2 && (p.maVr = o / t[1], o -= (d = s[u - (t[1] - 1)].vr) !== null && d !== void 0 ? d : 0);
        var E = i[u - (t[0] - 1)], y = (f = i[u - t[0]]) !== null && f !== void 0 ? f : E, I = E.close, b = (v = E.volume) !== null && v !== void 0 ? v : 0;
        I > y.close ? r -= b : I < y.close ? a -= b : n -= b;
      }
      s.push(p);
    }), s;
  }
}, ea = {
  name: "WR",
  shortName: "WR",
  calcParams: [6, 10, 14],
  figures: [
    { key: "wr1", title: "WR1: ", type: "line" },
    { key: "wr2", title: "WR2: ", type: "line" },
    { key: "wr3", title: "WR3: ", type: "line" }
  ],
  regenerateFigures: function(i) {
    return i.map(function(e, t) {
      return { key: "wr".concat(t + 1), title: "WR".concat(t + 1, ": "), type: "line" };
    });
  },
  calc: function(i, e) {
    var t = e.calcParams, r = e.figures;
    return i.map(function(a, n) {
      var o = {}, s = a.close;
      return t.forEach(function(l, u) {
        var h = l - 1;
        if (n >= h) {
          var c = br(i.slice(n - h, n + 1), "high", "low"), d = c[0], f = c[1], v = d - f;
          o[r[u].key] = v === 0 ? 0 : (s - d) / v * 100;
        }
      }), o;
    });
  }
}, Ye = {}, ra = [
  Ai,
  Mi,
  Pi,
  ki,
  Ri,
  Fi,
  Bi,
  Oi,
  Li,
  Vi,
  Ni,
  Yi,
  Wi,
  zi,
  Xi,
  Hi,
  Ui,
  Gi,
  qi,
  Zi,
  $i,
  ji,
  Ki,
  Ji,
  Qi,
  ta,
  ea
];
ra.forEach(function(i) {
  Ye[i.name] = wr.extend(i);
});
function ia(i) {
  Ye[i.name] = wr.extend(i);
}
function Cr(i) {
  var e;
  return (e = Ye[i]) !== null && e !== void 0 ? e : null;
}
function Tt(i, e) {
  var t, r = (t = e?.ignoreEvent) !== null && t !== void 0 ? t : !1;
  return ee(r) ? !r : !r.includes(i);
}
var Qe = 1, fe = -1, aa = "overlay_", jt = "overlay_figure_", na = (
  /** @class */
  (function() {
    function i(e) {
      this.groupId = "", this.totalStep = 1, this.currentStep = Qe, this.drawingMode = "step", this.lock = !1, this.visible = !0, this.zLevel = 0, this.needDefaultPointFigure = !1, this.needDefaultXAxisFigure = !1, this.needDefaultYAxisFigure = !1, this.mode = "normal", this.modeSensitivity = 8, this.points = [], this.styles = null, this.createPointFigures = null, this.createXAxisFigures = null, this.createYAxisFigures = null, this.performEventPressedMove = null, this.performEventMoveForDrawing = null, this.onDrawStart = null, this.onDrawing = null, this.onDrawEnd = null, this.onClick = null, this.onDoubleClick = null, this.onRightClick = null, this.onPressedMoveStart = null, this.onPressedMoving = null, this.onPressedMoveEnd = null, this.onMouseMove = null, this.onMouseEnter = null, this.onMouseLeave = null, this.onRemoved = null, this.onSelected = null, this.onDeselected = null, this._prevZLevel = 0, this._prevPressedPoint = null, this._prevPressedPoints = [], this.override(e);
    }
    return i.prototype.override = function(e) {
      var t, r;
      this._prevOverlay = ue(M(M({}, this), { _prevOverlay: null }));
      var a = e.id, n = e.name;
      e.currentStep;
      var o = e.points, s = e.styles, l = he(e, ["id", "name", "currentStep", "points", "styles"]);
      if (ot(this, l), $(this.name) || (this.name = n ?? ""), !$(this.id) && $(a) && (this.id = a), C(s) && ((t = this.styles) !== null && t !== void 0 || (this.styles = {}), ot(this.styles, s)), Dt(o) && o.length > 0) {
        this.points = re([], ae(o), !1), this.currentStep = fe;
        var u = this.points.length - 1, h = this.points[u];
        u > 0 && C(h) && ((r = this.performEventPressedMove) === null || r === void 0 || r.call(this, {
          currentStep: this.currentStep,
          mode: this.mode,
          points: this.points,
          performPointIndex: u,
          performPoint: h
        }));
      }
    }, i.prototype.getPrevZLevel = function() {
      return this._prevZLevel;
    }, i.prototype.setPrevZLevel = function(e) {
      this._prevZLevel = e;
    }, i.prototype.shouldUpdate = function() {
      var e = this._prevOverlay.zLevel !== this.zLevel, t = e || JSON.stringify(this._prevOverlay.points) !== JSON.stringify(this.points) || this._prevOverlay.visible !== this.visible || this._prevOverlay.extendData !== this.extendData || this._prevOverlay.styles !== this.styles;
      return { sort: e, draw: t };
    }, i.prototype.nextStep = function() {
      this.currentStep === this.totalStep - 1 ? this.currentStep = fe : this.currentStep++;
    }, i.prototype.forceComplete = function() {
      this.currentStep = fe;
    }, i.prototype.isDrawing = function() {
      return this.currentStep !== fe;
    }, i.prototype.isStart = function() {
      return this.currentStep === Qe;
    }, i.prototype.isContinuousDrawingMode = function() {
      return this.drawingMode === "continuous";
    }, i.prototype.startContinuousDrawing = function(e) {
      this.points = [], this.continuousDrawingModeEventMoveForDrawing(e), this.currentStep = 2;
    }, i.prototype.continuousDrawingModeEventMoveForDrawing = function(e) {
      var t = {};
      return B(e.timestamp) && (t.timestamp = e.timestamp), B(e.dataIndex) && (t.dataIndex = e.dataIndex), B(e.value) && (t.value = e.value), this.points.push(t), !0;
    }, i.prototype.stepDrawingModeEventMoveForDrawing = function(e) {
      var t, r = this.currentStep - 1, a = {};
      B(e.timestamp) && (a.timestamp = e.timestamp), B(e.dataIndex) && (a.dataIndex = e.dataIndex), B(e.value) && (a.value = e.value), this.points[r] = a, (t = this.performEventMoveForDrawing) === null || t === void 0 || t.call(this, {
        currentStep: this.currentStep,
        mode: this.mode,
        points: this.points,
        performPointIndex: r,
        performPoint: a
      });
    }, i.prototype.eventPressedPointMove = function(e, t) {
      var r;
      this.points[t].timestamp = e.timestamp, B(e.dataIndex) && (this.points[t].dataIndex = e.dataIndex), B(e.value) && (this.points[t].value = e.value), (r = this.performEventPressedMove) === null || r === void 0 || r.call(this, {
        currentStep: this.currentStep,
        points: this.points,
        mode: this.mode,
        performPointIndex: t,
        performPoint: this.points[t]
      });
    }, i.prototype.startPressedMove = function(e) {
      this._prevPressedPoint = M({}, e), this._prevPressedPoints = ue(this.points);
    }, i.prototype.eventPressedOtherMove = function(e, t) {
      var r = this;
      if (this._prevPressedPoint !== null) {
        var a = null;
        B(e.dataIndex) && B(this._prevPressedPoint.dataIndex) && (a = e.dataIndex - this._prevPressedPoint.dataIndex);
        var n = null;
        B(e.value) && B(this._prevPressedPoint.value) && (n = e.value - this._prevPressedPoint.value), this.points = this._prevPressedPoints.map(function(o) {
          var s, l, u = M({}, o);
          if (B(a) && (B(o.dataIndex) || B(o.timestamp))) {
            var h = B(o.timestamp) ? r.isContinuousDrawingMode() ? t.timestampToFloatIndex(o.timestamp) : t.timestampToDataIndex(o.timestamp) : o.dataIndex;
            u.dataIndex = h + a, u.timestamp = r.isContinuousDrawingMode() ? (s = t.floatIndexToTimestamp(u.dataIndex)) !== null && s !== void 0 ? s : void 0 : (l = t.dataIndexToTimestamp(u.dataIndex)) !== null && l !== void 0 ? l : void 0;
          }
          return B(n) && B(o.value) && (u.value = o.value + n), u;
        });
      }
    }, i.extend = function(e) {
      var t = (
        /** @class */
        (function(r) {
          X(a, r);
          function a() {
            return r.call(this, e) || this;
          }
          return a;
        })(i)
      );
      return t;
    }, i;
  })()
), oa = {
  name: "fibonacciLine",
  totalStep: 3,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(i) {
    var e, t, r, a = i.chart, n = i.coordinates, o = i.bounding, s = i.overlay, l = i.yAxis, u = s.points;
    if (n.length > 0) {
      var h = 0;
      if (!((e = l?.isInCandle()) !== null && e !== void 0) || e)
        h = (r = (t = a.getSymbol()) === null || t === void 0 ? void 0 : t.pricePrecision) !== null && r !== void 0 ? r : ut.PRICE;
      else {
        var c = a.getIndicators({ paneId: s.paneId });
        c.forEach(function(_) {
          h = Math.max(h, _.precision);
        });
      }
      var d = [], f = [], v = 0, p = o.width;
      if (n.length > 1 && B(u[0].value) && B(u[1].value)) {
        var g = [1, 0.786, 0.618, 0.5, 0.382, 0.236, 0], m = n[0].y - n[1].y, x = u[0].value - u[1].value;
        g.forEach(function(_) {
          var E, y = n[1].y + m * _, I = a.getDecimalFold().format(a.getThousandsSeparator().format((((E = u[1].value) !== null && E !== void 0 ? E : 0) + x * _).toFixed(h)));
          d.push({ coordinates: [{ x: v, y }, { x: p, y }] }), f.push({
            x: v,
            y,
            text: "".concat(I, " (").concat((_ * 100).toFixed(1), "%)"),
            baseline: "bottom"
          });
        });
      }
      return [
        {
          type: "line",
          attrs: d
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
}, sa = {
  name: "horizontalRayLine",
  totalStep: 3,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(i) {
    var e = i.coordinates, t = i.bounding, r = { x: 0, y: e[0].y };
    return C(e[1]) && e[0].x < e[1].x && (r.x = t.width), [
      {
        type: "line",
        attrs: { coordinates: [e[0], r] }
      }
    ];
  },
  performEventPressedMove: function(i) {
    var e = i.points, t = i.performPoint;
    e[0].value = t.value, e[1].value = t.value;
  },
  performEventMoveForDrawing: function(i) {
    var e = i.currentStep, t = i.points, r = i.performPoint;
    e === 2 && (t[0].value = r.value);
  }
}, la = {
  name: "horizontalSegment",
  totalStep: 3,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(i) {
    var e = i.coordinates, t = [];
    return e.length === 2 && t.push({ coordinates: e }), [
      {
        type: "line",
        attrs: t
      }
    ];
  },
  performEventPressedMove: function(i) {
    var e = i.points, t = i.performPoint;
    e[0].value = t.value, e[1].value = t.value;
  },
  performEventMoveForDrawing: function(i) {
    var e = i.currentStep, t = i.points, r = i.performPoint;
    e === 2 && (t[0].value = r.value);
  }
}, ua = {
  name: "horizontalStraightLine",
  totalStep: 2,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(i) {
    var e = i.coordinates, t = i.bounding;
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
}, We = (
  /** @class */
  (function() {
    function i() {
      this._children = [], this._callbacks = /* @__PURE__ */ new Map();
    }
    return i.prototype.registerEvent = function(e, t) {
      return this._callbacks.set(e, t), this;
    }, i.prototype.onEvent = function(e, t) {
      var r = this._callbacks.get(e);
      return C(r) && this.checkEventOn(t) ? r(t) : !1;
    }, i.prototype.dispatchEventToChildren = function(e, t) {
      var r = this._children.length - 1;
      if (r > -1) {
        for (var a = r; a > -1; a--)
          if (this._children[a].dispatchEvent(e, t))
            return !0;
      }
      return !1;
    }, i.prototype.dispatchEvent = function(e, t) {
      return this.dispatchEventToChildren(e, t) ? !0 : this.onEvent(e, t);
    }, i.prototype.addChild = function(e) {
      return this._children.push(e), this;
    }, i.prototype.clear = function() {
      this._children = [];
    }, i;
  })()
), lt = 2, ha = (
  /** @class */
  (function(i) {
    X(e, i);
    function e(t) {
      var r = i.call(this) || this;
      return r.attrs = t.attrs, r.styles = t.styles, r;
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
      var r = (
        /** @class */
        (function(a) {
          X(n, a);
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
      return r;
    }, e;
  })(We)
);
function ca(i, e) {
  var t, r, a = [];
  a = a.concat(e);
  try {
    for (var n = gt(a), o = n.next(); !o.done; o = n.next()) {
      var s = o.value, l = s.coordinates;
      if (l.length > 1)
        for (var u = 1; u < l.length; u++) {
          var h = l[u - 1], c = l[u];
          if (h.x === c.x) {
            if (Math.abs(h.y - i.y) + Math.abs(c.y - i.y) - Math.abs(h.y - c.y) < lt + lt && Math.abs(i.x - h.x) < lt)
              return !0;
          } else {
            var d = ze(h, c), f = Er(d, i), v = Math.abs(f - i.y);
            if (Math.abs(h.x - i.x) + Math.abs(c.x - i.x) - Math.abs(h.x - c.x) < lt + lt && v * v / (d[0] * d[0] + 1) < lt * lt)
              return !0;
          }
        }
    }
  } catch (p) {
    t = { error: p };
  } finally {
    try {
      o && !o.done && (r = n.return) && r.call(n);
    } finally {
      if (t) throw t.error;
    }
  }
  return !1;
}
function Er(i, e) {
  return i !== null ? e.x * i[0] + i[1] : e.y;
}
function me(i, e, t) {
  var r = ze(i, e);
  return Er(r, t);
}
function ze(i, e) {
  var t = i.x - e.x;
  if (t !== 0) {
    var r = (i.y - e.y) / t, a = i.y - r * i.x;
    return [r, a];
  }
  return null;
}
function Ir(i, e, t) {
  var r = e.length, a = B(t) ? t > 0 && t < 1 ? t : 0 : t ? 0.5 : 0;
  if (a > 0 && r > 2) {
    for (var n = e[0].x, o = e[0].y, s = 1; s < r - 1; s++) {
      var l = e[s - 1], u = e[s], h = e[s + 1], c = u.x - l.x, d = u.y - l.y, f = h.x - u.x, v = h.y - u.y, p = h.x - l.x, g = h.y - l.y, m = Math.sqrt(c * c + d * d), x = Math.sqrt(f * f + v * v), _ = x / (x + m), E = u.x + p * a * _, y = u.y + g * a * _;
      E = Math.min(E, Math.max(h.x, u.x)), y = Math.min(y, Math.max(h.y, u.y)), E = Math.max(E, Math.min(h.x, u.x)), y = Math.max(y, Math.min(h.y, u.y)), p = E - u.x, g = y - u.y;
      var I = u.x - p * m / x, b = u.y - g * m / x;
      I = Math.min(I, Math.max(l.x, u.x)), b = Math.min(b, Math.max(l.y, u.y)), I = Math.max(I, Math.min(l.x, u.x)), b = Math.max(b, Math.min(l.y, u.y)), p = u.x - I, g = u.y - b, E = u.x + p * x / m, y = u.y + g * x / m, i.bezierCurveTo(n, o, I, b, u.x, u.y), n = E, o = y;
    }
    var w = e[r - 1];
    i.bezierCurveTo(n, o, w.x, w.y, w.x, w.y);
  } else
    for (var s = 1; s < r; s++)
      i.lineTo(e[s].x, e[s].y);
}
function da(i, e, t) {
  var r = [];
  r = r.concat(e);
  var a = t.style, n = a === void 0 ? "solid" : a, o = t.smooth, s = o === void 0 ? !1 : o, l = t.size, u = l === void 0 ? 1 : l, h = t.color, c = h === void 0 ? "currentColor" : h, d = t.dashedValue, f = d === void 0 ? [2, 2] : d, v = t.lineCap, p = t.lineJoin, g = B(s) ? s > 0 : s;
  i.lineWidth = u, i.strokeStyle = c, $(v) ? i.lineCap = v : g ? i.lineCap = "round" : i.lineCap = "butt", $(p) ? i.lineJoin = p : g ? i.lineJoin = "round" : i.lineJoin = "miter", n === "dashed" ? i.setLineDash(f) : i.setLineDash([]);
  var m = u % 2 === 1 ? 0.5 : 0;
  r.forEach(function(x) {
    var _ = x.coordinates;
    _.length > 1 && (_.length === 2 && (_[0].x === _[1].x || _[0].y === _[1].y) ? (i.beginPath(), _[0].x === _[1].x ? (i.moveTo(_[0].x + m, _[0].y), i.lineTo(_[1].x + m, _[1].y)) : (i.moveTo(_[0].x, _[0].y + m), i.lineTo(_[1].x, _[1].y + m)), i.stroke(), i.closePath()) : (i.save(), u % 2 === 1 && i.translate(0.5, 0.5), i.beginPath(), i.moveTo(_[0].x, _[0].y), Ir(i, _, s), i.stroke(), i.closePath(), i.restore()));
  });
}
var va = {
  name: "line",
  checkEventOn: ca,
  draw: function(i, e, t) {
    da(i, e, t);
  }
};
function Sr(i, e, t) {
  var r = t ?? 0, a = [];
  if (i.length > 1)
    if (i[0].x === i[1].x) {
      var n = 0, o = e.height;
      if (a.push({ coordinates: [{ x: i[0].x, y: n }, { x: i[0].x, y: o }] }), i.length > 2) {
        a.push({ coordinates: [{ x: i[2].x, y: n }, { x: i[2].x, y: o }] });
        for (var s = i[0].x - i[2].x, l = 0; l < r; l++) {
          var u = s * (l + 1);
          a.push({ coordinates: [{ x: i[0].x + u, y: n }, { x: i[0].x + u, y: o }] });
        }
      }
    } else {
      var h = 0, c = e.width, d = ze(i[0], i[1]), f = d[0], v = d[1];
      if (a.push({ coordinates: [{ x: h, y: h * f + v }, { x: c, y: c * f + v }] }), i.length > 2) {
        var p = i[2].y - f * i[2].x;
        a.push({ coordinates: [{ x: h, y: h * f + p }, { x: c, y: c * f + p }] });
        for (var s = v - p, l = 0; l < r; l++) {
          var g = v + s * (l + 1);
          a.push({ coordinates: [{ x: h, y: h * f + g }, { x: c, y: c * f + g }] });
        }
      }
    }
  return a;
}
var fa = {
  name: "parallelStraightLine",
  totalStep: 4,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(i) {
    var e = i.coordinates, t = i.bounding;
    return [
      {
        type: "line",
        attrs: Sr(e, t)
      }
    ];
  }
}, pa = {
  name: "priceChannelLine",
  totalStep: 4,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(i) {
    var e = i.coordinates, t = i.bounding;
    return [
      {
        type: "line",
        attrs: Sr(e, t, 1)
      }
    ];
  }
}, ga = {
  name: "priceLine",
  totalStep: 2,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(i) {
    var e, t, r, a = i.chart, n = i.coordinates, o = i.bounding, s = i.overlay, l = i.yAxis, u = 0;
    if (!((e = l?.isInCandle()) !== null && e !== void 0) || e)
      u = (r = (t = a.getSymbol()) === null || t === void 0 ? void 0 : t.pricePrecision) !== null && r !== void 0 ? r : ut.PRICE;
    else {
      var h = a.getIndicators({ paneId: s.paneId });
      h.forEach(function(f) {
        u = Math.max(u, f.precision);
      });
    }
    var c = s.points[0].value, d = c === void 0 ? 0 : c;
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
          text: a.getDecimalFold().format(a.getThousandsSeparator().format(d.toFixed(u))),
          baseline: "bottom"
        }
      }
    ];
  }
};
function ma(i, e) {
  if (i.length > 1) {
    var t = { x: 0, y: 0 };
    return i[0].x === i[1].x && i[0].y !== i[1].y ? i[0].y < i[1].y ? t = {
      x: i[0].x,
      y: e.height
    } : t = {
      x: i[0].x,
      y: 0
    } : i[0].x > i[1].x ? t = {
      x: 0,
      y: me(i[0], i[1], { x: 0, y: i[0].y })
    } : t = {
      x: e.width,
      y: me(i[0], i[1], { x: e.width, y: i[0].y })
    }, { coordinates: [i[0], t] };
  }
  return [];
}
var ya = {
  name: "rayLine",
  totalStep: 3,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(i) {
    var e = i.coordinates, t = i.bounding;
    return [
      {
        type: "line",
        attrs: ma(e, t)
      }
    ];
  }
}, _a = {
  name: "segment",
  totalStep: 3,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(i) {
    var e = i.coordinates;
    return e.length === 2 ? [
      {
        type: "line",
        attrs: { coordinates: e }
      }
    ] : [];
  }
}, xa = {
  name: "straightLine",
  totalStep: 3,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(i) {
    var e = i.coordinates, t = i.bounding;
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
              y: me(e[0], e[1], { x: 0, y: e[0].y })
            },
            {
              x: t.width,
              y: me(e[0], e[1], { x: t.width, y: e[0].y })
            }
          ]
        }
      }
    ] : [];
  }
}, ba = {
  name: "verticalRayLine",
  totalStep: 3,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(i) {
    var e = i.coordinates, t = i.bounding;
    if (e.length === 2) {
      var r = { x: e[0].x, y: 0 };
      return e[0].y < e[1].y && (r.y = t.height), [
        {
          type: "line",
          attrs: { coordinates: [e[0], r] }
        }
      ];
    }
    return [];
  },
  performEventPressedMove: function(i) {
    var e = i.points, t = i.performPoint;
    e[0].timestamp = t.timestamp, e[0].dataIndex = t.dataIndex, e[1].timestamp = t.timestamp, e[1].dataIndex = t.dataIndex;
  },
  performEventMoveForDrawing: function(i) {
    var e = i.currentStep, t = i.points, r = i.performPoint;
    e === 2 && (t[0].timestamp = r.timestamp, t[0].dataIndex = r.dataIndex);
  }
}, wa = {
  name: "verticalSegment",
  totalStep: 3,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(i) {
    var e = i.coordinates;
    return e.length === 2 ? [
      {
        type: "line",
        attrs: { coordinates: e }
      }
    ] : [];
  },
  performEventPressedMove: function(i) {
    var e = i.points, t = i.performPoint;
    e[0].timestamp = t.timestamp, e[0].dataIndex = t.dataIndex, e[1].timestamp = t.timestamp, e[1].dataIndex = t.dataIndex;
  },
  performEventMoveForDrawing: function(i) {
    var e = i.currentStep, t = i.points, r = i.performPoint;
    e === 2 && (t[0].timestamp = r.timestamp, t[0].dataIndex = r.dataIndex);
  }
}, Ca = {
  name: "verticalStraightLine",
  totalStep: 2,
  needDefaultPointFigure: !0,
  needDefaultXAxisFigure: !0,
  needDefaultYAxisFigure: !0,
  createPointFigures: function(i) {
    var e = i.coordinates, t = i.bounding;
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
}, Ea = {
  name: "simpleAnnotation",
  totalStep: 2,
  styles: {
    line: { style: "dashed" }
  },
  createPointFigures: function(i) {
    var e, t = i.overlay, r = i.coordinates, a = "";
    C(t.extendData) && (nt(t.extendData) ? a = t.extendData(t) : a = (e = t.extendData) !== null && e !== void 0 ? e : "");
    var n = r[0].x, o = r[0].y - 6, s = o - 50, l = s - 5;
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
}, Ia = {
  name: "simpleTag",
  totalStep: 2,
  styles: {
    line: { style: "dashed" }
  },
  createPointFigures: function(i) {
    var e = i.bounding, t = i.coordinates;
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
  createYAxisFigures: function(i) {
    var e, t, r, a, n = i.chart, o = i.overlay, s = i.coordinates, l = i.bounding, u = i.yAxis, h = (e = u?.isFromZero()) !== null && e !== void 0 ? e : !1, c = "left", d = 0;
    h ? (c = "left", d = 0) : (c = "right", d = l.width);
    var f = "";
    return C(o.extendData) && (nt(o.extendData) ? f = o.extendData(o) : f = (t = o.extendData) !== null && t !== void 0 ? t : ""), !C(f) && B(o.points[0].value) && (f = pt(o.points[0].value, (a = (r = n.getSymbol()) === null || r === void 0 ? void 0 : r.pricePrecision) !== null && a !== void 0 ? a : ut.PRICE)), { type: "text", attrs: { x: d, y: s[0].y, text: f, align: c, baseline: "middle" } };
  }
}, Sa = {
  name: "brush",
  totalStep: 2,
  drawingMode: "continuous",
  needDefaultPointFigure: !1,
  needDefaultXAxisFigure: !1,
  needDefaultYAxisFigure: !1,
  createPointFigures: function(i) {
    var e = i.coordinates;
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
}, Tr = {}, Ta = [
  oa,
  sa,
  la,
  ua,
  fa,
  pa,
  ga,
  ya,
  _a,
  xa,
  ba,
  wa,
  Ca,
  Ea,
  Ia,
  Sa
];
Ta.forEach(function(i) {
  Tr[i.name] = na.extend(i);
});
function Aa(i) {
  var e;
  return (e = Tr[i]) !== null && e !== void 0 ? e : null;
}
var Ma = {
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
}, Pa = {
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
}, Da = {
  light: Ma,
  dark: Pa
};
function ka(i) {
  var e;
  return (e = Da[i]) !== null && e !== void 0 ? e : null;
}
var q = {
  CANDLE: "candle_pane",
  INDICATOR: "indicator_pane_",
  X_AXIS: "x_axis_pane"
}, Ra = 10, Fa = 80, Ba = 0.2, ke = 10, Oa = (
  /** @class */
  (function() {
    function i(e, t) {
      var r = this;
      this._styles = Ti(), this._formatter = {
        formatDate: function(v) {
          var p = v.dateTimeFormat, g = v.timestamp, m = v.template;
          return vi(p, g, m);
        },
        formatBigNumber: fi,
        formatExtendText: function(v) {
          return "";
        }
      }, this._innerFormatter = {
        formatDate: function(v, p, g) {
          return r._formatter.formatDate({ dateTimeFormat: r._dateTimeFormat, timestamp: v, template: p, type: g });
        },
        formatBigNumber: function(v) {
          return r._formatter.formatBigNumber(v);
        },
        formatExtendText: function(v) {
          return r._formatter.formatExtendText(v);
        }
      }, this._locale = "en-US", this._thousandsSeparator = {
        sign: ",",
        format: function(v) {
          return pi(v, r._thousandsSeparator.sign);
        }
      }, this._decimalFold = {
        threshold: 3,
        format: function(v) {
          return gi(v, r._decimalFold.threshold);
        }
      }, this._hotKey = {
        enabled: !0,
        exclude: []
      }, this._symbol = null, this._period = null, this._dataList = [], this._dataLoader = null, this._loading = !1, this._dataLoadMore = { forward: !1, backward: !1 }, this._zoomEnabled = !0, this._zoomAnchor = {
        main: "cursor",
        xAxis: "cursor"
      }, this._scrollEnabled = !0, this._totalBarSpace = 0, this._barSpace = Ra, this._offsetRightDistance = Fa, this._startLastBarRightSideDiffBarCount = 0, this._scrollLimitRole = "bar_count", this._minVisibleBarCount = { left: 2, right: 2 }, this._maxOffsetDistance = { left: 50, right: 50 }, this._visibleRange = je(), this._visibleRangeDataList = [], this._visibleRangeHighLowPrice = [
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
      var a = t ?? {}, n = a.styles, o = a.locale, s = a.timezone, l = a.formatter, u = a.thousandsSeparator, h = a.decimalFold, c = a.zoomAnchor, d = a.hotkey, f = a.layout;
      C(f) && ot(this._layoutOptions, f), this._calcOptimalBarSpace(), this._lastBarRightSideDiffBarCount = this._offsetRightDistance / this._barSpace, C(n) && this.setStyles(n), $(o) && this.setLocale(o), this.setTimezone(s ?? ""), C(l) && this.setFormatter(l), C(u) && this.setThousandsSeparator(u), C(h) && this.setDecimalFold(h), C(c) && this.setZoomAnchor(c), C(d) && this.setHotkey(d), this._taskScheduler = new _i(function() {
        r._chart.layout({
          measureWidth: !0,
          update: !0,
          buildYAxisTick: !0
        });
      });
    }
    return i.prototype.setStyles = function(e) {
      var t = this, r, a, n, o, s, l, u = null;
      if ($(e) ? u = ka(e) : u = e, ot(this._styles, u), Dt((n = (a = (r = u?.candle) === null || r === void 0 ? void 0 : r.tooltip) === null || a === void 0 ? void 0 : a.legend) === null || n === void 0 ? void 0 : n.template) && (this._styles.candle.tooltip.legend.template = u.candle.tooltip.legend.template), C((l = (s = (o = u?.candle) === null || o === void 0 ? void 0 : o.priceMark) === null || s === void 0 ? void 0 : s.last) === null || l === void 0 ? void 0 : l.extendTexts)) {
        this._clearLastPriceMarkExtendTextUpdateTimer();
        var h = [];
        this._styles.candle.priceMark.last.extendTexts.forEach(function(c) {
          var d = c.updateInterval;
          if (c.show && d > 0 && !h.includes(d)) {
            h.push(d);
            var f = setInterval(function() {
              t._chart.updatePane(0, q.CANDLE);
            }, d);
            t._lastPriceMarkExtendTextUpdateTimers.push(f);
          }
        });
      }
    }, i.prototype.getStyles = function() {
      return this._styles;
    }, i.prototype.setFormatter = function(e) {
      ot(this._formatter, e);
    }, i.prototype.getFormatter = function() {
      return this._formatter;
    }, i.prototype.getInnerFormatter = function() {
      return this._innerFormatter;
    }, i.prototype.setLocale = function(e) {
      this._locale = e;
    }, i.prototype.getLocale = function() {
      return this._locale;
    }, i.prototype.setTimezone = function(e) {
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
        var r = null;
        try {
          r = new Intl.DateTimeFormat("en", t);
        } catch {
        }
        r !== null && (this._dateTimeFormat = r);
      }
    }, i.prototype.getTimezone = function() {
      return this._dateTimeFormat.resolvedOptions().timeZone;
    }, i.prototype.getDateTimeFormat = function() {
      return this._dateTimeFormat;
    }, i.prototype.setThousandsSeparator = function(e) {
      ot(this._thousandsSeparator, e);
    }, i.prototype.getThousandsSeparator = function() {
      return this._thousandsSeparator;
    }, i.prototype.setDecimalFold = function(e) {
      ot(this._decimalFold, e);
    }, i.prototype.getDecimalFold = function() {
      return this._decimalFold;
    }, i.prototype.setHotkey = function(e) {
      ot(this._hotKey, e);
    }, i.prototype.getHotkey = function() {
      return this._hotKey;
    }, i.prototype.getHotKey = function() {
      return this._hotKey;
    }, i.prototype.setSymbol = function(e) {
      var t = this;
      this.resetData(function() {
        t._symbol = M(M({ pricePrecision: ut.PRICE, volumePrecision: ut.VOLUME }, t._symbol), e), t._synchronizeIndicatorSeriesPrecision();
      });
    }, i.prototype.getSymbol = function() {
      return this._symbol;
    }, i.prototype.setPeriod = function(e) {
      var t = this;
      this.resetData(function() {
        t._period = e;
      });
    }, i.prototype.getPeriod = function() {
      return this._period;
    }, i.prototype.getDataList = function() {
      return this._dataList;
    }, i.prototype.getVisibleRangeDataList = function() {
      return this._visibleRangeDataList;
    }, i.prototype.getVisibleRangeHighLowPrice = function() {
      return this._visibleRangeHighLowPrice;
    }, i.prototype._addData = function(e, t, r) {
      var a, n, o = !1, s = !1;
      if (Dt(e)) {
        var l = { backward: !1, forward: !1 };
        switch (ee(r) ? (l.backward = r, l.forward = r) : (l.backward = (a = r?.backward) !== null && a !== void 0 ? a : !1, l.forward = (n = r?.forward) !== null && n !== void 0 ? n : !1), t) {
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
        var u = this._dataList.length, h = e.timestamp, c = st(this._dataList[u - 1], "timestamp", 0);
        if (h > c) {
          this._dataList.push(e);
          var d = this.getLastBarRightSideDiffBarCount();
          d < 0 && this.setLastBarRightSideDiffBarCount(--d), o = !0, s = !0;
        } else h === c && (this._dataList[u - 1] = e, o = !0, s = !0);
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
    }, i.prototype.setDataLoader = function(e) {
      var t = this;
      this.resetData(function() {
        t._dataLoader = e;
      });
    }, i.prototype._calcOptimalBarSpace = function() {
      var e = 4, t = 1 - Ba * Math.atan(Math.max(e, this._barSpace) - e) / (Math.PI * 0.5), r = Math.min(Math.floor(this._barSpace * t), Math.floor(this._barSpace));
      r % 2 === 0 && r + 2 >= this._barSpace && --r, this._gapBarSpace = Math.max(1, r);
    }, i.prototype._adjustVisibleRange = function() {
      var e, t, r = this._dataList.length, a = this._totalBarSpace / this._barSpace, n = 0, o = 0;
      this._scrollLimitRole === "distance" ? (n = (this._totalBarSpace - this._maxOffsetDistance.right) / this._barSpace, o = (this._totalBarSpace - this._maxOffsetDistance.left) / this._barSpace) : (n = this._minVisibleBarCount.left, o = this._minVisibleBarCount.right), n = Math.max(0, n), o = Math.max(0, o);
      var s = a - Math.min(n, r);
      this._lastBarRightSideDiffBarCount > s && (this._lastBarRightSideDiffBarCount = s);
      var l = -r + Math.min(o, r);
      this._lastBarRightSideDiffBarCount < l && (this._lastBarRightSideDiffBarCount = l);
      var u = Math.round(this._lastBarRightSideDiffBarCount + r + 0.5), h = u;
      u > r && (u = r);
      var c = Math.round(u - a) - 1;
      c < 0 && (c = 0);
      var d = this._lastBarRightSideDiffBarCount > 0 ? Math.round(r + this._lastBarRightSideDiffBarCount - a) - 1 : c;
      this._visibleRange = { from: c, to: u, realFrom: d, realTo: h }, this.executeAction("onVisibleRangeChange", this._visibleRange), this._visibleRangeDataList = [], this._visibleRangeHighLowPrice = [
        { x: 0, price: Number.MIN_SAFE_INTEGER },
        { x: 0, price: Number.MAX_SAFE_INTEGER }
      ];
      for (var f = d; f < h; f++) {
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
      c === 0 ? this._dataLoadMore.forward && this._processDataLoad("forward") : u === r && this._dataLoadMore.backward && this._processDataLoad("backward");
    }, i.prototype._processDataLoad = function(e) {
      var t = this, r, a, n, o;
      if (!this._loading && C(this._dataLoader) && C(this._symbol) && C(this._period)) {
        this._loading = !0;
        var s = {
          type: e,
          symbol: this._symbol,
          period: this._period,
          timestamp: null,
          callback: function(l, u) {
            var h, c;
            t._loading = !1, t._addData(l, e, u), e === "init" && ((c = (h = t._dataLoader) === null || h === void 0 ? void 0 : h.subscribeBar) === null || c === void 0 || c.call(h, {
              symbol: t._symbol,
              period: t._period,
              callback: function(d) {
                t._addData(d, "update");
              }
            }));
          }
        };
        switch (e) {
          case "backward": {
            s.timestamp = (a = (r = this._dataList[this._dataList.length - 1]) === null || r === void 0 ? void 0 : r.timestamp) !== null && a !== void 0 ? a : null;
            break;
          }
          case "forward": {
            s.timestamp = (o = (n = this._dataList[0]) === null || n === void 0 ? void 0 : n.timestamp) !== null && o !== void 0 ? o : null;
            break;
          }
        }
        this._dataLoader.getBars(s);
      }
    }, i.prototype._processDataUnsubscribe = function() {
      var e, t;
      C(this._dataLoader) && C(this._symbol) && C(this._period) && ((t = (e = this._dataLoader).unsubscribeBar) === null || t === void 0 || t.call(e, {
        symbol: this._symbol,
        period: this._period
      }));
    }, i.prototype.resetData = function(e) {
      this._processDataUnsubscribe(), e?.(), this._loading = !1, this._processDataLoad("init");
    }, i.prototype.getBarSpace = function() {
      return {
        bar: this._barSpace,
        halfBar: this._barSpace / 2,
        gapBar: this._gapBarSpace,
        halfGapBar: Math.floor(this._gapBarSpace / 2)
      };
    }, i.prototype.setBarSpace = function(e, t) {
      e < this._layoutOptions.barSpaceLimit.min || e > this._layoutOptions.barSpaceLimit.max || this._barSpace === e || (this._barSpace = e, this._calcOptimalBarSpace(), t?.(), this._adjustVisibleRange(), this.setCrosshair(this._crosshair, { notInvalidate: !0 }), this._chart.layout({
        measureWidth: !0,
        update: !0,
        buildYAxisTick: !0,
        cacheYAxisWidth: !0
      }));
    }, i.prototype.getLayoutOptions = function() {
      return this._layoutOptions;
    }, i.prototype.setTotalBarSpace = function(e) {
      this._totalBarSpace !== e && (this._totalBarSpace = e, this._adjustVisibleRange(), this.setCrosshair(this._crosshair, { notInvalidate: !0 }));
    }, i.prototype.setOffsetRightDistance = function(e, t) {
      return this._offsetRightDistance = this._scrollLimitRole === "distance" ? Math.min(this._maxOffsetDistance.right, e) : e, this._lastBarRightSideDiffBarCount = this._offsetRightDistance / this._barSpace, (t ?? !1) && (this._adjustVisibleRange(), this.setCrosshair(this._crosshair, { notInvalidate: !0 }), this._chart.layout({
        measureWidth: !0,
        update: !0,
        buildYAxisTick: !0,
        cacheYAxisWidth: !0
      })), this;
    }, i.prototype.getInitialOffsetRightDistance = function() {
      return this._offsetRightDistance;
    }, i.prototype.getOffsetRightDistance = function() {
      return Math.max(0, this._lastBarRightSideDiffBarCount * this._barSpace);
    }, i.prototype.getLastBarRightSideDiffBarCount = function() {
      return this._lastBarRightSideDiffBarCount;
    }, i.prototype.setLastBarRightSideDiffBarCount = function(e) {
      this._lastBarRightSideDiffBarCount = e;
    }, i.prototype.setMaxOffsetLeftDistance = function(e) {
      this._scrollLimitRole = "distance", this._maxOffsetDistance.left = e;
    }, i.prototype.setMaxOffsetRightDistance = function(e) {
      this._scrollLimitRole = "distance", this._maxOffsetDistance.right = e;
    }, i.prototype.setLeftMinVisibleBarCount = function(e) {
      this._scrollLimitRole = "bar_count", this._minVisibleBarCount.left = e;
    }, i.prototype.setRightMinVisibleBarCount = function(e) {
      this._scrollLimitRole = "bar_count", this._minVisibleBarCount.right = e;
    }, i.prototype.getVisibleRange = function() {
      return this._visibleRange;
    }, i.prototype.startScroll = function() {
      this._startLastBarRightSideDiffBarCount = this._lastBarRightSideDiffBarCount;
    }, i.prototype.scroll = function(e) {
      if (this._scrollEnabled) {
        var t = e / this._barSpace, r = this._lastBarRightSideDiffBarCount * this._barSpace;
        this._lastBarRightSideDiffBarCount = this._startLastBarRightSideDiffBarCount - t, this._adjustVisibleRange(), this.setCrosshair(this._crosshair, { notInvalidate: !0 }), this._chart.layout({
          measureWidth: !0,
          update: !0,
          buildYAxisTick: !0,
          cacheYAxisWidth: !0
        });
        var a = Math.round(r - this._lastBarRightSideDiffBarCount * this._barSpace);
        a !== 0 && this.executeAction("onScroll", { distance: a });
      }
    }, i.prototype.getDataByDataIndex = function(e) {
      var t;
      return (t = this._dataList[e]) !== null && t !== void 0 ? t : null;
    }, i.prototype.coordinateToFloatIndex = function(e) {
      var t = this._dataList.length, r = (this._totalBarSpace - e) / this._barSpace, a = t + this._lastBarRightSideDiffBarCount - r;
      return Math.round(a * 1e6) / 1e6;
    }, i.prototype.dataIndexToTimestamp = function(e) {
      var t = this._dataList.length;
      if (t === 0)
        return null;
      var r = this.getDataByDataIndex(e);
      if (C(r))
        return r.timestamp;
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
              var h = new Date(n), c = h.getDate();
              h.setDate(1), h.setMonth(h.getMonth() + u * o);
              var d = new Date(h.getFullYear(), h.getMonth() + 1, 0).getDate();
              return h.setDate(Math.min(c, d)), h.getTime();
            }
            case "year": {
              var h = new Date(n);
              return h.setFullYear(h.getFullYear() + u * o), h.getTime();
            }
          }
        }
      }
      return null;
    }, i.prototype.timestampToDataIndex = function(e) {
      var t = this._dataList.length;
      if (t === 0)
        return 0;
      if (C(this._period)) {
        var r = null, a = 0, n = t - 1, o = this._dataList[n].timestamp;
        e > o && (r = o, a = n);
        var s = this._dataList[0].timestamp;
        if (e < s && (r = s, a = 0), B(r)) {
          var l = this._period, u = l.type, h = l.span;
          switch (u) {
            case "second":
              return a + Math.floor((e - r) / (h * 1e3));
            case "minute":
              return a + Math.floor((e - r) / (h * 60 * 1e3));
            case "hour":
              return a + Math.floor((e - r) / (h * 60 * 60 * 1e3));
            case "day":
              return a + Math.floor((e - r) / (h * 24 * 60 * 60 * 1e3));
            case "week":
              return a + Math.floor((e - r) / (h * 7 * 24 * 60 * 60 * 1e3));
            case "month": {
              var c = new Date(r), d = new Date(e), f = c.getFullYear(), v = d.getFullYear(), p = c.getMonth(), g = d.getMonth();
              return a + Math.floor(((v - f) * 12 + (g - p)) / h);
            }
            case "year": {
              var f = new Date(r).getFullYear(), v = new Date(e).getFullYear();
              return a + Math.floor((v - f) / h);
            }
          }
        }
      }
      return De(this._dataList, "timestamp", e);
    }, i.prototype.dataIndexToCoordinate = function(e) {
      var t = this._dataList.length, r = t + this._lastBarRightSideDiffBarCount - e;
      return Math.floor(this._totalBarSpace - (r - 0.5) * this._barSpace + 0.5);
    }, i.prototype.coordinateToDataIndex = function(e) {
      return Math.ceil(this.coordinateToFloatIndex(e)) - 1;
    }, i.prototype.floatIndexToTimestamp = function(e) {
      var t = this._dataList.length;
      if (t === 0)
        return null;
      var r = t - 1;
      if (e > r && t >= 2) {
        var a = this._dataList[r].timestamp, n = this._dataList[r - 1].timestamp, o = a - n;
        if (o > 0) {
          var s = e - r;
          return Math.round(a + s * o);
        }
      }
      if (e < 0 && t >= 2) {
        var l = this._dataList[0].timestamp, u = this._dataList[1].timestamp, o = u - l;
        if (o > 0)
          return Math.round(l + e * o);
      }
      var h = Math.floor(e), c = e - h, d = this.dataIndexToTimestamp(h);
      if (c === 0 || !B(d))
        return d;
      var f = this.dataIndexToTimestamp(h + 1);
      return B(f) ? Math.round(d + (f - d) * c) : d;
    }, i.prototype.timestampToFloatIndex = function(e) {
      var t = this._dataList.length;
      if (t === 0)
        return 0;
      var r = this._dataList[0].timestamp, a = this._dataList[t - 1].timestamp;
      if (e > a && t >= 2) {
        var n = this._dataList[t - 2].timestamp, o = a - n;
        if (o > 0) {
          var s = e - a, l = s / o;
          return t - 1 + l;
        }
      }
      if (e < r && t >= 2) {
        var u = this._dataList[1].timestamp, o = u - r;
        if (o > 0) {
          var h = r - e, c = h / o;
          return -c;
        }
      }
      for (var d = 0, f = t - 1, v = 0; d <= f; ) {
        var p = Math.floor((d + f) / 2), g = this._dataList[p].timestamp;
        g <= e ? (v = p, d = p + 1) : f = p - 1;
      }
      var m = this._dataList[v], x = v + 1 < t ? this._dataList[v + 1] : null;
      if (C(m) && C(x)) {
        var _ = m.timestamp, E = x.timestamp;
        if (e >= _ && E > _) {
          var y = (e - _) / (E - _);
          return v + Math.min(y, 1);
        }
      }
      return v;
    }, i.prototype.zoom = function(e, t, r) {
      var a = this, n;
      if (this._zoomEnabled) {
        var o = t ?? { x: (n = this._crosshair.x) !== null && n !== void 0 ? n : this._totalBarSpace / 2 };
        r === "xAxis" ? this._zoomAnchor.xAxis === "last_bar" && (o.x = this.dataIndexToCoordinate(this._dataList.length - 1)) : this._zoomAnchor.main === "last_bar" && (o.x = this.dataIndexToCoordinate(this._dataList.length - 1));
        var s = o.x, l = this.coordinateToFloatIndex(s), u = this._barSpace, h = this._barSpace + e * (this._barSpace / ke);
        this.setBarSpace(h, function() {
          a._lastBarRightSideDiffBarCount += l - a.coordinateToFloatIndex(s);
        });
        var c = this._barSpace / u;
        c !== 1 && this.executeAction("onZoom", { scale: c });
      }
    }, i.prototype.setZoomEnabled = function(e) {
      this._zoomEnabled = e;
    }, i.prototype.isZoomEnabled = function() {
      return this._zoomEnabled;
    }, i.prototype.setZoomAnchor = function(e) {
      $(e) ? (this._zoomAnchor.main = e, this._zoomAnchor.xAxis = e) : ($(e.main) && (this._zoomAnchor.main = e.main), $(e.xAxis) && (this._zoomAnchor.xAxis = e.xAxis));
    }, i.prototype.getZoomAnchor = function() {
      return M({}, this._zoomAnchor);
    }, i.prototype.setScrollEnabled = function(e) {
      this._scrollEnabled = e;
    }, i.prototype.isScrollEnabled = function() {
      return this._scrollEnabled;
    }, i.prototype.setCrosshair = function(e, t) {
      var r, a = t ?? {}, n = a.notInvalidate, o = a.notExecuteAction, s = a.forceInvalidate, l = e ?? {}, u = 0, h = 0;
      B(l.x) ? (u = this.coordinateToDataIndex(l.x), u < 0 ? h = 0 : u > this._dataList.length - 1 ? h = this._dataList.length - 1 : h = u) : (u = this._dataList.length - 1, h = u);
      var c = this._dataList[h], d = this.dataIndexToCoordinate(u), f = { x: this._crosshair.x, y: this._crosshair.y, paneId: this._crosshair.paneId };
      this._crosshair = M(M({}, l), { realX: d, kLineData: c, realDataIndex: u, dataIndex: h, timestamp: (r = this.dataIndexToTimestamp(u)) !== null && r !== void 0 ? r : void 0 }), (f.x !== l.x || f.y !== l.y || f.paneId !== l.paneId || (s ?? !1)) && (C(c) && !(o ?? !1) && this.hasAction("onCrosshairChange") && $(this._crosshair.paneId) && this.executeAction("onCrosshairChange", e), (n ?? !1) || this._chart.updatePane(
        1
        /* UpdateLevel.Overlay */
      ));
    }, i.prototype.getCrosshair = function() {
      return this._crosshair;
    }, i.prototype.executeAction = function(e, t) {
      var r;
      (r = this._actions.get(e)) === null || r === void 0 || r.execute(t);
    }, i.prototype.subscribeAction = function(e, t) {
      var r;
      this._actions.has(e) || this._actions.set(e, new xi()), (r = this._actions.get(e)) === null || r === void 0 || r.subscribe(t);
    }, i.prototype.unsubscribeAction = function(e, t) {
      var r = this._actions.get(e);
      C(r) && (r.unsubscribe(t), r.isEmpty() && this._actions.delete(e));
    }, i.prototype.hasAction = function(e) {
      var t = this._actions.get(e);
      return C(t) && !t.isEmpty();
    }, i.prototype._sortIndicators = function(e) {
      var t;
      $(e) ? (t = this._indicators.get(e)) === null || t === void 0 || t.sort(function(r, a) {
        return r.zLevel - a.zLevel;
      }) : this._indicators.forEach(function(r) {
        r.sort(function(a, n) {
          return a.zLevel - n.zLevel;
        });
      });
    }, i.prototype._calcIndicator = function(e) {
      var t = this, r = [];
      if (r = r.concat(e), r.length > 0) {
        var a = {};
        r.forEach(function(n) {
          a[n.id] = n.calcImp(t._dataList);
        }), this._taskScheduler.add(a);
      }
    }, i.prototype.addIndicator = function(e, t) {
      var r = e.name, a = this.getIndicatorsByFilter(e);
      if (a.length > 0)
        return !1;
      var n = e.paneId, o = this.getIndicatorsByPaneId(n), s = Cr(r), l = new s();
      return this._synchronizeIndicatorSeriesPrecision(l), l.override(e), t || (this.removeIndicator({ paneId: n }), o = []), o.push(l), this._indicators.set(n, o), this._sortIndicators(n), this._calcIndicator(l), !0;
    }, i.prototype.getIndicatorsByPaneId = function(e) {
      var t;
      return (t = this._indicators.get(e)) !== null && t !== void 0 ? t : [];
    }, i.prototype.getIndicatorsByFilter = function(e) {
      var t = e.paneId, r = e.name, a = e.id, n = function(s) {
        return C(a) ? s.id === a : !C(r) || s.name === r;
      }, o = [];
      return C(t) ? o = o.concat(this.getIndicatorsByPaneId(t).filter(n)) : this._indicators.forEach(function(s) {
        o = o.concat(s.filter(n));
      }), o;
    }, i.prototype.removeIndicator = function(e) {
      var t = this, r = !1, a = this.getIndicatorsByFilter(e);
      return a.forEach(function(n) {
        var o = t.getIndicatorsByPaneId(n.paneId), s = o.findIndex(function(l) {
          return l.id === n.id;
        });
        s > -1 && (o.splice(s, 1), r = !0), o.length === 0 && t._indicators.delete(n.paneId);
      }), r;
    }, i.prototype.hasIndicators = function(e) {
      return this._indicators.has(e);
    }, i.prototype._synchronizeIndicatorSeriesPrecision = function(e) {
      if (C(this._symbol)) {
        var t = this._symbol, r = t.pricePrecision, a = r === void 0 ? ut.PRICE : r, n = t.volumePrecision, o = n === void 0 ? ut.VOLUME : n, s = function(l) {
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
    }, i.prototype.overrideIndicator = function(e) {
      var t = this, r = !1, a = !1, n = this.getIndicatorsByFilter(e);
      return n.forEach(function(o) {
        var s = o.paneId;
        o.override(e);
        var l = o.paneId;
        if (s !== l) {
          var u = t.getIndicatorsByPaneId(s), h = u.findIndex(function(g) {
            return g.id === o.id;
          });
          h > -1 && u.splice(h, 1), u.length === 0 && t._indicators.delete(s);
          var c = t.getIndicatorsByPaneId(l);
          c.some(function(g) {
            return g.id === o.id;
          }) || (c.push(o), t._indicators.set(l, c)), a = !0;
        }
        var d = o.shouldUpdateImp(), f = d.calc, v = d.draw, p = d.sort;
        p && (a = !0), f ? t._calcIndicator(o) : v && (r = !0);
      }), a && this._sortIndicators(), r || a;
    }, i.prototype.getOverlaysByFilter = function(e) {
      var t, r = e.id, a = e.groupId, n = e.paneId, o = e.name, s = function(h) {
        return C(r) ? h.id === r : C(a) ? h.groupId === a && (!C(o) || h.name === o) : !C(o) || h.name === o;
      }, l = [];
      C(n) ? l = l.concat(this.getOverlaysByPaneId(n).filter(s)) : this._overlays.forEach(function(h) {
        l = l.concat(h.filter(s));
      });
      var u = (t = this._progressOverlayInfo) === null || t === void 0 ? void 0 : t.overlay;
      return C(u) && s(u) && l.push(u), l;
    }, i.prototype.getOverlaysByPaneId = function(e) {
      var t;
      if (!$(e)) {
        var r = [];
        return this._overlays.forEach(function(a) {
          r = r.concat(a);
        }), r;
      }
      return (t = this._overlays.get(e)) !== null && t !== void 0 ? t : [];
    }, i.prototype._sortOverlays = function(e) {
      var t;
      $(e) ? (t = this._overlays.get(e)) === null || t === void 0 || t.sort(function(r, a) {
        return r.zLevel - a.zLevel;
      }) : this._overlays.forEach(function(r) {
        r.sort(function(a, n) {
          return a.zLevel - n.zLevel;
        });
      });
    }, i.prototype.addOverlays = function(e, t) {
      var r = this, a = [], n = e.map(function(o, s) {
        var l, u, h, c, d, f, v, p;
        if (C(o.id)) {
          var g = null;
          try {
            for (var m = gt(r._overlays), x = m.next(); !x.done; x = m.next()) {
              var _ = x.value, E = _[1], y = E.find(function(T) {
                return T.id === o.id;
              });
              if (C(y)) {
                g = y;
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
        var I = Aa(o.name);
        if (C(I)) {
          var b = (h = o.id) !== null && h !== void 0 ? h : te(aa), y = new I(), w = (c = o.paneId) !== null && c !== void 0 ? c : q.CANDLE;
          o.id = b, (d = o.groupId) !== null && d !== void 0 || (o.groupId = b);
          var S = r.getOverlaysByPaneId(w).length;
          return (f = o.zLevel) !== null && f !== void 0 || (o.zLevel = S), y.override(o), a.includes(w) || a.push(w), y.isDrawing() ? r._progressOverlayInfo = { paneId: w, overlay: y, appointPaneFlag: t[s] } : (r._overlays.has(w) || r._overlays.set(w, []), (v = r._overlays.get(w)) === null || v === void 0 || v.push(y)), y.isStart() && ((p = y.onDrawStart) === null || p === void 0 || p.call(y, { overlay: y, chart: r._chart })), b;
        }
        return null;
      });
      return a.length > 0 && (this._sortOverlays(), a.forEach(function(o) {
        r._chart.updatePane(1, o);
      }), this._chart.updatePane(1, q.X_AXIS)), n;
    }, i.prototype.getProgressOverlayInfo = function() {
      return this._progressOverlayInfo;
    }, i.prototype.progressOverlayComplete = function() {
      var e;
      if (this._progressOverlayInfo !== null) {
        var t = this._progressOverlayInfo, r = t.overlay, a = t.paneId;
        r.isDrawing() || (this._overlays.has(a) || this._overlays.set(a, []), (e = this._overlays.get(a)) === null || e === void 0 || e.push(r), this._sortOverlays(a), this._progressOverlayInfo = null);
      }
    }, i.prototype.updateProgressOverlayInfo = function(e, t) {
      this._progressOverlayInfo !== null && (ee(t) && t && (this._progressOverlayInfo.appointPaneFlag = t), this._progressOverlayInfo.paneId = e, this._progressOverlayInfo.overlay.override({ paneId: e }));
    }, i.prototype.overrideOverlay = function(e) {
      var t = this, r = !1, a = [], n = this.getOverlaysByFilter(e);
      return n.forEach(function(o) {
        o.override(e);
        var s = o.shouldUpdate(), l = s.sort, u = s.draw;
        l && (r = !0), (l || u) && (a.includes(o.paneId) || a.push(o.paneId));
      }), r && this._sortOverlays(), a.length > 0 ? (a.forEach(function(o) {
        t._chart.updatePane(1, o);
      }), this._chart.updatePane(1, q.X_AXIS), !0) : !1;
    }, i.prototype.removeOverlay = function(e) {
      var t = this, r = [], a = this.getOverlaysByFilter(e);
      return a.forEach(function(n) {
        var o, s = n.paneId, l = t.getOverlaysByPaneId(n.paneId);
        if ((o = n.onRemoved) === null || o === void 0 || o.call(n, { overlay: n, chart: t._chart }), r.includes(s) || r.push(s), n.isDrawing())
          t._progressOverlayInfo = null;
        else {
          var u = l.findIndex(function(h) {
            return h.id === n.id;
          });
          u > -1 && l.splice(u, 1);
        }
        l.length === 0 && t._overlays.delete(s);
      }), r.length > 0 ? (r.forEach(function(n) {
        t._chart.updatePane(1, n);
      }), this._chart.updatePane(1, q.X_AXIS), !0) : !1;
    }, i.prototype.setPressedOverlayInfo = function(e) {
      this._pressedOverlayInfo = e;
    }, i.prototype.getPressedOverlayInfo = function() {
      return this._pressedOverlayInfo;
    }, i.prototype.setHoverOverlayInfo = function(e, t, r) {
      var a = this._hoverOverlayInfo, n = a.overlay, o = a.figureType, s = a.figureIndex, l = a.figure, u = e.overlay;
      if ((n?.id !== u?.id || o !== e.figureType || s !== e.figureIndex) && (this._hoverOverlayInfo = e, n?.id !== u?.id)) {
        var h = !1, c = !1;
        n !== null && (n.override({ zLevel: n.getPrevZLevel() }), c = !0, r(n, l) && (h = !0)), u !== null && (u.setPrevZLevel(u.zLevel), u.override({ zLevel: Number.MAX_SAFE_INTEGER }), c = !0, t(u, e.figure) && (h = !0)), c && this._sortOverlays(), h || this._chart.updatePane(
          1
          /* UpdateLevel.Overlay */
        );
      }
    }, i.prototype.getHoverOverlayInfo = function() {
      return this._hoverOverlayInfo;
    }, i.prototype.setClickOverlayInfo = function(e, t, r) {
      var a = this._clickOverlayInfo, n = a.paneId, o = a.overlay, s = a.figureType, l = a.figure, u = a.figureIndex, h = e.overlay;
      (o?.id !== h?.id || s !== e.figureType || u !== e.figureIndex) && (this._clickOverlayInfo = e, o?.id !== h?.id && (C(o) && r(o, l), C(h) && t(h, e.figure), this._chart.updatePane(1, e.paneId), n !== e.paneId && this._chart.updatePane(1, n), this._chart.updatePane(1, q.X_AXIS)));
    }, i.prototype.getClickOverlayInfo = function() {
      return this._clickOverlayInfo;
    }, i.prototype.isOverlayEmpty = function() {
      return this._overlays.size === 0 && this._progressOverlayInfo === null;
    }, i.prototype.isOverlayDrawing = function() {
      var e, t;
      return (t = (e = this._progressOverlayInfo) === null || e === void 0 ? void 0 : e.overlay.isDrawing()) !== null && t !== void 0 ? t : !1;
    }, i.prototype._clearLastPriceMarkExtendTextUpdateTimer = function() {
      this._lastPriceMarkExtendTextUpdateTimers.forEach(function(e) {
        clearInterval(e);
      }), this._lastPriceMarkExtendTextUpdateTimers = [];
    }, i.prototype._clearData = function() {
      this._dataLoadMore.backward = !1, this._dataLoadMore.forward = !1, this._loading = !1, this._dataList = [], this._visibleRangeDataList = [], this._visibleRangeHighLowPrice = [
        { x: 0, price: Number.MIN_SAFE_INTEGER },
        { x: 0, price: Number.MAX_SAFE_INTEGER }
      ], this._visibleRange = je(), this._crosshair = {};
    }, i.prototype.getChart = function() {
      return this._chart;
    }, i.prototype.destroy = function() {
      this._clearData(), this._clearLastPriceMarkExtendTextUpdateTimer(), this._taskScheduler.clear(), this._overlays.clear(), this._indicators.clear(), this._actions.clear();
    }, i;
  })()
), U = {
  MAIN: "main",
  X_AXIS: "xAxis",
  Y_AXIS: "yAxis",
  SEPARATOR: "separator"
}, le = 7;
function La() {
  return Oe(this, void 0, void 0, function() {
    return Le(this, function(i) {
      switch (i.label) {
        case 0:
          return [4, new Promise(function(e) {
            var t = new ResizeObserver(function(r) {
              e(r.every(function(a) {
                return "devicePixelContentBoxSize" in a;
              })), t.disconnect();
            });
            t.observe(document.body, { box: "device-pixel-content-box" });
          }).catch(function() {
            return !1;
          })];
        case 1:
          return [2, i.sent()];
      }
    });
  });
}
var tr = (
  /** @class */
  (function() {
    function i(e, t) {
      var r = this;
      this._supportedDevicePixelContentBox = !1, this._width = 0, this._height = 0, this._pixelWidth = 0, this._pixelHeight = 0, this._nextPixelWidth = 0, this._nextPixelHeight = 0, this._requestAnimationId = Yt, this._mediaQueryListener = function() {
        var a = Wt(r._element);
        r._nextPixelWidth = Math.round(r._element.clientWidth * a), r._nextPixelHeight = Math.round(r._element.clientHeight * a), r._resetPixelRatio();
      }, this._listener = t, this._element = Vt("canvas", e), this._ctx = this._element.getContext("2d"), La().then(function(a) {
        r._supportedDevicePixelContentBox = a, a ? (r._resizeObserver = new ResizeObserver(function(n) {
          var o = n.find(function(l) {
            return l.target === r._element;
          }), s = o?.devicePixelContentBoxSize[0];
          C(s) && (r._nextPixelWidth = s.inlineSize, r._nextPixelHeight = s.blockSize, (r._pixelWidth !== r._nextPixelWidth || r._pixelHeight !== r._nextPixelHeight) && r._resetPixelRatio());
        }), r._resizeObserver.observe(r._element, { box: "device-pixel-content-box" })) : (r._mediaQueryList = window.matchMedia("(resolution: ".concat(Wt(r._element), "dppx)")), r._mediaQueryList.addListener(r._mediaQueryListener));
      }).catch(function(a) {
        return !1;
      });
    }
    return i.prototype._resetPixelRatio = function() {
      var e = this;
      this._executeListener(function() {
        var t = e._element.clientWidth, r = e._element.clientHeight;
        e._width = t, e._height = r, e._pixelWidth = e._nextPixelWidth, e._pixelHeight = e._nextPixelHeight, e._element.width = e._nextPixelWidth, e._element.height = e._nextPixelHeight;
        var a = e._nextPixelWidth / t, n = e._nextPixelHeight / r;
        e._ctx.scale(a, n);
      });
    }, i.prototype._executeListener = function(e) {
      var t = this;
      this._requestAnimationId === Yt && (this._requestAnimationId = ce(function() {
        t._ctx.clearRect(0, 0, t._width, t._height), e?.(), t._listener(), t._requestAnimationId = Yt;
      }));
    }, i.prototype.update = function(e, t) {
      if (this._width !== e || this._height !== t) {
        if (this._element.style.width = "".concat(e, "px"), this._element.style.height = "".concat(t, "px"), !this._supportedDevicePixelContentBox) {
          var r = Wt(this._element);
          this._nextPixelWidth = Math.round(e * r), this._nextPixelHeight = Math.round(t * r), this._resetPixelRatio();
        }
      } else
        this._executeListener();
    }, i.prototype.getElement = function() {
      return this._element;
    }, i.prototype.getContext = function() {
      return this._ctx;
    }, i.prototype.destroy = function() {
      C(this._resizeObserver) && this._resizeObserver.unobserve(this._element), C(this._mediaQueryList) && this._mediaQueryList.removeListener(this._mediaQueryListener);
    }, i;
  })()
), Ar = (
  /** @class */
  (function(i) {
    X(e, i);
    function e(t, r) {
      var a = i.call(this) || this;
      return a._bounding = Ve(), a._cursor = "crosshair", a._forceCursor = null, a._pane = r, a._rootContainer = t, a._container = a.createContainer(), t.appendChild(a._container), a;
    }
    return e.prototype.setBounding = function(t) {
      return ot(this._bounding, t), this;
    }, e.prototype.getContainer = function() {
      return this._container;
    }, e.prototype.getBounding = function() {
      return this._bounding;
    }, e.prototype.getPane = function() {
      return this._pane;
    }, e.prototype.checkEventOn = function(t) {
      return !0;
    }, e.prototype.setCursor = function(t) {
      $(this._forceCursor) || t !== this._cursor && (this._cursor = t, this._container.style.cursor = this._cursor);
    }, e.prototype.setForceCursor = function(t) {
      var r;
      t !== this._forceCursor && (this._forceCursor = t, this._container.style.cursor = (r = this._forceCursor) !== null && r !== void 0 ? r : this._cursor);
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
  })(We)
), Xe = (
  /** @class */
  (function(i) {
    X(e, i);
    function e(t, r) {
      var a = i.call(this, t, r) || this;
      a._mainCanvas = new tr({
        position: "absolute",
        top: "0",
        left: "0",
        zIndex: "2",
        boxSizing: "border-box"
      }, function() {
        a.updateMain(a._mainCanvas.getContext());
      }), a._overlayCanvas = new tr({
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
      return Vt("div", {
        margin: "0",
        padding: "0",
        position: "absolute",
        top: "0",
        overflow: "hidden",
        boxSizing: "border-box",
        zIndex: "1"
      });
    }, e.prototype.updateImp = function(t, r, a) {
      var n = r.width, o = r.height, s = r.left;
      t.style.left = "".concat(s, "px");
      var l = a, u = t.clientWidth, h = t.clientHeight;
      switch ((n !== u || o !== h) && (t.style.width = "".concat(n, "px"), t.style.height = "".concat(o, "px"), l = 3), l) {
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
      this._mainCanvas.destroy(), this._overlayCanvas.destroy(), i.prototype.destroy.call(this);
    }, e.prototype.getImage = function(t) {
      var r = this.getBounding(), a = r.width, n = r.height, o = Vt("canvas", {
        width: "".concat(a, "px"),
        height: "".concat(n, "px"),
        boxSizing: "border-box"
      }), s = o.getContext("2d"), l = Wt(o);
      return o.width = a * l, o.height = n * l, s.scale(l, l), s.drawImage(this._mainCanvas.getElement(), 0, 0, a, n), t && s.drawImage(this._overlayCanvas.getElement(), 0, 0, a, n), o;
    }, e;
  })(Ar)
);
function Va(i, e) {
  var t, r, a = [];
  a = a.concat(e);
  try {
    for (var n = gt(a), o = n.next(); !o.done; o = n.next()) {
      var s = o.value, l = s.x, u = s.y, h = s.r, c = i.x - l, d = i.y - u;
      if (!(c * c + d * d > h * h))
        return !0;
    }
  } catch (f) {
    t = { error: f };
  } finally {
    try {
      o && !o.done && (r = n.return) && r.call(n);
    } finally {
      if (t) throw t.error;
    }
  }
  return !1;
}
function Na(i, e, t) {
  var r = [];
  r = r.concat(e);
  var a = t.style, n = a === void 0 ? "fill" : a, o = t.color, s = o === void 0 ? "currentColor" : o, l = t.borderSize, u = l === void 0 ? 1 : l, h = t.borderColor, c = h === void 0 ? "currentColor" : h, d = t.borderStyle, f = d === void 0 ? "solid" : d, v = t.borderDashedValue, p = v === void 0 ? [2, 2] : v, g = (n === "fill" || t.style === "stroke_fill") && (!$(s) || !ie(s));
  g && (i.fillStyle = s, r.forEach(function(m) {
    var x = m.x, _ = m.y, E = m.r;
    i.beginPath(), i.arc(x, _, E, 0, Math.PI * 2), i.closePath(), i.fill();
  })), (n === "stroke" || t.style === "stroke_fill") && u > 0 && !ie(c) && (i.strokeStyle = c, i.lineWidth = u, f === "dashed" ? i.setLineDash(p) : i.setLineDash([]), r.forEach(function(m) {
    var x = m.x, _ = m.y, E = m.r;
    (!g || E > u) && (i.beginPath(), i.arc(x, _, E, 0, Math.PI * 2), i.closePath(), i.stroke());
  }));
}
var Ya = {
  name: "circle",
  checkEventOn: Va,
  draw: function(i, e, t) {
    Na(i, e, t);
  }
};
function Wa(i, e) {
  var t, r, a = [];
  a = a.concat(e);
  try {
    for (var n = gt(a), o = n.next(); !o.done; o = n.next()) {
      for (var s = o.value, l = !1, u = s.coordinates, h = 0, c = u.length - 1; h < u.length; c = h++)
        u[h].y > i.y != u[c].y > i.y && i.x < (u[c].x - u[h].x) * (i.y - u[h].y) / (u[c].y - u[h].y) + u[h].x && (l = !l);
      if (l)
        return !0;
    }
  } catch (d) {
    t = { error: d };
  } finally {
    try {
      o && !o.done && (r = n.return) && r.call(n);
    } finally {
      if (t) throw t.error;
    }
  }
  return !1;
}
function za(i, e, t) {
  var r = [];
  r = r.concat(e);
  var a = t.style, n = a === void 0 ? "fill" : a, o = t.color, s = o === void 0 ? "currentColor" : o, l = t.borderSize, u = l === void 0 ? 1 : l, h = t.borderColor, c = h === void 0 ? "currentColor" : h, d = t.borderStyle, f = d === void 0 ? "solid" : d, v = t.borderDashedValue, p = v === void 0 ? [2, 2] : v;
  (n === "fill" || t.style === "stroke_fill") && (!$(s) || !ie(s)) && (i.fillStyle = s, r.forEach(function(g) {
    var m = g.coordinates;
    i.beginPath(), i.moveTo(m[0].x, m[0].y);
    for (var x = 1; x < m.length; x++)
      i.lineTo(m[x].x, m[x].y);
    i.closePath(), i.fill();
  })), (n === "stroke" || t.style === "stroke_fill") && u > 0 && !ie(c) && (i.strokeStyle = c, i.lineWidth = u, f === "dashed" ? i.setLineDash(p) : i.setLineDash([]), r.forEach(function(g) {
    var m = g.coordinates;
    i.beginPath(), i.moveTo(m[0].x, m[0].y);
    for (var x = 1; x < m.length; x++)
      i.lineTo(m[x].x, m[x].y);
    i.closePath(), i.stroke();
  }));
}
var Xa = {
  name: "polygon",
  checkEventOn: Wa,
  draw: function(i, e, t) {
    za(i, e, t);
  }
};
function Mr(i, e) {
  var t, r, a = [];
  a = a.concat(e);
  try {
    for (var n = gt(a), o = n.next(); !o.done; o = n.next()) {
      var s = o.value, l = s.x, u = s.width;
      u < lt * 2 && (l -= lt, u = lt * 2);
      var h = s.y, c = s.height;
      if (c < lt * 2 && (h -= lt, c = lt * 2), i.x >= l && i.x <= l + u && i.y >= h && i.y <= h + c)
        return !0;
    }
  } catch (d) {
    t = { error: d };
  } finally {
    try {
      o && !o.done && (r = n.return) && r.call(n);
    } finally {
      if (t) throw t.error;
    }
  }
  return !1;
}
function Pr(i, e, t) {
  var r, a = [];
  a = a.concat(e);
  var n = t.style, o = n === void 0 ? "fill" : n, s = t.color, l = s === void 0 ? "transparent" : s, u = t.borderSize, h = u === void 0 ? 1 : u, c = t.borderColor, d = c === void 0 ? "transparent" : c, f = t.borderStyle, v = f === void 0 ? "solid" : f, p = t.borderRadius, g = p === void 0 ? 0 : p, m = t.borderDashedValue, x = m === void 0 ? [2, 2] : m, _ = (r = i.roundRect) !== null && r !== void 0 ? r : i.rect, E = (o === "fill" || t.style === "stroke_fill") && (!$(l) || !ie(l));
  if (E && (i.fillStyle = l, a.forEach(function(b) {
    var w = b.x, S = b.y, T = b.width, A = b.height;
    i.beginPath(), _.call(i, w, S, T, A, g), i.closePath(), i.fill();
  })), (o === "stroke" || t.style === "stroke_fill") && h > 0 && !ie(d)) {
    i.strokeStyle = d, i.fillStyle = d, i.lineWidth = h, v === "dashed" ? i.setLineDash(x) : i.setLineDash([]);
    var y = h % 2 === 1 ? 0.5 : 0, I = Math.round(y * 2);
    a.forEach(function(b) {
      var w = b.x, S = b.y, T = b.width, A = b.height;
      T > h * 2 && A > h * 2 ? (i.beginPath(), _.call(i, w + y, S + y, T - I, A - I, g), i.closePath(), i.stroke()) : E || i.fillRect(w, S, T, A);
    });
  }
}
var Ha = {
  name: "rect",
  checkEventOn: Mr,
  draw: function(i, e, t) {
    Pr(i, e, t);
  }
};
function Dr(i, e) {
  var t = e.size, r = t === void 0 ? 12 : t, a = e.paddingLeft, n = a === void 0 ? 0 : a, o = e.paddingTop, s = o === void 0 ? 0 : o, l = e.paddingRight, u = l === void 0 ? 0 : l, h = e.paddingBottom, c = h === void 0 ? 0 : h, d = e.weight, f = d === void 0 ? "normal" : d, v = e.family, p = i.x, g = i.y, m = i.text, x = i.align, _ = x === void 0 ? "left" : x, E = i.baseline, y = E === void 0 ? "top" : E, I = i.width, b = i.height, w = I ?? n + Rt(m, r, f, v) + u, S = b ?? s + r + c, T = 0;
  switch (_) {
    case "left":
    case "start": {
      T = p;
      break;
    }
    case "right":
    case "end": {
      T = p - w;
      break;
    }
    default: {
      T = p - w / 2;
      break;
    }
  }
  var A = 0;
  switch (y) {
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
  return { x: T, y: A, width: w, height: S };
}
function Ua(i, e, t) {
  var r, a, n = [];
  n = n.concat(e);
  try {
    for (var o = gt(n), s = o.next(); !s.done; s = o.next()) {
      var l = s.value, u = Dr(l, t), h = u.x, c = u.y, d = u.width, f = u.height;
      if (i.x >= h && i.x <= h + d && i.y >= c && i.y <= c + f)
        return !0;
    }
  } catch (v) {
    r = { error: v };
  } finally {
    try {
      s && !s.done && (a = o.return) && a.call(o);
    } finally {
      if (r) throw r.error;
    }
  }
  return !1;
}
function Ga(i, e, t) {
  var r = [];
  r = r.concat(e);
  var a = t.color, n = a === void 0 ? "currentColor" : a, o = t.size, s = o === void 0 ? 12 : o, l = t.family, u = t.weight, h = t.paddingLeft, c = h === void 0 ? 0 : h, d = t.paddingTop, f = d === void 0 ? 0 : d, v = t.paddingRight, p = v === void 0 ? 0 : v, g = r.map(function(m) {
    return Dr(m, t);
  });
  Pr(i, g, M(M({}, t), { color: t.backgroundColor })), i.textAlign = "left", i.textBaseline = "top", i.font = Zt(s, u, l), i.fillStyle = n, r.forEach(function(m, x) {
    var _ = g[x];
    i.fillText(m.text, _.x + c, _.y + f, _.width - c - p);
  });
}
var qa = {
  name: "text",
  checkEventOn: Ua,
  draw: function(i, e, t) {
    Ga(i, e, t);
  }
};
function Za(i, e) {
  var t = i.x - e.x, r = i.y - e.y;
  return Math.sqrt(t * t + r * r);
}
function $a(i, e) {
  var t, r, a = [];
  a = a.concat(e);
  try {
    for (var n = gt(a), o = n.next(); !o.done; o = n.next()) {
      var s = o.value;
      if (Math.abs(Za(i, s) - s.r) < lt) {
        var l = s.r, u = s.startAngle, h = s.endAngle, c = l * Math.cos(u) + s.x, d = l * Math.sin(u) + s.y, f = l * Math.cos(h) + s.x, v = l * Math.sin(h) + s.y;
        if (i.x <= Math.max(c, f) + lt && i.x >= Math.min(c, f) - lt && i.y <= Math.max(d, v) + lt && i.y >= Math.min(d, v) - lt)
          return !0;
      }
    }
  } catch (p) {
    t = { error: p };
  } finally {
    try {
      o && !o.done && (r = n.return) && r.call(n);
    } finally {
      if (t) throw t.error;
    }
  }
  return !1;
}
function ja(i, e, t) {
  var r = [];
  r = r.concat(e);
  var a = t.style, n = a === void 0 ? "solid" : a, o = t.size, s = o === void 0 ? 1 : o, l = t.color, u = l === void 0 ? "currentColor" : l, h = t.dashedValue, c = h === void 0 ? [2, 2] : h;
  i.lineWidth = s, i.strokeStyle = u, n === "dashed" ? i.setLineDash(c) : i.setLineDash([]), r.forEach(function(d) {
    var f = d.x, v = d.y, p = d.r, g = d.startAngle, m = d.endAngle;
    i.beginPath(), i.arc(f, v, p, g, m), i.stroke(), i.closePath();
  });
}
var Ka = {
  name: "arc",
  checkEventOn: $a,
  draw: function(i, e, t) {
    ja(i, e, t);
  }
};
function er(i, e, t, r, a, n, o) {
  var s = ae(r, 7), l = s[0], u = s[1], h = s[2], c = s[3], d = s[4], f = s[5], v = s[6], p = o ? e + f : f + a, g = o ? t + v : v + n, m = Ja(e, t, l, u, h, c, d, p, g);
  m.forEach(function(x) {
    i.bezierCurveTo(x[0], x[1], x[2], x[3], x[4], x[5]);
  });
}
function Ja(i, e, t, r, a, n, o, s, l) {
  for (var u = Qa(i, e, t, r, a, n, o, s, l), h = u.cx, c = u.cy, d = u.startAngle, f = u.deltaAngle, v = [], p = Math.ceil(Math.abs(f) / (Math.PI / 2)), g = 0; g < p; g++) {
    var m = d + g * f / p, x = d + (g + 1) * f / p, _ = tn(h, c, t, r, a, m, x);
    v.push(_);
  }
  return v;
}
function Qa(i, e, t, r, a, n, o, s, l) {
  var u = a * Math.PI / 180, h = (i - s) / 2, c = (e - l) / 2, d = Math.cos(u) * h + Math.sin(u) * c, f = -Math.sin(u) * h + Math.cos(u) * c, v = Math.pow(d, 2) / Math.pow(t, 2) + Math.pow(f, 2) / Math.pow(r, 2);
  v > 1 && (t *= Math.sqrt(v), r *= Math.sqrt(v));
  var p = n === o ? -1 : 1, g = Math.pow(t, 2) * Math.pow(r, 2) - Math.pow(t, 2) * Math.pow(f, 2) - Math.pow(r, 2) * Math.pow(d, 2), m = Math.pow(t, 2) * Math.pow(f, 2) + Math.pow(r, 2) * Math.pow(d, 2), x = p * Math.sqrt(Math.abs(g / m)) * (t * f / r), _ = p * Math.sqrt(Math.abs(g / m)) * (-r * d / t), E = Math.cos(u) * x - Math.sin(u) * _ + (i + s) / 2, y = Math.sin(u) * x + Math.cos(u) * _ + (e + l) / 2, I = Math.atan2((f - _) / r, (d - x) / t), b = Math.atan2((-f - _) / r, (-d - x) / t) - I;
  return b < 0 && o === 1 ? b += 2 * Math.PI : b > 0 && o === 0 && (b -= 2 * Math.PI), { cx: E, cy: y, startAngle: I, deltaAngle: b };
}
function tn(i, e, t, r, a, n, o) {
  var s = Math.sin(o - n) * (Math.sqrt(4 + 3 * Math.pow(Math.tan((o - n) / 2), 2)) - 1) / 3, l = Math.cos(a), u = Math.sin(a), h = i + t * Math.cos(n) * l - r * Math.sin(n) * u, c = e + t * Math.cos(n) * u + r * Math.sin(n) * l, d = i + t * Math.cos(o) * l - r * Math.sin(o) * u, f = e + t * Math.cos(o) * u + r * Math.sin(o) * l, v = h + s * (-t * Math.sin(n) * l - r * Math.cos(n) * u), p = c + s * (-t * Math.sin(n) * u + r * Math.cos(n) * l), g = d - s * (-t * Math.sin(o) * l - r * Math.cos(o) * u), m = f - s * (-t * Math.sin(o) * u + r * Math.cos(o) * l);
  return [v, p, g, m, d, f];
}
function en(i, e, t) {
  var r = [];
  r = r.concat(e);
  var a = t.lineWidth, n = a === void 0 ? 1 : a, o = t.color, s = o === void 0 ? "currentColor" : o;
  i.lineWidth = n, i.strokeStyle = s, i.setLineDash([]), r.forEach(function(l) {
    var u = l.x, h = l.y, c = l.path, d = c.match(/[MLHVCSQTAZ][^MLHVCSQTAZ]*/gi);
    if (C(d)) {
      var f = u, v = h;
      i.beginPath(), d.forEach(function(p) {
        var g = 0, m = 0, x = 0, _ = 0, E = p[0], y = p.slice(1).trim().split(/[\s,]+/).map(Number);
        switch (E) {
          case "M":
            g = y[0] + f, m = y[1] + v, i.moveTo(g, m), x = g, _ = m;
            break;
          case "m":
            g += y[0], m += y[1], i.moveTo(g, m), x = g, _ = m;
            break;
          case "L":
            g = y[0] + f, m = y[1] + v, i.lineTo(g, m);
            break;
          case "l":
            g += y[0], m += y[1], i.lineTo(g, m);
            break;
          case "H":
            g = y[0] + f, i.lineTo(g, m);
            break;
          case "h":
            g += y[0], i.lineTo(g, m);
            break;
          case "V":
            m = y[0] + v, i.lineTo(g, m);
            break;
          case "v":
            m += y[0], i.lineTo(g, m);
            break;
          case "C":
            i.bezierCurveTo(y[0] + f, y[1] + v, y[2] + f, y[3] + v, y[4] + f, y[5] + v), g = y[4] + f, m = y[5] + v;
            break;
          case "c":
            i.bezierCurveTo(g + y[0], m + y[1], g + y[2], m + y[3], g + y[4], m + y[5]), g += y[4], m += y[5];
            break;
          case "S":
            i.bezierCurveTo(g, m, y[0] + f, y[1] + v, y[2] + f, y[3] + v), g = y[2] + f, m = y[3] + v;
            break;
          case "s":
            i.bezierCurveTo(g, m, g + y[0], m + y[1], g + y[2], m + y[3]), g += y[2], m += y[3];
            break;
          case "Q":
            i.quadraticCurveTo(y[0] + f, y[1] + v, y[2] + f, y[3] + v), g = y[2] + f, m = y[3] + v;
            break;
          case "q":
            i.quadraticCurveTo(g + y[0], m + y[1], g + y[2], m + y[3]), g += y[2], m += y[3];
            break;
          case "T":
            i.quadraticCurveTo(g, m, y[0] + f, y[1] + v), g = y[0] + f, m = y[1] + v;
            break;
          case "t":
            i.quadraticCurveTo(g, m, g + y[0], m + y[1]), g += y[0], m += y[1];
            break;
          case "A":
            er(i, g, m, y, f, v, !1), g = y[5] + f, m = y[6] + v;
            break;
          case "a":
            er(i, g, m, y, f, v, !0), g += y[5], m += y[6];
            break;
          case "Z":
          case "z":
            i.closePath(), g = x, m = _;
            break;
        }
      }), t.style === "fill" ? i.fill() : i.stroke();
    }
  });
}
var rn = {
  name: "path",
  checkEventOn: Mr,
  draw: function(i, e, t) {
    en(i, e, t);
  }
}, kr = {}, an = [Ya, va, Xa, Ha, qa, Ka, rn];
an.forEach(function(i) {
  kr[i.name] = ha.extend(i);
});
function nn(i) {
  var e;
  return (e = kr[i]) !== null && e !== void 0 ? e : null;
}
var Et = (
  /** @class */
  (function(i) {
    X(e, i);
    function e(t) {
      var r = i.call(this) || this;
      return r._widget = t, r;
    }
    return e.prototype.getWidget = function() {
      return this._widget;
    }, e.prototype.createFigure = function(t, r) {
      var a = nn(t.name);
      if (a !== null) {
        var n = new a(t);
        if (C(r)) {
          for (var o in r)
            r.hasOwnProperty(o) && n.registerEvent(o, r[o]);
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
  })(We)
), on = (
  /** @class */
  (function(i) {
    X(e, i);
    function e() {
      return i !== null && i.apply(this, arguments) || this;
    }
    return e.prototype.drawImp = function(t) {
      var r, a, n = this.getWidget(), o = this.getWidget().getPane(), s = o.getChart(), l = n.getBounding(), u = s.getStyles().grid, h = u.show;
      if (h) {
        t.save(), t.globalCompositeOperation = "destination-over";
        var c = u.horizontal, d = c.show;
        if (d) {
          var f = o.getYAxisComponentById(), v = f.getTicks().map(function(x) {
            return {
              coordinates: [
                { x: 0, y: x.coord },
                { x: l.width, y: x.coord }
              ]
            };
          });
          (r = this.createFigure({
            name: "line",
            attrs: v,
            styles: c
          })) === null || r === void 0 || r.draw(t);
        }
        var p = u.vertical, g = p.show;
        if (g) {
          var m = s.getXAxisPane().getXAxisComponent(), v = m.getTicks().map(function(_) {
            return {
              coordinates: [
                { x: _.coord, y: 0 },
                { x: _.coord, y: l.height }
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
  })(Et)
), Rr = (
  /** @class */
  (function(i) {
    X(e, i);
    function e() {
      return i !== null && i.apply(this, arguments) || this;
    }
    return e.prototype.eachChildren = function(t) {
      for (var r = this.getWidget().getPane(), a = r.getChart().getChartStore(), n = a.getVisibleRangeDataList(), o = a.getBarSpace(), s = n.length, l = 0; l < s; )
        t(n[l], o, l), ++l;
    }, e;
  })(Et)
), Fr = (
  /** @class */
  (function(i) {
    X(e, i);
    function e() {
      var t = i.apply(this, re([], ae(arguments), !1)) || this;
      return t._boundCandleBarClickEvent = function(r) {
        return function() {
          return t.getWidget().getPane().getChart().getChartStore().executeAction("onCandleBarClick", r), !1;
        };
      }, t;
    }
    return e.prototype.drawImp = function(t) {
      var r = this, a = this.getWidget().getPane(), n = a.getId() === q.CANDLE, o = a.getChart().getChartStore(), s = this.getCandleBarOptions();
      if (s !== null) {
        var l = s.type, u = s.styles, h = 0, c = 0;
        if (s.type === "ohlc") {
          var d = o.getBarSpace().gapBar;
          h = Math.min(Math.max(Math.round(d * 0.2), 1), 8), h > 2 && h % 2 === 1 && h--, c = Math.floor(h / 2);
        }
        var f = a.getYAxisComponentById(s.yAxisId);
        this.eachChildren(function(v, p) {
          var g, m = v.x, x = v.data, _ = x.current, E = x.prev;
          if (C(_)) {
            var y = _.open, I = _.high, b = _.low, w = _.close, S = u.compareRule === "current_open" ? y : (g = E?.close) !== null && g !== void 0 ? g : w, T = [];
            w > S ? (T[0] = u.upColor, T[1] = u.upBorderColor, T[2] = u.upWickColor) : w < S ? (T[0] = u.downColor, T[1] = u.downBorderColor, T[2] = u.downWickColor) : (T[0] = u.noChangeColor, T[1] = u.noChangeBorderColor, T[2] = u.noChangeWickColor);
            var A = f.convertToPixel(y), D = f.convertToPixel(w), R = [
              A,
              D,
              f.convertToPixel(I),
              f.convertToPixel(b)
            ];
            R.sort(function(L, F) {
              return L - F;
            });
            var P = p.gapBar % 2 === 0 ? 1 : 0, k = [];
            switch (l) {
              case "candle_solid": {
                k = r._createSolidBar(m, R, p, T, P);
                break;
              }
              case "candle_stroke": {
                k = r._createStrokeBar(m, R, p, T, P);
                break;
              }
              case "candle_up_stroke": {
                w > y ? k = r._createStrokeBar(m, R, p, T, P) : k = r._createSolidBar(m, R, p, T, P);
                break;
              }
              case "candle_down_stroke": {
                y > w ? k = r._createStrokeBar(m, R, p, T, P) : k = r._createSolidBar(m, R, p, T, P);
                break;
              }
              case "ohlc": {
                k = [
                  {
                    name: "rect",
                    attrs: [
                      {
                        x: m - c,
                        y: R[0],
                        width: h,
                        height: R[3] - R[0]
                      },
                      {
                        x: m - p.halfGapBar,
                        y: A + h > R[3] ? R[3] - h : A,
                        width: p.halfGapBar - c,
                        height: h
                      },
                      {
                        x: m + c,
                        y: D + h > R[3] ? R[3] - h : D,
                        width: p.halfGapBar - c,
                        height: h
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
                mouseClickEvent: r._boundCandleBarClickEvent(v)
              }), (F = r.createFigure(L, O ?? void 0)) === null || F === void 0 || F.draw(t);
            });
          }
        });
      }
    }, e.prototype.getCandleBarOptions = function() {
      var t = this.getWidget().getPane(), r = t.getDefaultYAxisId();
      if (!C(r))
        return null;
      var a = t.getChart().getStyles().candle;
      return {
        yAxisId: r,
        type: a.type,
        styles: a.bar
      };
    }, e.prototype._createSolidBar = function(t, r, a, n, o) {
      return [
        {
          name: "rect",
          attrs: {
            x: t,
            y: r[0],
            width: 1,
            height: r[3] - r[0]
          },
          styles: { color: n[2] }
        },
        {
          name: "rect",
          attrs: {
            x: t - a.halfGapBar,
            y: r[1],
            width: a.gapBar + o,
            height: Math.max(1, r[2] - r[1])
          },
          styles: {
            style: "stroke_fill",
            color: n[0],
            borderColor: n[1]
          }
        }
      ];
    }, e.prototype._createStrokeBar = function(t, r, a, n, o) {
      return [
        {
          name: "rect",
          attrs: [
            {
              x: t,
              y: r[0],
              width: 1,
              height: r[1] - r[0]
            },
            {
              x: t,
              y: r[2],
              width: 1,
              height: r[3] - r[2]
            }
          ],
          styles: { color: n[2] }
        },
        {
          name: "rect",
          attrs: {
            x: t - a.halfGapBar,
            y: r[1],
            width: a.gapBar + o,
            height: Math.max(1, r[2] - r[1])
          },
          styles: {
            style: "stroke",
            borderColor: n[1]
          }
        }
      ];
    }, e;
  })(Rr)
), sn = (
  /** @class */
  (function(i) {
    X(e, i);
    function e() {
      return i !== null && i.apply(this, arguments) || this;
    }
    return e.prototype.getCandleBarOptions = function() {
      var t, r, a = this.getWidget().getPane(), n = a.getChart().getChartStore(), o = n.getIndicatorsByPaneId(a.getId());
      try {
        for (var s = gt(o), l = s.next(); !l.done; l = s.next()) {
          var u = l.value, h = a.getYAxisComponentById(u.yAxisId);
          if (u.shouldOhlc && u.visible && !h.isInCandle()) {
            var c = u.styles, d = n.getStyles().indicator, f = st(c, "ohlc.compareRule", d.ohlc.compareRule), v = st(c, "ohlc.upColor", d.ohlc.upColor), p = st(c, "ohlc.downColor", d.ohlc.downColor), g = st(c, "ohlc.noChangeColor", d.ohlc.noChangeColor);
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
          l && !l.done && (r = s.return) && r.call(s);
        } finally {
          if (t) throw t.error;
        }
      }
      return null;
    }, e.prototype.drawImp = function(t) {
      var r = this;
      i.prototype.drawImp.call(this, t);
      var a = this.getWidget(), n = a.getPane(), o = n.getChart(), s = a.getBounding(), l = o.getXAxisPane().getXAxisComponent(), u = o.getChartStore(), h = u.getIndicatorsByPaneId(n.getId()), c = u.getStyles().indicator;
      t.save(), h.forEach(function(d) {
        var f = n.getYAxisComponentById(d.yAxisId);
        if (d.visible) {
          d.zLevel < 0 ? t.globalCompositeOperation = "destination-over" : t.globalCompositeOperation = "source-over";
          var v = !1;
          if (d.draw !== null && (t.save(), v = d.draw({
            ctx: t,
            chart: o,
            indicator: d,
            bounding: s,
            xAxis: l,
            yAxis: f
          }), t.restore()), !v) {
            var p = d.result, g = [];
            r.eachChildren(function(m, x) {
              var _, E, y, I = x.halfGapBar, b = m.dataIndex, w = m.x, S = l.convertToPixel(b - 1), T = l.convertToPixel(b + 1), A = (_ = p[b - 1]) !== null && _ !== void 0 ? _ : null, D = (E = p[b]) !== null && E !== void 0 ? E : null, R = (y = p[b + 1]) !== null && y !== void 0 ? y : null, P = { x: S }, k = { x: w }, L = { x: T };
              d.figures.forEach(function(F) {
                var O = F.key, N = A?.[O];
                B(N) && (P[O] = f.convertToPixel(N));
                var K = D?.[O];
                B(K) && (k[O] = f.convertToPixel(K));
                var et = R?.[O];
                B(et) && (L[O] = f.convertToPixel(et));
              }), Ne(d, b, x, c, function(F, O, N) {
                var K, et, J, tt, rt;
                if (C(D?.[F.key])) {
                  var at = k[F.key], Y = (K = F.attrs) === null || K === void 0 ? void 0 : K.call(F, {
                    data: { prev: A, current: D, next: R },
                    coordinate: { prev: P, current: k, next: L },
                    bounding: s,
                    barSpace: x,
                    xAxis: l,
                    yAxis: f
                  });
                  switch (F.type) {
                    case "text": {
                      Y = M({
                        x: w,
                        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment -- ignore
                        y: at,
                        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment -- ignore
                        text: D?.[F.key],
                        align: "center",
                        baseline: "middle"
                      }, Y);
                      break;
                    }
                    case "circle": {
                      Y = M({ x: w, y: at, r: Math.max(1, I) }, Y);
                      break;
                    }
                    case "rect":
                    case "bar": {
                      var G = (et = F.baseValue) !== null && et !== void 0 ? et : f.getRange().from, W = f.convertToPixel(G), Z = Math.abs(W - at);
                      G !== D?.[F.key] && (Z = Math.max(1, Z));
                      var H = 0;
                      at > W ? H = W : H = at;
                      var it = (J = Y?.width) !== null && J !== void 0 ? J : I * 2;
                      Y = M({ x: w - it / 2, y: H, width: Math.max(1, it), height: Z }, Y);
                      break;
                    }
                    case "line": {
                      C(g[N]) || (g[N] = []), B(k[F.key]) && B(L[F.key]) && g[N].push({
                        coordinates: (tt = Y?.coordinates) !== null && tt !== void 0 ? tt : [
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
                  var j = F.type;
                  C(Y) && j !== "line" && ((rt = r.createFigure({
                    name: j === "bar" ? "rect" : j,
                    attrs: Y,
                    styles: O
                  })) === null || rt === void 0 || rt.draw(t));
                }
              });
            }), g.forEach(function(m) {
              var x, _, E, y;
              if (m.length > 1) {
                for (var I = [
                  {
                    coordinates: [m[0].coordinates[0], m[0].coordinates[1]],
                    styles: m[0].styles
                  }
                ], b = 1; b < m.length; b++) {
                  var w = I[I.length - 1], S = m[b], T = w.coordinates[w.coordinates.length - 1];
                  T.x === S.coordinates[0].x && T.y === S.coordinates[0].y && w.styles.style === S.styles.style && w.styles.color === S.styles.color && w.styles.size === S.styles.size && w.styles.smooth === S.styles.smooth && ((x = w.styles.dashedValue) === null || x === void 0 ? void 0 : x[0]) === ((_ = S.styles.dashedValue) === null || _ === void 0 ? void 0 : _[0]) && ((E = w.styles.dashedValue) === null || E === void 0 ? void 0 : E[1]) === ((y = S.styles.dashedValue) === null || y === void 0 ? void 0 : y[1]) ? w.coordinates.push(S.coordinates[1]) : I.push({
                    coordinates: [S.coordinates[0], S.coordinates[1]],
                    styles: S.styles
                  });
                }
                I.forEach(function(A) {
                  var D, R = A.coordinates, P = A.styles;
                  (D = r.createFigure({
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
  })(Fr)
), ln = (
  /** @class */
  (function(i) {
    X(e, i);
    function e() {
      return i !== null && i.apply(this, arguments) || this;
    }
    return e.prototype.drawImp = function(t) {
      var r = this.getWidget(), a = r.getPane(), n = r.getBounding(), o = r.getPane().getChart().getChartStore(), s = o.getCrosshair(), l = o.getStyles().crosshair;
      if ($(s.paneId) && l.show) {
        if (s.paneId === a.getId()) {
          var u = s.y;
          this._drawLine(t, [
            { x: 0, y: u },
            { x: n.width, y: u }
          ], l.horizontal);
        }
        var h = s.realX;
        this._drawLine(t, [
          { x: h, y: 0 },
          { x: h, y: n.height }
        ], l.vertical);
      }
    }, e.prototype._drawLine = function(t, r, a) {
      var n;
      if (a.show) {
        var o = a.line;
        o.show && ((n = this.createFigure({
          name: "line",
          attrs: { coordinates: r },
          styles: o
        })) === null || n === void 0 || n.draw(t));
      }
    }, e;
  })(Et)
), Br = (
  /** @class */
  (function(i) {
    X(e, i);
    function e(t) {
      var r = i.call(this, t) || this;
      return r._activeFeatureInfo = null, r._featureClickEvent = function(a, n) {
        return function() {
          var o = r.getWidget().getPane();
          return o.getChart().getChartStore().executeAction(a, n), !0;
        };
      }, r._featureMouseMoveEvent = function(a) {
        return function() {
          return r._activeFeatureInfo = a, !0;
        };
      }, r.registerEvent("mouseMoveEvent", function(a) {
        return r._activeFeatureInfo = null, !1;
      }), r;
    }
    return e.prototype.drawImp = function(t) {
      var r = this.getWidget(), a = r.getPane(), n = a.getChart().getChartStore(), o = n.getCrosshair();
      if (C(o.kLineData)) {
        var s = r.getBounding(), l = n.getStyles().indicator.tooltip, u = l.offsetLeft, h = l.offsetTop, c = l.offsetRight;
        this.drawIndicatorTooltip(t, u, h, s.width - c);
      }
    }, e.prototype.drawIndicatorTooltip = function(t, r, a, n) {
      var o = this, s = this.getWidget().getPane(), l = s.getChart().getChartStore(), u = l.getStyles().indicator, h = u.tooltip;
      if (this.isDrawTooltip(l.getCrosshair(), h)) {
        var c = l.getIndicatorsByPaneId(s.getId()), d = h.title, f = h.legend;
        c.forEach(function(v) {
          var p = 0, g = { x: r, y: a }, m = o.getIndicatorTooltipData(v), x = m.name, _ = m.calcParamsText, E = m.legends, y = m.features, I = x.length > 0, b = E.length > 0;
          if (I || b) {
            var w = o.classifyTooltipFeatures(y);
            if (p = o.drawStandardTooltipFeatures(t, w[0], g, v, r, p, n), I) {
              var S = x;
              _.length > 0 && (S = "".concat(S).concat(_));
              var T = d.color;
              p = o.drawStandardTooltipLegends(t, [
                {
                  title: { text: "", color: T },
                  value: { text: S, color: T }
                }
              ], g, r, p, n, d);
            }
            p = o.drawStandardTooltipFeatures(t, w[1], g, v, r, p, n), b && (p = o.drawStandardTooltipLegends(t, E, g, r, p, n, f)), p = o.drawStandardTooltipFeatures(t, w[2], g, v, r, p, n), a = g.y + p;
          }
        });
      }
      return a;
    }, e.prototype.drawStandardTooltipFeatures = function(t, r, a, n, o, s, l) {
      var u = this;
      if (r.length > 0) {
        var h = 0, c = 0;
        r.forEach(function(v) {
          var p = v.marginLeft, g = p === void 0 ? 0 : p, m = v.marginTop, x = m === void 0 ? 0 : m, _ = v.marginRight, E = _ === void 0 ? 0 : _, y = v.marginBottom, I = y === void 0 ? 0 : y, b = v.paddingLeft, w = b === void 0 ? 0 : b, S = v.paddingTop, T = S === void 0 ? 0 : S, A = v.paddingRight, D = A === void 0 ? 0 : A, R = v.paddingBottom, P = R === void 0 ? 0 : R, k = v.size, L = k === void 0 ? 0 : k, F = v.type, O = v.content, N = 0;
          if (F === "icon_font") {
            var K = O;
            t.font = Zt(L, "normal", K.family), N = t.measureText(K.code).width;
          } else
            N = L;
          h += g + w + N + D + E, c = Math.max(c, x + T + L + P + I);
        }), a.x + h > l ? (a.x = o, a.y += s, s = c) : s = Math.max(s, c);
        var d = this.getWidget().getPane(), f = d.getId();
        r.forEach(function(v) {
          var p, g, m, x, _, E = v.marginLeft, y = E === void 0 ? 0 : E, I = v.marginTop, b = I === void 0 ? 0 : I, w = v.marginRight, S = w === void 0 ? 0 : w, T = v.paddingLeft, A = T === void 0 ? 0 : T, D = v.paddingTop, R = D === void 0 ? 0 : D, P = v.paddingRight, k = P === void 0 ? 0 : P, L = v.paddingBottom, F = L === void 0 ? 0 : L, O = v.backgroundColor, N = v.activeBackgroundColor, K = v.borderRadius, et = v.size, J = et === void 0 ? 0 : et, tt = v.color, rt = v.activeColor, at = v.type, Y = v.content, G = tt, W = O;
          ((p = u._activeFeatureInfo) === null || p === void 0 ? void 0 : p.paneId) === f && ((g = u._activeFeatureInfo.indicator) === null || g === void 0 ? void 0 : g.id) === n?.id && u._activeFeatureInfo.feature.id === v.id && (G = rt ?? tt, W = N ?? O);
          var Z = "onCandleTooltipFeatureClick", H = {
            paneId: f,
            feature: v
          };
          C(n) && (Z = "onIndicatorTooltipFeatureClick", H.indicator = n);
          var it = {
            mouseDownEvent: u._featureClickEvent(Z, H),
            mouseMoveEvent: u._featureMouseMoveEvent(H)
          }, j = 0;
          if (at === "icon_font") {
            var Q = Y;
            (m = u.createFigure({
              name: "text",
              attrs: { text: Q.code, x: a.x + y, y: a.y + b },
              styles: {
                paddingLeft: A,
                paddingTop: R,
                paddingRight: k,
                paddingBottom: F,
                borderRadius: K,
                size: J,
                family: Q.family,
                color: G,
                backgroundColor: W
              }
            }, it)) === null || m === void 0 || m.draw(t), j = t.measureText(Q.code).width;
          } else {
            (x = u.createFigure({
              name: "rect",
              attrs: { x: a.x + y, y: a.y + b, width: J, height: J },
              styles: {
                paddingLeft: A,
                paddingTop: R,
                paddingRight: k,
                paddingBottom: F,
                color: W
              }
            }, it)) === null || x === void 0 || x.draw(t);
            var mt = Y;
            (_ = u.createFigure({
              name: "path",
              attrs: { path: mt.path, x: a.x + y + A, y: a.y + b + R, width: J, height: J },
              styles: {
                style: mt.style,
                lineWidth: mt.lineWidth,
                color: G
              }
            })) === null || _ === void 0 || _.draw(t), j = J;
          }
          a.x += y + A + j + k + S;
        });
      }
      return s;
    }, e.prototype.drawStandardTooltipLegends = function(t, r, a, n, o, s, l) {
      var u = this;
      if (r.length > 0) {
        var h = l.marginLeft, c = l.marginTop, d = l.marginRight, f = l.marginBottom, v = l.size, p = l.family, g = l.weight;
        t.font = Zt(v, g, p), r.forEach(function(m) {
          var x, _, E = m.title, y = m.value, I = t.measureText(E.text).width, b = t.measureText(y.text).width, w = I + b, S = c + v + f;
          a.x + h + w + d > s ? (a.x = n, a.y += o, o = S) : o = Math.max(o, S), E.text.length > 0 && ((x = u.createFigure({
            name: "text",
            attrs: { x: a.x + h, y: a.y + c, text: E.text },
            styles: { color: E.color, size: v, family: p, weight: g }
          })) === null || x === void 0 || x.draw(t)), (_ = u.createFigure({
            name: "text",
            attrs: { x: a.x + h + I, y: a.y + c, text: y.text },
            styles: { color: y.color, size: v, family: p, weight: g }
          })) === null || _ === void 0 || _.draw(t), a.x += h + w + d;
        });
      }
      return o;
    }, e.prototype.isDrawTooltip = function(t, r) {
      var a = r.showRule;
      return a === "always" || a === "follow_cross" && $(t.paneId);
    }, e.prototype.getIndicatorTooltipData = function(t) {
      var r, a = this.getWidget().getPane().getChart().getChartStore(), n = a.getStyles().indicator, o = n.tooltip, s = o.title, l = "", u = "";
      if (s.show && (s.showName && (l = t.shortName), s.showParams)) {
        var h = t.calcParams;
        h.length > 0 && (u = "(".concat(h.join(","), ")"));
      }
      var c = { name: l, calcParamsText: u, legends: [], features: o.features }, d = a.getCrosshair().dataIndex, f = t.result, v = a.getInnerFormatter(), p = a.getDecimalFold(), g = a.getThousandsSeparator(), m = [];
      if (t.visible) {
        var x = a.getBarSpace(), _ = (r = f[d]) !== null && r !== void 0 ? r : {}, E = o.legend.defaultValue;
        Ne(t, d, x, n, function(k, L) {
          if ($(k.title)) {
            var F = L.color, O = _[k.key];
            B(O) && (O = pt(O, t.precision), t.shouldFormatBigNumber && (O = v.formatBigNumber(O)), O = p.format(g.format(O))), m.push({ title: { text: k.title, color: F }, value: { text: O ?? E, color: F } });
          }
        }), c.legends = m;
      }
      if (nt(t.createTooltipDataSource)) {
        var y = this.getWidget(), I = y.getPane(), b = I.getChart(), w = t.createTooltipDataSource({
          chart: b,
          indicator: t,
          crosshair: a.getCrosshair(),
          bounding: y.getBounding(),
          xAxis: I.getChart().getXAxisPane().getXAxisComponent(),
          yAxis: I.getYAxisComponentById(t.yAxisId)
        }), S = w.name, T = w.calcParamsText, A = w.legends, D = w.features;
        if (s.show && ($(S) && s.showName && (c.name = S), $(T) && s.showParams && (c.calcParamsText = T)), C(D) && (c.features = D), C(A) && t.visible) {
          var R = [], P = n.tooltip.legend.color;
          A.forEach(function(k) {
            var L = { text: "", color: P };
            kt(k.title) ? L = k.title : L.text = k.title;
            var F = { text: "", color: P };
            kt(k.value) ? F = k.value : F.text = k.value, B(Number(F.text)) && (F.text = p.format(g.format(F.text))), R.push({ title: L, value: F });
          }), c.legends = R;
        }
      }
      return c;
    }, e.prototype.classifyTooltipFeatures = function(t) {
      var r = [], a = [], n = [];
      return t.forEach(function(o) {
        switch (o.position) {
          case "left": {
            r.push(o);
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
      }), [r, a, n];
    }, e;
  })(Et)
), Or = (
  /** @class */
  (function(i) {
    X(e, i);
    function e(t) {
      var r = i.call(this, t) || this;
      return r._initEvent(), r;
    }
    return e.prototype._initEvent = function() {
      var t = this, r = this.getWidget(), a = r.getPane(), n = a.getId(), o = a.getChart(), s = o.getChartStore();
      this.registerEvent("mouseMoveEvent", function(l) {
        var u, h = s.getProgressOverlayInfo();
        if (h !== null) {
          var c = h.overlay, d = h.paneId;
          c.isStart() && (s.updateProgressOverlayInfo(n), d = n);
          var f = c.points.length - 1;
          return c.isDrawing() && d === n && (c.stepDrawingModeEventMoveForDrawing(t._coordinateToPoint(c, l)), (u = c.onDrawing) === null || u === void 0 || u.call(c, M({ chart: o, overlay: c }, l))), t._figureMouseMoveEvent(c, "point", f, { key: "".concat(jt, "point_").concat(f), type: "circle", attrs: {} })(l);
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
        }), r.setForceCursor(null), !1;
      }).registerEvent("mouseClickEvent", function(l) {
        var u, h, c = s.getProgressOverlayInfo();
        if (c !== null) {
          var d = c.overlay, f = c.paneId;
          d.isStart() && (s.updateProgressOverlayInfo(n, !0), f = n);
          var v = d.points.length - 1;
          return d.isDrawing() && f === n && (d.stepDrawingModeEventMoveForDrawing(t._coordinateToPoint(d, l)), (u = d.onDrawing) === null || u === void 0 || u.call(d, M({ chart: o, overlay: d }, l)), d.nextStep(), d.isDrawing() || (s.progressOverlayComplete(), (h = d.onDrawEnd) === null || h === void 0 || h.call(d, M({ chart: o, overlay: d }, l)))), t._figureMouseClickEvent(d, "point", v, {
            key: "".concat(jt, "point_").concat(v),
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
        var u, h = s.getProgressOverlayInfo();
        if (h !== null) {
          var c = h.overlay, d = h.paneId;
          c.isDrawing() && d === n && (c.forceComplete(), c.isDrawing() || (s.progressOverlayComplete(), (u = c.onDrawEnd) === null || u === void 0 || u.call(c, M({ chart: o, overlay: c }, l))));
          var f = c.points.length - 1;
          return t._figureMouseClickEvent(c, "point", f, {
            key: "".concat(jt, "point_").concat(f),
            type: "circle",
            attrs: {}
          })(l);
        }
        return !1;
      }).registerEvent("mouseRightClickEvent", function(l) {
        var u = s.getProgressOverlayInfo();
        if (u !== null) {
          var h = u.overlay;
          if (h.isDrawing()) {
            var c = h.points.length - 1;
            return t._figureMouseRightClickEvent(h, "point", c, {
              key: "".concat(jt, "point_").concat(c),
              type: "circle",
              attrs: {}
            })(l);
          }
        }
        return !1;
      }).registerEvent("mouseDownEvent", function(l) {
        var u, h = s.getProgressOverlayInfo();
        if (h !== null) {
          var c = h.overlay;
          if (c.isContinuousDrawingMode() && c.isStart()) {
            s.updateProgressOverlayInfo(n, !0);
            var d = t._coordinateToPoint(c, l);
            return c.startContinuousDrawing(d), (u = c.onDrawStart) === null || u === void 0 || u.call(c, M({ chart: o, overlay: c }, l)), !0;
          }
        }
        return !1;
      }).registerEvent("mouseUpEvent", function(l) {
        var u, h, c = s.getProgressOverlayInfo();
        if (c !== null) {
          var d = c.overlay;
          if (d.isContinuousDrawingMode() && d.isDrawing() && !d.isStart())
            return d.forceComplete(), s.progressOverlayComplete(), (u = d.onDrawEnd) === null || u === void 0 || u.call(d, M({ chart: o, overlay: d }, l)), !0;
        }
        var f = s.getPressedOverlayInfo(), v = f.overlay, p = f.figure;
        return v !== null && Tt("onPressedMoveEnd", p) && ((h = v.onPressedMoveEnd) === null || h === void 0 || h.call(v, M({ chart: o, overlay: v, figure: p ?? void 0 }, l))), s.setPressedOverlayInfo({
          paneId: n,
          overlay: null,
          figureType: "none",
          figureIndex: -1,
          figure: null
        }), !1;
      }).registerEvent("pressedMouseMoveEvent", function(l) {
        var u, h, c = s.getProgressOverlayInfo();
        if (c !== null) {
          var d = c.overlay;
          if (d.isContinuousDrawingMode() && d.isDrawing() && !d.isStart()) {
            var f = t._coordinateToPoint(d, l);
            return d.continuousDrawingModeEventMoveForDrawing(f), (u = d.onDrawing) === null || u === void 0 || u.call(d, M({ chart: o, overlay: d }, l)), t.getWidget().setForceCursor("pointer"), !0;
          }
        }
        var v = s.getPressedOverlayInfo(), p = v.overlay, g = v.figureType, m = v.figureIndex, x = v.figure;
        if (p !== null && Tt("onPressedMoving", x) && !p.lock) {
          var f = t._coordinateToPoint(p, l);
          g === "point" ? p.eventPressedPointMove(f, m) : p.eventPressedOtherMove(f, t.getWidget().getPane().getChart().getChartStore());
          var _ = !1;
          return (h = p.onPressedMoving) === null || h === void 0 || h.call(p, M(M({ chart: o, overlay: p, figure: x ?? void 0 }, l), { preventDefault: function() {
            _ = !0;
          } })), _ ? t.getWidget().setForceCursor(null) : t.getWidget().setForceCursor("pointer"), !0;
        }
        return t.getWidget().setForceCursor(null), !1;
      });
    }, e.prototype._createFigureEvents = function(t, r, a, n) {
      return t.isDrawing() ? null : {
        mouseMoveEvent: this._figureMouseMoveEvent(t, r, a, n),
        mouseDownEvent: this._figureMouseDownEvent(t, r, a, n),
        mouseClickEvent: this._figureMouseClickEvent(t, r, a, n),
        mouseRightClickEvent: this._figureMouseRightClickEvent(t, r, a, n),
        mouseDoubleClickEvent: this._figureMouseDoubleClickEvent(t, r, a, n)
      };
    }, e.prototype._processOverlayMouseEnterEvent = function(t, r, a) {
      return nt(t.onMouseEnter) && Tt("onMouseEnter", r) ? (t.onMouseEnter(M({ chart: this.getWidget().getPane().getChart(), overlay: t, figure: r ?? void 0 }, a)), !0) : !1;
    }, e.prototype._processOverlayMouseLeaveEvent = function(t, r, a) {
      return nt(t.onMouseLeave) && Tt("onMouseLeave", r) ? (t.onMouseLeave(M({ chart: this.getWidget().getPane().getChart(), overlay: t, figure: r ?? void 0 }, a)), !0) : !1;
    }, e.prototype._processOverlaySelectedEvent = function(t, r, a) {
      var n;
      return Tt("onSelected", r) ? ((n = t.onSelected) === null || n === void 0 || n.call(t, M({ chart: this.getWidget().getPane().getChart(), overlay: t, figure: r ?? void 0 }, a)), !0) : !1;
    }, e.prototype._processOverlayDeselectedEvent = function(t, r, a) {
      var n;
      return Tt("onDeselected", r) ? ((n = t.onDeselected) === null || n === void 0 || n.call(t, M({ chart: this.getWidget().getPane().getChart(), overlay: t, figure: r ?? void 0 }, a)), !0) : !1;
    }, e.prototype._figureMouseMoveEvent = function(t, r, a, n) {
      var o = this;
      return function(s) {
        var l, u = o.getWidget().getPane(), h = !t.isDrawing() && Tt("onMouseMove", n);
        if (h) {
          var c = !1;
          (l = t.onMouseMove) === null || l === void 0 || l.call(t, M(M({ chart: u.getChart(), overlay: t, figure: n }, s), { preventDefault: function() {
            c = !0;
          } })), c ? o.getWidget().setForceCursor(null) : o.getWidget().setForceCursor("pointer");
        }
        return u.getChart().getChartStore().setHoverOverlayInfo({ paneId: u.getId(), overlay: t, figureType: r, figure: n, figureIndex: a }, function(d, f) {
          return o._processOverlayMouseEnterEvent(d, f, s);
        }, function(d, f) {
          return o._processOverlayMouseLeaveEvent(d, f, s);
        }), h;
      };
    }, e.prototype._figureMouseDownEvent = function(t, r, a, n) {
      var o = this;
      return function(s) {
        var l;
        if (t.lock)
          return !1;
        var u = o.getWidget().getPane(), h = u.getId();
        return t.startPressedMove(o._coordinateToPoint(t, s)), Tt("onPressedMoveStart", n) ? ((l = t.onPressedMoveStart) === null || l === void 0 || l.call(t, M({ chart: u.getChart(), overlay: t, figure: n }, s)), u.getChart().getChartStore().setPressedOverlayInfo({ paneId: h, overlay: t, figureType: r, figureIndex: a, figure: n }), !t.isDrawing()) : !1;
      };
    }, e.prototype._figureMouseClickEvent = function(t, r, a, n) {
      var o = this;
      return function(s) {
        var l, u = o.getWidget().getPane(), h = u.getId(), c = !t.isDrawing() && Tt("onClick", n);
        return c && ((l = t.onClick) === null || l === void 0 || l.call(t, M({ chart: o.getWidget().getPane().getChart(), overlay: t, figure: n }, s))), u.getChart().getChartStore().setClickOverlayInfo({ paneId: h, overlay: t, figureType: r, figureIndex: a, figure: n }, function(d, f) {
          return o._processOverlaySelectedEvent(d, f, s);
        }, function(d, f) {
          return o._processOverlayDeselectedEvent(d, f, s);
        }), c;
      };
    }, e.prototype._figureMouseDoubleClickEvent = function(t, r, a, n) {
      var o = this;
      return function(s) {
        var l;
        return Tt("onDoubleClick", n) ? ((l = t.onDoubleClick) === null || l === void 0 || l.call(t, M(M({}, s), { chart: o.getWidget().getPane().getChart(), figure: n, overlay: t })), !t.isDrawing()) : !1;
      };
    }, e.prototype._figureMouseRightClickEvent = function(t, r, a, n) {
      var o = this;
      return function(s) {
        var l;
        if (Tt("onRightClick", n)) {
          var u = !1;
          return (l = t.onRightClick) === null || l === void 0 || l.call(t, M(M({ chart: o.getWidget().getPane().getChart(), overlay: t, figure: n }, s), { preventDefault: function() {
            u = !0;
          } })), u || o.getWidget().getPane().getChart().getChartStore().removeOverlay(t), !t.isDrawing();
        }
        return !1;
      };
    }, e.prototype._coordinateToPoint = function(t, r) {
      var a, n, o = {}, s = this.getWidget().getPane(), l = s.getChart(), u = s.getId(), h = l.getChartStore();
      if (this.coordinateToPointTimestampDataIndexFlag()) {
        var c = t;
        if (c.isContinuousDrawingMode()) {
          var d = h.coordinateToFloatIndex(r.x);
          o.dataIndex = d, o.timestamp = (a = h.floatIndexToTimestamp(d)) !== null && a !== void 0 ? a : void 0;
        } else {
          var f = l.getXAxisPane().getXAxisComponent(), v = f.convertFromPixel(r.x);
          o.dataIndex = v, o.timestamp = (n = h.dataIndexToTimestamp(v)) !== null && n !== void 0 ? n : void 0;
        }
      }
      if (this.coordinateToPointValueFlag()) {
        var p = s.getYAxisComponentById(), g = p.convertFromPixel(r.y);
        if (t.mode !== "normal" && u === q.CANDLE && B(o.dataIndex)) {
          var m = h.getDataByDataIndex(o.dataIndex);
          if (m !== null) {
            var x = t.modeSensitivity;
            if (g > m.high)
              if (t.mode === "weak_magnet") {
                var _ = p.convertToPixel(m.high), E = p.reverse ? _ + x : _ - x, y = p.convertFromPixel(E);
                g < y && (g = m.high);
              } else
                g = m.high;
            else if (g < m.low)
              if (t.mode === "weak_magnet") {
                var I = p.convertToPixel(m.low), E = p.reverse ? I - x : I + x, y = p.convertFromPixel(E);
                g > y && (g = m.low);
              } else
                g = m.low;
            else {
              var b = Math.max(m.open, m.close), w = Math.min(m.open, m.close);
              g > b ? g - b < m.high - g ? g = b : g = m.high : g < w ? g - m.low < w - g ? g = m.low : g = w : b - g < g - w ? g = b : g = w;
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
    }, e.prototype.dispatchEvent = function(t, r) {
      var a = this.getWidget().getPane().getChart().getChartStore().isOverlayDrawing();
      return a ? this.onEvent(t, r) : i.prototype.dispatchEvent.call(this, t, r);
    }, e.prototype.drawImp = function(t) {
      var r = this, a = this.getCompleteOverlays();
      a.forEach(function(o) {
        o.visible && r._drawOverlay(t, o);
      });
      var n = this.getProgressOverlay();
      C(n) && n.visible && this._drawOverlay(t, n);
    }, e.prototype._drawOverlay = function(t, r) {
      var a = r.points, n = this.getWidget().getPane(), o = n.getChart(), s = o.getChartStore(), l = n.getYAxisComponentById(), u = r.isContinuousDrawingMode(), h = a.map(function(d) {
        var f, v = null;
        u && B(d.timestamp) ? v = s.timestampToFloatIndex(d.timestamp) : B(d.timestamp) ? v = s.timestampToDataIndex(d.timestamp) : B(d.dataIndex) && (v = d.dataIndex);
        var p = { x: 0, y: 0 };
        return B(v) && (p.x = s.dataIndexToCoordinate(v)), B(d.value) && (p.y = (f = l?.convertToPixel(d.value)) !== null && f !== void 0 ? f : 0), p;
      });
      if (h.length > 0) {
        var c = [].concat(this.getFigures(r, h));
        this.drawFigures(t, r, c);
      }
      this.drawDefaultFigures(t, r, h);
    }, e.prototype.drawFigures = function(t, r, a) {
      var n = this, o = this.getWidget().getPane().getChart().getStyles().overlay;
      a.forEach(function(s, l) {
        var u = s.type, h = s.styles, c = s.attrs, d = [].concat(c);
        d.forEach(function(f) {
          var v, p, g = n._createFigureEvents(r, "other", l, s), m = M(M(M({}, o[u]), (v = r.styles) === null || v === void 0 ? void 0 : v[u]), h);
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
      var t = this.getWidget().getPane(), r = t.getChart().getChartStore().getProgressOverlayInfo();
      return C(r) && r.paneId === t.getId() ? r.overlay : null;
    }, e.prototype.getFigures = function(t, r) {
      var a, n, o = this.getWidget(), s = o.getPane(), l = s.getChart(), u = s.getYAxisComponentById(), h = l.getXAxisPane().getXAxisComponent(), c = o.getBounding();
      return (n = (a = t.createPointFigures) === null || a === void 0 ? void 0 : a.call(t, { chart: l, overlay: t, coordinates: r, bounding: c, xAxis: h, yAxis: u })) !== null && n !== void 0 ? n : [];
    }, e.prototype.drawDefaultFigures = function(t, r, a) {
      var n = this, o, s;
      if (r.needDefaultPointFigure) {
        var l = this.getWidget().getPane().getChart().getChartStore(), u = l.getHoverOverlayInfo(), h = l.getClickOverlayInfo();
        if (((o = u.overlay) === null || o === void 0 ? void 0 : o.id) === r.id && u.figureType !== "none" || ((s = h.overlay) === null || s === void 0 ? void 0 : s.id) === r.id && h.figureType !== "none") {
          var c = l.getStyles().overlay, d = r.styles, f = M(M({}, c.point), d?.point);
          a.forEach(function(v, p) {
            var g, m, x, _, E, y = v.x, I = v.y, b = f.radius, w = f.color, S = f.borderColor, T = f.borderSize;
            ((g = u.overlay) === null || g === void 0 ? void 0 : g.id) === r.id && u.figureType === "point" && ((m = u.figure) === null || m === void 0 ? void 0 : m.key) === "".concat(jt, "point_").concat(p) && (b = f.activeRadius, w = f.activeColor, S = f.activeBorderColor, T = f.activeBorderSize), (_ = n.createFigure({
              name: "circle",
              attrs: { x: y, y: I, r: b + T },
              styles: { color: S }
            }, (x = n._createFigureEvents(r, "point", p, {
              key: "".concat(jt, "point_").concat(p),
              type: "circle",
              attrs: { x: y, y: I, r: b + T },
              styles: { color: S }
            })) !== null && x !== void 0 ? x : void 0)) === null || _ === void 0 || _.draw(t), (E = n.createFigure({
              name: "circle",
              attrs: { x: y, y: I, r: b },
              styles: { color: w }
            })) === null || E === void 0 || E.draw(t);
          });
        }
      }
    }, e;
  })(Et)
), Lr = (
  /** @class */
  (function(i) {
    X(e, i);
    function e(t, r) {
      var a = i.call(this, t, r) || this;
      return a._gridView = new on(a), a._indicatorView = new sn(a), a._crosshairLineView = new ln(a), a._tooltipView = a.createTooltipView(), a._overlayView = new Or(a), a.addChild(a._tooltipView), a.addChild(a._overlayView), a;
    }
    return e.prototype.getName = function() {
      return U.MAIN;
    }, e.prototype.updateMain = function(t) {
      this.updateMainContent(t), this._indicatorView.draw(t), this._gridView.draw(t);
    }, e.prototype.createTooltipView = function() {
      return new Br(this);
    }, e.prototype.updateMainContent = function(t) {
    }, e.prototype.updateOverlayContent = function(t) {
    }, e.prototype.updateOverlay = function(t) {
      this._overlayView.draw(t), this._crosshairLineView.draw(t), this.updateOverlayContent(t), this._tooltipView.draw(t);
    }, e;
  })(Xe)
), un = (
  /** @class */
  (function(i) {
    X(e, i);
    function e() {
      var t = i.apply(this, re([], ae(arguments), !1)) || this;
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
      }), t._animationFrameTime = 0, t._animation = new Pe({ iterationCount: 1 / 0 }).doFrame(function(r) {
        t._animationFrameTime = r;
        var a = t.getWidget().getPane();
        a.getChart().updatePane(0, a.getId());
      }), t;
    }
    return e.prototype.drawImp = function(t) {
      var r, a, n, o = this.getWidget(), s = o.getPane(), l = s.getChart(), u = l.getDataList(), h = u.length - 1, c = o.getBounding(), d = s.getYAxisComponentById(), f = l.getStyles().candle.area, v = [], p = Number.MAX_SAFE_INTEGER, g = Number.MIN_SAFE_INTEGER, m = null;
      if (this.eachChildren(function(b) {
        var w = b.x, S = b.data.current, T = S?.[f.value];
        if (B(T)) {
          var A = d.convertToPixel(T);
          g === Number.MIN_SAFE_INTEGER && (g = w), v.push({ x: w, y: A }), p = Math.min(p, A), b.dataIndex === h && (m = { x: w, y: A });
        }
      }), v.length > 0) {
        (r = this.createFigure({
          name: "line",
          attrs: { coordinates: v },
          styles: {
            color: f.lineColor,
            size: f.lineSize,
            smooth: f.smooth
          }
        })) === null || r === void 0 || r.draw(t);
        var x = f.backgroundColor, _ = "";
        if (Dt(x)) {
          var E = t.createLinearGradient(0, c.height, 0, p);
          try {
            x.forEach(function(b) {
              var w = b.offset, S = b.color;
              E.addColorStop(w, S);
            });
          } catch {
          }
          _ = E;
        } else
          _ = x;
        t.fillStyle = _, t.beginPath(), t.moveTo(g, c.height), t.lineTo(v[0].x, v[0].y), Ir(t, v, f.smooth), t.lineTo(v[v.length - 1].x, c.height), t.closePath(), t.fill();
      }
      var y = f.point;
      if (y.show && C(m)) {
        (a = this.createFigure({
          name: "circle",
          attrs: {
            x: m.x,
            y: m.y,
            r: y.radius
          },
          styles: {
            style: "fill",
            color: y.color
          }
        })) === null || a === void 0 || a.draw(t);
        var I = y.rippleRadius;
        y.animation && (I = y.radius + this._animationFrameTime / y.animationDuration * (y.rippleRadius - y.radius), this._animation.setDuration(y.animationDuration).start()), (n = this._ripplePoint) === null || n === void 0 || n.setAttrs({
          x: m.x,
          y: m.y,
          r: I
        }).setStyles({ style: "fill", color: y.rippleColor }).draw(t);
      } else
        this.stopAnimation();
    }, e.prototype.stopAnimation = function() {
      this._animation.stop();
    }, e;
  })(Rr)
), hn = (
  /** @class */
  (function(i) {
    X(e, i);
    function e() {
      return i !== null && i.apply(this, arguments) || this;
    }
    return e.prototype.drawImp = function(t) {
      var r, a, n = this.getWidget(), o = n.getPane(), s = o.getChart().getChartStore(), l = s.getStyles().candle.priceMark, u = l.high, h = l.low;
      if (l.show && (u.show || h.show)) {
        var c = s.getVisibleRangeHighLowPrice(), d = (a = (r = s.getSymbol()) === null || r === void 0 ? void 0 : r.pricePrecision) !== null && a !== void 0 ? a : ut.PRICE, f = o.getYAxisComponentById(), v = c[0], p = v.price, g = v.x, m = c[1], x = m.price, _ = m.x, E = f.convertToPixel(p), y = f.convertToPixel(x), I = s.getDecimalFold(), b = s.getThousandsSeparator();
        u.show && p !== Number.MIN_SAFE_INTEGER && this._drawMark(t, I.format(b.format(pt(p, d))), { x: g, y: E }, E < y ? [-2, -5] : [2, 5], u), h.show && x !== Number.MAX_SAFE_INTEGER && this._drawMark(t, I.format(b.format(pt(x, d))), { x: _, y }, E < y ? [2, 5] : [-2, -5], h);
      }
    }, e.prototype._drawMark = function(t, r, a, n, o) {
      var s, l, u, h = a.x, c = a.y + n[0];
      (s = this.createFigure({
        name: "line",
        attrs: {
          coordinates: [
            { x: h - 2, y: c + n[0] },
            { x: h, y: c },
            { x: h + 2, y: c + n[0] }
          ]
        },
        styles: { color: o.color }
      })) === null || s === void 0 || s.draw(t);
      var d = 0, f = 0, v = "left", p = this.getWidget().getBounding().width;
      h > p / 2 ? (d = h - 5, f = d - o.textOffset, v = "right") : (d = h + 5, v = "left", f = d + o.textOffset);
      var g = c + n[1];
      (l = this.createFigure({
        name: "line",
        attrs: {
          coordinates: [
            { x: h, y: c },
            { x: h, y: g },
            { x: d, y: g }
          ]
        },
        styles: { color: o.color }
      })) === null || l === void 0 || l.draw(t), (u = this.createFigure({
        name: "text",
        attrs: {
          x: f,
          y: g,
          text: r,
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
  })(Et)
), cn = (
  /** @class */
  (function(i) {
    X(e, i);
    function e() {
      return i !== null && i.apply(this, arguments) || this;
    }
    return e.prototype.drawImp = function(t) {
      var r, a, n, o = this.getWidget(), s = o.getPane(), l = o.getBounding(), u = s.getChart().getChartStore(), h = u.getStyles().candle.priceMark, c = h.last, d = c.line;
      if (h.show && c.show && d.show) {
        var f = s.getYAxisComponentById(), v = u.getDataList(), p = v[v.length - 1];
        if (C(p)) {
          var g = p.close, m = p.open, x = c.compareRule === "current_open" ? m : (a = (r = v[v.length - 2]) === null || r === void 0 ? void 0 : r.close) !== null && a !== void 0 ? a : g, _ = f.convertToNicePixel(g), E = "";
          g > x ? E = c.upColor : g < x ? E = c.downColor : E = c.noChangeColor, (n = this.createFigure({
            name: "line",
            attrs: {
              coordinates: [
                { x: 0, y: _ },
                { x: l.width, y: _ }
              ]
            },
            styles: {
              style: d.style,
              color: E,
              size: d.size,
              dashedValue: d.dashedValue
            }
          })) === null || n === void 0 || n.draw(t);
        }
      }
    }, e;
  })(Et)
), dn = {
  second: "HH:mm:ss",
  minute: "HH:mm",
  hour: "MM-DD HH:mm",
  day: "YYYY-MM-DD",
  week: "YYYY-MM-DD",
  month: "YYYY-MM",
  year: "YYYY"
}, Vr = {
  second: "HH:mm:ss",
  minute: "YYYY-MM-DD HH:mm",
  hour: "YYYY-MM-DD HH:mm",
  day: "YYYY-MM-DD",
  week: "YYYY-MM-DD",
  month: "YYYY-MM",
  year: "YYYY"
}, vn = {
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
}, fn = {
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
}, pn = {
  "zh-CN": vn,
  "en-US": fn
};
function rr(i, e) {
  var t;
  return (t = pn[e][i]) !== null && t !== void 0 ? t : i;
}
var gn = (
  /** @class */
  (function(i) {
    X(e, i);
    function e() {
      return i !== null && i.apply(this, arguments) || this;
    }
    return e.prototype.drawImp = function(t) {
      var r = this.getWidget(), a = r.getPane().getChart().getChartStore(), n = a.getCrosshair();
      if (C(n.kLineData)) {
        var o = r.getBounding(), s = a.getStyles(), l = s.candle, u = s.indicator;
        if (l.tooltip.showType === "rect" && u.tooltip.showType === "rect") {
          var h = this.isDrawTooltip(n, l.tooltip), c = this.isDrawTooltip(n, u.tooltip);
          this._drawRectTooltip(t, h, c, l.tooltip.offsetTop);
        } else if (l.tooltip.showType === "standard" && u.tooltip.showType === "standard") {
          var d = l.tooltip, f = d.offsetLeft, v = d.offsetTop, p = d.offsetRight, g = o.width - p, m = this._drawCandleStandardTooltip(t, f, v, g);
          this.drawIndicatorTooltip(t, f, m, g);
        } else if (l.tooltip.showType === "rect" && u.tooltip.showType === "standard") {
          var x = l.tooltip, f = x.offsetLeft, v = x.offsetTop, p = x.offsetRight, g = o.width - p, _ = this.drawIndicatorTooltip(t, f, v, g), h = this.isDrawTooltip(n, l.tooltip);
          this._drawRectTooltip(t, h, !1, _);
        } else {
          var E = l.tooltip, f = E.offsetLeft, v = E.offsetTop, p = E.offsetRight, g = o.width - p, y = this._drawCandleStandardTooltip(t, f, v, g), c = this.isDrawTooltip(n, u.tooltip);
          this._drawRectTooltip(t, !1, c, y);
        }
      }
    }, e.prototype._drawCandleStandardTooltip = function(t, r, a, n) {
      var o, s = this.getWidget().getPane().getChart().getChartStore(), l = s.getStyles().candle, u = l.tooltip, h = u.legend, c = 0, d = { x: r, y: a }, f = s.getCrosshair();
      if (this.isDrawTooltip(f, u)) {
        var v = u.title;
        if (v.show) {
          var p = (o = s.getPeriod()) !== null && o !== void 0 ? o : {}, g = p.type, m = g === void 0 ? "" : g, x = p.span, _ = x === void 0 ? "" : x, E = qe(v.template, M(M({}, s.getSymbol()), { period: "".concat(_).concat(rr(m, s.getLocale())) })), y = v.color, I = this.drawStandardTooltipLegends(t, [
            {
              title: { text: "", color: y },
              value: { text: E, color: y }
            }
          ], { x: r, y: a }, r, 0, n, v);
          d.y = d.y + I;
        }
        var b = this._getCandleTooltipLegends(), w = this.classifyTooltipFeatures(u.features);
        c = this.drawStandardTooltipFeatures(t, w[0], d, null, r, c, n), c = this.drawStandardTooltipFeatures(t, w[1], d, null, r, c, n), b.length > 0 && (c = this.drawStandardTooltipLegends(t, b, d, r, c, n, h)), c = this.drawStandardTooltipFeatures(t, w[2], d, null, r, c, n);
      }
      return d.y + c;
    }, e.prototype._drawRectTooltip = function(t, r, a, n) {
      var o = this, s, l, u = this.getWidget(), h = u.getPane(), c = h.getChart().getChartStore(), d = c.getStyles(), f = d.candle, v = d.indicator, p = f.tooltip, g = v.tooltip;
      if (r || a) {
        var m = this._getCandleTooltipLegends(), x = p.offsetLeft, _ = p.offsetTop, E = p.offsetRight, y = p.offsetBottom, I = p.legend, b = I.marginLeft, w = I.marginRight, S = I.marginTop, T = I.marginBottom, A = I.size, D = I.weight, R = I.family, P = p.rect, k = P.position, L = P.paddingLeft, F = P.paddingRight, O = P.paddingTop, N = P.paddingBottom, K = P.offsetLeft, et = P.offsetRight, J = P.offsetTop, tt = P.offsetBottom, rt = P.borderSize, at = P.borderRadius, Y = P.borderColor, G = P.color, W = 0, Z = 0, H = 0;
        r && (t.font = Zt(A, D, R), m.forEach(function(Ft) {
          var St = Ft.title, Ct = Ft.value, Mt = "".concat(St.text).concat(Ct.text), Bt = t.measureText(Mt).width + b + w;
          W = Math.max(W, Bt);
        }), H += (T + S + A) * m.length);
        var it = g.legend, j = it.marginLeft, Q = it.marginRight, mt = it.marginTop, xt = it.marginBottom, ht = it.size, bt = it.weight, _t = it.family, Xt = [];
        if (a) {
          var At = c.getIndicatorsByPaneId(h.getId());
          t.font = Zt(ht, bt, _t), At.forEach(function(Ft) {
            var St = o.getIndicatorTooltipData(Ft).legends;
            Xt.push(St), St.forEach(function(Ct) {
              var Mt = Ct.title, Bt = Ct.value, ve = "".concat(Mt.text).concat(Bt.text), ii = t.measureText(ve).width + j + Q;
              W = Math.max(W, ii), H += mt + xt + ht;
            });
          });
        }
        if (Z += W, Z !== 0 && H !== 0) {
          var $t = c.getCrosshair(), Ht = u.getBounding(), wt = h.getYAxisWidget().getBounding();
          Z += rt * 2 + L + F, H += rt * 2 + O + N;
          var ct = Ht.width / 2, dt = k === "pointer" && $t.paneId === q.CANDLE, yt = ((s = $t.realX) !== null && s !== void 0 ? s : 0) > ct, It = 0;
          if (dt) {
            var Ue = $t.realX;
            yt ? It = Ue - et - Z : It = Ue + K;
          } else {
            var de = this.getWidget().getPane().getYAxisComponentById();
            yt ? (It = K + x, de.inside && de.position === "left" && (It += wt.width)) : (It = Ht.width - et - Z - E, de.inside && de.position === "right" && (It -= wt.width));
          }
          var Ut = n + J;
          if (dt) {
            var ti = $t.y;
            Ut = ti - H / 2, Ut + H > Ht.height - tt - y && (Ut = Ht.height - tt - H - y), Ut < n + J && (Ut = n + J + _);
          }
          (l = this.createFigure({
            name: "rect",
            attrs: {
              x: It,
              y: Ut,
              width: Z,
              height: H
            },
            styles: {
              style: "stroke_fill",
              color: G,
              borderColor: Y,
              borderSize: rt,
              borderRadius: at
            }
          })) === null || l === void 0 || l.draw(t);
          var ei = It + rt + L + b, Nt = Ut + rt + O;
          if (r && m.forEach(function(Ft) {
            var St, Ct;
            Nt += S;
            var Mt = Ft.title;
            (St = o.createFigure({
              name: "text",
              attrs: {
                x: ei,
                y: Nt,
                text: Mt.text
              },
              styles: {
                color: Mt.color,
                size: A,
                family: R,
                weight: D
              }
            })) === null || St === void 0 || St.draw(t);
            var Bt = Ft.value;
            (Ct = o.createFigure({
              name: "text",
              attrs: {
                x: It + Z - rt - w - F,
                y: Nt,
                text: Bt.text,
                align: "right"
              },
              styles: {
                color: Bt.color,
                size: A,
                family: R,
                weight: D
              }
            })) === null || Ct === void 0 || Ct.draw(t), Nt += A + T;
          }), a) {
            var ri = It + rt + L + j;
            Xt.forEach(function(Ft) {
              Ft.forEach(function(St) {
                var Ct, Mt;
                Nt += mt;
                var Bt = St.title, ve = St.value;
                (Ct = o.createFigure({
                  name: "text",
                  attrs: {
                    x: ri,
                    y: Nt,
                    text: Bt.text
                  },
                  styles: {
                    color: Bt.color,
                    size: ht,
                    family: _t,
                    weight: bt
                  }
                })) === null || Ct === void 0 || Ct.draw(t), (Mt = o.createFigure({
                  name: "text",
                  attrs: {
                    x: It + Z - rt - Q - F,
                    y: Nt,
                    text: ve.text,
                    align: "right"
                  },
                  styles: {
                    color: ve.color,
                    size: ht,
                    family: _t,
                    weight: bt
                  }
                })) === null || Mt === void 0 || Mt.draw(t), Nt += ht + xt;
              });
            });
          }
        }
      }
    }, e.prototype._getCandleTooltipLegends = function() {
      var t, r, a, n, o, s, l, u, h = this.getWidget().getPane().getChart().getChartStore(), c = h.getStyles().candle, d = h.getDataList(), f = h.getInnerFormatter(), v = h.getDecimalFold(), p = h.getThousandsSeparator(), g = h.getLocale(), m = (t = h.getSymbol()) !== null && t !== void 0 ? t : {}, x = m.pricePrecision, _ = x === void 0 ? ut.PRICE : x, E = m.volumePrecision, y = E === void 0 ? ut.VOLUME : E, I = h.getPeriod(), b = (r = h.getCrosshair().dataIndex) !== null && r !== void 0 ? r : 0, w = c.tooltip, S = w.legend, T = S.color, A = S.defaultValue, D = S.template, R = (a = d[b - 1]) !== null && a !== void 0 ? a : null, P = d[b], k = (n = R?.close) !== null && n !== void 0 ? n : P.close, L = P.close - k, F = M(M({}, P), { time: f.formatDate(P.timestamp, Vr[(o = I?.type) !== null && o !== void 0 ? o : "day"], "tooltip"), open: v.format(p.format(pt(P.open, _))), high: v.format(p.format(pt(P.high, _))), low: v.format(p.format(pt(P.low, _))), close: v.format(p.format(pt(P.close, _))), volume: v.format(p.format(f.formatBigNumber(pt((s = P.volume) !== null && s !== void 0 ? s : A, y)))), turnover: v.format(p.format(pt((l = P.turnover) !== null && l !== void 0 ? l : A, _))), change: k === 0 ? A : "".concat(p.format(pt(L / k * 100)), "%") }), O = nt(D) ? D({ prev: R, current: P, next: (u = d[b + 1]) !== null && u !== void 0 ? u : null }, c) : D;
      return O.map(function(N) {
        var K = N.title, et = N.value, J = { text: "", color: T };
        kt(K) ? J = M({}, K) : J.text = K, J.text = rr(J.text, g);
        var tt = { text: A, color: T };
        return kt(et) ? tt = M({}, et) : tt.text = et, C(/{change}/.exec(tt.text)) && (tt.color = L === 0 ? c.priceMark.last.noChangeColor : L > 0 ? c.priceMark.last.upColor : c.priceMark.last.downColor), tt.text = qe(tt.text, F), { title: J, value: tt };
      });
    }, e;
  })(Br)
), mn = (
  /** @class */
  (function(i) {
    X(e, i);
    function e(t) {
      var r = i.call(this, t) || this;
      return r._activeFeatureInfo = null, r._featureClickEvent = function(a) {
        return function() {
          var n = r.getWidget().getPane();
          return n.getChart().getChartStore().executeAction("onCrosshairFeatureClick", a), !0;
        };
      }, r._featureMouseMoveEvent = function(a) {
        return function() {
          return r._activeFeatureInfo = a, r.getWidget().setForceCursor("pointer"), !0;
        };
      }, r.registerEvent("mouseMoveEvent", function(a) {
        return r._activeFeatureInfo = null, r.getWidget().setForceCursor(null), !1;
      }), r;
    }
    return e.prototype.drawImp = function(t) {
      var r = this, a, n, o = this.getWidget(), s = o.getPane(), l = o.getPane().getChart().getChartStore(), u = l.getCrosshair(), h = this.getWidget(), c = h.getPane().getYAxisComponentById();
      if ($(u.paneId) && u.paneId === s.getId() && c.isInCandle()) {
        var d = l.getStyles().crosshair, f = d.horizontal.features;
        if (d.show && d.horizontal.show && f.length > 0) {
          var v = c.position === "right", p = h.getBounding(), g = 0, m = d.horizontal.text;
          if (c.inside && m.show) {
            var x = c.convertFromPixel(u.y), _ = c.getRange(), E = c.displayValueToText(c.realValueToDisplayValue(c.valueToRealValue(x, { range: _ }), { range: _ }), (n = (a = l.getSymbol()) === null || a === void 0 ? void 0 : a.pricePrecision) !== null && n !== void 0 ? n : ut.PRICE);
            E = l.getDecimalFold().format(l.getThousandsSeparator().format(E)), g = m.paddingLeft + Rt(E, m.size, m.weight, m.family) + m.paddingRight;
          }
          var y = g;
          v && (y = p.width - g);
          var I = u.y;
          f.forEach(function(b) {
            var w, S, T, A, D = b.marginLeft, R = D === void 0 ? 0 : D, P = b.marginTop, k = P === void 0 ? 0 : P, L = b.marginRight, F = L === void 0 ? 0 : L, O = b.paddingLeft, N = O === void 0 ? 0 : O, K = b.paddingTop, et = K === void 0 ? 0 : K, J = b.paddingRight, tt = J === void 0 ? 0 : J, rt = b.paddingBottom, at = rt === void 0 ? 0 : rt, Y = b.color, G = b.activeColor, W = b.backgroundColor, Z = b.activeBackgroundColor, H = b.borderRadius, it = b.size, j = it === void 0 ? 0 : it, Q = b.type, mt = b.content, xt = j;
            if (Q === "icon_font") {
              var ht = mt;
              xt = N + Rt(ht.code, j, "normal", ht.family) + tt;
            }
            v ? y -= xt + F : y += R;
            var bt = Y, _t = W;
            ((w = r._activeFeatureInfo) === null || w === void 0 ? void 0 : w.feature.id) === b.id && (bt = G ?? Y, _t = Z ?? W);
            var Xt = {
              mouseDownEvent: r._featureClickEvent({ crosshair: u, feature: b }),
              mouseMoveEvent: r._featureMouseMoveEvent({ crosshair: u, feature: b })
            };
            if (Q === "icon_font") {
              var ht = mt;
              (S = r.createFigure({
                name: "text",
                attrs: {
                  text: ht.code,
                  x: y,
                  y: I + k,
                  baseline: "middle"
                },
                styles: {
                  paddingLeft: N,
                  paddingTop: et,
                  paddingRight: tt,
                  paddingBottom: at,
                  borderRadius: H,
                  size: j,
                  family: ht.family,
                  color: bt,
                  backgroundColor: _t
                }
              }, Xt)) === null || S === void 0 || S.draw(t);
            } else {
              (T = r.createFigure({
                name: "rect",
                attrs: { x: y, y: I + k - j / 2, width: j, height: j },
                styles: {
                  paddingLeft: N,
                  paddingTop: et,
                  paddingRight: tt,
                  paddingBottom: at,
                  color: _t
                }
              }, Xt)) === null || T === void 0 || T.draw(t);
              var At = mt;
              (A = r.createFigure({
                name: "path",
                attrs: { path: At.path, x: y, y: I + k + et - j / 2, width: j, height: j },
                styles: {
                  style: At.style,
                  lineWidth: At.lineWidth,
                  color: bt
                }
              })) === null || A === void 0 || A.draw(t);
            }
            v ? y -= R : y += xt + F;
          });
        }
      }
    }, e;
  })(Et)
), yn = (
  /** @class */
  (function(i) {
    X(e, i);
    function e(t, r) {
      var a = i.call(this, t, r) || this;
      return a._candleBarView = new Fr(a), a._candleAreaView = new un(a), a._candleHighLowPriceView = new hn(a), a._candleLastPriceLineView = new cn(a), a._crosshairFeatureView = new mn(a), a.addChild(a._candleBarView), a.addChild(a._crosshairFeatureView), a;
    }
    return e.prototype.updateMainContent = function(t) {
      var r = this.getPane().getChart().getStyles().candle;
      r.type !== "area" ? (this._candleBarView.draw(t), this._candleHighLowPriceView.draw(t), this._candleAreaView.stopAnimation()) : this._candleAreaView.draw(t), this._candleLastPriceLineView.draw(t);
    }, e.prototype.updateOverlayContent = function(t) {
      this._crosshairFeatureView.draw(t);
    }, e.prototype.createTooltipView = function() {
      return new gn(this);
    }, e;
  })(Lr)
), Nr = (
  /** @class */
  (function(i) {
    X(e, i);
    function e() {
      return i !== null && i.apply(this, arguments) || this;
    }
    return e.prototype.drawImp = function(t) {
      var r = this, a, n, o = this.getWidget(), s = o.getPane(), l = o.getBounding(), u = this.getAxis(), h = this.getAxisStyles(s.getChart().getStyles());
      if (h.show) {
        h.axisLine.show && ((a = this.createFigure({
          name: "line",
          attrs: this.createAxisLine(l, h),
          styles: h.axisLine
        })) === null || a === void 0 || a.draw(t));
        var c = u.getTicks();
        if (h.tickLine.show) {
          var d = this.createTickLines(c, l, h);
          d.forEach(function(v) {
            var p;
            (p = r.createFigure({
              name: "line",
              attrs: v,
              styles: h.tickLine
            })) === null || p === void 0 || p.draw(t);
          });
        }
        if (h.tickText.show) {
          var f = this.createTickTexts(c, l, h);
          (n = this.createFigure({
            name: "text",
            attrs: f,
            styles: h.tickText
          })) === null || n === void 0 || n.draw(t);
        }
      }
    }, e;
  })(Et)
), _n = (
  /** @class */
  (function(i) {
    X(e, i);
    function e() {
      return i !== null && i.apply(this, arguments) || this;
    }
    return e.prototype.getAxis = function() {
      return this.getWidget().getAxisComponent();
    }, e.prototype.getAxisStyles = function(t) {
      return t.yAxis;
    }, e.prototype.createAxisLine = function(t, r) {
      var a = this.getAxis(), n = r.axisLine.size, o = 0;
      return a.isFromZero() ? o = 0 : o = t.width - n, {
        coordinates: [
          { x: o, y: 0 },
          { x: o, y: t.height }
        ]
      };
    }, e.prototype.createTickLines = function(t, r, a) {
      var n = this.getAxis(), o = a.axisLine, s = a.tickLine, l = 0, u = 0;
      return n.isFromZero() ? (l = 0, o.show && (l += o.size), u = l + s.length) : (l = r.width, o.show && (l -= o.size), u = l - s.length), t.map(function(h) {
        return {
          coordinates: [
            { x: l, y: h.coord },
            { x: u, y: h.coord }
          ]
        };
      });
    }, e.prototype.createTickTexts = function(t, r, a) {
      var n = this.getAxis(), o = a.axisLine, s = a.tickLine, l = a.tickText, u = 0;
      n.isFromZero() ? (u = l.marginStart, o.show && (u += o.size), s.show && (u += s.length)) : (u = r.width - l.marginEnd, o.show && (u -= o.size), s.show && (u -= s.length));
      var h = this.getAxis().isFromZero() ? "left" : "right";
      return t.map(function(c) {
        return {
          x: u,
          y: c.coord,
          text: c.text,
          align: h,
          baseline: "middle"
        };
      });
    }, e;
  })(Nr)
), xn = (
  /** @class */
  (function(i) {
    X(e, i);
    function e() {
      return i !== null && i.apply(this, arguments) || this;
    }
    return e.prototype.drawImp = function(t) {
      var r = this, a, n, o, s, l = this.getWidget(), u = l.getPane(), h = l.getBounding(), c = u.getChart().getChartStore(), d = c.getStyles().candle.priceMark, f = d.last, v = f.text;
      if (d.show && f.show && v.show) {
        var p = (n = (a = c.getSymbol()) === null || a === void 0 ? void 0 : a.pricePrecision) !== null && n !== void 0 ? n : ut.PRICE, g = l.getAxisComponent(), m = c.getDataList(), x = m[m.length - 1];
        if (C(x)) {
          var _ = x.close, E = x.open, y = f.compareRule === "current_open" ? E : (s = (o = m[m.length - 2]) === null || o === void 0 ? void 0 : o.close) !== null && s !== void 0 ? s : _, I = g.convertToNicePixel(_), b = "";
          _ > y ? b = f.upColor : _ < y ? b = f.downColor : b = f.noChangeColor;
          var w = 0, S = "left";
          g.isFromZero() ? (w = 0, S = "left") : (w = h.width, S = "right");
          var T = [], A = g.getRange(), D = g.displayValueToText(g.realValueToDisplayValue(g.valueToRealValue(_, { range: A }), { range: A }), p);
          D = c.getDecimalFold().format(c.getThousandsSeparator().format(D));
          var R = v.paddingLeft, P = v.paddingRight, k = v.paddingTop, L = v.paddingBottom, F = v.size, O = v.family, N = v.weight, K = R + Rt(D, F, N, O) + P, et = k + F + L;
          T.push({
            name: "text",
            attrs: {
              x: w,
              y: I,
              width: K,
              height: et,
              text: D,
              align: S,
              baseline: "middle"
            },
            styles: M(M({}, v), { backgroundColor: b })
          });
          var J = c.getInnerFormatter().formatExtendText, tt = F / 2, rt = I - tt - k, at = I + tt + L;
          f.extendTexts.forEach(function(Y, G) {
            var W = J({ type: "last_price", data: x, index: G });
            if (W.length > 0 && Y.show) {
              var Z = Y.size / 2, H = 0;
              Y.position === "above_price" ? (rt -= Y.paddingBottom + Z, H = rt, rt -= Z + Y.paddingTop) : (at += Y.paddingTop + Z, H = at, at += Z + Y.paddingBottom), K = Math.max(K, Y.paddingLeft + Rt(W, Y.size, Y.weight, Y.family) + Y.paddingRight), T.push({
                name: "text",
                attrs: {
                  x: w,
                  y: H,
                  width: K,
                  height: Y.paddingTop + Y.size + Y.paddingBottom,
                  text: W,
                  align: S,
                  baseline: "middle"
                },
                styles: M(M({}, Y), { backgroundColor: b })
              });
            }
          }), T.forEach(function(Y) {
            var G;
            Y.attrs.width = K, (G = r.createFigure(Y)) === null || G === void 0 || G.draw(t);
          });
        }
      }
    }, e;
  })(Et)
), bn = (
  /** @class */
  (function(i) {
    X(e, i);
    function e() {
      return i !== null && i.apply(this, arguments) || this;
    }
    return e.prototype.drawImp = function(t) {
      var r = this, a = this.getWidget(), n = a.getPane(), o = a.getBounding(), s = n.getChart().getChartStore(), l = s.getStyles().indicator, u = l.lastValueMark, h = u.text;
      if (u.show) {
        var c = a, d = c.getAxisComponent(), f = d.getRange(), v = s.getDataList(), p = s.getBarSpace(), g = v.length - 1, m = /* @__PURE__ */ new Set([d.id]), x = n.getDefaultYAxisId();
        n.isManualYAxis(d.id) && C(x) && m.add(x);
        var _ = s.getIndicatorsByPaneId(n.getId()).filter(function(b) {
          return m.has(b.yAxisId);
        }), E = s.getInnerFormatter(), y = s.getDecimalFold(), I = s.getThousandsSeparator();
        _.forEach(function(b) {
          var w, S = b.result, T = (w = S[g]) !== null && w !== void 0 ? w : {};
          if (C(T) && b.visible) {
            var A = b.precision;
            Ne(b, g, p, l, function(D, R) {
              var P, k = T[D.key];
              if (B(k)) {
                var L = d.convertToNicePixel(k), F = d.displayValueToText(d.realValueToDisplayValue(d.valueToRealValue(k, { range: f }), { range: f }), A);
                b.shouldFormatBigNumber && (F = E.formatBigNumber(F)), F = y.format(I.format(F));
                var O = 0, N = "left";
                d.isFromZero() ? (O = 0, N = "left") : (O = o.width, N = "right"), (P = r.createFigure({
                  name: "text",
                  attrs: {
                    x: O,
                    y: L,
                    text: F,
                    align: N,
                    baseline: "middle"
                  },
                  styles: M(M({}, h), { backgroundColor: R.color })
                })) === null || P === void 0 || P.draw(t);
              }
            });
          }
        });
      }
    }, e;
  })(Et)
), Yr = (
  /** @class */
  (function(i) {
    X(e, i);
    function e() {
      return i !== null && i.apply(this, arguments) || this;
    }
    return e.prototype.coordinateToPointTimestampDataIndexFlag = function() {
      return !1;
    }, e.prototype.drawDefaultFigures = function(t, r, a) {
      this.drawFigures(t, r, this.getDefaultFigures(r, a));
    }, e.prototype.getDefaultFigures = function(t, r) {
      var a, n = this.getWidget(), o = n.getPane(), s = o.getChart().getChartStore(), l = s.getClickOverlayInfo(), u = [];
      if (t.needDefaultYAxisFigure && t.id === ((a = l.overlay) === null || a === void 0 ? void 0 : a.id) && l.paneId === o.getId()) {
        var h = o.getYAxisComponentById(), c = n.getBounding(), d = Number.MAX_SAFE_INTEGER, f = Number.MIN_SAFE_INTEGER, v = h.isFromZero(), p = "left", g = 0;
        v ? (p = "left", g = 0) : (p = "right", g = c.width);
        var m = s.getDecimalFold(), x = s.getThousandsSeparator();
        r.forEach(function(_, E) {
          var y, I, b = t.points[E];
          if (B(b.value)) {
            d = Math.min(d, _.y), f = Math.max(f, _.y);
            var w = m.format(x.format(pt(b.value, (I = (y = s.getSymbol()) === null || y === void 0 ? void 0 : y.pricePrecision) !== null && I !== void 0 ? I : ut.PRICE)));
            u.push({ type: "text", attrs: { x: g, y: _.y, text: w, align: p, baseline: "middle" }, ignoreEvent: !0 });
          }
        }), r.length > 1 && u.unshift({ type: "rect", attrs: { x: 0, y: d, width: c.width, height: f - d }, ignoreEvent: !0 });
      }
      return u;
    }, e.prototype.getFigures = function(t, r) {
      var a, n, o = this.getWidget(), s = o.getPane(), l = s.getChart(), u = s.getYAxisComponentById(), h = l.getXAxisPane().getXAxisComponent(), c = o.getBounding();
      return (n = (a = t.createYAxisFigures) === null || a === void 0 ? void 0 : a.call(t, { chart: l, overlay: t, coordinates: r, bounding: c, xAxis: h, yAxis: u })) !== null && n !== void 0 ? n : [];
    }, e;
  })(Or)
), Wr = (
  /** @class */
  (function(i) {
    X(e, i);
    function e() {
      return i !== null && i.apply(this, arguments) || this;
    }
    return e.prototype.drawImp = function(t) {
      var r, a = this.getWidget(), n = a.getPane(), o = a.getPane().getChart().getChartStore(), s = o.getCrosshair();
      if ($(s.paneId) && this.compare(s, n.getId())) {
        var l = o.getStyles().crosshair;
        if (l.show) {
          var u = this.getDirectionStyles(l), h = u.text;
          if (u.show && h.show) {
            var c = a.getBounding(), d = "getAxisComponent" in a ? a.getAxisComponent() : n.getYAxisComponentById(), f = this.getText(s, o, d);
            t.font = Zt(h.size, h.weight, h.family), (r = this.createFigure({
              name: "text",
              attrs: this.getTextAttrs(f, t.measureText(f).width, s, c, d, h),
              styles: h
            })) === null || r === void 0 || r.draw(t);
          }
        }
      }
    }, e.prototype.compare = function(t, r) {
      return t.paneId === r;
    }, e.prototype.getDirectionStyles = function(t) {
      return t.horizontal;
    }, e.prototype.getText = function(t, r, a) {
      var n, o, s, l = a, u = a.convertFromPixel(t.y), h = 0, c = !1;
      if (l.isInCandle())
        h = (o = (n = r.getSymbol()) === null || n === void 0 ? void 0 : n.pricePrecision) !== null && o !== void 0 ? o : ut.PRICE;
      else {
        var d = l.id, f = this.getWidget().getPane();
        f.isManualYAxis(d) && (d = (s = f.getDefaultYAxisId()) !== null && s !== void 0 ? s : d);
        var v = r.getIndicatorsByPaneId(t.paneId).filter(function(m) {
          return m.yAxisId === d;
        });
        v.forEach(function(m) {
          h = Math.max(m.precision, h), c || (c = m.shouldFormatBigNumber);
        });
      }
      var p = l.getRange(), g = l.displayValueToText(l.realValueToDisplayValue(l.valueToRealValue(u, { range: p }), { range: p }), h);
      return c && (g = r.getInnerFormatter().formatBigNumber(g)), r.getDecimalFold().format(r.getThousandsSeparator().format(g));
    }, e.prototype.getTextAttrs = function(t, r, a, n, o, s) {
      var l = o, u = 0, h = "left";
      return l.isFromZero() ? (u = 0, h = "left") : (u = n.width, h = "right"), { x: u, y: a.y, text: t, align: h, baseline: "middle" };
    }, e;
  })(Et)
), wn = (
  /** @class */
  (function(i) {
    X(e, i);
    function e(t, r, a) {
      var n = i.call(this, t, r) || this;
      return n._yAxisView = new _n(n), n._candleLastPriceLabelView = new xn(n), n._indicatorLastValueView = new bn(n), n._overlayYAxisView = new Yr(n), n._crosshairHorizontalLabelView = new Wr(n), n._yAxis = a, n.setCursor("ns-resize"), n.addChild(n._overlayYAxisView), n;
    }
    return e.prototype.getAxisComponent = function() {
      return this._yAxis;
    }, e.prototype.getName = function() {
      return U.Y_AXIS;
    }, e.prototype.updateMain = function(t) {
      this._yAxisView.draw(t);
      var r = this.getPane(), a = r.isDefaultYAxis(this._yAxis.id) || r.isManualYAxis(this._yAxis.id);
      a && this.getAxisComponent().isInCandle() && this._candleLastPriceLabelView.draw(t), this._indicatorLastValueView.draw(t);
    }, e.prototype.updateOverlay = function(t) {
      this._overlayYAxisView.draw(t), this._crosshairHorizontalLabelView.draw(t);
    }, e;
  })(Xe)
), se = 8;
function ir() {
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
var zr = (
  /** @class */
  (function() {
    function i(e) {
      this.scrollZoomEnabled = !0, this._range = ir(), this._prevRange = ir(), this._ticks = [], this._autoCalcTickFlag = !0, this._parent = e;
    }
    return i.prototype.getParent = function() {
      return this._parent;
    }, i.prototype.buildTicks = function(e) {
      return this._autoCalcTickFlag && (this._range = this.createRangeImp()), this._prevRange.from !== this._range.from || this._prevRange.to !== this._range.to || e ? (this._prevRange = this._range, this._ticks = this.createTicksImp(), !0) : !1;
    }, i.prototype.getTicks = function() {
      return this._ticks;
    }, i.prototype.setRange = function(e) {
      this._autoCalcTickFlag = !1, this._range = e;
    }, i.prototype.getRange = function() {
      return this._range;
    }, i.prototype.setAutoCalcTickFlag = function(e) {
      this._autoCalcTickFlag = e;
    }, i.prototype.getAutoCalcTickFlag = function() {
      return this._autoCalcTickFlag;
    }, i;
  })()
), be = "yAxis_", we = (
  /** @class */
  (function(i) {
    X(e, i);
    function e(t, r) {
      var a = i.call(this, t) || this;
      a.id = "", a.paneId = "", a.reverse = !1, a.inside = !1, a.position = "right", a.gap = {
        top: 0.2,
        bottom: 0.1
      }, a.createRange = function(d) {
        return d.defaultRange;
      }, a.minSpan = function(d) {
        return qt(-d);
      }, a.valueToRealValue = function(d) {
        return d;
      }, a.realValueToDisplayValue = function(d) {
        return d;
      }, a.displayValueToRealValue = function(d) {
        return d;
      }, a.realValueToValue = function(d) {
        return d;
      }, a.displayValueToText = function(d, f) {
        return pt(d, f);
      };
      var n = r.minSpan, o = r.valueToRealValue, s = r.realValueToDisplayValue, l = r.displayValueToRealValue, u = r.realValueToValue, h = r.displayValueToText, c = he(r, ["minSpan", "valueToRealValue", "realValueToDisplayValue", "displayValueToRealValue", "realValueToValue", "displayValueToText"]);
      return nt(n) && (a.minSpan = n), nt(o) && (a.valueToRealValue = o), nt(s) && (a.realValueToDisplayValue = s), nt(l) && (a.displayValueToRealValue = l), nt(u) && (a.realValueToValue = u), nt(h) && (a.displayValueToText = h), a.override(c), a;
    }
    return e.prototype.override = function(t) {
      var r = t.id, a = t.name, n = t.gap, o = he(t, ["id", "name", "gap"]);
      C(r) && this.id.length === 0 && (this.id = r), !$(this.name) && $(a) && (this.name = a), ot(this.gap, n), ot(this, o);
    }, e.prototype._getIndicatorsByYAxisIds = function() {
      var t = this.getParent(), r = /* @__PURE__ */ new Set([this.id]);
      if (t.isManualYAxis(this.id)) {
        var a = t.getDefaultYAxisId();
        C(a) && r.add(a);
      }
      return t.getChart().getChartStore().getIndicatorsByPaneId(t.getId()).filter(function(n) {
        return r.has(n.yAxisId);
      });
    }, e.prototype._shouldUseCandleData = function() {
      var t = this.getParent();
      return this.isInCandle() && (t.isDefaultYAxis(this.id) || t.isManualYAxis(this.id));
    }, e.prototype.createRangeImp = function() {
      var t, r, a = this.getParent(), n = a.getChart(), o = n.getChartStore(), s = a.getId(), l = Number.MAX_SAFE_INTEGER, u = Number.MIN_SAFE_INTEGER, h = !1, c = Number.MAX_SAFE_INTEGER, d = Number.MIN_SAFE_INTEGER, f = Number.MAX_SAFE_INTEGER, v = this._getIndicatorsByYAxisIds();
      v.forEach(function(W) {
        h || (h = W.shouldOhlc), f = Math.min(f, W.precision), B(W.minValue) && (c = Math.min(c, W.minValue)), B(W.maxValue) && (d = Math.max(d, W.maxValue));
      });
      var p = 4, g = this.isInCandle();
      if (g) {
        var m = (r = (t = o.getSymbol()) === null || t === void 0 ? void 0 : t.pricePrecision) !== null && r !== void 0 ? r : ut.PRICE;
        f !== Number.MAX_SAFE_INTEGER ? p = Math.min(f, m) : p = m;
      } else
        f !== Number.MAX_SAFE_INTEGER && (p = f);
      var x = o.getVisibleRangeDataList(), _ = n.getStyles().candle, E = _.type === "area", y = _.area.value, I = this._shouldUseCandleData(), b = I && !E || !g && h;
      x.forEach(function(W) {
        var Z = W.dataIndex, H = W.data.current;
        if (C(H) && (b && (l = Math.min(l, H.low), u = Math.max(u, H.high)), I && E)) {
          var it = H[y];
          B(it) && (l = Math.min(l, it), u = Math.max(u, it));
        }
        v.forEach(function(j) {
          var Q, mt = j.result, xt = j.figures, ht = (Q = mt[Z]) !== null && Q !== void 0 ? Q : {};
          xt.forEach(function(bt) {
            var _t = ht[bt.key];
            B(_t) && (l = Math.min(l, _t), u = Math.max(u, _t));
          });
        });
      }), l !== Number.MAX_SAFE_INTEGER && u !== Number.MIN_SAFE_INTEGER ? (l = Math.min(c, l), u = Math.max(d, u)) : (l = 0, u = 10);
      var w = u - l, S = {
        from: l,
        to: u,
        range: w,
        realFrom: l,
        realTo: u,
        realRange: w,
        displayFrom: l,
        displayTo: u,
        displayRange: w
      }, T = this.createRange({
        chart: n,
        paneId: s,
        defaultRange: S
      }), A = T.realFrom, D = T.realTo, R = T.realRange, P = this.minSpan(p);
      if (A === D || R < P) {
        var k = c === A, L = d === D, F = se / 2;
        A = k ? A : L ? A - se * P : A - F * P, D = L ? D : k ? D + se * P : D + F * P;
      }
      var O = this.getBounding().height, N = this.gap, K = N.top, et = N.bottom, J = K;
      J >= 1 && (J = J / O);
      var tt = et;
      tt >= 1 && (tt = tt / O), R = D - A, A = A - R * tt, D = D + R * J;
      var rt = this.realValueToValue(A, { range: T }), at = this.realValueToValue(D, { range: T }), Y = this.realValueToDisplayValue(A, { range: T }), G = this.realValueToDisplayValue(D, { range: T });
      return {
        from: rt,
        to: at,
        range: at - rt,
        realFrom: A,
        realTo: D,
        realRange: D - A,
        displayFrom: Y,
        displayTo: G,
        displayRange: G - Y
      };
    }, e.prototype.isInCandle = function() {
      return this.getParent().getId() === q.CANDLE;
    }, e.prototype.isFromZero = function() {
      return this.position === "left" && this.inside || this.position === "right" && !this.inside;
    }, e.prototype.createTicksImp = function() {
      var t = this, r, a, n = this.getRange(), o = n.displayFrom, s = n.displayTo, l = n.displayRange, u = [];
      if (l >= 0) {
        var h = mi(l / se), c = yi(h), d = $e(Math.ceil(o / h) * h, c), f = $e(Math.floor(s / h) * h, c), v = 0, p = d;
        if (h !== 0)
          for (; p <= f; ) {
            var g = p.toFixed(c);
            u[v] = { text: g, coord: 0, value: g }, ++v, p += h;
          }
      }
      var m = this.getParent(), x = this.getBounding().height, _ = m.getChart().getChartStore(), E = [], y = this._getIndicatorsByYAxisIds(), I = _.getStyles(), b = 0, w = !1;
      this._shouldUseCandleData() ? b = (a = (r = _.getSymbol()) === null || r === void 0 ? void 0 : r.pricePrecision) !== null && a !== void 0 ? a : ut.PRICE : y.forEach(function(P) {
        b = Math.max(b, P.precision), w || (w = P.shouldFormatBigNumber);
      });
      var S = _.getInnerFormatter(), T = _.getThousandsSeparator(), A = _.getDecimalFold(), D = I.xAxis.tickText.size, R = NaN;
      return u.forEach(function(P) {
        var k = P.value, L = t.displayValueToText(+k, b), F = t.convertToPixel(t.realValueToValue(t.displayValueToRealValue(+k, { range: n }), { range: n }));
        w && (L = S.formatBigNumber(k)), L = A.format(T.format(L));
        var O = B(R);
        F > D && F < x - D && (O && Math.abs(R - F) > D * 2 || !O) && (E.push({ text: L, coord: F, value: k }), R = F);
      }), nt(this.createTicks) ? this.createTicks({
        range: this.getRange(),
        bounding: this.getBounding(),
        defaultTicks: E
      }) : E;
    }, e.prototype.getAutoSize = function() {
      var t, r, a = this.getParent(), n = a.getChart(), o = n.getChartStore(), s = o.getStyles(), l = s.yAxis, u = l.size;
      if (u !== "auto")
        return u;
      var h = 0;
      if (l.show && (l.axisLine.show && (h += l.axisLine.size), l.tickLine.show && (h += l.tickLine.length), l.tickText.show)) {
        var c = 0;
        this.getTicks().forEach(function(N) {
          c = Math.max(c, Rt(N.text, l.tickText.size, l.tickText.weight, l.tickText.family));
        }), h += l.tickText.marginStart + l.tickText.marginEnd + c;
      }
      var d = s.candle.priceMark, f = d.show && d.last.show && d.last.text.show, v = 0, p = s.crosshair, g = p.show && p.horizontal.show && p.horizontal.text.show, m = 0;
      if (f || g) {
        var x = (r = (t = o.getSymbol()) === null || t === void 0 ? void 0 : t.pricePrecision) !== null && r !== void 0 ? r : ut.PRICE, _ = this.getRange().displayTo;
        if (f) {
          var E = o.getDataList(), y = E[E.length - 1];
          if (C(y)) {
            var I = d.last.text, b = I.paddingLeft, w = I.paddingRight, S = I.size, T = I.family, A = I.weight;
            v = b + Rt(pt(y.close, x), S, A, T) + w;
            var D = o.getInnerFormatter().formatExtendText;
            d.last.extendTexts.forEach(function(N, K) {
              var et = D({ type: "last_price", data: y, index: K });
              et.length > 0 && N.show && (v = Math.max(v, N.paddingLeft + Rt(et, N.size, N.weight, N.family) + N.paddingRight));
            });
          }
        }
        if (g) {
          var R = this._getIndicatorsByYAxisIds(), P = 0, k = !1;
          R.forEach(function(N) {
            P = Math.max(N.precision, P), k || (k = N.shouldFormatBigNumber);
          });
          var L = 2;
          if (this._shouldUseCandleData()) {
            var F = s.indicator.lastValueMark;
            F.show && F.text.show ? L = Math.max(P, x) : L = x;
          } else
            L = P;
          var O = pt(_, L);
          k && (O = o.getInnerFormatter().formatBigNumber(O)), O = o.getDecimalFold().format(O), m += p.horizontal.text.paddingLeft + p.horizontal.text.paddingRight + p.horizontal.text.borderSize * 2 + Rt(O, p.horizontal.text.size, p.horizontal.text.weight, p.horizontal.text.family);
        }
      }
      return Math.max(h, v, m);
    }, e.prototype.getBounding = function() {
      var t, r;
      return (r = (t = this.getParent().getYAxisWidgetById(this.id)) === null || t === void 0 ? void 0 : t.getBounding()) !== null && r !== void 0 ? r : this.getParent().getMainWidget().getBounding();
    }, e.prototype.convertFromPixel = function(t) {
      var r = this.getBounding().height, a = this.getRange(), n = a.realFrom, o = a.realRange, s = this.reverse ? t / r : 1 - t / r, l = s * o + n;
      return this.realValueToValue(l, { range: a });
    }, e.prototype.convertToPixel = function(t) {
      var r = this.getRange(), a = this.valueToRealValue(t, { range: r }), n = this.getBounding().height, o = r.realFrom, s = r.realRange, l = (a - o) / s;
      return this.reverse ? Math.round(l * n) : Math.round((1 - l) * n);
    }, e.prototype.convertToNicePixel = function(t) {
      var r = this.getBounding().height, a = this.convertToPixel(t);
      return Math.round(Math.max(r * 0.05, Math.min(a, r * 0.98)));
    }, e.extend = function(t) {
      var r = (
        /** @class */
        (function(a) {
          X(n, a);
          function n(o) {
            return a.call(this, o, t) || this;
          }
          return n;
        })(e)
      );
      return r;
    }, e;
  })(zr)
), Cn = {
  name: "normal"
}, En = {
  name: "percentage",
  minSpan: function() {
    return Math.pow(10, -2);
  },
  displayValueToText: function(i) {
    return "".concat(pt(i, 2), "%");
  },
  valueToRealValue: function(i, e) {
    var t = e.range;
    return (i - t.from) / t.range * t.realRange + t.realFrom;
  },
  realValueToValue: function(i, e) {
    var t = e.range;
    return (i - t.realFrom) / t.realRange * t.range + t.from;
  },
  createRange: function(i) {
    var e = i.chart, t = i.defaultRange, r = e.getDataList(), a = e.getVisibleRange(), n = r[a.from];
    if (C(n)) {
      var o = t.from, s = t.to, l = t.range, u = (t.from - n.close) / n.close * 100, h = (t.to - n.close) / n.close * 100, c = h - u;
      return {
        from: o,
        to: s,
        range: l,
        realFrom: u,
        realTo: h,
        realRange: c,
        displayFrom: u,
        displayTo: h,
        displayRange: c
      };
    }
    return t;
  }
}, In = {
  name: "logarithm",
  minSpan: function(i) {
    return 0.05 * qt(-i);
  },
  valueToRealValue: function(i) {
    return i < 0 ? -Ot(Math.abs(i)) : Ot(i);
  },
  realValueToDisplayValue: function(i) {
    return i < 0 ? -qt(Math.abs(i)) : qt(i);
  },
  displayValueToRealValue: function(i) {
    return i < 0 ? -Ot(Math.abs(i)) : Ot(i);
  },
  realValueToValue: function(i) {
    return i < 0 ? -qt(Math.abs(i)) : qt(i);
  },
  createRange: function(i) {
    var e = i.defaultRange, t = e.from, r = e.to, a = e.range, n = t < 0 ? -Ot(Math.abs(t)) : Ot(t), o = r < 0 ? -Ot(Math.abs(r)) : Ot(r);
    return {
      from: t,
      to: r,
      range: a,
      realFrom: n,
      realTo: o,
      realRange: o - n,
      displayFrom: t,
      displayTo: r,
      displayRange: a
    };
  }
}, ar = {
  normal: we.extend(Cn),
  percentage: we.extend(En),
  logarithm: we.extend(In)
};
function Sn(i) {
  var e;
  return (e = ar[i]) !== null && e !== void 0 ? e : ar.normal;
}
var Xr = (
  /** @class */
  (function() {
    function i(e, t) {
      this._bounding = Ve(), this._chart = e, this._id = t, this._container = Vt("div", {
        width: "100%",
        margin: "0",
        padding: "0",
        position: "relative",
        overflow: "hidden",
        boxSizing: "border-box"
      });
    }
    return i.prototype.getContainer = function() {
      return this._container;
    }, i.prototype.getId = function() {
      return this._id;
    }, i.prototype.getChart = function() {
      return this._chart;
    }, i.prototype.getBounding = function() {
      return this._bounding;
    }, i.prototype.update = function(e) {
      this._bounding.height !== this._container.clientHeight && (this._container.style.height = "".concat(this._bounding.height, "px")), this.updateImp(e ?? 3, this._container, this._bounding);
    }, i;
  })()
), Hr = (
  /** @class */
  (function(i) {
    X(e, i);
    function e(t, r) {
      var a = i.call(this, t, r.id) || this;
      a._yAxisWidgets = /* @__PURE__ */ new Map(), a._yAxisComponents = /* @__PURE__ */ new Map(), a._manualYAxisIds = /* @__PURE__ */ new Set(), a._defaultYAxisId = null, a._yAxesBounding = {};
      var n = a.getContainer();
      return a._mainWidget = a.createMainWidget(n), a._options = r, a;
    }
    return e.prototype.setOptions = function(t) {
      return ot(this._options, t), B(t.height) && t.height > 0 && this.setBounding({ height: this._options.height }), this;
    }, e.prototype.setAxisCursor = function(t, r) {
      var a, n, o = null, s = "default";
      this.getId() === q.X_AXIS ? (o = this.getMainWidget().getContainer(), s = "ew-resize") : (o = (n = (a = this.getYAxisWidgetById(r)) === null || a === void 0 ? void 0 : a.getContainer()) !== null && n !== void 0 ? n : null, s = "ns-resize"), !(!C(o) || !ee(t)) && (t ? o.style.cursor = s : o.style.cursor = "default");
    }, e.prototype.createOrOverrideYAxis = function(t) {
      var r, a, n, o, s, l = M(M({}, t), { paneId: this.getId() }), u = l.id, h = (r = l.name) !== null && r !== void 0 ? r : "normal", c = (a = l.needWidget) !== null && a !== void 0 ? a : !0, d = this._yAxisComponents.get(u), f = !C(d) || C(l.name) && d.name !== l.name;
      if (f) {
        if ((n = this._yAxisWidgets.get(u)) === null || n === void 0 || n.destroy(), this._yAxisWidgets.delete(u), d = this.createYAxisComponent(h), d.id = u, d.paneId = this.getId(), this._yAxisComponents.set(u, d), (o = this._defaultYAxisId) !== null && o !== void 0 || (this._defaultYAxisId = u), c) {
          var v = this.createYAxisWidget(this.getContainer(), d);
          C(v) && this._yAxisWidgets.set(u, v);
        }
      } else if (ee(l.needWidget) && C(d)) {
        var v = this._yAxisWidgets.get(u);
        if (l.needWidget && !C(v)) {
          var p = this.createYAxisWidget(this.getContainer(), d);
          C(p) && this._yAxisWidgets.set(u, p);
        } else !l.needWidget && C(v) && (v.destroy(), this._yAxisWidgets.delete(u));
      }
      if (!C(d))
        throw new Error("create yAxis failed.");
      d.setAutoCalcTickFlag(!0), d.override(M(M({}, l), { name: h })), this.setAxisCursor(d.scrollZoomEnabled, u);
      var g = this.getBounding();
      return (s = this._yAxisWidgets.get(u)) === null || s === void 0 || s.setBounding({ height: g.height, top: g.top }), d;
    }, e.prototype.getOptions = function() {
      return this._options;
    }, e.prototype.getYAxisComponents = function() {
      return Array.from(this._yAxisComponents.values());
    }, e.prototype.getWidgetYAxisComponents = function() {
      var t = this;
      return Array.from(this._yAxisWidgets.keys()).map(function(r) {
        return t._yAxisComponents.get(r);
      });
    }, e.prototype.hasYAxisComponent = function(t) {
      return this._yAxisComponents.has(t);
    }, e.prototype.setManualYAxis = function(t, r) {
      r ? this._manualYAxisIds.add(t) : this._manualYAxisIds.delete(t);
    }, e.prototype.isManualYAxis = function(t) {
      return this._manualYAxisIds.has(t);
    }, e.prototype.removeYAxis = function(t) {
      var r = this, a, n = this._yAxisComponents.get(t);
      if (!C(n))
        return !1;
      this._yAxisComponents.delete(t), this._manualYAxisIds.delete(t), this._defaultYAxisId === t && (this._defaultYAxisId = (a = this._yAxisComponents.keys().next().value) !== null && a !== void 0 ? a : null);
      var o = this._yAxisWidgets.get(t);
      return C(o) && (o.destroy(), this._yAxisWidgets.delete(t)), this._yAxesBounding = Object.keys(this._yAxesBounding).reduce(function(s, l) {
        return l !== t && (s[l] = r._yAxesBounding[l]), s;
      }, {}), !0;
    }, e.prototype.getDefaultYAxisId = function() {
      return this._defaultYAxisId;
    }, e.prototype.isDefaultYAxis = function(t) {
      return this._defaultYAxisId === t;
    }, e.prototype.getYAxisComponentById = function(t) {
      var r = t ?? this.getDefaultYAxisId();
      return this._yAxisComponents.get(r);
    }, e.prototype.getYAxisWidgetById = function(t) {
      var r, a = t ?? this.getDefaultYAxisId();
      return C(a) && (r = this._yAxisWidgets.get(a)) !== null && r !== void 0 ? r : null;
    }, e.prototype.setYAxesBounding = function(t) {
      this._yAxesBounding = t;
    }, e.prototype.setBounding = function(t, r, a, n) {
      var o = this;
      ot(this.getBounding(), t);
      var s = {};
      C(t.height) && (s.height = t.height), C(t.top) && (s.top = t.top), this._mainWidget.setBounding(s);
      var l = C(r);
      return l && this._mainWidget.setBounding(r), this._yAxisWidgets.size > 0 && this._yAxisWidgets.forEach(function(u, h) {
        var c, d, f, v;
        if (u.setBounding(s), C(o._yAxesBounding[h])) {
          u.setBounding(o._yAxesBounding[h]);
          return;
        }
        var p = o.getYAxisComponentById(h);
        p.position === "left" ? C(a) && u.setBounding(M(M({}, a), { left: 0 })) : C(n) && (u.setBounding(n), l && u.setBounding({
          left: ((c = r.left) !== null && c !== void 0 ? c : 0) + ((d = r.width) !== null && d !== void 0 ? d : 0) + ((f = r.right) !== null && f !== void 0 ? f : 0) - ((v = n.width) !== null && v !== void 0 ? v : 0)
        }));
      }), this;
    }, e.prototype.getMainWidget = function() {
      return this._mainWidget;
    }, e.prototype.getYAxisWidget = function() {
      return this.getYAxisWidgetById();
    }, e.prototype.getYAxisWidgets = function() {
      return Array.from(this._yAxisWidgets.values());
    }, e.prototype.updateImp = function(t) {
      this._mainWidget.update(t), this._yAxisWidgets.forEach(function(r) {
        r.update(t);
      });
    }, e.prototype.destroy = function() {
      this._mainWidget.destroy(), this._yAxisWidgets.forEach(function(t) {
        t.destroy();
      });
    }, e.prototype.getImage = function(t) {
      var r = this.getBounding(), a = r.width, n = r.height, o = Vt("canvas", {
        width: "".concat(a, "px"),
        height: "".concat(n, "px"),
        boxSizing: "border-box"
      }), s = o.getContext("2d"), l = Wt(o);
      o.width = a * l, o.height = n * l, s.scale(l, l);
      var u = this._mainWidget.getBounding();
      return s.drawImage(this._mainWidget.getImage(t), u.left, 0, u.width, u.height), this._yAxisWidgets.forEach(function(h) {
        var c = h.getBounding();
        s.drawImage(h.getImage(t), c.left, 0, c.width, c.height);
      }), o;
    }, e.prototype.createYAxisComponent = function(t) {
      throw new Error("createYAxisComponent is not implemented.");
    }, e.prototype.createYAxisWidget = function(t, r) {
      return null;
    }, e;
  })(Xr)
), Ur = (
  /** @class */
  (function(i) {
    X(e, i);
    function e() {
      return i !== null && i.apply(this, arguments) || this;
    }
    return e.prototype.createYAxisComponent = function(t) {
      var r = Sn(t ?? "default");
      return new r(this);
    }, e.prototype.createMainWidget = function(t) {
      return new Lr(t, this);
    }, e.prototype.createYAxisWidget = function(t, r) {
      return new wn(t, this, r);
    }, e;
  })(Hr)
), Tn = (
  /** @class */
  (function(i) {
    X(e, i);
    function e() {
      return i !== null && i.apply(this, arguments) || this;
    }
    return e.prototype.createMainWidget = function(t) {
      return new yn(t, this);
    }, e;
  })(Ur)
), An = (
  /** @class */
  (function(i) {
    X(e, i);
    function e() {
      return i !== null && i.apply(this, arguments) || this;
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
    }, e.prototype.createTickLines = function(t, r, a) {
      var n = a.tickLine, o = a.axisLine.size;
      return t.map(function(s) {
        return {
          coordinates: [
            { x: s.coord, y: 0 },
            { x: s.coord, y: o + n.length }
          ]
        };
      });
    }, e.prototype.createTickTexts = function(t, r, a) {
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
  })(Nr)
), Mn = (
  /** @class */
  (function(i) {
    X(e, i);
    function e() {
      return i !== null && i.apply(this, arguments) || this;
    }
    return e.prototype.coordinateToPointTimestampDataIndexFlag = function() {
      return !0;
    }, e.prototype.coordinateToPointValueFlag = function() {
      return !1;
    }, e.prototype.getCompleteOverlays = function() {
      return this.getWidget().getPane().getChart().getChartStore().getOverlaysByPaneId();
    }, e.prototype.getProgressOverlay = function() {
      var t, r;
      return (r = (t = this.getWidget().getPane().getChart().getChartStore().getProgressOverlayInfo()) === null || t === void 0 ? void 0 : t.overlay) !== null && r !== void 0 ? r : null;
    }, e.prototype.getDefaultFigures = function(t, r) {
      var a, n = [], o = this.getWidget(), s = o.getPane(), l = s.getChart().getChartStore(), u = l.getClickOverlayInfo();
      if (t.needDefaultXAxisFigure && t.id === ((a = u.overlay) === null || a === void 0 ? void 0 : a.id)) {
        var h = Number.MAX_SAFE_INTEGER, c = Number.MIN_SAFE_INTEGER;
        r.forEach(function(d, f) {
          h = Math.min(h, d.x), c = Math.max(c, d.x);
          var v = t.points[f];
          if (B(v.timestamp)) {
            var p = l.getInnerFormatter().formatDate(v.timestamp, "YYYY-MM-DD HH:mm", "crosshair");
            n.push({ type: "text", attrs: { x: d.x, y: 0, text: p, align: "center" }, ignoreEvent: !0 });
          }
        }), r.length > 1 && n.unshift({ type: "rect", attrs: { x: h, y: 0, width: c - h, height: o.getBounding().height }, ignoreEvent: !0 });
      }
      return n;
    }, e.prototype.getFigures = function(t, r) {
      var a, n, o = this.getWidget(), s = o.getPane(), l = s.getChart(), u = s.getYAxisComponentById(), h = l.getXAxisPane().getXAxisComponent(), c = o.getBounding();
      return (n = (a = t.createXAxisFigures) === null || a === void 0 ? void 0 : a.call(t, { chart: l, overlay: t, coordinates: r, bounding: c, xAxis: h, yAxis: u })) !== null && n !== void 0 ? n : [];
    }, e;
  })(Yr)
), Pn = (
  /** @class */
  (function(i) {
    X(e, i);
    function e() {
      return i !== null && i.apply(this, arguments) || this;
    }
    return e.prototype.compare = function(t) {
      return C(t.timestamp);
    }, e.prototype.getDirectionStyles = function(t) {
      return t.vertical;
    }, e.prototype.getText = function(t, r) {
      var a, n, o = t.timestamp;
      return r.getInnerFormatter().formatDate(o, Vr[(n = (a = r.getPeriod()) === null || a === void 0 ? void 0 : a.type) !== null && n !== void 0 ? n : "day"], "crosshair");
    }, e.prototype.getTextAttrs = function(t, r, a, n, o, s) {
      var l = a.realX, u = 0, h = "center";
      return l - r / 2 - s.paddingLeft < 0 ? (u = 0, h = "left") : l + r / 2 + s.paddingRight > n.width ? (u = n.width, h = "right") : u = l, { x: u, y: 0, text: t, align: h, baseline: "top" };
    }, e;
  })(Wr)
), Dn = (
  /** @class */
  (function(i) {
    X(e, i);
    function e(t, r) {
      var a = i.call(this, t, r) || this;
      return a._xAxisView = new An(a), a._overlayXAxisView = new Mn(a), a._crosshairVerticalLabelView = new Pn(a), a.setCursor("ew-resize"), a.addChild(a._overlayXAxisView), a;
    }
    return e.prototype.getName = function() {
      return U.X_AXIS;
    }, e.prototype.updateMain = function(t) {
      this._xAxisView.draw(t);
    }, e.prototype.updateOverlay = function(t) {
      this._overlayXAxisView.draw(t), this._crosshairVerticalLabelView.draw(t);
    }, e;
  })(Xe)
), kn = (
  /** @class */
  (function(i) {
    X(e, i);
    function e(t, r) {
      var a = i.call(this, t) || this;
      return a.override(r), a;
    }
    return e.prototype.override = function(t) {
      var r = t.name, a = t.scrollZoomEnabled, n = t.createTicks;
      !$(this.name) && $(r) && (this.name = r), this.scrollZoomEnabled = a ?? this.scrollZoomEnabled, this.createTicks = n ?? this.createTicks;
    }, e.prototype.createRangeImp = function() {
      var t = this.getParent().getChart().getChartStore(), r = t.getVisibleRange(), a = r.realFrom, n = r.realTo, o = a, s = n, l = n - a + 1, u = {
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
      var t, r = this.getRange(), a = r.realFrom, n = r.realTo, o = r.from, s = this.getParent().getChart().getChartStore(), l = s.getInnerFormatter().formatDate, u = s.getPeriod(), h = [], c = s.getBarSpace().bar, d = s.getStyles().xAxis.tickText, f = Math.max(Rt("YYYY-MM-DD HH:mm:ss", d.size, d.weight, d.family), this.getBounding().width / se), v = Math.ceil(f / c);
      v % 2 !== 0 && (v += 1);
      for (var p = Math.max(0, Math.floor(a / v) * v), g = p; g < n; g += v)
        if (g >= o) {
          var m = s.dataIndexToTimestamp(g);
          B(m) && h.push({
            coord: this.convertToPixel(g),
            value: m,
            text: l(m, dn[(t = u?.type) !== null && t !== void 0 ? t : "day"], "xAxis")
          });
        }
      return nt(this.createTicks) ? this.createTicks({
        range: this.getRange(),
        bounding: this.getBounding(),
        defaultTicks: h
      }) : h;
    }, e.prototype.getAutoSize = function() {
      var t = this.getParent().getChart().getStyles(), r = t.xAxis, a = r.size;
      if (a !== "auto")
        return a;
      var n = t.crosshair, o = 0;
      r.show && (r.axisLine.show && (o += r.axisLine.size), r.tickLine.show && (o += r.tickLine.length), r.tickText.show && (o += r.tickText.marginStart + r.tickText.marginEnd + r.tickText.size));
      var s = 0;
      return n.show && n.vertical.show && n.vertical.text.show && (s += n.vertical.text.paddingTop + n.vertical.text.paddingBottom + n.vertical.text.borderSize * 2 + n.vertical.text.size), Math.max(o, s);
    }, e.prototype.getBounding = function() {
      return this.getParent().getMainWidget().getBounding();
    }, e.prototype.convertTimestampFromPixel = function(t) {
      var r = this.getParent().getChart().getChartStore(), a = r.coordinateToDataIndex(t);
      return r.dataIndexToTimestamp(a);
    }, e.prototype.convertTimestampToPixel = function(t) {
      var r = this.getParent().getChart().getChartStore(), a = r.timestampToDataIndex(t);
      return r.dataIndexToCoordinate(a);
    }, e.prototype.convertFromPixel = function(t) {
      return this.getParent().getChart().getChartStore().coordinateToDataIndex(t);
    }, e.prototype.convertToPixel = function(t) {
      return this.getParent().getChart().getChartStore().dataIndexToCoordinate(t);
    }, e.extend = function(t) {
      var r = (
        /** @class */
        (function(a) {
          X(n, a);
          function n(o) {
            return a.call(this, o, t) || this;
          }
          return n;
        })(e)
      );
      return r;
    }, e;
  })(zr)
), Rn = {
  name: "normal"
}, nr = {
  normal: kn.extend(Rn)
};
function Fn(i) {
  var e;
  return (e = nr[i]) !== null && e !== void 0 ? e : nr.normal;
}
var Bn = (
  /** @class */
  (function(i) {
    X(e, i);
    function e(t, r) {
      var a = i.call(this, t, r) || this;
      return a.overrideXAxis({ name: "normal", scrollZoomEnabled: !0 }), a;
    }
    return e.prototype.setOptions = function(t) {
      return i.prototype.setOptions.call(this, t);
    }, e.prototype.overrideXAxis = function(t) {
      var r = t.name;
      return (!C(this._xAxis) || C(r) && this._xAxis.name !== r) && (this._xAxis = this.createXAxisComponent(r ?? "normal")), this._xAxis.override(t), this.setAxisCursor(this._xAxis.scrollZoomEnabled), this;
    }, e.prototype.getXAxisComponent = function() {
      return this._xAxis;
    }, e.prototype.createXAxisComponent = function(t) {
      var r = Fn(t);
      return new r(this);
    }, e.prototype.createMainWidget = function(t) {
      return new Dn(t, this);
    }, e;
  })(Hr)
);
function On(i, e) {
  var t = 0;
  return function() {
    var r = Date.now();
    r - t > e && (i.apply(this, arguments), t = r);
  };
}
var Ln = (
  /** @class */
  (function(i) {
    X(e, i);
    function e(t, r) {
      var a = i.call(this, t, r) || this;
      return a._dragFlag = !1, a._dragStartY = 0, a._topPaneHeight = 0, a._bottomPaneHeight = 0, a._topPane = null, a._bottomPane = null, a._pressedMouseMoveEvent = On(a._pressedTouchMouseMoveEvent, 20), a.registerEvent("touchStartEvent", a._mouseDownEvent.bind(a)).registerEvent("touchMoveEvent", a._pressedMouseMoveEvent.bind(a)).registerEvent("touchEndEvent", a._mouseUpEvent.bind(a)).registerEvent("mouseDownEvent", a._mouseDownEvent.bind(a)).registerEvent("mouseUpEvent", a._mouseUpEvent.bind(a)).registerEvent("pressedMouseMoveEvent", a._pressedMouseMoveEvent.bind(a)).registerEvent("mouseEnterEvent", a._mouseEnterEvent.bind(a)).registerEvent("mouseLeaveEvent", a._mouseLeaveEvent.bind(a)), a;
    }
    return e.prototype.getName = function() {
      return U.SEPARATOR;
    }, e.prototype._dragEnabled = function(t, r) {
      return t.getOptions().state === "normal" && r.getOptions().state === "normal" && r.getOptions().dragEnabled;
    }, e.prototype._findAdjustablePane = function(t, r) {
      for (var a = this.getPane().getChart().getDrawPanes(), n = t; n >= 0 && n < a.length; n += r) {
        var o = a[n];
        if (o.getId() !== q.X_AXIS && o.getOptions().state === "normal")
          return o;
      }
      return null;
    }, e.prototype._findDragPanes = function() {
      var t = this.getPane(), r = t.getChart().getDrawPanes(), a = r.indexOf(t.getTopPane()), n = r.indexOf(t.getBottomPane());
      if (a === -1 || n === -1)
        return null;
      var o = this._findAdjustablePane(a, -1), s = this._findAdjustablePane(n, 1);
      return C(o) && C(s) && this._dragEnabled(o, s) ? { topPane: o, bottomPane: s } : null;
    }, e.prototype._mouseDownEvent = function(t) {
      var r = this._findDragPanes();
      return C(r) ? (this._topPane = r.topPane, this._bottomPane = r.bottomPane, this._dragFlag = !0, this._dragStartY = t.pageY, this._topPaneHeight = this._topPane.getBounding().height, this._bottomPaneHeight = this._bottomPane.getBounding().height, !0) : (this._topPane = null, this._bottomPane = null, !1);
    }, e.prototype._mouseUpEvent = function() {
      return this._dragFlag = !1, this._topPane = null, this._bottomPane = null, this._topPaneHeight = 0, this._bottomPaneHeight = 0, this._mouseLeaveEvent();
    }, e.prototype._pressedTouchMouseMoveEvent = function(t) {
      var r = t.pageY - this._dragStartY, a = r < 0;
      if (C(this._topPane) && C(this._bottomPane) && this._dragEnabled(this._topPane, this._bottomPane)) {
        var n = null, o = null, s = 0, l = 0;
        a ? (n = this._topPane, o = this._bottomPane, s = this._topPaneHeight, l = this._bottomPaneHeight) : (n = this._bottomPane, o = this._topPane, s = this._bottomPaneHeight, l = this._topPaneHeight);
        var u = n.getOptions().minHeight;
        if (s > u) {
          var h = Math.max(s - Math.abs(r), u), c = s - h;
          n.setBounding({ height: h });
          var d = l + c;
          o.setBounding({ height: d }), n.setOptions({ height: h }), o.setOptions({ height: d });
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
        var r = this.getPane().getChart(), a = r.getStyles().separator;
        return this.getContainer().style.background = a.activeBackgroundColor, !0;
      }
      return !1;
    }, e.prototype._mouseLeaveEvent = function() {
      return this._dragFlag ? !1 : (this.getContainer().style.background = "transparent", !0);
    }, e.prototype.createContainer = function() {
      return Vt("div", {
        width: "100%",
        height: "".concat(le, "px"),
        margin: "0",
        padding: "0",
        position: "absolute",
        top: "-3px",
        zIndex: "20",
        boxSizing: "border-box",
        cursor: "ns-resize"
      });
    }, e.prototype.updateImp = function(t, r, a) {
      if (a === 4 || a === 2) {
        var n = this.getPane().getChart().getStyles().separator;
        t.style.top = "".concat(-Math.floor((le - n.size) / 2), "px"), t.style.height = "".concat(le, "px");
      }
    }, e;
  })(Ar)
), Vn = (
  /** @class */
  (function(i) {
    X(e, i);
    function e(t, r, a, n) {
      var o = i.call(this, t, r) || this;
      return o.getContainer().style.overflow = "", o._topPane = a, o._bottomPane = n, o._separatorWidget = new Ln(o.getContainer(), o), o;
    }
    return e.prototype.setBounding = function(t) {
      return ot(this.getBounding(), t), this;
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
      var r = this.getBounding(), a = r.width, n = r.height, o = this.getChart().getStyles().separator, s = Vt("canvas", {
        width: "".concat(a, "px"),
        height: "".concat(n, "px"),
        boxSizing: "border-box"
      }), l = s.getContext("2d"), u = Wt(s);
      return s.width = a * u, s.height = n * u, l.scale(u, u), l.fillStyle = o.color, l.fillRect(0, 0, a, n), s;
    }, e.prototype.updateImp = function(t, r, a) {
      if (t === 4 || t === 2) {
        var n = this.getChart().getStyles().separator;
        r.style.backgroundColor = n.color, r.style.height = "".concat(a.height, "px"), r.style.marginLeft = "".concat(a.left, "px"), r.style.width = "".concat(a.width, "px"), this._separatorWidget.update(t);
      }
    }, e;
  })(Xr)
);
function or() {
  return typeof window > "u" ? !1 : window.navigator.userAgent.toLowerCase().includes("firefox");
}
function Ce() {
  return typeof window > "u" ? !1 : /iPhone|iPad|iPod|iOS/.test(window.navigator.userAgent);
}
function Nn() {
  return /Mac|iPhone|iPad|iPod|iOS/.test(window.navigator.userAgent);
}
var pe = {
  ResetClick: 500,
  LongTap: 500,
  PreventFiresTouchEvents: 500
}, Kt = {
  CancelClick: 5,
  CancelTap: 5,
  DoubleClick: 5,
  DoubleTap: 30
}, oe = {
  Left: 0,
  Middle: 1,
  Right: 2
}, Yn = 10, Wn = (
  /** @class */
  (function() {
    function i(e, t, r) {
      var a = this;
      this._clickCount = 0, this._clickTimeoutId = null, this._clickCoordinate = { x: Number.NEGATIVE_INFINITY, y: Number.POSITIVE_INFINITY }, this._tapCount = 0, this._tapTimeoutId = null, this._tapCoordinate = { x: Number.NEGATIVE_INFINITY, y: Number.POSITIVE_INFINITY }, this._longTapTimeoutId = null, this._longTapActive = !1, this._mouseMoveStartCoordinate = null, this._touchMoveStartCoordinate = null, this._touchMoveExceededManhattanDistance = !1, this._cancelClick = !1, this._cancelTap = !1, this._unsubscribeOutsideMouseEvents = null, this._unsubscribeOutsideTouchEvents = null, this._unsubscribeMobileSafariEvents = null, this._unsubscribeMousemove = null, this._unsubscribeMouseWheel = null, this._unsubscribeContextMenu = null, this._unsubscribeRootMouseEvents = null, this._unsubscribeRootTouchEvents = null, this._startPinchMiddleCoordinate = null, this._startPinchDistance = 0, this._pinchPrevented = !1, this._preventTouchDragProcess = !1, this._mousePressed = !1, this._lastTouchEventTimeStamp = 0, this._activeTouchId = null, this._acceptMouseLeave = !Ce(), this._onFirefoxOutsideMouseUp = function(n) {
        a._mouseUpHandler(n);
      }, this._onMobileSafariDoubleClick = function(n) {
        if (a._firesTouchEvents(n)) {
          if (++a._tapCount, a._tapTimeoutId !== null && a._tapCount > 1) {
            var o = a._mouseTouchMoveWithDownInfo(a._getCoordinate(n), a._tapCoordinate).manhattanDistance;
            o < Kt.DoubleTap && !a._cancelTap && a._processEvent(a._makeCompatEvent(n), a._handler.doubleTapEvent), a._resetTapTimeout();
          }
        } else if (++a._clickCount, a._clickTimeoutId !== null && a._clickCount > 1) {
          var o = a._mouseTouchMoveWithDownInfo(a._getCoordinate(n), a._clickCoordinate).manhattanDistance;
          o < Kt.DoubleClick && !a._cancelClick && a._processEvent(a._makeCompatEvent(n), a._handler.mouseDoubleClickEvent), a._resetClickTimeout();
        }
      }, this._target = e, this._handler = t, this._options = r, this._init();
    }
    return i.prototype.destroy = function() {
      this._unsubscribeOutsideMouseEvents !== null && (this._unsubscribeOutsideMouseEvents(), this._unsubscribeOutsideMouseEvents = null), this._unsubscribeOutsideTouchEvents !== null && (this._unsubscribeOutsideTouchEvents(), this._unsubscribeOutsideTouchEvents = null), this._unsubscribeMousemove !== null && (this._unsubscribeMousemove(), this._unsubscribeMousemove = null), this._unsubscribeMouseWheel !== null && (this._unsubscribeMouseWheel(), this._unsubscribeMouseWheel = null), this._unsubscribeContextMenu !== null && (this._unsubscribeContextMenu(), this._unsubscribeContextMenu = null), this._unsubscribeRootMouseEvents !== null && (this._unsubscribeRootMouseEvents(), this._unsubscribeRootMouseEvents = null), this._unsubscribeRootTouchEvents !== null && (this._unsubscribeRootTouchEvents(), this._unsubscribeRootTouchEvents = null), this._unsubscribeMobileSafariEvents !== null && (this._unsubscribeMobileSafariEvents(), this._unsubscribeMobileSafariEvents = null), this._clearLongTapTimeout(), this._resetClickTimeout();
    }, i.prototype._mouseEnterHandler = function(e) {
      var t = this, r, a, n;
      (r = this._unsubscribeMousemove) === null || r === void 0 || r.call(this), (a = this._unsubscribeMouseWheel) === null || a === void 0 || a.call(this), (n = this._unsubscribeContextMenu) === null || n === void 0 || n.call(this);
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
    }, i.prototype._resetClickTimeout = function() {
      this._clickTimeoutId !== null && clearTimeout(this._clickTimeoutId), this._clickCount = 0, this._clickTimeoutId = null, this._clickCoordinate = { x: Number.NEGATIVE_INFINITY, y: Number.POSITIVE_INFINITY };
    }, i.prototype._resetTapTimeout = function() {
      this._tapTimeoutId !== null && clearTimeout(this._tapTimeoutId), this._tapCount = 0, this._tapTimeoutId = null, this._tapCoordinate = { x: Number.NEGATIVE_INFINITY, y: Number.POSITIVE_INFINITY };
    }, i.prototype._mouseMoveHandler = function(e) {
      this._mousePressed || this._touchMoveStartCoordinate !== null || this._firesTouchEvents(e) || (this._processEvent(this._makeCompatEvent(e), this._handler.mouseMoveEvent), this._acceptMouseLeave = !0);
    }, i.prototype._mouseWheelHandler = function(e) {
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
          var r = Math.sign(t) * Math.min(1, Math.abs(t));
          this._handler.mouseWheelVertEvent(this._makeCompatEvent(e), r);
        }
      }
    }, i.prototype._contextMenuHandler = function(e) {
      this._preventDefault(e);
    }, i.prototype._touchMoveHandler = function(e) {
      var t = this._touchWithId(e.changedTouches, this._activeTouchId);
      if (t !== null && (this._lastTouchEventTimeStamp = this._eventTimeStamp(e), this._startPinchMiddleCoordinate === null && !this._preventTouchDragProcess)) {
        this._pinchPrevented = !0;
        var r = this._mouseTouchMoveWithDownInfo(this._getCoordinate(t), this._touchMoveStartCoordinate), a = r.xOffset, n = r.yOffset, o = r.manhattanDistance;
        if (!(!this._touchMoveExceededManhattanDistance && o < Kt.CancelTap)) {
          if (!this._touchMoveExceededManhattanDistance) {
            var s = a * 0.5, l = n >= s && !this._options.treatVertDragAsPageScroll(), u = s > n && !this._options.treatHorzDragAsPageScroll();
            !l && !u && (this._preventTouchDragProcess = !0), this._touchMoveExceededManhattanDistance = !0, this._cancelTap = !0, this._clearLongTapTimeout(), this._resetTapTimeout();
          }
          this._preventTouchDragProcess || this._processEvent(this._makeCompatEvent(e, t), this._handler.touchMoveEvent);
        }
      }
    }, i.prototype._mouseMoveWithDownHandler = function(e) {
      if (e.button === oe.Left) {
        var t = this._mouseTouchMoveWithDownInfo(this._getCoordinate(e), this._mouseMoveStartCoordinate), r = t.manhattanDistance;
        r >= Kt.CancelClick && (this._cancelClick = !0, this._resetClickTimeout()), this._cancelClick && this._processEvent(this._makeCompatEvent(e), this._handler.pressedMouseMoveEvent);
      }
    }, i.prototype._mouseTouchMoveWithDownInfo = function(e, t) {
      var r = Math.abs(t.x - e.x), a = Math.abs(t.y - e.y), n = r + a;
      return { xOffset: r, yOffset: a, manhattanDistance: n };
    }, i.prototype._touchEndHandler = function(e) {
      var t = this._touchWithId(e.changedTouches, this._activeTouchId);
      if (t === null && e.touches.length === 0 && (t = e.changedTouches[0]), t !== null) {
        this._activeTouchId = null, this._lastTouchEventTimeStamp = this._eventTimeStamp(e), this._clearLongTapTimeout(), this._touchMoveStartCoordinate = null, this._unsubscribeRootTouchEvents !== null && (this._unsubscribeRootTouchEvents(), this._unsubscribeRootTouchEvents = null);
        var r = this._makeCompatEvent(e, t);
        if (this._processEvent(r, this._handler.touchEndEvent), ++this._tapCount, this._tapTimeoutId !== null && this._tapCount > 1) {
          var a = this._mouseTouchMoveWithDownInfo(this._getCoordinate(t), this._tapCoordinate).manhattanDistance;
          a < Kt.DoubleTap && !this._cancelTap && this._processEvent(r, this._handler.doubleTapEvent), this._resetTapTimeout();
        } else
          this._cancelTap || (this._processEvent(r, this._handler.tapEvent), C(this._handler.tapEvent) && this._preventDefault(e));
        this._tapCount === 0 && this._preventDefault(e), e.touches.length === 0 && this._longTapActive && (this._longTapActive = !1, this._preventDefault(e));
      }
    }, i.prototype._mouseUpHandler = function(e) {
      if (e.button === oe.Left) {
        var t = this._makeCompatEvent(e);
        if (this._mouseMoveStartCoordinate = null, this._mousePressed = !1, this._unsubscribeRootMouseEvents !== null && (this._unsubscribeRootMouseEvents(), this._unsubscribeRootMouseEvents = null), or()) {
          var r = this._target.ownerDocument.documentElement;
          r.removeEventListener("mouseleave", this._onFirefoxOutsideMouseUp);
        }
        if (!this._firesTouchEvents(e))
          if (this._processEvent(t, this._handler.mouseUpEvent), ++this._clickCount, this._clickTimeoutId !== null && this._clickCount > 1) {
            var a = this._mouseTouchMoveWithDownInfo(this._getCoordinate(e), this._clickCoordinate).manhattanDistance;
            a < Kt.DoubleClick && !this._cancelClick && this._processEvent(t, this._handler.mouseDoubleClickEvent), this._resetClickTimeout();
          } else
            this._cancelClick || this._processEvent(t, this._handler.mouseClickEvent);
      }
    }, i.prototype._clearLongTapTimeout = function() {
      this._longTapTimeoutId !== null && (clearTimeout(this._longTapTimeoutId), this._longTapTimeoutId = null);
    }, i.prototype._touchStartHandler = function(e) {
      if (this._activeTouchId === null) {
        var t = e.changedTouches[0];
        this._activeTouchId = t.identifier, this._lastTouchEventTimeStamp = this._eventTimeStamp(e);
        var r = this._target.ownerDocument.documentElement;
        this._cancelTap = !1, this._touchMoveExceededManhattanDistance = !1, this._preventTouchDragProcess = !1, this._touchMoveStartCoordinate = this._getCoordinate(t), this._unsubscribeRootTouchEvents !== null && (this._unsubscribeRootTouchEvents(), this._unsubscribeRootTouchEvents = null);
        {
          var a = this._touchMoveHandler.bind(this), n = this._touchEndHandler.bind(this);
          this._unsubscribeRootTouchEvents = function() {
            r.removeEventListener("touchmove", a), r.removeEventListener("touchend", n);
          }, r.addEventListener("touchmove", a, { passive: !1 }), r.addEventListener("touchend", n, { passive: !1 }), this._clearLongTapTimeout(), this._longTapTimeoutId = setTimeout(this._longTapHandler.bind(this, e), pe.LongTap);
        }
        this._processEvent(this._makeCompatEvent(e, t), this._handler.touchStartEvent), this._tapTimeoutId === null && (this._tapCount = 0, this._tapTimeoutId = setTimeout(this._resetTapTimeout.bind(this), pe.ResetClick), this._tapCoordinate = this._getCoordinate(t));
      }
    }, i.prototype._mouseDownHandler = function(e) {
      if (e.button === oe.Right) {
        this._preventDefault(e), this._processEvent(this._makeCompatEvent(e), this._handler.mouseRightClickEvent);
        return;
      }
      if (e.button === oe.Left) {
        var t = this._target.ownerDocument.documentElement;
        or() && t.addEventListener("mouseleave", this._onFirefoxOutsideMouseUp), this._cancelClick = !1, this._mouseMoveStartCoordinate = this._getCoordinate(e), this._unsubscribeRootMouseEvents !== null && (this._unsubscribeRootMouseEvents(), this._unsubscribeRootMouseEvents = null);
        {
          var r = this._mouseMoveWithDownHandler.bind(this), a = this._mouseUpHandler.bind(this);
          this._unsubscribeRootMouseEvents = function() {
            t.removeEventListener("mousemove", r), t.removeEventListener("mouseup", a);
          }, t.addEventListener("mousemove", r), t.addEventListener("mouseup", a);
        }
        this._mousePressed = !0, !this._firesTouchEvents(e) && (this._processEvent(this._makeCompatEvent(e), this._handler.mouseDownEvent), this._clickTimeoutId === null && (this._clickCount = 0, this._clickTimeoutId = setTimeout(this._resetClickTimeout.bind(this), pe.ResetClick), this._clickCoordinate = this._getCoordinate(e)));
      }
    }, i.prototype._init = function() {
      var e = this;
      this._target.addEventListener("mouseenter", this._mouseEnterHandler.bind(this)), this._target.addEventListener("touchcancel", this._clearLongTapTimeout.bind(this));
      {
        var t = this._target.ownerDocument, r = function(a) {
          e._handler.mouseDownOutsideEvent != null && (a.composed && e._target.contains(a.composedPath()[0]) || a.target !== null && e._target.contains(a.target) || e._handler.mouseDownOutsideEvent({ x: 0, y: 0, pageX: 0, pageY: 0 }));
        };
        this._unsubscribeOutsideTouchEvents = function() {
          t.removeEventListener("touchstart", r);
        }, this._unsubscribeOutsideMouseEvents = function() {
          t.removeEventListener("mousedown", r);
        }, t.addEventListener("mousedown", r), t.addEventListener("touchstart", r, { passive: !0 });
      }
      Ce() && (this._unsubscribeMobileSafariEvents = function() {
        e._target.removeEventListener("dblclick", e._onMobileSafariDoubleClick);
      }, this._target.addEventListener("dblclick", this._onMobileSafariDoubleClick)), this._target.addEventListener("mouseleave", this._mouseLeaveHandler.bind(this)), this._target.addEventListener("touchstart", this._touchStartHandler.bind(this), { passive: !0 }), this._target.addEventListener("mousedown", function(a) {
        if (a.button === oe.Middle)
          return a.preventDefault(), !1;
      }), this._target.addEventListener("mousedown", this._mouseDownHandler.bind(this)), this._initPinch(), this._target.addEventListener("touchmove", function() {
      }, { passive: !1 });
    }, i.prototype._initPinch = function() {
      var e = this;
      !C(this._handler.pinchStartEvent) && !C(this._handler.pinchEvent) && !C(this._handler.pinchEndEvent) || (this._target.addEventListener("touchstart", function(t) {
        e._checkPinchState(t.touches);
      }, { passive: !0 }), this._target.addEventListener("touchmove", function(t) {
        if (!(t.touches.length !== 2 || e._startPinchMiddleCoordinate === null) && C(e._handler.pinchEvent)) {
          var r = e._getTouchDistance(t.touches[0], t.touches[1]), a = r / e._startPinchDistance;
          e._handler.pinchEvent(M(M({}, e._startPinchMiddleCoordinate), { pageX: 0, pageY: 0 }), a), e._preventDefault(t);
        }
      }, { passive: !1 }), this._target.addEventListener("touchend", function(t) {
        e._checkPinchState(t.touches);
      }));
    }, i.prototype._checkPinchState = function(e) {
      e.length === 1 && (this._pinchPrevented = !1), e.length !== 2 || this._pinchPrevented || this._longTapActive ? this._stopPinch() : this._startPinch(e);
    }, i.prototype._startPinch = function(e) {
      var t = this._target.getBoundingClientRect();
      this._startPinchMiddleCoordinate = {
        x: (e[0].clientX - t.left + (e[1].clientX - t.left)) / 2,
        y: (e[0].clientY - t.top + (e[1].clientY - t.top)) / 2
      }, this._startPinchDistance = this._getTouchDistance(e[0], e[1]), C(this._handler.pinchStartEvent) && this._handler.pinchStartEvent({ x: 0, y: 0, pageX: 0, pageY: 0 }), this._clearLongTapTimeout();
    }, i.prototype._stopPinch = function() {
      this._startPinchMiddleCoordinate !== null && (this._startPinchMiddleCoordinate = null, C(this._handler.pinchEndEvent) && this._handler.pinchEndEvent({ x: 0, y: 0, pageX: 0, pageY: 0 }));
    }, i.prototype._mouseLeaveHandler = function(e) {
      var t, r, a;
      (t = this._unsubscribeMousemove) === null || t === void 0 || t.call(this), (r = this._unsubscribeMouseWheel) === null || r === void 0 || r.call(this), (a = this._unsubscribeContextMenu) === null || a === void 0 || a.call(this), !this._firesTouchEvents(e) && this._acceptMouseLeave && (this._processEvent(this._makeCompatEvent(e), this._handler.mouseLeaveEvent), this._acceptMouseLeave = !Ce());
    }, i.prototype._longTapHandler = function(e) {
      var t = this._touchWithId(e.touches, this._activeTouchId);
      t !== null && (this._processEvent(this._makeCompatEvent(e, t), this._handler.longTapEvent), this._cancelTap = !0, this._longTapActive = !0);
    }, i.prototype._firesTouchEvents = function(e) {
      var t;
      return C((t = e.sourceCapabilities) === null || t === void 0 ? void 0 : t.firesTouchEvents) ? e.sourceCapabilities.firesTouchEvents : this._eventTimeStamp(e) < this._lastTouchEventTimeStamp + pe.PreventFiresTouchEvents;
    }, i.prototype._processEvent = function(e, t) {
      t?.call(this._handler, e);
    }, i.prototype._makeCompatEvent = function(e, t) {
      var r = this, a = t ?? e, n = this._target.getBoundingClientRect();
      return {
        x: a.clientX - n.left,
        y: a.clientY - n.top,
        pageX: a.pageX,
        pageY: a.pageY,
        isTouch: !e.type.startsWith("mouse") && e.type !== "contextmenu" && e.type !== "click" && e.type !== "wheel",
        preventDefault: function() {
          e.type !== "touchstart" && r._preventDefault(e);
        }
      };
    }, i.prototype._getTouchDistance = function(e, t) {
      var r = e.clientX - t.clientX, a = e.clientY - t.clientY;
      return Math.sqrt(r * r + a * a);
    }, i.prototype._preventDefault = function(e) {
      e.cancelable && e.preventDefault();
    }, i.prototype._getCoordinate = function(e) {
      return {
        x: e.pageX,
        y: e.pageY
      };
    }, i.prototype._eventTimeStamp = function(e) {
      var t;
      return (t = e.timeStamp) !== null && t !== void 0 ? t : performance.now();
    }, i.prototype._touchWithId = function(e, t) {
      for (var r = 0; r < e.length; ++r)
        if (e[r].identifier === t)
          return e[r];
      return null;
    }, i;
  })()
), sr = {
  name: "scrollLeft",
  keys: "Shift+ArrowLeft",
  action: function(i) {
    var e = i.chart;
    e.scrollByDistance(-3 * e.getBarSpace().bar);
  }
}, lr = {
  name: "scrollRight",
  keys: "Shift+ArrowRight",
  action: function(i) {
    var e = i.chart;
    e.scrollByDistance(3 * e.getBarSpace().bar);
  }
}, ur = {
  name: "zoomIn",
  keys: ["Shift+Equal", "Shift+NumpadAdd"],
  action: function(i) {
    var e = i.chart;
    e.zoomAtCoordinate(1.05);
  }
}, hr = {
  name: "zoomOut",
  keys: ["Shift+Minus", "Shift+NumpadSubtract"],
  action: function(i) {
    var e = i.chart;
    e.zoomAtCoordinate(0.95);
  }
}, Jt, Gr = (Jt = {}, Jt[sr.name] = sr, Jt[lr.name] = lr, Jt[ur.name] = ur, Jt[hr.name] = hr, Jt);
function zn(i) {
  var e;
  return (e = Gr[i]) !== null && e !== void 0 ? e : null;
}
function Xn() {
  return Object.keys(Gr);
}
var Hn = {
  command: "meta",
  cmd: "meta",
  control: "ctrl",
  option: "alt",
  mod: Nn() ? "meta" : "ctrl"
}, cr = {
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
}, Ee = ["ctrl", "alt", "shift", "meta"], Un = (
  /** @class */
  (function() {
    function i(e, t) {
      var r = this;
      this._flingStartTime = (/* @__PURE__ */ new Date()).getTime(), this._flingScrollRequestId = null, this._startScrollCoordinate = null, this._touchCoordinate = null, this._touchCancelCrosshair = !1, this._touchZoomed = !1, this._pinchScale = 1, this._mouseDownWidget = null, this._prevYAxisRanges = /* @__PURE__ */ new Map(), this._xAxisStartScaleCoordinate = null, this._xAxisStartScaleDistance = 0, this._xAxisScale = 1, this._yAxisStartScaleDistance = 0, this._mouseMoveTriggerWidgetInfo = { pane: null, widget: null }, this._boundKeyBoardDownEvent = function(a) {
        var n, o, s, l = a.target, u = l?.tagName.toLowerCase();
        if (!(u === "input" || u === "textarea" || l?.isContentEditable === !0)) {
          var h = r._chart.getHotKey(), c = h.enabled, d = h.exclude;
          if (c) {
            var f = [];
            a.ctrlKey && f.push("ctrl"), a.altKey && f.push("alt"), a.shiftKey && f.push("shift"), a.metaKey && f.push("meta");
            var v = a.code.trim().toLowerCase();
            /^key[a-z]$/.test(v) ? f.push(v.slice(3)) : /^digit[0-9]$/.test(v) ? f.push(v.slice(5)) : f.push((n = cr[v]) !== null && n !== void 0 ? n : v);
            for (var p = f.join("+"), g = Xn(), m = g.length - 1; m >= 0; m--) {
              var x = g[m], _ = zn(x);
              if (!d.includes(x) && C(_)) {
                var E = Dt(_.keys) ? _.keys : [_.keys], y = E.some(function(b) {
                  var w = [], S = "";
                  return b.replace(/\+\+$/, "+Plus").replace(/\+=$/, "+Equal").split("+").forEach(function(T) {
                    var A, D, R = (A = Hn[T.trim().toLowerCase()]) !== null && A !== void 0 ? A : T, P = R.trim().toLowerCase(), k = "";
                    /^key[a-z]$/.test(P) ? k = P.slice(3) : /^digit[0-9]$/.test(P) ? k = P.slice(5) : k = (D = cr[P]) !== null && D !== void 0 ? D : P, Ee.includes(k) ? w.includes(k) || w.push(k) : k.length > 0 && (S = k);
                  }), w.sort(function(T, A) {
                    return Ee.indexOf(T) - Ee.indexOf(A);
                  }), re(re([], ae(w), !1), [S], !1).filter(function(T) {
                    return T.length > 0;
                  }).join("+") === p;
                });
                if (y) {
                  var I = { chart: r._chart, event: a, key: p, hotkey: _ };
                  if (!nt(_.check) || _.check(I)) {
                    (!((o = _.preventDefault) !== null && o !== void 0) || o) && a.preventDefault(), (s = _.stopPropagation) !== null && s !== void 0 && s && a.stopPropagation(), _.action(I);
                    return;
                  }
                }
              }
            }
          }
        }
      }, this._chart = t, this._event = new Wn(e, this, {
        treatVertDragAsPageScroll: function() {
          return !1;
        },
        treatHorzDragAsPageScroll: function() {
          return !1;
        }
      }), document.addEventListener("keydown", this._boundKeyBoardDownEvent);
    }
    return i.prototype._getYAxisByWidget = function(e) {
      return e.getName() === U.Y_AXIS ? e.getAxisComponent() : e.getPane().getYAxisComponentById();
    }, i.prototype._getYAxisScaleTargetByWidget = function(e) {
      var t = this._getYAxisByWidget(e), r = e.getPane();
      return r.isManualYAxis(t.id) ? r.getYAxisComponentById() : t;
    }, i.prototype._syncYAxisValueRange = function(e, t) {
      var r = e.getRange(), a = t.from, n = t.to, o = e.valueToRealValue(a, { range: r }), s = e.valueToRealValue(n, { range: r }), l = e.realValueToDisplayValue(o, { range: r }), u = e.realValueToDisplayValue(s, { range: r });
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
    }, i.prototype._syncManualYAxesValueRange = function(e, t) {
      var r = this, a = t.getRange();
      e.getPane().getYAxisComponents().forEach(function(n) {
        var o = n;
        o !== t && e.getPane().isManualYAxis(o.id) && r._syncYAxisValueRange(o, a);
      });
    }, i.prototype._resetYAxisAndManualYAxes = function(e, t) {
      t.setAutoCalcTickFlag(!0), e.getPane().getYAxisComponents().forEach(function(r) {
        var a = r;
        e.getPane().isManualYAxis(a.id) && a.setAutoCalcTickFlag(!0);
      }), this._chart.layout({
        measureWidth: !0,
        update: !0,
        buildYAxisTick: !0
      });
    }, i.prototype.pinchStartEvent = function() {
      return this._touchZoomed = !0, this._pinchScale = 1, !0;
    }, i.prototype.pinchEvent = function(e, t) {
      var r = this._findWidgetByEvent(e), a = r.pane, n = r.widget;
      if (a?.getId() !== q.X_AXIS && n?.getName() === U.MAIN) {
        var o = this._makeWidgetEvent(e, n), s = (t - this._pinchScale) * 5;
        return this._pinchScale = t, this._chart.getChartStore().zoom(s, { x: o.x, y: o.y }, "main"), !0;
      }
      return !1;
    }, i.prototype.mouseWheelHortEvent = function(e, t) {
      var r = this._chart.getChartStore();
      return r.startScroll(), r.scroll(t), !0;
    }, i.prototype.mouseWheelVertEvent = function(e, t) {
      var r = this._findWidgetByEvent(e).widget, a = this._makeWidgetEvent(e, r), n = r?.getName();
      if (n === U.MAIN)
        return this._chart.getChartStore().zoom(t, { x: a.x, y: a.y }, "main"), !0;
      if (n === U.Y_AXIS) {
        var o = r, s = this._getYAxisByWidget(o);
        if (s.scrollZoomEnabled) {
          var l = 1 + t * 0.05, u = this._getYAxisScaleTargetByWidget(o);
          return this._zoomYAxis(u, l), this._syncManualYAxesValueRange(o, u), !0;
        }
      }
      return !1;
    }, i.prototype.mouseDownEvent = function(e) {
      var t, r, a = this._findWidgetByEvent(e), n = a.pane, o = a.widget;
      if (this._mouseDownWidget = o, o !== null) {
        var s = this._makeWidgetEvent(e, o), l = o.getName();
        switch (l) {
          case U.SEPARATOR:
            return o.dispatchEvent("mouseDownEvent", s);
          case U.MAIN: {
            var u = o.dispatchEvent("mouseDownEvent", s);
            if (!u) {
              var h = n.getYAxisComponents();
              try {
                for (var c = gt(h), d = c.next(); !d.done; d = c.next()) {
                  var f = d.value, v = f;
                  if (!v.getAutoCalcTickFlag()) {
                    var p = v.getRange();
                    this._prevYAxisRanges.set(v, M({}, p));
                  }
                }
              } catch (g) {
                t = { error: g };
              } finally {
                try {
                  d && !d.done && (r = c.return) && r.call(c);
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
    }, i.prototype.mouseMoveEvent = function(e) {
      var t, r, a, n = this._findWidgetByEvent(e), o = n.pane, s = n.widget, l = this._makeWidgetEvent(e, s);
      if ((((t = this._mouseMoveTriggerWidgetInfo.pane) === null || t === void 0 ? void 0 : t.getId()) !== o?.getId() || ((r = this._mouseMoveTriggerWidgetInfo.widget) === null || r === void 0 ? void 0 : r.getName()) !== s?.getName()) && (s?.dispatchEvent("mouseEnterEvent", l), (a = this._mouseMoveTriggerWidgetInfo.widget) === null || a === void 0 || a.dispatchEvent("mouseLeaveEvent", l), this._mouseMoveTriggerWidgetInfo = { pane: o, widget: s }), s !== null) {
        var u = s.getName();
        switch (u) {
          case U.MAIN: {
            var h = s.dispatchEvent("mouseMoveEvent", l), c = { x: l.x, y: l.y, paneId: o?.getId() };
            return h ? (s.getForceCursor() !== "pointer" && (c = void 0), s.setCursor("pointer")) : s.setCursor("crosshair"), this._chart.getChartStore().setCrosshair(c), h;
          }
          case U.SEPARATOR:
          case U.X_AXIS:
          case U.Y_AXIS: {
            var h = s.dispatchEvent("mouseMoveEvent", l);
            return this._chart.getChartStore().setCrosshair(), h;
          }
        }
      }
      return !1;
    }, i.prototype.pressedMouseMoveEvent = function(e) {
      var t, r;
      if (this._mouseDownWidget !== null && this._mouseDownWidget.getName() === U.SEPARATOR)
        return this._mouseDownWidget.dispatchEvent("pressedMouseMoveEvent", e);
      var a = this._findWidgetByEvent(e), n = a.pane, o = a.widget;
      if (o !== null && ((t = this._mouseDownWidget) === null || t === void 0 ? void 0 : t.getPane().getId()) === n?.getId() && ((r = this._mouseDownWidget) === null || r === void 0 ? void 0 : r.getName()) === o.getName()) {
        var s = this._makeWidgetEvent(e, o), l = o.getName();
        switch (l) {
          case U.MAIN: {
            var u = void 0, h = o.dispatchEvent("pressedMouseMoveEvent", s);
            return h ? this._chart.updatePane(
              1
              /* UpdateLevel.Overlay */
            ) : this._processMainScrollingEvent(o, s), (!h || o.getForceCursor() === "pointer") && (u = { x: s.x, y: s.y, paneId: n?.getId() }), this._chart.getChartStore().setCrosshair(u, { forceInvalidate: !0 }), h;
          }
          case U.X_AXIS:
            return this._processXAxisScrollingEvent(o, s);
          case U.Y_AXIS:
            return this._processYAxisScalingEvent(o, s);
        }
      }
      return !1;
    }, i.prototype.mouseUpEvent = function(e) {
      var t = this._findWidgetByEvent(e).widget, r = !1;
      if (t !== null) {
        var a = this._makeWidgetEvent(e, t), n = t.getName();
        switch (n) {
          case U.MAIN:
          case U.SEPARATOR:
          case U.X_AXIS:
          case U.Y_AXIS: {
            r = t.dispatchEvent("mouseUpEvent", a);
            break;
          }
        }
        r && this._chart.updatePane(
          1
          /* UpdateLevel.Overlay */
        );
      }
      return this._mouseDownWidget = null, this._startScrollCoordinate = null, this._prevYAxisRanges.clear(), this._xAxisStartScaleCoordinate = null, this._xAxisStartScaleDistance = 0, this._xAxisScale = 1, this._yAxisStartScaleDistance = 0, r;
    }, i.prototype.mouseClickEvent = function(e) {
      var t = this._findWidgetByEvent(e).widget;
      if (t !== null) {
        var r = this._makeWidgetEvent(e, t);
        return t.dispatchEvent("mouseClickEvent", r);
      }
      return !1;
    }, i.prototype.mouseRightClickEvent = function(e) {
      var t = this._findWidgetByEvent(e).widget, r = !1;
      if (t !== null) {
        var a = this._makeWidgetEvent(e, t), n = t.getName();
        switch (n) {
          case U.MAIN:
          case U.X_AXIS:
          case U.Y_AXIS: {
            r = t.dispatchEvent("mouseRightClickEvent", a);
            break;
          }
        }
        r && this._chart.updatePane(
          1
          /* UpdateLevel.Overlay */
        );
      }
      return !1;
    }, i.prototype.mouseDoubleClickEvent = function(e) {
      var t = this._findWidgetByEvent(e).widget;
      if (t !== null) {
        var r = t.getName();
        switch (r) {
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
    }, i.prototype.mouseLeaveEvent = function() {
      return this._chart.getChartStore().setCrosshair(), !0;
    }, i.prototype.touchStartEvent = function(e) {
      var t, r, a, n = this._findWidgetByEvent(e), o = n.pane, s = n.widget;
      if (s !== null) {
        var l = this._makeWidgetEvent(e, s);
        (a = l.preventDefault) === null || a === void 0 || a.call(l);
        var u = s.getName();
        switch (u) {
          case U.MAIN: {
            var h = this._chart.getChartStore();
            if (s.dispatchEvent("mouseDownEvent", l))
              return this._touchCancelCrosshair = !0, this._touchCoordinate = null, h.setCrosshair(void 0, { notInvalidate: !0 }), this._chart.updatePane(
                1
                /* UpdateLevel.Overlay */
              ), !0;
            this._flingScrollRequestId !== null && (Me(this._flingScrollRequestId), this._flingScrollRequestId = null), this._flingStartTime = (/* @__PURE__ */ new Date()).getTime();
            var c = o.getYAxisComponents();
            try {
              for (var d = gt(c), f = d.next(); !f.done; f = d.next()) {
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
                f && !f.done && (r = d.return) && r.call(d);
              } finally {
                if (t) throw t.error;
              }
            }
            if (this._startScrollCoordinate = { x: l.x, y: l.y }, h.startScroll(), this._touchZoomed = !1, this._touchCoordinate !== null) {
              var m = l.x - this._touchCoordinate.x, x = l.y - this._touchCoordinate.y, _ = Math.sqrt(m * m + x * x);
              _ < Yn ? (this._touchCoordinate = { x: l.x, y: l.y }, h.setCrosshair({ x: l.x, y: l.y, paneId: o?.getId() })) : (this._touchCoordinate = null, this._touchCancelCrosshair = !0, h.setCrosshair());
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
    }, i.prototype.touchMoveEvent = function(e) {
      var t, r, a, n = this._findWidgetByEvent(e), o = n.pane, s = n.widget;
      if (s !== null) {
        var l = this._makeWidgetEvent(e, s), u = s.getName(), h = this._chart.getChartStore();
        switch (u) {
          case U.MAIN:
            return s.dispatchEvent("pressedMouseMoveEvent", l) ? ((t = l.preventDefault) === null || t === void 0 || t.call(l), h.setCrosshair(void 0, { notInvalidate: !0 }), this._chart.updatePane(
              1
              /* UpdateLevel.Overlay */
            ), !0) : (this._touchCoordinate !== null ? ((r = l.preventDefault) === null || r === void 0 || r.call(l), h.setCrosshair({ x: l.x, y: l.y, paneId: o?.getId() })) : this._processMainScrollingEvent(s, l), !0);
          case U.X_AXIS:
            return (a = l.preventDefault) === null || a === void 0 || a.call(l), this._processXAxisScrollingEvent(s, l);
          case U.Y_AXIS:
            return this._processYAxisScalingEvent(s, l);
        }
      }
      return !1;
    }, i.prototype.touchEndEvent = function(e) {
      var t = this, r = this._findWidgetByEvent(e).widget;
      if (r !== null) {
        var a = this._makeWidgetEvent(e, r), n = r.getName();
        switch (n) {
          case U.MAIN: {
            if (r.dispatchEvent("mouseUpEvent", a), this._startScrollCoordinate !== null) {
              var o = (/* @__PURE__ */ new Date()).getTime() - this._flingStartTime, s = a.x - this._startScrollCoordinate.x, l = s / (o > 0 ? o : 1) * 20;
              if (o < 200 && Math.abs(l) > 0) {
                var u = this._chart.getChartStore(), h = function() {
                  t._flingScrollRequestId = ce(function() {
                    u.startScroll(), u.scroll(l), l = l * (1 - 0.025), Math.abs(l) < 1 ? t._flingScrollRequestId !== null && (Me(t._flingScrollRequestId), t._flingScrollRequestId = null) : h();
                  });
                };
                h();
              }
            }
            return !0;
          }
          case U.X_AXIS:
          case U.Y_AXIS: {
            var c = r.dispatchEvent("mouseUpEvent", a);
            c && this._chart.updatePane(
              1
              /* UpdateLevel.Overlay */
            );
          }
        }
        this._startScrollCoordinate = null, this._prevYAxisRanges.clear(), this._xAxisStartScaleCoordinate = null, this._xAxisStartScaleDistance = 0, this._xAxisScale = 1, this._yAxisStartScaleDistance = 0;
      }
      return !1;
    }, i.prototype.tapEvent = function(e) {
      var t = this._findWidgetByEvent(e), r = t.pane, a = t.widget, n = !1;
      if (a !== null) {
        var o = this._makeWidgetEvent(e, a), s = a.dispatchEvent("mouseClickEvent", o);
        if (a.getName() === U.MAIN) {
          var l = this._makeWidgetEvent(e, a), u = this._chart.getChartStore();
          s ? (this._touchCancelCrosshair = !0, this._touchCoordinate = null, u.setCrosshair(void 0, { notInvalidate: !0 }), n = !0) : (!this._touchCancelCrosshair && !this._touchZoomed && (this._touchCoordinate = { x: l.x, y: l.y }, u.setCrosshair({ x: l.x, y: l.y, paneId: r?.getId() }, { notInvalidate: !0 }), n = !0), this._touchCancelCrosshair = !1);
        }
        (n || s) && this._chart.updatePane(
          1
          /* UpdateLevel.Overlay */
        );
      }
      return n;
    }, i.prototype.doubleTapEvent = function(e) {
      return this.mouseDoubleClickEvent(e);
    }, i.prototype.longTapEvent = function(e) {
      var t = this._findWidgetByEvent(e), r = t.pane, a = t.widget;
      if (a !== null && a.getName() === U.MAIN) {
        var n = this._makeWidgetEvent(e, a);
        return this._touchCoordinate = { x: n.x, y: n.y }, this._chart.getChartStore().setCrosshair({ x: n.x, y: n.y, paneId: r?.getId() }), !0;
      }
      return !1;
    }, i.prototype._processMainScrollingEvent = function(e, t) {
      var r, a, n;
      if (this._startScrollCoordinate !== null) {
        var o = e.getPane().getYAxisComponents();
        try {
          for (var s = gt(o), l = s.next(); !l.done; l = s.next()) {
            var u = l.value, h = u, c = this._prevYAxisRanges.get(h);
            if (C(c) && !h.getAutoCalcTickFlag() && h.scrollZoomEnabled) {
              (n = t.preventDefault) === null || n === void 0 || n.call(t);
              var d = c.from, f = c.to, v = c.range, p = 0;
              h.reverse ? p = this._startScrollCoordinate.y - t.y : p = t.y - this._startScrollCoordinate.y;
              var g = e.getBounding(), m = p / g.height, x = v * m, _ = d + x, E = f + x, y = h.valueToRealValue(_, { range: c }), I = h.valueToRealValue(E, { range: c }), b = h.realValueToDisplayValue(y, { range: c }), w = h.realValueToDisplayValue(I, { range: c });
              h.setRange({
                from: _,
                to: E,
                range: E - _,
                realFrom: y,
                realTo: I,
                realRange: I - y,
                displayFrom: b,
                displayTo: w,
                displayRange: w - b
              });
            }
          }
        } catch (T) {
          r = { error: T };
        } finally {
          try {
            l && !l.done && (a = s.return) && a.call(s);
          } finally {
            if (r) throw r.error;
          }
        }
        var S = t.x - this._startScrollCoordinate.x;
        this._chart.getChartStore().scroll(S);
      }
    }, i.prototype._processXAxisScrollStartEvent = function(e, t) {
      var r = e.dispatchEvent("mouseDownEvent", t);
      return r && this._chart.updatePane(
        1
        /* UpdateLevel.Overlay */
      ), this._xAxisStartScaleCoordinate = { x: t.x, y: t.y }, this._xAxisStartScaleDistance = t.pageX, r;
    }, i.prototype._processXAxisScrollingEvent = function(e, t) {
      var r = e.dispatchEvent("pressedMouseMoveEvent", t);
      if (r)
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
      return r;
    }, i.prototype._processYAxisScaleStartEvent = function(e, t) {
      var r = e.dispatchEvent("mouseDownEvent", t);
      r && this._chart.updatePane(
        1
        /* UpdateLevel.Overlay */
      );
      var a = this._getYAxisScaleTargetByWidget(e), n = a.getRange();
      return this._prevYAxisRanges.set(a, M({}, n)), this._yAxisStartScaleDistance = t.pageY, r;
    }, i.prototype._processYAxisScalingEvent = function(e, t) {
      var r, a = e.dispatchEvent("pressedMouseMoveEvent", t);
      if (a)
        this._chart.updatePane(
          1
          /* UpdateLevel.Overlay */
        );
      else {
        var n = this._getYAxisByWidget(e), o = this._getYAxisScaleTargetByWidget(e), s = this._prevYAxisRanges.get(o);
        if (C(s) && n.scrollZoomEnabled && this._yAxisStartScaleDistance !== 0) {
          (r = t.preventDefault) === null || r === void 0 || r.call(t);
          var l = t.pageY / this._yAxisStartScaleDistance;
          this._zoomYAxis(o, l, s), this._syncManualYAxesValueRange(e, o);
        }
      }
      return a;
    }, i.prototype._zoomYAxis = function(e, t, r) {
      var a = r ?? e.getRange(), n = a.from, o = a.to, s = a.range, l = s * t, u = (l - s) / 2, h = n - u, c = o + u, d = e.valueToRealValue(h, { range: a }), f = e.valueToRealValue(c, { range: a }), v = e.realValueToDisplayValue(d, { range: a }), p = e.realValueToDisplayValue(f, { range: a });
      e.setRange({
        from: h,
        to: c,
        range: l,
        realFrom: d,
        realTo: f,
        realRange: f - d,
        displayFrom: v,
        displayTo: p,
        displayRange: p - v
      }), this._chart.layout({
        measureWidth: !0,
        update: !0,
        buildYAxisTick: !0
      });
    }, i.prototype._findWidgetByEvent = function(e) {
      var t, r, a, n, o, s, l = e.x, u = e.y, h = this._chart.getSeparatorPanes(), c = this._chart.getStyles().separator.size;
      try {
        for (var d = gt(h), f = d.next(); !f.done; f = d.next()) {
          var v = f.value, p = v[1], g = p.getBounding(), m = g.top - Math.round((le - c) / 2);
          if (l >= g.left && l <= g.left + g.width && u >= m && u <= m + le)
            return { pane: p, widget: p.getWidget() };
        }
      } catch (P) {
        t = { error: P };
      } finally {
        try {
          f && !f.done && (r = d.return) && r.call(d);
        } finally {
          if (t) throw t.error;
        }
      }
      var x = this._chart.getDrawPanes(), _ = null;
      try {
        for (var E = gt(x), y = E.next(); !y.done; y = E.next()) {
          var I = y.value, g = I.getBounding();
          if (l >= g.left && l <= g.left + g.width && u >= g.top && u <= g.top + g.height) {
            _ = I;
            break;
          }
        }
      } catch (P) {
        a = { error: P };
      } finally {
        try {
          y && !y.done && (n = E.return) && n.call(E);
        } finally {
          if (a) throw a.error;
        }
      }
      var b = null;
      if (_ !== null) {
        if (!C(b)) {
          var w = _.getMainWidget(), S = w.getBounding();
          l >= S.left && l <= S.left + S.width && u >= S.top && u <= S.top + S.height && (b = w);
        }
        if (!C(b))
          try {
            for (var T = gt(_.getYAxisWidgets()), A = T.next(); !A.done; A = T.next()) {
              var D = A.value, R = D.getBounding();
              if (l >= R.left && l <= R.left + R.width && u >= R.top && u <= R.top + R.height) {
                b = D;
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
      return { pane: _, widget: b };
    }, i.prototype._makeWidgetEvent = function(e, t) {
      var r, a, n, o = (r = t?.getBounding()) !== null && r !== void 0 ? r : null;
      return M(M({}, e), { x: e.x - ((a = o?.left) !== null && a !== void 0 ? a : 0), y: e.y - ((n = o?.top) !== null && n !== void 0 ? n : 0) });
    }, i.prototype.destroy = function() {
      document.removeEventListener("keydown", this._boundKeyBoardDownEvent), this._event.destroy();
    }, i;
  })()
), qr = (
  /** @class */
  (function() {
    function i(e, t) {
      var r = this;
      this._chartBounding = Ve(), this._drawPanes = [], this._separatorPanes = /* @__PURE__ */ new Map(), this._layoutUpdateOptions = {
        sort: !0,
        measureHeight: !0,
        measureWidth: !0,
        secondMeasureWidth: !1,
        update: !0,
        buildYAxisTick: !1,
        cacheYAxisWidth: !1,
        forceBuildYAxisTick: !1
      }, this._layoutPending = !1, this._resizeObserver = null, this._resizeRequestAnimationId = Yt, this._scheduleResize = function() {
        r._resizeRequestAnimationId === Yt && (r._resizeRequestAnimationId = ce(function() {
          r._resizeRequestAnimationId = Yt, (r._chartBounding.width !== Math.floor(r._chartContainer.clientWidth) || r._chartBounding.height !== Math.floor(r._chartContainer.clientHeight)) && r.resize();
        }));
      }, this._cacheYAxisWidth = { left: 0, right: 0 }, this._initContainer(e), this._chartEvent = new Un(this._chartContainer, this), this._chartStore = new Oa(this, t);
      var a = this._chartStore.getLayoutOptions(), n = a.pane;
      this._candlePane = this._createPane(Tn, M(M({}, n), { id: q.CANDLE })), this._candlePane.createOrOverrideYAxis(M(M({}, a.yAxis), { id: te(be) })), this._xAxisPane = this._createPane(Bn, M(M({}, n), { id: q.X_AXIS, order: Number.MAX_SAFE_INTEGER })), this._layout(), this._initResizeListener();
    }
    return i.prototype._initContainer = function(e) {
      this._container = e, this._chartContainer = Vt("div", {
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
    }, i.prototype._cacheChartBounding = function() {
      this._chartBounding.width = Math.floor(this._chartContainer.clientWidth), this._chartBounding.height = Math.floor(this._chartContainer.clientHeight);
    }, i.prototype._initResizeListener = function() {
      var e = this;
      C(ResizeObserver) ? (this._resizeObserver = new ResizeObserver(function() {
        e._scheduleResize();
      }), this._resizeObserver.observe(this._chartContainer)) : window.addEventListener("resize", this._scheduleResize);
    }, i.prototype._createPane = function(e, t) {
      var r = new e(this, t);
      return this._drawPanes.push(r), r;
    }, i.prototype.getDrawPaneById = function(e) {
      if (e === q.CANDLE)
        return this._candlePane;
      if (e === q.X_AXIS)
        return this._xAxisPane;
      var t = this._drawPanes.find(function(r) {
        return r.getId() === e;
      });
      return t ?? null;
    }, i.prototype.getContainer = function() {
      return this._container;
    }, i.prototype.getChartStore = function() {
      return this._chartStore;
    }, i.prototype.getXAxisPane = function() {
      return this._xAxisPane;
    }, i.prototype.getDrawPanes = function() {
      return this._drawPanes;
    }, i.prototype.getSeparatorPanes = function() {
      return this._separatorPanes;
    }, i.prototype.layout = function(e) {
      var t = this, r, a, n, o, s, l, u, h;
      (r = e.sort) !== null && r !== void 0 && r && (this._layoutUpdateOptions.sort = e.sort), (a = e.measureHeight) !== null && a !== void 0 && a && (this._layoutUpdateOptions.measureHeight = e.measureHeight), (n = e.measureWidth) !== null && n !== void 0 && n && (this._layoutUpdateOptions.measureWidth = e.measureWidth), (o = e.secondMeasureWidth) !== null && o !== void 0 && o && (this._layoutUpdateOptions.secondMeasureWidth = e.secondMeasureWidth), (s = e.update) !== null && s !== void 0 && s && (this._layoutUpdateOptions.update = e.update), (l = e.buildYAxisTick) !== null && l !== void 0 && l && (this._layoutUpdateOptions.buildYAxisTick = e.buildYAxisTick), (u = e.cacheYAxisWidth) !== null && u !== void 0 && u && (this._layoutUpdateOptions.cacheYAxisWidth = e.cacheYAxisWidth), (h = e.forceBuildYAxisTick) !== null && h !== void 0 && h && (this._layoutUpdateOptions.forceBuildYAxisTick = e.forceBuildYAxisTick), this._layoutPending || (this._layoutPending = !0, Promise.resolve().then(function(c) {
        t._layout(), t._layoutPending = !1;
      }).catch(function(c) {
      }));
    }, i.prototype._layout = function() {
      var e = this, t, r = this._layoutUpdateOptions, a = r.sort, n = r.measureHeight, o = r.measureWidth, s = r.secondMeasureWidth, l = r.update, u = r.buildYAxisTick, h = r.cacheYAxisWidth, c = r.forceBuildYAxisTick;
      if (a) {
        for (; C(this._chartContainer.firstChild); )
          this._chartContainer.removeChild(this._chartContainer.firstChild);
        this._separatorPanes.clear(), this._drawPanes.sort(function(w, S) {
          return w.getOptions().order - S.getOptions().order;
        });
        var d = null;
        this._drawPanes.forEach(function(w) {
          if (w.getId() !== q.X_AXIS) {
            if (C(d)) {
              var S = new Vn(e, "", d, w);
              e._chartContainer.appendChild(S.getContainer()), e._separatorPanes.set(w, S);
            }
            d = w;
          }
          e._chartContainer.appendChild(w.getContainer());
        });
      }
      if (n) {
        var f = this._chartBounding.height, v = this.getStyles().separator.size, p = this._xAxisPane.getXAxisComponent().getAutoSize(), g = this._drawPanes.filter(function(w) {
          return w.getId() !== q.X_AXIS;
        }), m = g.find(function(w) {
          return w.getOptions().state === "maximize";
        }), x = Math.max(f - p, 0), _ = /* @__PURE__ */ new Map(), E = v;
        if (C(m))
          E = 0, g.forEach(function(w) {
            _.set(w, w === m ? x : 0);
          });
        else {
          x = Math.max(x - this._separatorPanes.size * v, 0);
          var y = (t = g.find(function(w) {
            return w.getId() === q.CANDLE && w.getOptions().state === "normal";
          })) !== null && t !== void 0 ? t : g.find(function(w) {
            return w.getOptions().state === "normal";
          });
          g.forEach(function(w) {
            if (w !== y) {
              var S = w.getOptions(), T = S.minHeight;
              if (S.state === "normal") {
                T = Math.max(S.minHeight, S.height);
                var A = Math.max(x, 0);
                T > A && (T = A);
              }
              x -= T, _.set(w, T);
            }
          }), C(y) && _.set(y, Math.max(x, 0));
        }
        this._drawPanes.forEach(function(w) {
          var S;
          w.getId() !== q.X_AXIS && w.setBounding({ height: (S = _.get(w)) !== null && S !== void 0 ? S : 0 });
        }), this._xAxisPane.setBounding({ height: p });
        var I = 0;
        this._drawPanes.forEach(function(w) {
          var S = e._separatorPanes.get(w);
          C(S) && (S.setBounding({ height: E, top: I }), I += E), w.setBounding({ top: I }), I += w.getBounding().height;
        });
      }
      var b = function() {
        var w = o;
        if ((u || c) && e._drawPanes.forEach(function(G) {
          G.getYAxisComponents().forEach(function(W) {
            var Z = W.buildTicks(c);
            w || (w = Z);
          });
        }), w) {
          var S = e._chartBounding.width, T = e.getStyles(), A = [], D = [], R = [], P = [], k = function(G, W, Z) {
            var H;
            G[W] = Math.max((H = G[W]) !== null && H !== void 0 ? H : 0, Z);
          };
          e._drawPanes.forEach(function(G) {
            var W = [], Z = [], H = [], it = [];
            G.getId() !== q.X_AXIS && G.getWidgetYAxisComponents().forEach(function(j) {
              var Q = j;
              Q.position === "left" ? Q.inside ? Z.push(Q) : W.push(Q) : Q.inside ? H.push(Q) : it.push(Q);
            }), W.forEach(function(j, Q) {
              k(A, Q, j.getAutoSize());
            }), Z.forEach(function(j, Q) {
              k(D, Q, j.getAutoSize());
            }), H.forEach(function(j, Q) {
              k(R, Q, j.getAutoSize());
            }), it.forEach(function(j, Q) {
              k(P, Q, j.getAutoSize());
            });
          });
          var L = A.reduce(function(G, W) {
            return G + W;
          }, 0), F = P.reduce(function(G, W) {
            return G + W;
          }, 0);
          h && (L = Math.max(e._cacheYAxisWidth.left, L), F = Math.max(e._cacheYAxisWidth.right, F)), e._cacheYAxisWidth.left = L, e._cacheYAxisWidth.right = F;
          var O = S, N = 0, K = 0;
          O -= L, N = L, O -= F, K = F, e._chartStore.setTotalBarSpace(O);
          var et = { width: S }, J = { width: O, left: N, right: K }, tt = { width: L }, rt = { width: F }, at = T.separator.fill, Y = {};
          at ? Y = et : Y = J, e._drawPanes.forEach(function(G) {
            var W, Z;
            (W = e._separatorPanes.get(G)) === null || W === void 0 || W.setBounding(Y);
            var H = {}, it = 0, j = 0, Q = 0, mt = 0, xt = [], ht = [], bt = [], _t = [];
            G.getId() !== q.X_AXIS && G.getWidgetYAxisComponents().forEach(function(wt) {
              var ct = wt;
              ct.position === "left" ? ct.inside ? ht.push(ct) : xt.push(ct) : ct.inside ? bt.push(ct) : _t.push(ct);
            });
            var Xt = xt.reduce(function(wt, ct, dt) {
              var yt;
              return wt + ((yt = A[dt]) !== null && yt !== void 0 ? yt : 0);
            }, 0);
            it = L - Xt;
            for (var At = xt.length - 1; At >= 0; At--) {
              var $t = xt[At], Ht = (Z = A[At]) !== null && Z !== void 0 ? Z : 0;
              H[$t.id] = { width: Ht, left: it }, it += Ht;
            }
            ht.forEach(function(wt, ct) {
              var dt, yt = (dt = D[ct]) !== null && dt !== void 0 ? dt : 0;
              H[wt.id] = { width: yt, left: N + j }, j += yt;
            }), bt.forEach(function(wt, ct) {
              var dt, yt = (dt = R[ct]) !== null && dt !== void 0 ? dt : 0;
              Q += yt, H[wt.id] = { width: yt, left: N + O - Q };
            }), _t.forEach(function(wt, ct) {
              var dt, yt = (dt = P[ct]) !== null && dt !== void 0 ? dt : 0;
              H[wt.id] = { width: yt, left: N + O + mt }, mt += yt;
            }), G.setYAxesBounding(H), G.setBounding(et, J, tt, rt);
          });
        }
      };
      b(), s && b(), l && (this._xAxisPane.getXAxisComponent().buildTicks(!0), this.updatePane(
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
    }, i.prototype.updatePane = function(e, t) {
      var r = this;
      if (C(t)) {
        var a = this.getDrawPaneById(t);
        a?.update(e);
      } else
        this._drawPanes.forEach(function(n) {
          var o;
          n.update(e), (o = r._separatorPanes.get(n)) === null || o === void 0 || o.update(e);
        });
    }, i.prototype.getDom = function(e, t) {
      var r, a;
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
              return (a = (r = n.getYAxisWidget()) === null || r === void 0 ? void 0 : r.getContainer()) !== null && a !== void 0 ? a : null;
          }
        }
      } else
        return this._chartContainer;
      return null;
    }, i.prototype.getSize = function(e, t) {
      var r, a;
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
              return (a = (r = n.getYAxisWidget()) === null || r === void 0 ? void 0 : r.getBounding()) !== null && a !== void 0 ? a : null;
          }
        }
      } else
        return this._chartBounding;
      return null;
    }, i.prototype._resetYAxisAutoCalcTickFlag = function() {
      this._drawPanes.forEach(function(e) {
        e.getYAxisComponents().forEach(function(t) {
          t.setAutoCalcTickFlag(!0);
        });
      });
    }, i.prototype.setSymbol = function(e) {
      e !== this.getSymbol() && (this._resetYAxisAutoCalcTickFlag(), this._chartStore.setSymbol(e));
    }, i.prototype.getSymbol = function() {
      return this._chartStore.getSymbol();
    }, i.prototype.setPeriod = function(e) {
      e !== this.getPeriod() && (this._resetYAxisAutoCalcTickFlag(), this._chartStore.setPeriod(e));
    }, i.prototype.getPeriod = function() {
      return this._chartStore.getPeriod();
    }, i.prototype.setStyles = function(e) {
      var t = this;
      this._setOptions(function() {
        t._chartStore.setStyles(e);
      });
    }, i.prototype.getStyles = function() {
      return this._chartStore.getStyles();
    }, i.prototype.setFormatter = function(e) {
      var t = this;
      this._setOptions(function() {
        t._chartStore.setFormatter(e);
      });
    }, i.prototype.getFormatter = function() {
      return this._chartStore.getFormatter();
    }, i.prototype.setLocale = function(e) {
      var t = this;
      this._setOptions(function() {
        t._chartStore.setLocale(e);
      });
    }, i.prototype.getLocale = function() {
      return this._chartStore.getLocale();
    }, i.prototype.setTimezone = function(e) {
      var t = this;
      this._setOptions(function() {
        t._chartStore.setTimezone(e);
      });
    }, i.prototype.getTimezone = function() {
      return this._chartStore.getTimezone();
    }, i.prototype.setThousandsSeparator = function(e) {
      var t = this;
      this._setOptions(function() {
        t._chartStore.setThousandsSeparator(e);
      });
    }, i.prototype.getThousandsSeparator = function() {
      return this._chartStore.getThousandsSeparator();
    }, i.prototype.setDecimalFold = function(e) {
      var t = this;
      this._setOptions(function() {
        t._chartStore.setDecimalFold(e);
      });
    }, i.prototype.getDecimalFold = function() {
      return this._chartStore.getDecimalFold();
    }, i.prototype.setHotkey = function(e) {
      this._chartStore.setHotkey(e);
    }, i.prototype.getHotkey = function() {
      return this._chartStore.getHotkey();
    }, i.prototype.getHotKey = function() {
      return this._chartStore.getHotKey();
    }, i.prototype._setOptions = function(e) {
      e(), this.layout({
        measureHeight: !0,
        measureWidth: !0,
        update: !0,
        buildYAxisTick: !0,
        forceBuildYAxisTick: !0
      });
    }, i.prototype.setOffsetRightDistance = function(e) {
      this._chartStore.setOffsetRightDistance(e, !0);
    }, i.prototype.getOffsetRightDistance = function() {
      return this._chartStore.getOffsetRightDistance();
    }, i.prototype.setMaxOffsetLeftDistance = function(e) {
      e < 0 || this._chartStore.setMaxOffsetLeftDistance(e);
    }, i.prototype.setMaxOffsetRightDistance = function(e) {
      e < 0 || this._chartStore.setMaxOffsetRightDistance(e);
    }, i.prototype.setLeftMinVisibleBarCount = function(e) {
      e < 0 || this._chartStore.setLeftMinVisibleBarCount(Math.ceil(e));
    }, i.prototype.setRightMinVisibleBarCount = function(e) {
      e < 0 || this._chartStore.setRightMinVisibleBarCount(Math.ceil(e));
    }, i.prototype.setBarSpace = function(e) {
      this._chartStore.setBarSpace(e);
    }, i.prototype.getBarSpace = function() {
      return this._chartStore.getBarSpace();
    }, i.prototype.getVisibleRange = function() {
      return this._chartStore.getVisibleRange();
    }, i.prototype._removeOrphanYAxes = function() {
      var e = this, t = !1;
      return this._drawPanes.forEach(function(r) {
        var a = r.getId();
        if (a !== q.X_AXIS) {
          var n = /* @__PURE__ */ new Set(), o = r.getDefaultYAxisId();
          C(o) && n.add(o), e._chartStore.getIndicatorsByPaneId(a).forEach(function(s) {
            n.add(s.yAxisId);
          }), r.getYAxisComponents().forEach(function(s) {
            !n.has(s.id) && !r.isManualYAxis(s.id) && (t = r.removeYAxis(s.id) || t);
          });
        }
      }), t;
    }, i.prototype._createOrUseIndicatorYAxis = function(e, t) {
      var r = !1;
      return e.hasYAxisComponent(t) || (e.createOrOverrideYAxis(M(M({}, this._chartStore.getLayoutOptions().yAxis), { id: t })), r = !0), e.isManualYAxis(t) && (e.setManualYAxis(t, !1), r = !0), r;
    }, i.prototype.resetData = function() {
      this._chartStore.resetData();
    }, i.prototype.getDataList = function() {
      return this._chartStore.getDataList();
    }, i.prototype.setDataLoader = function(e) {
      this._resetYAxisAutoCalcTickFlag(), this._chartStore.setDataLoader(e);
    }, i.prototype.createIndicator = function(e, t) {
      var r, a, n, o, s = $(e) ? { name: e } : e;
      if (Cr(s.name) === null)
        return null;
      (r = s.id) !== null && r !== void 0 || (s.id = te("".concat(s.name, "_"))), (a = s.paneId) !== null && a !== void 0 || (s.paneId = te(q.INDICATOR));
      var l = this.getDrawPaneById(s.paneId);
      (n = s.yAxisId) !== null && n !== void 0 || (s.yAxisId = (o = l?.getDefaultYAxisId()) !== null && o !== void 0 ? o : te(be));
      var u = this._chartStore.addIndicator(s, t ?? !1);
      if (u) {
        var h = !1, c = this.getDrawPaneById(s.paneId);
        return C(c) || (c = this._createPane(Ur, M(M({}, this._chartStore.getLayoutOptions().pane), { id: s.paneId })), h = !0), this._createOrUseIndicatorYAxis(c, s.yAxisId), this._removeOrphanYAxes(), this.layout({
          sort: h,
          measureHeight: !0,
          measureWidth: !0,
          update: !0,
          buildYAxisTick: !0,
          forceBuildYAxisTick: !0
        }), s.id;
      }
      return null;
    }, i.prototype.overrideIndicator = function(e) {
      var t = this, r = this._chartStore.getIndicatorsByFilter(e);
      if (r.length === 0)
        return !1;
      var a = this._chartStore.overrideIndicator(e);
      return r.forEach(function(n) {
        var o = t.getDrawPaneById(n.paneId);
        C(o) && (a = t._createOrUseIndicatorYAxis(o, n.yAxisId) || a);
      }), a && (this._removeOrphanYAxes(), this.layout({
        measureWidth: !0,
        update: !0,
        buildYAxisTick: !0,
        forceBuildYAxisTick: !0
      })), a;
    }, i.prototype.getIndicators = function(e) {
      return this._chartStore.getIndicatorsByFilter(e ?? {});
    }, i.prototype.removeIndicator = function(e) {
      var t = this, r = this._chartStore.removeIndicator(e ?? {});
      if (r) {
        this._removeOrphanYAxes();
        var a = !1, n = [];
        this._drawPanes.forEach(function(o) {
          var s = o.getId();
          if (s !== q.X_AXIS && s !== q.CANDLE) {
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
      return r;
    }, i.prototype.createOverlay = function(e) {
      var t = this, r = [], a = [], n = function(s) {
        !C(s.paneId) || t.getDrawPaneById(s.paneId) === null ? (s.paneId = q.CANDLE, a.push(!1)) : a.push(!0), r.push(s);
      };
      $(e) ? n({ name: e }) : Dt(e) ? e.forEach(function(s) {
        var l = null;
        $(s) ? l = { name: s } : l = s, n(l);
      }) : n(e);
      var o = this._chartStore.addOverlays(r, a);
      return Dt(e) ? o : o[0];
    }, i.prototype.getOverlays = function(e) {
      return this._chartStore.getOverlaysByFilter(e ?? {});
    }, i.prototype.overrideOverlay = function(e) {
      return this._chartStore.overrideOverlay(e);
    }, i.prototype.removeOverlay = function(e) {
      return this._chartStore.removeOverlay(e ?? {});
    }, i.prototype.setPaneOptions = function(e) {
      var t, r, a, n = !1, o = !1, s = !1, l = C(e.id);
      try {
        for (var u = gt(this._drawPanes), h = u.next(); !h.done; h = u.next()) {
          var c = h.value, d = c.getId();
          if (l && e.id === d || !l) {
            if (d !== q.X_AXIS) {
              var f = c.getOptions(), v = f.state;
              if (B(e.height) && e.height > 0) {
                var p = Math.max((a = e.minHeight) !== null && a !== void 0 ? a : f.minHeight, 0), g = Math.max(p, e.height);
                o = !0, n = !0, c.setBounding({ height: g });
              }
              C(e.state) && (o = !0, n = !0, v === "normal" && e.state !== "normal" ? c.setOptions({ height: c.getBounding().height }) : v !== "normal" && e.state === "normal" && !B(e.height) && c.setBounding({
                height: Math.max(f.minHeight, f.height)
              }));
            }
            if (B(e.order) && (o = !0, s = !0), c.setOptions(e), d === e.id)
              break;
          }
        }
      } catch (m) {
        t = { error: m };
      } finally {
        try {
          h && !h.done && (r = u.return) && r.call(u);
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
    }, i.prototype.createYAxis = function(e) {
      var t, r, a = (t = e.paneId) !== null && t !== void 0 ? t : q.CANDLE, n = this.getDrawPaneById(a);
      if (!C(n) || a === q.X_AXIS)
        return null;
      var o = (r = e.id) !== null && r !== void 0 ? r : te(be);
      return n.hasYAxisComponent(o) || (n.createOrOverrideYAxis(M(M(M({}, this._chartStore.getLayoutOptions().yAxis), e), { id: o, paneId: a })), n.setManualYAxis(o, !0), this.layout({
        measureWidth: !0,
        update: !0,
        buildYAxisTick: !0,
        forceBuildYAxisTick: !0
      })), o;
    }, i.prototype.removeYAxis = function(e) {
      var t, r, a = e.id, n = e.name;
      if (!C(a) && !C(n))
        return !1;
      var o = !1, s = function(d) {
        var f = l.getDrawPaneById(d.paneId);
        if (!C(f) || f.isDefaultYAxis(d.id) && d.paneId === q.CANDLE)
          return "continue";
        var v = l._chartStore.getIndicatorsByPaneId(d.paneId);
        if (v.some(function(p) {
          return p.yAxisId === d.id;
        }))
          return "continue";
        o = f.removeYAxis(d.id) || o;
      }, l = this;
      try {
        for (var u = gt(this.getYAxes(e)), h = u.next(); !h.done; h = u.next()) {
          var c = h.value;
          s(c);
        }
      } catch (d) {
        t = { error: d };
      } finally {
        try {
          h && !h.done && (r = u.return) && r.call(u);
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
    }, i.prototype.getYAxes = function(e) {
      var t, r, a = e.paneId, n = e.id, o = e.name, s = function(u) {
        return C(n) ? u.id === n : !C(o) || u.name === o;
      }, l = [];
      return C(a) ? l = l.concat((r = (t = this.getDrawPaneById(a)) === null || t === void 0 ? void 0 : t.getYAxisComponents().filter(s)) !== null && r !== void 0 ? r : []) : this._drawPanes.forEach(function(u) {
        u.getId() !== q.X_AXIS && (l = l.concat(u.getYAxisComponents().filter(s)));
      }), l;
    }, i.prototype.overrideYAxis = function(e) {
      var t = this, r = this.getYAxes({ paneId: e.paneId, id: e.id });
      r.length !== 0 && (r.forEach(function(a) {
        var n;
        (n = t.getDrawPaneById(a.paneId)) === null || n === void 0 || n.createOrOverrideYAxis(M(M({}, e), { id: a.id }));
      }), this.layout({
        measureWidth: !0,
        update: !0,
        buildYAxisTick: !0,
        forceBuildYAxisTick: !0
      }));
    }, i.prototype.overrideXAxis = function(e) {
      this._xAxisPane.overrideXAxis(e), this.layout({
        measureHeight: !0,
        update: !0,
        buildYAxisTick: !0,
        forceBuildYAxisTick: !0
      });
    }, i.prototype.getPaneOptions = function(e) {
      var t;
      if (C(e)) {
        var r = this.getDrawPaneById(e);
        return (t = r?.getOptions()) !== null && t !== void 0 ? t : null;
      }
      return this._drawPanes.map(function(a) {
        return a.getOptions();
      });
    }, i.prototype.setZoomEnabled = function(e) {
      this._chartStore.setZoomEnabled(e);
    }, i.prototype.isZoomEnabled = function() {
      return this._chartStore.isZoomEnabled();
    }, i.prototype.setZoomAnchor = function(e) {
      this._chartStore.setZoomAnchor(e);
    }, i.prototype.getZoomAnchor = function() {
      return this._chartStore.getZoomAnchor();
    }, i.prototype.setScrollEnabled = function(e) {
      this._chartStore.setScrollEnabled(e);
    }, i.prototype.isScrollEnabled = function() {
      return this._chartStore.isScrollEnabled();
    }, i.prototype.scrollByDistance = function(e, t) {
      var r = this, a = B(t) && t > 0 ? t : 0;
      if (this._chartStore.startScroll(), a > 0) {
        var n = new Pe({ duration: a });
        n.doFrame(function(o) {
          var s = e * (o / a);
          r._chartStore.scroll(s);
        }), n.start();
      } else
        this._chartStore.scroll(e);
    }, i.prototype.scrollToRealTime = function(e) {
      var t = this._chartStore.getBarSpace().bar, r = this._chartStore.getLastBarRightSideDiffBarCount() - this._chartStore.getInitialOffsetRightDistance() / t, a = r * t;
      this.scrollByDistance(a, e);
    }, i.prototype.scrollToDataIndex = function(e, t) {
      var r = (this._chartStore.getLastBarRightSideDiffBarCount() + (this.getDataList().length - 1 - e)) * this._chartStore.getBarSpace().bar;
      this.scrollByDistance(r, t);
    }, i.prototype.scrollToTimestamp = function(e, t) {
      var r = De(this.getDataList(), "timestamp", e);
      this.scrollToDataIndex(r, t);
    }, i.prototype.zoomAtCoordinate = function(e, t, r) {
      var a = this, n = B(r) && r > 0 ? r : 0, o = this._chartStore.getBarSpace().bar, s = o * e, l = s - o;
      if (n > 0) {
        var u = 0, h = new Pe({ duration: n });
        h.doFrame(function(c) {
          var d = l * (c / n), f = (d - u) / a._chartStore.getBarSpace().bar * ke;
          a._chartStore.zoom(f, t ?? null, "main"), u = d;
        }), h.start();
      } else
        this._chartStore.zoom(l / o * ke, t ?? null, "main");
    }, i.prototype.zoomAtDataIndex = function(e, t, r) {
      var a = this._chartStore.dataIndexToCoordinate(t);
      this.zoomAtCoordinate(e, { x: a, y: 0 }, r);
    }, i.prototype.zoomAtTimestamp = function(e, t, r) {
      var a = De(this.getDataList(), "timestamp", t);
      this.zoomAtDataIndex(e, a, r);
    }, i.prototype.convertToPixel = function(e, t) {
      var r = this, a, n = t ?? {}, o = n.paneId, s = o === void 0 ? q.CANDLE : o, l = n.yAxisId, u = n.absolute, h = u === void 0 ? !1 : u, c = [];
      if (s !== q.X_AXIS) {
        var d = this.getDrawPaneById(s);
        if (d !== null) {
          var f = d.getBounding(), v = [].concat(e), p = this._xAxisPane.getXAxisComponent(), g = d.getYAxisComponentById(l);
          c = v.map(function(m) {
            var x = {}, _ = m.dataIndex;
            if (B(m.timestamp) && (_ = r._chartStore.timestampToDataIndex(m.timestamp)), B(_) && (x.x = p.convertToPixel(_)), B(m.value)) {
              var E = g.convertToPixel(m.value);
              x.y = h ? f.top + E : E;
            }
            return x;
          });
        }
      }
      return Dt(e) ? c : (a = c[0]) !== null && a !== void 0 ? a : {};
    }, i.prototype.convertFromPixel = function(e, t) {
      var r = this, a, n = t ?? {}, o = n.paneId, s = o === void 0 ? q.CANDLE : o, l = n.yAxisId, u = n.absolute, h = u === void 0 ? !1 : u, c = [];
      if (s !== q.X_AXIS) {
        var d = this.getDrawPaneById(s);
        if (d !== null) {
          var f = d.getBounding(), v = [].concat(e), p = this._xAxisPane.getXAxisComponent(), g = d.getYAxisComponentById(l);
          c = v.map(function(m) {
            var x, _ = {};
            if (B(m.x)) {
              var E = p.convertFromPixel(m.x);
              _.dataIndex = E, _.timestamp = (x = r._chartStore.dataIndexToTimestamp(E)) !== null && x !== void 0 ? x : void 0;
            }
            if (B(m.y)) {
              var y = h ? m.y - f.top : m.y;
              _.value = g.convertFromPixel(y);
            }
            return _;
          });
        }
      }
      return Dt(e) ? c : (a = c[0]) !== null && a !== void 0 ? a : {};
    }, i.prototype.executeAction = function(e, t) {
      var r;
      if (e === "onCrosshairChange") {
        var a = null;
        C(t) && (a = M({}, t), (r = a.paneId) !== null && r !== void 0 || (a.paneId = q.CANDLE)), this._chartStore.setCrosshair(a, { notExecuteAction: !0 });
      }
    }, i.prototype.subscribeAction = function(e, t) {
      this._chartStore.subscribeAction(e, t);
    }, i.prototype.unsubscribeAction = function(e, t) {
      this._chartStore.unsubscribeAction(e, t);
    }, i.prototype.getConvertPictureUrl = function(e, t, r) {
      var a = this, n = this._chartBounding, o = n.width, s = n.height, l = Vt("canvas", {
        width: "".concat(o, "px"),
        height: "".concat(s, "px"),
        boxSizing: "border-box"
      }), u = l.getContext("2d"), h = Wt(l);
      l.width = o * h, l.height = s * h, u.scale(h, h), u.fillStyle = r ?? "#FFFFFF", u.fillRect(0, 0, o, s);
      var c = e ?? !1;
      return this._drawPanes.forEach(function(d) {
        var f = a._separatorPanes.get(d);
        if (C(f)) {
          var v = f.getBounding();
          u.drawImage(f.getImage(c), v.left, v.top, v.width, v.height);
        }
        var p = d.getBounding();
        u.drawImage(d.getImage(c), 0, p.top, o, p.height);
      }), l.toDataURL("image/".concat(t ?? "jpeg"));
    }, i.prototype.resize = function() {
      this._cacheChartBounding(), this.layout({
        measureHeight: !0,
        measureWidth: !0,
        secondMeasureWidth: !0,
        update: !0,
        buildYAxisTick: !0,
        forceBuildYAxisTick: !0
      });
    }, i.prototype.destroy = function() {
      this._resizeRequestAnimationId !== Yt && (Me(this._resizeRequestAnimationId), this._resizeRequestAnimationId = Yt), C(this._resizeObserver) ? (this._resizeObserver.disconnect(), this._resizeObserver = null) : window.removeEventListener("resize", this._scheduleResize), this._chartEvent.destroy(), this._drawPanes.forEach(function(e) {
        e.destroy();
      }), this._drawPanes = [], this._separatorPanes.clear(), this._chartStore.destroy(), this._container.removeChild(this._chartContainer);
    }, i;
  })()
), ye = /* @__PURE__ */ new Map(), Gn = 1;
function qn(i, e) {
  var t = null;
  if ($(i) ? t = document.getElementById(i) : t = i, t === null)
    return null;
  var r = ye.get(t.id);
  if (C(r))
    return r;
  var a = "k_line_chart_".concat(Gn++);
  return r = new qr(t, e), r.id = a, t.setAttribute("k-line-chart-id", a), ye.set(a, r), r;
}
function Zn(i) {
  var e, t, r = null;
  if (i instanceof qr)
    r = i.id;
  else {
    var a = null;
    $(i) ? a = document.getElementById(i) : a = i, r = (e = a?.getAttribute("k-line-chart-id")) !== null && e !== void 0 ? e : null;
  }
  r !== null && ((t = ye.get(r)) === null || t === void 0 || t.destroy(), ye.delete(r));
}
const Zr = "STOCK_EVA_API_MA", $r = "STOCK_EVA_API_VOLUME", jr = "STOCK_EVA_API_MACD", Kr = "STOCK_EVA_API_RSI", dr = ["ma5", "ma10", "ma20", "ma60", "ma120", "ma250"], $n = ["macd", "macd_signal", "macd_hist"], vr = /* @__PURE__ */ new WeakSet();
function jn(i) {
  return Date.parse(`${i}T00:00:00+08:00`);
}
function Kn(i) {
  return i.map((e) => ({
    timestamp: jn(e.trade_date),
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
function Ie(i, e) {
  return i.map(
    (t) => Object.fromEntries(e.map((r) => [r, t[r] ?? null]))
  );
}
function Jn(i) {
  vr.has(i) || (i({
    name: Zr,
    shortName: "API MA",
    series: "price",
    figures: dr.map((e) => ({
      key: e,
      title: `${e.toUpperCase()}: `,
      type: "line"
    })),
    calc: (e) => Ie(e, dr)
  }), i({
    name: $r,
    shortName: "成交量",
    series: "volume",
    shouldFormatBigNumber: !0,
    figures: [{ key: "volume", title: "VOL: ", type: "bar", baseValue: 0 }],
    calc: (e) => e.map((t) => ({
      volume: typeof t.volume == "number" && Number.isFinite(t.volume) ? t.volume : null
    }))
  }), i({
    name: jr,
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
    calc: (e) => Ie(e, $n)
  }), i({
    name: Kr,
    shortName: "API RSI14",
    figures: [{ key: "rsi14", title: "RSI14: ", type: "line" }],
    calc: (e) => Ie(e, ["rsi14"])
  }), vr.add(i));
}
const Qn = {
  init: (i) => {
    const e = qn(i, {
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
  dispose: (i) => {
    Zn(i);
  },
  registerIndicator: ia
};
function to(i, e, t = Qn) {
  const r = Kn(e.series);
  Jn(t.registerIndicator);
  const a = t.init(i);
  return a.setSymbol({
    ticker: e.symbol,
    pricePrecision: 4,
    volumePrecision: 0
  }), a.setPeriod({ span: 1, type: "day" }), a.setDataLoader({
    getBars: ({ callback: n }) => {
      n(r, { forward: !1, backward: !1 });
    }
  }), a.createIndicator({ name: Zr, paneId: "candle_pane" }, !0), a.createIndicator($r), a.createIndicator(jr), a.createIndicator(Kr), () => t.dispose(i);
}
const He = /^(sh|sz)\.[0-9]{6}$/;
function Jr(i) {
  return i === "portfolio" || i === "watchlists";
}
function eo(i, e) {
  if (!He.test(i)) throw new TypeError("invalid security symbol");
  return `#security/${encodeURIComponent(i)}?from=${e}`;
}
function ro(i) {
  const e = /^#security\/([^?]+)(?:\?(.*))?$/.exec(i);
  if (!e) return null;
  const t = decodeURIComponent(e[1]).toLowerCase(), r = new URLSearchParams(e[2] ?? "").get("from");
  return !He.test(t) || !Jr(r) ? null : { symbol: t, sourceView: r };
}
function io(i, e) {
  const t = (n) => {
    if (!(n instanceof Element)) return null;
    const o = n.closest("[data-security-symbol]");
    if (!o) return null;
    const s = o.dataset.securitySymbol?.toLowerCase() ?? "", l = o.dataset.securitySource ?? "";
    return !He.test(s) || !Jr(l) ? null : { symbol: s, sourceView: l };
  }, r = (n) => {
    const o = t(n.target);
    o && (n.preventDefault(), e(o.symbol, o.sourceView));
  }, a = (n) => {
    if (n.repeat || n.key !== "Enter" && n.key !== " ") return;
    const o = t(n.target);
    o && (n.preventDefault(), e(o.symbol, o.sourceView));
  };
  return i.addEventListener("click", r), i.addEventListener("keydown", a), () => {
    i.removeEventListener("click", r), i.removeEventListener("keydown", a);
  };
}
const ao = { phase: "idle" };
function no(i) {
  if (i.phase === "idle")
    throw new Error("cockpit response received before load");
  return { symbol: i.symbol, sourceView: i.sourceView };
}
function ge(i, e) {
  if (e.type === "load")
    return {
      phase: "loading",
      symbol: e.symbol,
      sourceView: e.sourceView
    };
  const t = no(i);
  if (e.type === "success") {
    if (e.response.status === "empty") {
      const r = e.response.quality_issues.includes(
        "no_effective_trading_data"
      ) ? "no_effective_trading_data" : "no_market_data";
      return {
        ...t,
        phase: "empty",
        reason: r,
        response: e.response
      };
    }
    return { ...t, phase: "ready", response: e.response };
  }
  return e.type === "empty" ? { ...t, phase: "empty", reason: e.reason } : e.status === 409 ? { ...t, phase: "quality-error", message: e.message } : { ...t, phase: "connection-error", message: e.message };
}
function z(i, e, t) {
  const r = document.createElement(i);
  return e !== void 0 && (r.textContent = e), t && (r.className = t), r;
}
function ft(i, e = 4) {
  return i === null ? "—" : new Intl.NumberFormat("zh-CN", {
    maximumFractionDigits: e
  }).format(i);
}
function oo(i) {
  return i === "portfolio" ? "持仓" : "自选预警";
}
function fr(i) {
  const e = z("dl", void 0, "security-metadata"), t = [
    ["证券", i.symbol],
    ["实际数据日", i.as_of ?? "—"],
    ["来源", i.source],
    ["价格口径", i.price_adjustment],
    ["公式版本", i.formula_version],
    ["状态", i.status],
    [
      "质量问题",
      i.quality_issues.length ? i.quality_issues.join("、") : "无"
    ]
  ];
  for (const [r, a] of t) {
    const n = z("div");
    n.append(z("dt", r), z("dd", a, "mono")), e.append(n);
  }
  return e;
}
function pr(i, e) {
  const t = z("article", void 0, "panel indicator-panel");
  t.append(z("h3", i));
  const r = z("dl", void 0, "indicator-values");
  for (const [a, n] of e) {
    const o = z("div");
    o.append(z("dt", a), z("dd", ft(n), "mono")), r.append(o);
  }
  return t.append(r), t;
}
function vt(i) {
  return z("td", i);
}
function so(i) {
  const e = z("div", void 0, "table-wrap security-table"), t = z("table");
  t.setAttribute("aria-label", "技术指标数值替代");
  const r = z(
    "caption",
    `最近 ${Math.min(i.length, 20)} 个有效交易日；完整图表共 ${i.length} 条`
  ), a = z("thead"), n = z("tr");
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
    const l = z("th", s);
    l.scope = "col", n.append(l);
  }
  a.append(n);
  const o = z("tbody");
  for (const s of i.slice(-20)) {
    const l = z("tr");
    l.append(
      vt(s.trade_date),
      vt(ft(s.open)),
      vt(ft(s.high)),
      vt(ft(s.low)),
      vt(ft(s.close)),
      vt(ft(s.volume, 0)),
      vt(ft(s.ma5)),
      vt(ft(s.ma10)),
      vt(ft(s.ma20)),
      vt(ft(s.ma60)),
      vt(ft(s.ma120)),
      vt(ft(s.ma250)),
      vt(ft(s.macd)),
      vt(ft(s.macd_signal)),
      vt(ft(s.macd_hist)),
      vt(ft(s.rsi14))
    ), o.append(l);
  }
  return t.append(r, a, o), e.append(t), e;
}
function lo(i, e) {
  const t = e === "no_effective_trading_data" ? "该窗口没有有效交易数据（记录可能全部为停牌占位）。" : "该窗口没有可用行情。", r = z("div", void 0, "panel security-state");
  r.append(
    z("p", "EMPTY / 真实空态", "panel-kicker"),
    z(
      "h2",
      e === "no_effective_trading_data" ? "没有有效交易数据" : "没有可用行情"
    ),
    z("p", t, "state-message")
  ), i.append(r);
}
function uo(i, e, t) {
  const r = i.querySelector("#security-back"), a = i.querySelector("#security-status"), n = i.querySelector("#security-content");
  if (!r || !a || !n)
    throw new Error("security cockpit root is incomplete");
  if (n.replaceChildren(), e.phase === "idle")
    return a.textContent = "尚未选择证券", null;
  if (r.href = `#${e.sourceView}`, r.dataset.viewTarget = e.sourceView, r.textContent = `← 返回${oo(e.sourceView)}`, e.phase === "loading") {
    a.textContent = `正在读取 ${e.symbol} 的交易日和后端分析…`;
    const v = z("div", void 0, "panel security-state");
    return v.append(
      z("p", "LOADING / 后端分析", "panel-kicker"),
      z("h2", e.symbol),
      z("p", "先读取有效交易日，再请求最多 260 日分析。")
    ), n.append(v), null;
  }
  if (e.phase === "empty")
    return a.textContent = `${e.symbol} 无可绘制数据`, e.response && n.append(fr(e.response)), lo(n, e.reason), null;
  if (e.phase === "quality-error") {
    a.textContent = `${e.symbol} 未通过数据质量门禁`;
    const v = z("div", void 0, "panel security-state");
    return v.append(
      z("p", "HTTP 409 / FAIL CLOSED", "panel-kicker"),
      z("h2", "数据质量门禁"),
      z("p", e.message, "state-message"),
      z("p", "未使用未复权数据降级，也未绘制蜡烛。", "state-message")
    ), n.append(v), null;
  }
  if (e.phase === "connection-error") {
    a.textContent = `${e.symbol} 连接失败`;
    const v = z("div", void 0, "panel security-state");
    return v.append(
      z("p", "CONNECTION ERROR / 本地服务", "panel-kicker"),
      z("h2", "无法连接本地 API"),
      z("p", e.message, "state-message"),
      z("p", "请确认 Stock EVA API 仅在本机运行。", "state-message")
    ), n.append(v), null;
  }
  const o = e.response;
  a.textContent = `${o.symbol} 已加载 ${o.series.length} 个有效交易日`, n.append(fr(o));
  const s = z("article", void 0, "panel security-chart-panel"), l = z("div", void 0, "panel-heading"), u = z("div");
  u.append(
    z("p", "QFQ DAILY / API INDICATORS", "panel-kicker"),
    z("h2", `${o.symbol} 技术驾驶舱`)
  ), l.append(u, z("span", `截至 ${o.as_of ?? "—"}`, "quiet-tag"));
  const h = z(
    "p",
    `前复权日 K、真实成交量和后端 MA，共 ${o.series.length} 个有效交易日。`,
    "state-message"
  ), c = z("div", void 0, "security-chart");
  c.id = "security-chart", c.dataset.testid = "security-chart", c.setAttribute("role", "img"), c.setAttribute(
    "aria-label",
    `${o.symbol} 前复权日 K、成交量、MA5、10、20、60、120、250、MACD、RSI14 图表`
  ), s.append(l, h, c), n.append(s);
  const d = o.series[o.series.length - 1], f = z("div", void 0, "indicator-grid");
  return f.append(
    pr("MACD（12, 26, 9）", [
      ["MACD", d.macd],
      ["Signal", d.macd_signal],
      ["Histogram", d.macd_hist]
    ]),
    pr("RSI（14）", [["RSI14", d.rsi14]])
  ), n.append(f, so(o.series)), t(c, o);
}
let Lt = ao, Re = null, _e = null;
function ho() {
  const i = document.querySelector("#security-analysis");
  if (!i) throw new Error("security cockpit section is missing");
  return i;
}
function gr() {
  _e?.(), _e = uo(
    ho(),
    Lt,
    to
  );
}
async function Qr(i, e) {
  Re?.abort();
  const t = new AbortController();
  Re = t, window.stockEvaActivateView?.("security", !1), Lt = ge(Lt, {
    type: "load",
    symbol: i,
    sourceView: e
  }), gr();
  try {
    const r = await ui(
      i,
      fetch,
      t.signal
    );
    Lt = ge(Lt, {
      type: "success",
      response: r
    });
  } catch (r) {
    if (t.signal.aborted) return;
    r instanceof _r ? Lt = ge(Lt, {
      type: "empty",
      reason: "no_market_data"
    }) : Lt = ge(Lt, {
      type: "failure",
      status: r instanceof Te ? r.status : null,
      message: r instanceof Te ? r.detail : r instanceof Error ? r.message : "未知连接错误"
    });
  }
  gr();
}
function co(i, e) {
  const t = eo(i, e);
  window.history.pushState(null, "", t), Qr(i, e);
}
function Se() {
  const i = ro(window.location.hash);
  i ? Qr(i.symbol, i.sourceView) : (Re?.abort(), _e?.(), _e = null);
}
function mr() {
  io(document, co), window.addEventListener("popstate", Se), window.addEventListener("hashchange", Se), Se();
}
document.readyState === "loading" ? document.addEventListener("DOMContentLoaded", mr, { once: !0 }) : mr();
