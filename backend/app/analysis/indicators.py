from collections.abc import Callable
from dataclasses import dataclass

import numpy as np
import talib

FORMULA_VERSION = "ta-lib-0.7.0-r0-v1"

IndicatorCalculator = Callable[[np.ndarray], tuple[np.ndarray, ...]]


@dataclass(frozen=True)
class IndicatorDefinition:
    output_fields: tuple[str, ...]
    parameters: dict[str, int]
    warmup_periods: int
    calculate: IndicatorCalculator


def _sma(period: int) -> IndicatorDefinition:
    return IndicatorDefinition(
        output_fields=(f"ma{period}",),
        parameters={"timeperiod": period},
        warmup_periods=period - 1,
        calculate=lambda close: (talib.SMA(close, timeperiod=period),),
    )


INDICATOR_REGISTRY: dict[str, IndicatorDefinition] = {
    "ma5": _sma(5),
    "ma10": _sma(10),
    "ma20": _sma(20),
    "ma60": _sma(60),
    "ma120": _sma(120),
    "ma250": _sma(250),
    "macd": IndicatorDefinition(
        output_fields=("macd", "macd_signal", "macd_hist"),
        parameters={"fastperiod": 12, "slowperiod": 26, "signalperiod": 9},
        warmup_periods=33,
        calculate=lambda close: talib.MACD(
            close,
            fastperiod=12,
            slowperiod=26,
            signalperiod=9,
        ),
    ),
    "rsi14": IndicatorDefinition(
        output_fields=("rsi14",),
        parameters={"timeperiod": 14},
        warmup_periods=14,
        calculate=lambda close: (talib.RSI(close, timeperiod=14),),
    ),
}


def calculate_indicators(closes: list[float]) -> dict[str, list[float | None]]:
    close = np.asarray(closes, dtype=np.float64)
    result: dict[str, list[float | None]] = {}
    for definition in INDICATOR_REGISTRY.values():
        outputs = definition.calculate(close)
        for field, values in zip(definition.output_fields, outputs, strict=True):
            result[field] = [None if np.isnan(value) else float(value) for value in values]
    return result
