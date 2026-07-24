import hashlib
import json
from datetime import date
from typing import Any

from backend.app.market.models import DailyBar
from backend.app.market.store import MarketStore
from backend.app.strategy.dsl import ValidatedRule
from backend.app.strategy.models import SignalResult, StrategyEvaluation


class InsufficientData(ValueError):
    pass


class StrategyEngine:
    def __init__(self, market_store: MarketStore) -> None:
        self.market_store = market_store

    def evaluate(
        self,
        rule: ValidatedRule,
        *,
        symbols: list[str],
        as_of_date: date,
    ) -> StrategyEvaluation:
        unique_symbols = sorted(set(symbols))
        results: list[SignalResult] = []
        fingerprint_rows: list[dict[str, object]] = []
        available_dates = [
            item
            for item in self.market_store.available_dates(source="baostock")
            if item <= as_of_date
        ]

        for symbol in unique_symbols:
            bars = (
                self.market_store.symbol_bars(
                    symbol,
                    min(available_dates),
                    as_of_date,
                    source="baostock",
                )
                if available_dates
                else []
            )
            fingerprint_rows.extend(self._fingerprint_rows(bars))
            results.append(self._evaluate_symbol(rule, symbol, bars, as_of_date))

        canonical_input = json.dumps(
            fingerprint_rows,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return StrategyEvaluation(
            rule_hash=rule.rule_hash,
            as_of_date=as_of_date,
            data_fingerprint=hashlib.sha256(canonical_input.encode()).hexdigest(),
            results=results,
        )

    @staticmethod
    def _fingerprint_rows(bars: list[DailyBar]) -> list[dict[str, object]]:
        return [
            {
                "date": bar.trade_date.isoformat(),
                "symbol": bar.symbol,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "preclose": bar.preclose,
                "volume": bar.volume,
                "amount": bar.amount,
                "adjust_factor": bar.adjust_factor,
                "is_suspended": bar.is_suspended,
                "quality_status": bar.quality_status,
                "source": bar.source,
            }
            for bar in bars
        ]

    def _evaluate_symbol(
        self,
        rule: ValidatedRule,
        symbol: str,
        bars: list[DailyBar],
        as_of_date: date,
    ) -> SignalResult:
        signal_bar = next((bar for bar in bars if bar.trade_date == as_of_date), None)
        if signal_bar is None:
            return self._unavailable(symbol, as_of_date, "missing_signal_date", "insufficient")
        if signal_bar.is_suspended:
            return self._unavailable(symbol, as_of_date, "suspended_on_signal_date", "excluded")
        if signal_bar.adjust_factor is None:
            return self._unavailable(symbol, as_of_date, "missing_adjust_factor", "excluded")
        if signal_bar.quality_status != "ready":
            return self._unavailable(symbol, as_of_date, "quality_not_ready", "excluded")

        effective = [
            bar
            for bar in bars
            if bar.trade_date <= as_of_date
            and not bar.is_suspended
            and bar.adjust_factor is not None
            and bar.quality_status == "ready"
        ]
        try:
            matched, explanation = self._evaluate_node(rule.ast, effective, len(effective) - 1)
        except InsufficientData as exc:
            return SignalResult(
                symbol=symbol,
                status="insufficient_data",
                matched=None,
                signal_date=as_of_date,
                quality_status="insufficient",
                quality_issues=[str(exc)],
                explanation={
                    "status": "insufficient_data",
                    "data_date": as_of_date.isoformat(),
                    "quality_status": "insufficient",
                },
            )
        return SignalResult(
            symbol=symbol,
            status="matched" if matched else "not_matched",
            matched=matched,
            signal_date=as_of_date,
            quality_status="ready",
            quality_issues=[],
            explanation=explanation,
        )

    @staticmethod
    def _unavailable(
        symbol: str,
        signal_date: date,
        issue: str,
        kind: str,
    ) -> SignalResult:
        return SignalResult(
            symbol=symbol,
            status="excluded" if kind == "excluded" else "insufficient_data",
            matched=None,
            signal_date=signal_date,
            quality_status=kind,
            quality_issues=[issue],
            explanation={
                "status": kind,
                "data_date": signal_date.isoformat(),
                "quality_status": kind,
                "issue": issue,
            },
        )

    def _evaluate_node(
        self,
        node: dict[str, Any],
        bars: list[DailyBar],
        index: int,
    ) -> tuple[bool, dict[str, object]]:
        operation = node["op"]
        if operation in {"and", "or"}:
            children = [self._evaluate_node(child, bars, index) for child in node["args"]]
            values = [item[0] for item in children]
            matched = all(values) if operation == "and" else any(values)
            return matched, {
                "op": operation,
                "matched": matched,
                "data_date": bars[index].trade_date.isoformat(),
                "quality_status": "ready",
                "children": [item[1] for item in children],
            }
        if operation == "not":
            matched, child = self._evaluate_node(node["arg"], bars, index)
            return not matched, {
                "op": "not",
                "matched": not matched,
                "data_date": bars[index].trade_date.isoformat(),
                "quality_status": "ready",
                "child": child,
            }
        if operation == "compare":
            left = self._operand(node["left"], bars, index)
            right = self._operand(node["right"], bars, index)
            matched = self._compare(left["value"], node["operator"], right["value"])
            return matched, {
                "op": operation,
                "operator": node["operator"],
                "matched": matched,
                "data_date": bars[index].trade_date.isoformat(),
                "quality_status": "ready",
                "left": left,
                "right": right,
            }
        current_left = self._operand(node["left"], bars, index)
        current_right = self._operand(node["right"], bars, index)
        previous_left = self._operand(node["left"], bars, index - 1)
        previous_right = self._operand(node["right"], bars, index - 1)
        if operation == "crosses_above":
            matched = (
                previous_left["value"] <= previous_right["value"]
                and current_left["value"] > current_right["value"]
            )
        else:
            matched = (
                previous_left["value"] >= previous_right["value"]
                and current_left["value"] < current_right["value"]
            )
        return matched, {
            "op": operation,
            "matched": matched,
            "data_date": bars[index].trade_date.isoformat(),
            "quality_status": "ready",
            "previous": {
                "date": bars[index - 1].trade_date.isoformat(),
                "left": previous_left["value"],
                "right": previous_right["value"],
            },
            "current": {
                "date": bars[index].trade_date.isoformat(),
                "left": current_left["value"],
                "right": current_right["value"],
            },
            "left": current_left,
            "right": current_right,
        }

    @staticmethod
    def _compare(left: float, operator: str, right: float) -> bool:
        return {
            ">": left > right,
            ">=": left >= right,
            "<": left < right,
            "<=": left <= right,
            "==": left == right,
            "!=": left != right,
        }[operator]

    def _operand(
        self,
        operand: dict[str, Any],
        bars: list[DailyBar],
        index: int,
    ) -> dict[str, object]:
        if index < 0:
            raise InsufficientData("not_enough_effective_trading_days")
        operand_type = operand["type"]
        if operand_type == "constant":
            return {"operand": operand, "value": float(operand["value"]), "window": None}
        if operand_type == "field":
            value = self._field_value(bars[index], operand["name"])
            return {
                "operand": operand,
                "value": value,
                "window": {
                    "start": bars[index].trade_date.isoformat(),
                    "end": bars[index].trade_date.isoformat(),
                    "effective_days": 1,
                },
            }
        name = operand["name"]
        if name == "volume_ratio_5d":
            if index < 5:
                raise InsufficientData("volume_ratio_5d_requires_6_effective_days")
            prior = [bar.volume for bar in bars[index - 5 : index]]
            mean = sum(prior) / 5
            if mean == 0:
                raise InsufficientData("volume_ratio_5d_prior_mean_is_zero")
            return {
                "operand": operand,
                "value": bars[index].volume / mean,
                "window": {
                    "start": bars[index - 5].trade_date.isoformat(),
                    "end": bars[index].trade_date.isoformat(),
                    "prior_effective_days": 5,
                },
            }
        period = operand["period"]
        values = [self._field_value(bar, operand["field"]) for bar in bars]
        if name == "SMA":
            if index + 1 < period:
                raise InsufficientData(f"SMA({period}) requires {period} effective days")
            value = sum(values[index - period + 1 : index + 1]) / period
            start_index = index - period + 1
        elif name == "EMA":
            if index + 1 < period:
                raise InsufficientData(f"EMA({period}) requires {period} effective days")
            value = sum(values[:period]) / period
            alpha = 2 / (period + 1)
            for current in values[period : index + 1]:
                value = alpha * current + (1 - alpha) * value
            start_index = 0
        else:
            if index < period:
                raise InsufficientData(f"RSI({period}) requires {period + 1} effective days")
            changes = [
                values[position] - values[position - 1]
                for position in range(index - period + 1, index + 1)
            ]
            gains = sum(max(change, 0) for change in changes) / period
            losses = sum(max(-change, 0) for change in changes) / period
            value = 50.0 if gains == losses == 0 else 100.0 if losses == 0 else 100 - 100 / (
                1 + gains / losses
            )
            start_index = index - period
        return {
            "operand": operand,
            "value": value,
            "window": {
                "start": bars[start_index].trade_date.isoformat(),
                "end": bars[index].trade_date.isoformat(),
                "effective_days": index - start_index + 1,
            },
        }

    @staticmethod
    def _field_value(bar: DailyBar, field: str) -> float:
        value = float(getattr(bar, field))
        if field in {"open", "high", "low", "close", "preclose"}:
            return value * float(bar.adjust_factor)
        return value
