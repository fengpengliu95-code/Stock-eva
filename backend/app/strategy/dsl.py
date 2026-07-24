import hashlib
import json
import math
from dataclasses import dataclass
from typing import Any

MAX_DEPTH = 5
MAX_PREDICATES = 20
ALLOWED_FIELDS = {"open", "high", "low", "close", "preclose", "volume", "amount"}
COMPARE_OPERATORS = {">", ">=", "<", "<=", "==", "!="}


class RuleValidationError(ValueError):
    pass


@dataclass(frozen=True)
class ValidatedRule:
    ast: dict[str, Any]
    canonical_json: str
    rule_hash: str
    depth: int
    predicate_count: int


def _exact_keys(value: dict[str, Any], required: set[str]) -> None:
    if set(value) != required:
        raise RuleValidationError(
            f"expected keys {sorted(required)}, got {sorted(value)}"
        )


def _validate_operand(value: object) -> None:
    if not isinstance(value, dict):
        raise RuleValidationError("operand must be an object")
    operand_type = value.get("type")
    if operand_type == "field":
        _exact_keys(value, {"type", "name"})
        if value["name"] not in ALLOWED_FIELDS:
            raise RuleValidationError("field is not allowed")
        return
    if operand_type == "constant":
        _exact_keys(value, {"type", "value"})
        constant = value["value"]
        if isinstance(constant, bool) or not isinstance(constant, int | float):
            raise RuleValidationError("constant must be numeric")
        if not math.isfinite(float(constant)):
            raise RuleValidationError("constant must be finite")
        return
    if operand_type == "indicator":
        name = value.get("name")
        if name == "volume_ratio_5d":
            _exact_keys(value, {"type", "name"})
            return
        if name not in {"SMA", "EMA", "RSI"}:
            raise RuleValidationError("indicator is not allowed")
        _exact_keys(value, {"type", "name", "field", "period"})
        if value["field"] != "close":
            raise RuleValidationError("first-version indicators require close")
        period = value["period"]
        if isinstance(period, bool) or not isinstance(period, int) or not 2 <= period <= 250:
            raise RuleValidationError("indicator period must be between 2 and 250")
        return
    raise RuleValidationError("operand type is not allowed")


def _validate_node(node: object, depth: int) -> tuple[int, int]:
    if depth > MAX_DEPTH:
        raise RuleValidationError(f"rule depth exceeds {MAX_DEPTH}")
    if not isinstance(node, dict):
        raise RuleValidationError("rule node must be an object")
    operation = node.get("op")
    if operation in {"and", "or"}:
        _exact_keys(node, {"op", "args"})
        args = node["args"]
        if not isinstance(args, list) or len(args) < 2:
            raise RuleValidationError(f"{operation} requires at least two arguments")
        child_stats = [_validate_node(child, depth + 1) for child in args]
        return max(item[0] for item in child_stats), sum(item[1] for item in child_stats)
    if operation == "not":
        _exact_keys(node, {"op", "arg"})
        return _validate_node(node["arg"], depth + 1)
    if operation == "compare":
        _exact_keys(node, {"op", "left", "operator", "right"})
        if node["operator"] not in COMPARE_OPERATORS:
            raise RuleValidationError("comparison operator is not allowed")
        _validate_operand(node["left"])
        _validate_operand(node["right"])
        return depth, 1
    if operation in {"crosses_above", "crosses_below"}:
        _exact_keys(node, {"op", "left", "right"})
        _validate_operand(node["left"])
        _validate_operand(node["right"])
        return depth, 1
    raise RuleValidationError("operation is not allowed")


def validate_rule(ast: object) -> ValidatedRule:
    depth, predicates = _validate_node(ast, 1)
    if predicates > MAX_PREDICATES:
        raise RuleValidationError(f"predicate count exceeds {MAX_PREDICATES}")
    canonical = json.dumps(ast, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return ValidatedRule(
        ast=json.loads(canonical),
        canonical_json=canonical,
        rule_hash=hashlib.sha256(canonical.encode()).hexdigest(),
        depth=depth,
        predicate_count=predicates,
    )
