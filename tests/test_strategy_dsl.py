import copy

import pytest

from backend.app.strategy.dsl import RuleValidationError, validate_rule


def ma_cross_rule() -> dict[str, object]:
    return {
        "op": "crosses_above",
        "left": {
            "type": "indicator",
            "name": "SMA",
            "field": "close",
            "period": 5,
        },
        "right": {
            "type": "indicator",
            "name": "SMA",
            "field": "close",
            "period": 20,
        },
    }


def test_valid_rule_has_stable_canonical_hash() -> None:
    rule = ma_cross_rule()
    reordered = {
        "right": rule["right"],
        "left": rule["left"],
        "op": rule["op"],
    }

    first = validate_rule(rule)
    second = validate_rule(reordered)

    assert first.depth == 1
    assert first.predicate_count == 1
    assert first.rule_hash == second.rule_hash
    assert first.canonical_json == second.canonical_json


def test_validated_rule_is_detached_from_mutable_input() -> None:
    source = ma_cross_rule()
    validated = validate_rule(source)

    source["op"] = "eval"

    assert validated.ast["op"] == "crosses_above"


@pytest.mark.parametrize(
    "mutation",
    [
        lambda rule: rule.update({"op": "python"}),
        lambda rule: rule["left"].update({"period": 1}),
        lambda rule: rule["left"].update({"period": 251}),
        lambda rule: rule["left"].update({"field": "__class__"}),
        lambda rule: rule["left"].update({"source": "close > 0"}),
        lambda rule: rule.update({"code": "eval('1')"}),
    ],
)
def test_rule_rejects_unknown_operations_fields_periods_and_extra_keys(mutation) -> None:
    rule = copy.deepcopy(ma_cross_rule())
    mutation(rule)

    with pytest.raises(RuleValidationError):
        validate_rule(rule)


def test_rule_rejects_depth_above_five() -> None:
    rule: dict[str, object] = ma_cross_rule()
    for _ in range(5):
        rule = {"op": "not", "arg": rule}

    with pytest.raises(RuleValidationError, match="depth"):
        validate_rule(rule)


def test_rule_rejects_more_than_twenty_predicates() -> None:
    predicate = {
        "op": "compare",
        "left": {"type": "field", "name": "close"},
        "operator": ">",
        "right": {"type": "constant", "value": 0},
    }
    rule = {"op": "and", "args": [copy.deepcopy(predicate) for _ in range(21)]}

    with pytest.raises(RuleValidationError, match="predicate"):
        validate_rule(rule)


def test_only_whitelisted_indicator_shapes_are_accepted() -> None:
    valid = {
        "op": "compare",
        "left": {"type": "indicator", "name": "volume_ratio_5d"},
        "operator": ">",
        "right": {"type": "constant", "value": 1},
    }
    assert validate_rule(valid).predicate_count == 1

    invalid = copy.deepcopy(valid)
    invalid["left"]["period"] = 5
    with pytest.raises(RuleValidationError):
        validate_rule(invalid)
