"""RED tests for the R2-F4.1 promoted calendar authority."""

import copy
import fcntl
import hashlib
import io
import json
import logging
import multiprocessing
import os
import shutil
import sqlite3
import warnings
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from pydantic import BaseModel

from backend.app.market import calendar_generation as calendar_module
from backend.app.market.calendar_generation import (
    SCHEMA_SHA256,
    CalendarGenerationStore,
    CalendarGenerationV1,
    CalendarMachineDayV1,
    CalendarMachineObservationV1,
    CalendarMaintenanceAttempt,
    CalendarSourceBundleV1,
    CalendarStoreUnavailable,
    OfficialCalendarScheduleV1,
    body_sha256,
    build_generation_sha256,
    build_observation,
    build_observation_sha256,
    build_schedule,
    build_schedule_sha256,
    build_source_sha256,
    bundled_sha256,
    canonical_json_bytes,
    domain_sha256,
    load_source,
)


def _schedule(exchange: str, closed: tuple[date, ...], body: bytes) -> OfficialCalendarScheduleV1:
    return OfficialCalendarScheduleV1(
        exchange=exchange,
        year=2027,
        coverage_start=date(2027, 1, 1),
        coverage_end=date(2027, 12, 31),
        title="Reviewed calendar",
        notice_no="review-2027",
        official_url=f"https://{'www.sse.com.cn' if exchange == 'SSE' else 'www.szse.cn'}/notice",
        published_on=date(2026, 12, 20),
        body_sha256=body_sha256(body),
        closed_dates=closed,
        review_id="reviewer-1",
        reviewed_on=datetime(2026, 12, 21, 1, tzinfo=UTC),
        schedule_sha256="0" * 64,
    )


def _source() -> CalendarSourceBundleV1:
    sse = _schedule("SSE", (date(2027, 1, 1),), b"sse body")
    szse = _schedule("SZSE", (date(2027, 1, 1),), b"szse body")
    return CalendarSourceBundleV1(
        schema_version=1,
        rule_version="cn-a-share-weekends-closed-v1",
        year=2027,
        schedules=(sse, szse),
        source_sha256="0" * 64,
    )


def _valid_source() -> CalendarSourceBundleV1:
    raw = _source()
    schedules = tuple(
        schedule.model_copy(update={"schedule_sha256": build_schedule_sha256(schedule)})
        for schedule in raw.schedules
    )
    with_schedules = raw.model_copy(update={"schedules": schedules})
    return with_schedules.model_copy(update={"source_sha256": build_source_sha256(with_schedules)})


def _bundled_2026_revision() -> CalendarSourceBundleV1:
    """Build a 2026 source from the checked-in calendar plus one future closure."""
    config_path = (
        Path(__file__).parents[1]
        / "backend"
        / "app"
        / "market"
        / "calendars"
        / "cn_a_share_2026.json"
    )
    config = json.loads(config_path.read_text(encoding="utf-8"))
    closed = tuple(date.fromisoformat(value) for value in config["closed_dates"])
    assert all(value.weekday() < 5 for value in closed)
    future_closure = date(2026, 12, 23)
    assert future_closure.weekday() < 5
    assert future_closure not in closed
    closed = closed + (future_closure,)
    bodies = (b"bundled-2026-sse-revision", b"bundled-2026-szse-revision")
    schedules = tuple(
        OfficialCalendarScheduleV1(
            exchange=metadata["exchange"],
            year=2026,
            coverage_start=date(2026, 1, 1),
            coverage_end=date(2026, 12, 31),
            title=metadata["title"],
            notice_no=metadata["notice_no"],
            official_url=metadata["url"],
            published_on=date.fromisoformat(config["published_on"]),
            body_sha256=body_sha256(body),
            closed_dates=closed,
            review_id="independent-r2f4-1-review",
            reviewed_on=datetime(2026, 12, 21, 1, tzinfo=UTC),
            schedule_sha256="0" * 64,
        )
        for metadata, body in zip(config["sources"], bodies, strict=True)
    )
    schedules = tuple(
        schedule.model_copy(update={"schedule_sha256": build_schedule_sha256(schedule)})
        for schedule in schedules
    )
    source = CalendarSourceBundleV1(
        schema_version=1,
        rule_version="cn-a-share-weekends-closed-v1",
        year=2026,
        schedules=schedules,
        source_sha256="0" * 64,
    )
    return source.model_copy(update={"source_sha256": build_source_sha256(source)})


def _machine_for(
    source: CalendarSourceBundleV1, observed_at: datetime
) -> CalendarMachineObservationV1:
    start, end = date(source.year, 1, 1), date(source.year, 12, 31)
    closed = set(source.schedules[0].closed_dates)
    days = tuple(
        CalendarMachineDayV1(
            date=start + timedelta(days=offset),
            is_open=(start + timedelta(days=offset)).weekday() < 5
            and start + timedelta(days=offset) not in closed,
        )
        for offset in range((end - start).days + 1)
    )
    return build_observation(
        provider="baostock",
        contract_version="r2f4.1-baostock-calendar-days-v1",
        range_start=start,
        range_end=end,
        observed_at=observed_at,
        days=days,
    )


def _promote_old_2026_authority(
    tmp_path: Path,
) -> tuple[Path, CalendarGenerationStore, CalendarSourceBundleV1, datetime]:
    now = datetime(2026, 12, 22, 1, tzinfo=UTC)
    source = _bundled_2026_revision()
    store = CalendarGenerationStore(tmp_path / "calendar_generations.sqlite3")
    assert store.stage_execute(source, now).outcome == "STAGED"
    attempt = store.reserve_attempt(source, now)
    result = store.promote(
        attempt,
        source,
        (b"bundled-2026-sse-revision", b"bundled-2026-szse-revision"),
        _machine_for(source, now),
        now + timedelta(hours=1),
    )
    assert result.outcome == "PROMOTED"
    assert store.read().generation is not None
    return store.path, store, source, now


def _canonical_tree_snapshot(root: Path) -> tuple[tuple[str, str, bytes | None], ...]:
    return tuple(
        (
            str(path.relative_to(root)),
            "directory" if path.is_dir() else "file",
            None if path.is_dir() else path.read_bytes(),
        )
        for path in sorted(root.rglob("*"))
    )


def _source_for_year(
    year: int,
    closed: tuple[date, ...],
    published_on: date = date(2026, 12, 20),
) -> CalendarSourceBundleV1:
    bodies = (f"synthetic-{year}-sse".encode(), f"synthetic-{year}-szse".encode())
    schedules = tuple(
        OfficialCalendarScheduleV1(
            exchange=exchange,
            year=year,
            coverage_start=date(year, 1, 1),
            coverage_end=date(year, 12, 31),
            title=f"Synthetic reviewed {year}",
            notice_no=f"synthetic-{year}",
            official_url=f"https://{host}/synthetic/{year}",
            published_on=published_on,
            body_sha256=body_sha256(body),
            closed_dates=closed,
            review_id="d2-independent-review",
            reviewed_on=datetime(2026, 12, 21, 1, tzinfo=UTC),
            schedule_sha256="0" * 64,
        )
        for exchange, host, body in zip(
            ("SSE", "SZSE"), ("www.sse.com.cn", "www.szse.cn"), bodies, strict=True
        )
    )
    schedules = tuple(
        schedule.model_copy(update={"schedule_sha256": build_schedule_sha256(schedule)})
        for schedule in schedules
    )
    source = CalendarSourceBundleV1(
        schema_version=1,
        rule_version="cn-a-share-weekends-closed-v1",
        year=year,
        schedules=schedules,
        source_sha256="0" * 64,
    )
    return source.model_copy(update={"source_sha256": build_source_sha256(source)})


def _tamper_triggered_update(
    path: Path,
    trigger_name: str,
    update_sql: str,
    parameters: tuple[object, ...],
) -> str:
    """Apply one private SQL tamper and restore the exact immutable trigger SQL."""
    connection = sqlite3.connect(path)
    trigger_row = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?", (trigger_name,)
    ).fetchone()
    assert trigger_row is not None
    trigger_sql = trigger_row[0]
    connection.execute(f'DROP TRIGGER "{trigger_name}"')
    try:
        connection.execute(update_sql, parameters)
    finally:
        connection.execute(trigger_sql)
    restored = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?", (trigger_name,)
    ).fetchone()
    assert restored is not None and restored[0] == trigger_sql
    connection.commit()
    connection.close()
    return trigger_sql


def _assert_repeated_unavailable(store: CalendarGenerationStore, root: Path) -> None:
    before_tree = _canonical_tree_snapshot(root)
    db_path = store.path
    before_bytes = db_path.read_bytes()
    before_mtime = db_path.stat().st_mtime_ns
    for _ in range(2):
        assert store.read().status == "unavailable"
        assert _canonical_tree_snapshot(root) == before_tree
        assert db_path.read_bytes() == before_bytes
        assert db_path.stat().st_mtime_ns == before_mtime


def _promote_two_generations(tmp_path: Path) -> tuple[Path, CalendarGenerationStore, datetime]:
    now = datetime(2026, 12, 22, 1, tzinfo=UTC)
    source_2026 = _bundled_2026_revision()
    source_2027 = _valid_source()
    path = tmp_path / "calendar_generations.sqlite3"
    store = CalendarGenerationStore(path)
    assert store.stage_execute(source_2026, now).outcome == "STAGED"
    assert store.stage_execute(source_2027, now).outcome == "STAGED"
    attempt_2026 = store.reserve_attempt(source_2026, now)
    assert (
        store.promote(
            attempt_2026,
            source_2026,
            (b"bundled-2026-sse-revision", b"bundled-2026-szse-revision"),
            _machine_for(source_2026, now),
            now + timedelta(hours=1),
        ).outcome
        == "PROMOTED"
    )
    attempt_2027 = store.reserve_attempt(source_2027, now)
    assert (
        store.promote(
            attempt_2027,
            source_2027,
            (b"sse body", b"szse body"),
            _machine_for(source_2027, now),
            now + timedelta(hours=2),
        ).outcome
        == "PROMOTED"
    )
    assert store.read().generation is not None and store.read().generation.sequence == 2
    return path, store, now


def _manual_promotion(
    tmp_path: Path,
    source: CalendarSourceBundleV1,
    now: datetime,
    target_year: int,
    promoted_at: datetime,
    bodies: tuple[bytes, bytes] | None = None,
) -> tuple[Path, CalendarGenerationV1]:
    path = tmp_path / "calendar_generations.sqlite3"
    store = CalendarGenerationStore(path)
    assert store.stage_execute(source, now).outcome == "STAGED"
    attempt = store.reserve_attempt(source, now, target_year=target_year)
    machine = _machine_for(source, now)
    if bodies is None:
        bodies = tuple(
            f"synthetic-{source.year}-{exchange.lower()}".encode() for exchange in ("SSE", "SZSE")
        )
    assert tuple(body_sha256(body) for body in bodies) == tuple(
        schedule.body_sha256 for schedule in source.schedules
    )
    connection = sqlite3.connect(path)
    connection.execute(
        "UPDATE calendar_maintenance_attempt SET finished_at=?,outcome='PROMOTED',"
        "official_requests=2,machine_requests=1 WHERE target_year=? AND slot_date=?",
        (
            promoted_at.isoformat(timespec="microseconds").replace("+00:00", "Z"),
            target_year,
            attempt.slot_date.isoformat(),
        ),
    )
    for body in bodies:
        connection.execute(
            "INSERT INTO calendar_official_object(body_sha256,body_bytes) VALUES (?,?)",
            (body_sha256(body), body),
        )
    generation0 = CalendarGenerationV1(
        sequence=1,
        parent_sha256=None,
        bundled_sha256=bundled_sha256(),
        source_sha256=source.source_sha256,
        attempt_target_year=target_year,
        attempt_slot_date=now.date(),
        official_body_hashes=tuple(body_sha256(body) for body in bodies),
        machine=machine,
        promoted_at=promoted_at,
        generation_sha256="0" * 64,
    )
    generation = generation0.model_copy(
        update={"generation_sha256": build_generation_sha256(generation0)}
    )
    payload = canonical_json_bytes(generation.model_dump(mode="json")).decode()
    connection.execute(
        "INSERT INTO calendar_generation_promotion VALUES (?,?,?,?,?,?,?,?)",
        (
            1,
            generation.generation_sha256,
            None,
            source.source_sha256,
            target_year,
            now.date().isoformat(),
            payload,
            promoted_at.isoformat(timespec="microseconds").replace("+00:00", "Z"),
        ),
    )
    connection.execute(
        "UPDATE calendar_generation_head SET sequence=1,generation_sha256=? WHERE singleton=1",
        (generation.generation_sha256,),
    )
    connection.commit()
    connection.close()
    return path, generation


def test_hash_vectors_use_exact_projection_and_raw_body() -> None:
    assert canonical_json_bytes({"zero": 0, "false": False, "null": None, "empty": []}) == (
        b'{"empty":[],"false":false,"null":null,"zero":0}\n'
    )
    assert body_sha256(b"abc") == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    source = _valid_source()
    assert build_schedule_sha256(source.schedules[0]) != "0" * 64
    assert build_source_sha256(source) != "0" * 64
    assert source.schedules[0].schedule_sha256 == (
        "8c8994f04d2b8d3b03db9a1cedca85777efe7e669e7b9462294434ac50380bf8"
    )
    assert source.schedules[1].schedule_sha256 == (
        "6449fe372ba112d68995e830f2724abff92a4fd3a5e004159058f9ac8fd97066"
    )
    assert source.source_sha256 == (
        "cb5859c257544781d3630de74fe02142c443bed8b4ef4d144b1ebb2d9d2039bb"
    )


def test_machine_generation_bundled_and_schema_vectors_are_frozen() -> None:
    now = datetime(2026, 12, 22, 1, tzinfo=UTC)
    source = _valid_source()
    machine = _machine_for(source, now)
    generation0 = CalendarGenerationV1(
        sequence=1,
        parent_sha256=None,
        bundled_sha256="78b3f37eabcc081be52ae7b529176e1ff403a2c188a3ecb4fafb11fe0b142a7e",
        source_sha256=source.source_sha256,
        attempt_target_year=2027,
        attempt_slot_date=now.date(),
        official_body_hashes=tuple(schedule.body_sha256 for schedule in source.schedules),
        machine=machine,
        promoted_at=datetime(2026, 12, 22, 2, tzinfo=UTC),
        generation_sha256="0" * 64,
    )
    generation = generation0.model_copy(
        update={
            "generation_sha256": "093ff54f0a85ef229576a7a0b3327231379c09d744b6ce32496918cdb8207f8f"
        }
    )
    assert machine.observation_sha256 == (
        "5f46ebfc42ed592cebcd4be769a24c08544398df8e6fa682b9299d7abab6c900"
    )
    assert build_observation_sha256(machine) == machine.observation_sha256
    assert build_generation_sha256(generation) == (
        "093ff54f0a85ef229576a7a0b3327231379c09d744b6ce32496918cdb8207f8f"
    )
    assert bundled_sha256() == ("78b3f37eabcc081be52ae7b529176e1ff403a2c188a3ecb4fafb11fe0b142a7e")
    assert SCHEMA_SHA256 == ("cce5586b0a2294ae9f5f754be9e779507f12244f5030d2837b82dbc04ed62c1c")


def test_store_is_lazy_and_stage_never_promotes(tmp_path) -> None:
    store = CalendarGenerationStore(tmp_path / "calendar_generations.sqlite3")
    assert not (tmp_path / "calendar_generations.sqlite3").exists()
    source = _valid_source()
    now = datetime(2026, 12, 22, 1, tzinfo=UTC)
    plan = store.plan_stage(source, now)
    assert plan.outcome == "STAGED"
    assert not (tmp_path / "calendar_generations.sqlite3").exists()
    result = store.stage_execute(source, now)
    assert result.outcome == "STAGED"
    assert store.read().generation is None


def test_repeated_stage_is_idempotent_and_empty_store_uses_bundled_calendar(tmp_path) -> None:
    source = _valid_source()
    store = CalendarGenerationStore(tmp_path / "calendar_generations.sqlite3")
    now = datetime(2026, 12, 22, 1, tzinfo=UTC)
    first = store.stage_execute(source, now)
    second = store.stage_execute(source, now)
    assert first.source_sha256 == second.source_sha256
    assert second.outcome == "ALREADY_STAGED"
    assert store.read().bundled is not None
    assert store.read().generation is None


def test_reader_rejects_candidate_with_inner_digest_tamper_even_if_outer_hash_matches(
    tmp_path,
) -> None:
    now = datetime(2026, 12, 22, 1, tzinfo=UTC)
    source = _valid_source()
    path = tmp_path / "calendar_generations.sqlite3"
    store = CalendarGenerationStore(path)
    assert store.stage_execute(source, now).outcome == "STAGED"
    with sqlite3.connect(path) as connection:
        row = connection.execute(
            "SELECT source_sha256,payload_json FROM calendar_generation_candidate"
        ).fetchone()
    assert row is not None
    payload = json.loads(row[1])
    payload["schedules"][0]["schedule_sha256"] = "f" * 64
    payload["source_sha256"] = "0" * 64
    payload["source_sha256"] = domain_sha256(
        "stock-eva/r2f4.1/calendar-source/v1",
        {key: value for key, value in payload.items() if key != "source_sha256"},
    )
    mutated_payload = canonical_json_bytes(payload).decode()
    assert payload["source_sha256"] != row[0]
    _tamper_triggered_update(
        path,
        "calendar_candidate_no_update",
        "UPDATE calendar_generation_candidate SET source_sha256=?,payload_json=? "
        "WHERE source_sha256=?",
        (payload["source_sha256"], mutated_payload, row[0]),
    )
    _assert_repeated_unavailable(store, tmp_path)


def test_reader_rejects_candidate_quarantine_admission_tamper(tmp_path) -> None:
    now = datetime(2026, 12, 22, 1, tzinfo=UTC)
    source = _valid_source()
    changed = source.schedules[1].model_copy(
        update={"closed_dates": (date(2027, 1, 1), date(2027, 1, 4)), "schedule_sha256": "0" * 64}
    )
    changed = changed.model_copy(update={"schedule_sha256": build_schedule_sha256(changed)})
    conflict = source.model_copy(
        update={"schedules": (source.schedules[0], changed), "source_sha256": "0" * 64}
    )
    conflict = conflict.model_copy(update={"source_sha256": build_source_sha256(conflict)})
    path = tmp_path / "calendar_generations.sqlite3"
    store = CalendarGenerationStore(path)
    assert store.stage_execute(conflict, now).outcome == "SOURCE_CONFLICT"
    with sqlite3.connect(path) as connection:
        row = connection.execute(
            "SELECT source_sha256,admission,reason FROM calendar_generation_candidate"
        ).fetchone()
    assert row is not None and (row[1], row[2]) == ("quarantined", "SOURCE_CONFLICT")
    _tamper_triggered_update(
        path,
        "calendar_candidate_no_update",
        "UPDATE calendar_generation_candidate SET admission='awaiting_machine',reason='STAGED' "
        "WHERE source_sha256=?",
        (row[0],),
    )
    _assert_repeated_unavailable(store, tmp_path)


def test_reader_rejects_candidate_staged_before_source_review(tmp_path) -> None:
    now = datetime(2026, 12, 22, 1, tzinfo=UTC)
    source = _valid_source()
    path = tmp_path / "calendar_generations.sqlite3"
    store = CalendarGenerationStore(path)
    assert store.stage_execute(source, now).outcome == "STAGED"
    _tamper_triggered_update(
        path,
        "calendar_candidate_no_update",
        "UPDATE calendar_generation_candidate SET staged_at=? WHERE source_sha256=?",
        ("2025-01-01T00:00:00.000000Z", source.source_sha256),
    )
    _assert_repeated_unavailable(store, tmp_path)


def test_invalid_source_rejected_before_store_initialization(tmp_path) -> None:
    source = _valid_source().model_copy(update={"source_sha256": "0" * 64})
    store = CalendarGenerationStore(tmp_path / "calendar_generations.sqlite3")
    with pytest.raises(ValueError):
        store.plan_stage(source, datetime(2026, 12, 22, 1, tzinfo=UTC))
    assert not (tmp_path / "calendar_generations.sqlite3").exists()


def test_disagreeing_valid_schedules_are_quarantined_without_promotion(tmp_path) -> None:
    valid = _valid_source()
    changed = valid.schedules[1].model_copy(
        update={"closed_dates": (date(2027, 1, 1), date(2027, 1, 4)), "schedule_sha256": "0" * 64}
    )
    changed = changed.model_copy(update={"schedule_sha256": build_schedule_sha256(changed)})
    conflict = valid.model_copy(
        update={"schedules": (valid.schedules[0], changed), "source_sha256": "0" * 64}
    )
    conflict = conflict.model_copy(update={"source_sha256": build_source_sha256(conflict)})
    store = CalendarGenerationStore(tmp_path / "calendar_generations.sqlite3")
    result = store.stage_execute(conflict, datetime(2026, 12, 22, 1, tzinfo=UTC))
    assert result.outcome == "SOURCE_CONFLICT"
    assert result.admission == "quarantined"
    assert store.read().generation is None


def test_next_year_promotion_advances_generation_without_changing_bundled_base(tmp_path) -> None:
    source = _valid_source()
    now = datetime(2026, 12, 22, 1, tzinfo=UTC)
    store = CalendarGenerationStore(tmp_path / "calendar_generations.sqlite3")
    store.stage_execute(source, now)
    attempt = store.reserve_attempt(source, now)
    start, end = date(2027, 1, 1), date(2027, 12, 31)
    days = tuple(
        CalendarMachineDayV1(
            date=start + timedelta(days=i),
            is_open=(start + timedelta(days=i)).weekday() < 5
            and start + timedelta(days=i) != date(2027, 1, 1),
        )
        for i in range((end - start).days + 1)
    )
    machine = build_observation(
        provider="baostock",
        contract_version="r2f4.1-baostock-calendar-days-v1",
        range_start=start,
        range_end=end,
        observed_at=now,
        days=days,
    )
    promoted = store.promote(
        attempt, source, (b"sse body", b"szse body"), machine, now + timedelta(hours=1)
    )
    assert promoted.outcome == "PROMOTED"
    result = store.read()
    assert result.status == "ready"
    assert result.generation is not None and result.generation.sequence == 1
    assert result.bundled is not None
    assert result.calendar is not None
    assert result.calendar.bundled_sha256 == result.generation.bundled_sha256
    assert result.calendar.generation_sha256 == result.generation.generation_sha256


def test_reader_rejects_schema_guard_removal(tmp_path) -> None:
    source = _valid_source()
    path = tmp_path / "calendar_generations.sqlite3"
    store = CalendarGenerationStore(path)
    store.stage_execute(source, datetime(2026, 12, 22, 1, tzinfo=UTC))
    import sqlite3

    with sqlite3.connect(path) as connection:
        connection.execute("DROP TRIGGER calendar_candidate_no_delete")
    assert store.read().status == "unavailable"


def test_reader_rejects_symlink_store(tmp_path) -> None:
    source = _valid_source()
    real = tmp_path / "real.sqlite3"
    CalendarGenerationStore(real).stage_execute(source, datetime(2026, 12, 22, 1, tzinfo=UTC))
    alias = tmp_path / "alias.sqlite3"
    alias.symlink_to(real)
    assert CalendarGenerationStore(alias).read().status == "unavailable"


def test_reservation_never_initializes_missing_store(tmp_path) -> None:
    store = CalendarGenerationStore(tmp_path / "absent.sqlite3")
    with pytest.raises(CalendarStoreUnavailable):
        store.reserve_attempt(_valid_source(), datetime(2026, 12, 22, 1, tzinfo=UTC))
    assert not (tmp_path / "absent.sqlite3").exists()
    assert not (tmp_path / "absent.sqlite3.lock").exists()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("year", True),
        ("closed_dates", (date(2027, 1, 3), date(2027, 1, 2))),
        ("closed_dates", (date(2027, 1, 2), date(2027, 1, 2))),
        ("closed_dates", (date(2027, 1, 2),)),
    ],
)
def test_schedule_rejects_strict_or_invalid_identity_fields(field, value) -> None:
    data = _schedule("SSE", (date(2027, 1, 1),), b"body").model_dump(mode="python")
    data[field] = value
    with pytest.raises(ValueError):
        OfficialCalendarScheduleV1(**data)


def test_model_extra_and_naive_review_are_rejected() -> None:
    data = _schedule("SSE", (date(2027, 1, 1),), b"body").model_dump(mode="python")
    data["extra"] = 1
    with pytest.raises(ValueError):
        OfficialCalendarScheduleV1(**data)
    data.pop("extra")
    data["reviewed_on"] = datetime(2026, 12, 21, 1)
    with pytest.raises(ValueError):
        OfficialCalendarScheduleV1(**data)


def test_construct_bypassed_source_is_revalidated_at_stage_boundary(tmp_path) -> None:
    source = _valid_source()
    values = source.model_dump(mode="python")
    values["source_sha256"] = "0" * 64
    bypassed = BaseModel.model_construct.__func__(CalendarSourceBundleV1, **values)
    store = CalendarGenerationStore(tmp_path / "calendar_generations.sqlite3")
    with pytest.raises(ValueError):
        store.plan_stage(bypassed, datetime(2026, 12, 22, 1, tzinfo=UTC))
    assert list(tmp_path.iterdir()) == []


def test_source_package_duplicate_keys_and_oversize_fail_before_store_access(tmp_path) -> None:
    duplicate = tmp_path / "duplicate.json"
    duplicate.write_text('{"year": 2027, "year": 2027}', encoding="utf-8")
    with pytest.raises(ValueError):
        load_source(duplicate)
    oversized = tmp_path / "oversized.json"
    oversized.write_bytes(b"{" + b"x" * (256 * 1024) + b"}")
    with pytest.raises(ValueError):
        load_source(oversized)


def test_decomposed_unicode_is_not_normalized_in_hash_projection() -> None:
    values = {"s": "e\u0301"}
    assert canonical_json_bytes(values) == b'{"s":"e\xcc\x81"}\n'
    assert canonical_json_bytes(values) != canonical_json_bytes({"s": "\u00e9"})
    assert hashlib.sha256(b"abc").hexdigest() == body_sha256(b"abc")


def test_different_body_hashes_with_same_semantic_maps_are_stageable(tmp_path) -> None:
    source = _valid_source()
    store = CalendarGenerationStore(tmp_path / "calendar_generations.sqlite3")
    result = store.stage_execute(source, datetime(2026, 12, 22, 1, tzinfo=UTC))
    assert result.outcome == "STAGED"


def test_ancestor_symlink_is_rejected_without_lock_or_database_creation(tmp_path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(target, target_is_directory=True)
    path = alias / "calendar_generations.sqlite3"
    with pytest.raises(RuntimeError):
        CalendarGenerationStore(path).stage_execute(
            _valid_source(), datetime(2026, 12, 22, 1, tzinfo=UTC)
        )
    assert not (target / "calendar_generations.sqlite3").exists()
    assert not (target / "calendar_generations.sqlite3.lock").exists()


def test_reader_does_not_change_database_bytes_or_mtime(tmp_path) -> None:
    path = tmp_path / "calendar_generations.sqlite3"
    store = CalendarGenerationStore(path)
    store.stage_execute(_valid_source(), datetime(2026, 12, 22, 1, tzinfo=UTC))
    before_bytes, before_mtime = path.read_bytes(), path.stat().st_mtime_ns
    assert store.read().status == "ready"
    assert path.read_bytes() == before_bytes
    assert path.stat().st_mtime_ns == before_mtime


def test_machine_observation_rejects_missing_closed_day_and_invalid_flag() -> None:
    start, end = date(2027, 1, 1), date(2027, 12, 31)
    days = [
        CalendarMachineDayV1(
            date=start + timedelta(days=i),
            is_open=(start + timedelta(days=i)).weekday() < 5
            and start + timedelta(days=i) != date(2027, 1, 1),
        )
        for i in range((end - start).days + 1)
    ]
    days.pop(1)
    with pytest.raises(ValueError):
        CalendarMachineObservationV1(
            provider="baostock",
            contract_version="r2f4.1-baostock-calendar-days-v1",
            range_start=start,
            range_end=end,
            observed_at=datetime(2026, 12, 22, 1, tzinfo=UTC),
            days=tuple(days),
            observation_sha256="0" * 64,
        )


def test_construct_bypassed_duplicate_machine_is_rejected_at_promotion_boundary(tmp_path) -> None:
    source = _valid_source()
    now = datetime(2026, 12, 22, 1, tzinfo=UTC)
    path = tmp_path / "calendar_generations.sqlite3"
    store = CalendarGenerationStore(path)
    store.stage_execute(source, now)
    attempt = store.reserve_attempt(source, now)
    start, end = date(2027, 1, 1), date(2027, 12, 31)
    days = tuple(
        CalendarMachineDayV1(
            date=start + timedelta(days=i),
            is_open=(start + timedelta(days=i)).weekday() < 5
            and start + timedelta(days=i) != date(2027, 1, 1),
        )
        for i in range((end - start).days + 1)
    )
    values = {
        "provider": "baostock",
        "contract_version": "r2f4.1-baostock-calendar-days-v1",
        "range_start": start,
        "range_end": end,
        "observed_at": now,
        "days": days[:-1] + (days[-2],),
        "observation_sha256": "0" * 64,
    }
    bypassed = BaseModel.model_construct.__func__(CalendarMachineObservationV1, **values)
    bypassed = BaseModel.model_construct.__func__(
        CalendarMachineObservationV1,
        **{**values, "observation_sha256": build_observation_sha256(bypassed)},
    )
    result = store.promote(
        attempt, source, (b"sse body", b"szse body"), bypassed, now + timedelta(hours=1)
    )
    assert result.outcome == "MACHINE_CONFLICT"
    assert store.read().generation is None


def _staged_store(tmp_path):
    path = tmp_path / "calendar_generations.sqlite3"
    store = CalendarGenerationStore(path)
    store.stage_execute(_valid_source(), datetime(2026, 12, 22, 1, tzinfo=UTC))
    assert store.read().status == "ready"
    return path, store


def test_reader_rejects_hardlinked_lock(tmp_path) -> None:
    path, store = _staged_store(tmp_path)
    os.link(path.with_name(path.name + ".lock"), tmp_path / "foreign.lock")
    assert store.read().status == "unavailable"


def test_reader_rejects_world_writable_lock(tmp_path) -> None:
    path, store = _staged_store(tmp_path)
    path.with_name(path.name + ".lock").chmod(0o666)
    assert store.read().status == "unavailable"


@pytest.mark.parametrize("sidecar", ["-journal", "-wal", "-shm"])
def test_reader_rejects_any_sqlite_sidecar_even_broken_symlink(tmp_path, sidecar) -> None:
    path, store = _staged_store(tmp_path)
    path.with_name(path.name + sidecar).symlink_to(tmp_path / "absent-sidecar")
    assert store.read().status == "unavailable"


def test_reader_lock_is_nonblocking(tmp_path) -> None:
    path, store = _staged_store(tmp_path)
    with path.with_name(path.name + ".lock").open("rb") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        assert store.read().status == "unavailable"


def test_repeated_reservation_same_slot_is_same_spent_identity(tmp_path) -> None:
    path, store = _staged_store(tmp_path)
    source, now = _valid_source(), datetime(2026, 12, 22, 1, tzinfo=UTC)
    store.reserve_attempt(source, now)
    with pytest.raises(RuntimeError):
        store.reserve_attempt(source, now)
    with sqlite3.connect(path) as connection:
        assert (
            connection.execute("SELECT COUNT(*) FROM calendar_maintenance_attempt").fetchone()[0]
            == 1
        )


def test_new_source_same_slot_cannot_reuse_spent_reservation(tmp_path) -> None:
    path, store = _staged_store(tmp_path)
    source, now = _valid_source(), datetime(2026, 12, 22, 1, tzinfo=UTC)
    store.reserve_attempt(source, now)
    changed_schedule = source.schedules[0].model_copy(
        update={"title": "revision", "schedule_sha256": "0" * 64}
    )
    changed_schedule = changed_schedule.model_copy(
        update={"schedule_sha256": build_schedule_sha256(changed_schedule)}
    )
    changed = source.model_copy(
        update={"schedules": (changed_schedule, source.schedules[1]), "source_sha256": "0" * 64}
    )
    changed = changed.model_copy(update={"source_sha256": build_source_sha256(changed)})
    store.stage_execute(changed, now)
    with pytest.raises(RuntimeError):
        store.reserve_attempt(changed, now)
    with sqlite3.connect(path) as connection:
        row = connection.execute(
            "SELECT source_sha256 FROM calendar_maintenance_attempt"
        ).fetchone()
        assert row[0] == source.source_sha256


def test_latest_quarantined_candidate_blocks_older_source(tmp_path) -> None:
    path, store = _staged_store(tmp_path)
    source, now = _valid_source(), datetime(2026, 12, 22, 1, tzinfo=UTC)
    store.stage_execute(source, now)
    changed = source.schedules[1].model_copy(
        update={"closed_dates": (date(2027, 1, 1), date(2027, 1, 4)), "schedule_sha256": "0" * 64}
    )
    changed = changed.model_copy(update={"schedule_sha256": build_schedule_sha256(changed)})
    conflict = source.model_copy(
        update={"schedules": (source.schedules[0], changed), "source_sha256": "0" * 64}
    )
    conflict = conflict.model_copy(update={"source_sha256": build_source_sha256(conflict)})
    store.stage_execute(conflict, now)
    with pytest.raises(RuntimeError):
        store.reserve_attempt(source, now)
    with sqlite3.connect(path) as connection:
        assert (
            connection.execute("SELECT COUNT(*) FROM calendar_maintenance_attempt").fetchone()[0]
            == 0
        )


def test_construct_bypassed_attempt_identity_cannot_rebind_promotion(tmp_path) -> None:
    path, store = _staged_store(tmp_path)
    source, now = _valid_source(), datetime(2026, 12, 22, 1, tzinfo=UTC)
    attempt = store.reserve_attempt(source, now)
    values = attempt.model_dump(mode="python")
    values["started_at"] = now + timedelta(hours=1)
    forged = BaseModel.model_construct.__func__(type(attempt), **values)
    start, end = date(2027, 1, 1), date(2027, 12, 31)
    days = tuple(
        CalendarMachineDayV1(
            date=start + timedelta(days=i),
            is_open=(start + timedelta(days=i)).weekday() < 5
            and start + timedelta(days=i) != date(2027, 1, 1),
        )
        for i in range((end - start).days + 1)
    )
    machine = build_observation(
        provider="baostock",
        contract_version="r2f4.1-baostock-calendar-days-v1",
        range_start=start,
        range_end=end,
        observed_at=now,
        days=days,
    )
    result = store.promote(
        forged, source, (b"sse body", b"szse body"), machine, now + timedelta(hours=1)
    )
    assert result.outcome != "PROMOTED"
    with sqlite3.connect(path) as connection:
        assert (
            connection.execute("SELECT sequence FROM calendar_generation_head").fetchone()[0] == 0
        )


def test_store_writer_refuses_corrupt_existing_database(tmp_path) -> None:
    path = tmp_path / "calendar_generations.sqlite3"
    path.write_bytes(b"foreign database bytes")
    before = path.read_bytes()
    with pytest.raises(RuntimeError):
        CalendarGenerationStore(path).stage_execute(
            _valid_source(), datetime(2026, 12, 22, 1, tzinfo=UTC)
        )
    assert path.read_bytes() == before


def test_quality_new_promotion_cannot_commit_backwards_timestamp(tmp_path):
    source = _valid_source()
    store = calendar_module.CalendarGenerationStore(tmp_path / "calendar_generations.sqlite3")
    first_started = datetime(2026, 12, 22, 1, tzinfo=UTC)
    store.stage_execute(source, first_started)
    first_attempt = store.reserve_attempt(source, first_started)
    first = store.promote(
        first_attempt,
        source,
        (b"sse body", b"szse body"),
        _machine_for(source, first_started),
        datetime(2027, 1, 1, 0, tzinfo=UTC),
    )
    assert first.outcome == "PROMOTED"
    assert store.read().status == "ready"
    with sqlite3.connect(store.path) as connection:
        first_row = connection.execute("SELECT * FROM calendar_generation_promotion").fetchone()

    schedule = source.schedules[0].model_copy(
        update={"title": "revision", "schedule_sha256": "0" * 64}
    )
    schedule = schedule.model_copy(update={"schedule_sha256": build_schedule_sha256(schedule)})
    revised = source.model_copy(
        update={"schedules": (schedule, source.schedules[1]), "source_sha256": "0" * 64}
    )
    revised = revised.model_copy(update={"source_sha256": build_source_sha256(revised)})
    second_started = datetime(2026, 12, 23, 1, tzinfo=UTC)
    store.stage_execute(revised, second_started)
    second_attempt = store.reserve_attempt(revised, second_started)
    second = store.promote(
        second_attempt,
        revised,
        (b"sse body", b"szse body"),
        _machine_for(revised, second_started),
        datetime(2026, 12, 23, 2, tzinfo=UTC),
    )
    assert second.outcome != "PROMOTED"
    current = store.read()
    assert current.status == "ready"
    assert current.generation.generation_sha256 == first.generation_sha256
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("SELECT * FROM calendar_generation_promotion").fetchall() == [
            first_row
        ]


def test_quality_equal_promotion_timestamp_is_allowed(tmp_path):
    source = _valid_source()
    first_time = datetime(2027, 1, 1, 0, tzinfo=UTC)
    first_started = datetime(2026, 12, 22, 1, tzinfo=UTC)
    second_started = datetime(2026, 12, 23, 1, tzinfo=UTC)
    schedule = source.schedules[0].model_copy(
        update={"title": "equal-time revision", "schedule_sha256": "0" * 64}
    )
    schedule = schedule.model_copy(update={"schedule_sha256": build_schedule_sha256(schedule)})
    revised = source.model_copy(
        update={"schedules": (schedule, source.schedules[1]), "source_sha256": "0" * 64}
    )
    revised = revised.model_copy(update={"source_sha256": build_source_sha256(revised)})
    store = calendar_module.CalendarGenerationStore(tmp_path / "calendar_generations.sqlite3")
    store.stage_execute(source, first_started)
    first_attempt = store.reserve_attempt(source, first_started)
    assert (
        store.promote(
            first_attempt,
            source,
            (b"sse body", b"szse body"),
            _machine_for(source, first_started),
            first_time,
        ).outcome
        == "PROMOTED"
    )
    store.stage_execute(revised, second_started)
    second_attempt = store.reserve_attempt(revised, second_started)
    assert (
        store.promote(
            second_attempt,
            revised,
            (b"sse body", b"szse body"),
            _machine_for(revised, second_started),
            first_time,
        ).outcome
        == "PROMOTED"
    )


def test_quality_source_rejects_hardlink_created_after_descriptor_check(tmp_path, monkeypatch):
    source = tmp_path / "source.json"
    alias = tmp_path / "source-alias.json"
    source.write_text(_valid_source().model_dump_json(), encoding="utf-8")
    initial_bytes = source.read_bytes()
    real_read = os.read
    injected = False

    def read_after_link(fd, size):
        nonlocal injected
        if not injected:
            os.link(source, alias)
            injected = True
        return real_read(fd, size)

    monkeypatch.setattr(calendar_module.os, "read", read_after_link)
    with pytest.raises(calendar_module.CalendarGenerationError):
        load_source(source)
    assert injected
    assert source.stat().st_nlink == 2
    assert source.read_bytes() == alias.read_bytes() == initial_bytes


def test_reader_preserves_bytes_on_corrupt_store(tmp_path) -> None:
    path = tmp_path / "calendar_generations.sqlite3"
    path.write_bytes(b"not sqlite")
    before = path.read_bytes()
    assert CalendarGenerationStore(path).read().status == "unavailable"
    assert path.read_bytes() == before


def test_promotion_rejects_machine_observation_after_promotion(tmp_path) -> None:
    source, now = _valid_source(), datetime(2026, 12, 22, 1, tzinfo=UTC)
    path, store = _staged_store(tmp_path)
    attempt = store.reserve_attempt(source, now)
    start, end = date(2027, 1, 1), date(2027, 12, 31)
    days = tuple(
        CalendarMachineDayV1(
            date=start + timedelta(days=i),
            is_open=(start + timedelta(days=i)).weekday() < 5
            and start + timedelta(days=i) != date(2027, 1, 1),
        )
        for i in range((end - start).days + 1)
    )
    machine = build_observation(
        provider="baostock",
        contract_version="r2f4.1-baostock-calendar-days-v1",
        range_start=start,
        range_end=end,
        observed_at=now + timedelta(hours=3),
        days=days,
    )
    result = store.promote(
        attempt, source, (b"sse body", b"szse body"), machine, now + timedelta(hours=1)
    )
    assert result.outcome == "MACHINE_UNAVAILABLE"
    assert store.read().generation is None


def test_load_source_accepts_its_canonical_json_file(tmp_path) -> None:
    path = tmp_path / "source.json"
    path.write_text(_valid_source().model_dump_json(), encoding="utf-8")
    loaded = load_source(path)
    assert loaded.source_sha256 == _valid_source().source_sha256


def test_source_schema_version_bool_is_not_equal_to_integer_one() -> None:
    with pytest.raises(ValueError):
        _valid_source().model_copy(update={"schema_version": True})


def test_snapshot_configs_cannot_be_reassigned(tmp_path) -> None:
    snapshot = _staged_store(tmp_path)[1].read().calendar
    with pytest.raises((AttributeError, TypeError)):
        snapshot.configs = {}


def test_reader_rejects_orphan_promoted_attempt_without_generation(tmp_path) -> None:
    now = datetime(2026, 12, 22, 1, tzinfo=UTC)
    source = _valid_source()
    path = tmp_path / "calendar_generations.sqlite3"
    store = CalendarGenerationStore(path)
    assert store.stage_execute(source, now).outcome == "STAGED"
    attempt = store.reserve_attempt(source, now)
    finished = now + timedelta(hours=1)
    with sqlite3.connect(path) as connection:
        changed = connection.execute(
            "UPDATE calendar_maintenance_attempt SET finished_at=?,outcome='PROMOTED',"
            "official_requests=2,machine_requests=1 WHERE target_year=? AND slot_date=?",
            (
                finished.isoformat(timespec="microseconds").replace("+00:00", "Z"),
                attempt.target_year,
                attempt.slot_date.isoformat(),
            ),
        ).rowcount
        assert changed == 1
        row = connection.execute(
            "SELECT outcome,finished_at,official_requests,machine_requests "
            "FROM calendar_maintenance_attempt"
        ).fetchone()
    assert row == (
        "PROMOTED",
        finished.isoformat(timespec="microseconds").replace("+00:00", "Z"),
        2,
        1,
    )
    _assert_repeated_unavailable(store, tmp_path)


def test_reader_accepts_positive_two_generation_chain(tmp_path) -> None:
    _path, store, _now = _promote_two_generations(tmp_path)
    result = store.read()
    assert result.status == "ready"
    assert result.generation is not None and result.generation.sequence == 2


def test_reader_rejects_backwards_promotion_time_with_consistent_identities(tmp_path) -> None:
    path, store, now = _promote_two_generations(tmp_path)
    with sqlite3.connect(path) as connection:
        raw = connection.execute(
            "SELECT payload_json FROM calendar_generation_promotion WHERE sequence=2"
        ).fetchone()[0]
    generation = CalendarGenerationV1.model_validate_json(raw)
    earlier = now + timedelta(minutes=30)
    generation = generation.model_copy(update={"promoted_at": earlier})
    generation = generation.model_copy(
        update={"generation_sha256": build_generation_sha256(generation)}
    )
    _tamper_triggered_update(
        path,
        "calendar_promotion_no_update",
        "UPDATE calendar_generation_promotion SET generation_sha256=?,payload_json=?,promoted_at=? "
        "WHERE sequence=2",
        (
            generation.generation_sha256,
            canonical_json_bytes(generation.model_dump(mode="json")).decode(),
            earlier.isoformat(timespec="microseconds").replace("+00:00", "Z"),
        ),
    )
    _tamper_triggered_update(
        path,
        "calendar_attempt_terminal_only",
        "UPDATE calendar_maintenance_attempt SET finished_at=? WHERE target_year=2027",
        (earlier.isoformat(timespec="microseconds").replace("+00:00", "Z"),),
    )
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE calendar_generation_head SET generation_sha256=? WHERE singleton=1",
            (generation.generation_sha256,),
        )
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
    _assert_repeated_unavailable(store, tmp_path)


def test_reader_rejects_promoted_quarantined_source_even_with_matching_sse_machine(
    tmp_path,
) -> None:
    now = datetime(2026, 12, 22, 1, tzinfo=UTC)
    source = _valid_source()
    path = tmp_path / "calendar_generations.sqlite3"
    store = CalendarGenerationStore(path)
    store.stage_execute(source, now)
    attempt = store.reserve_attempt(source, now)
    assert (
        store.promote(
            attempt,
            source,
            (b"sse body", b"szse body"),
            _machine_for(source, now),
            now + timedelta(hours=1),
        ).outcome
        == "PROMOTED"
    )
    changed = source.schedules[1].model_copy(
        update={"closed_dates": (date(2027, 1, 1), date(2027, 1, 4))}
    )
    changed = changed.model_copy(update={"schedule_sha256": build_schedule_sha256(changed)})
    conflict = source.model_copy(update={"schedules": (source.schedules[0], changed)})
    conflict = conflict.model_copy(update={"source_sha256": build_source_sha256(conflict)})
    assert store.plan_stage(conflict, now).outcome == "SOURCE_CONFLICT"
    _tamper_triggered_update(
        path,
        "calendar_candidate_no_update",
        "UPDATE calendar_generation_candidate SET source_sha256=?,payload_json=?,"
        "admission='quarantined',reason='SOURCE_CONFLICT' WHERE source_sha256=?",
        (
            conflict.source_sha256,
            canonical_json_bytes(conflict.model_dump(mode="json")).decode(),
            source.source_sha256,
        ),
    )
    with sqlite3.connect(path) as connection:
        raw = connection.execute(
            "SELECT payload_json FROM calendar_generation_promotion"
        ).fetchone()[0]
    generation = CalendarGenerationV1.model_validate_json(raw).model_copy(
        update={"source_sha256": conflict.source_sha256}
    )
    generation = generation.model_copy(
        update={"generation_sha256": build_generation_sha256(generation)}
    )
    _tamper_triggered_update(
        path,
        "calendar_promotion_no_update",
        "UPDATE calendar_generation_promotion SET generation_sha256=?,source_sha256=?,"
        "payload_json=?",
        (
            generation.generation_sha256,
            conflict.source_sha256,
            canonical_json_bytes(generation.model_dump(mode="json")).decode(),
        ),
    )
    _tamper_triggered_update(
        path,
        "calendar_attempt_terminal_only",
        "UPDATE calendar_maintenance_attempt SET source_sha256=?",
        (conflict.source_sha256,),
    )
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE calendar_generation_head SET generation_sha256=? WHERE singleton=1",
            (generation.generation_sha256,),
        )
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
    _assert_repeated_unavailable(store, tmp_path)


def test_reader_rejects_head_rewind_on_two_generation_chain(tmp_path) -> None:
    path, store, _now = _promote_two_generations(tmp_path)
    with sqlite3.connect(path) as connection:
        first = connection.execute(
            "SELECT generation_sha256 FROM calendar_generation_promotion WHERE sequence=1"
        ).fetchone()
        assert first is not None
        changed = connection.execute(
            "UPDATE calendar_generation_head SET sequence=1,generation_sha256=? WHERE singleton=1",
            (first[0],),
        ).rowcount
        assert changed == 1
    _assert_repeated_unavailable(store, tmp_path)


def test_reader_rejects_missing_parent_on_two_generation_chain(tmp_path) -> None:
    path, store, _now = _promote_two_generations(tmp_path)
    _tamper_triggered_update(
        path,
        "calendar_promotion_no_update",
        "UPDATE calendar_generation_promotion SET parent_sha256=NULL WHERE sequence=2",
        (),
    )
    _assert_repeated_unavailable(store, tmp_path)


def test_reader_rejects_self_consistent_generation_with_wrong_bundled_base(tmp_path) -> None:
    path, store, _source, _now = _promote_old_2026_authority(tmp_path)
    with sqlite3.connect(path) as connection:
        row = connection.execute(
            "SELECT payload_json,generation_sha256 FROM calendar_generation_promotion "
            "WHERE sequence=1"
        ).fetchone()
    assert row is not None
    payload = json.loads(row[0])
    payload["bundled_sha256"] = "e" * 64
    payload["generation_sha256"] = "0" * 64
    payload["generation_sha256"] = domain_sha256(
        "stock-eva/r2f4.1/calendar-generation/v1",
        {key: value for key, value in payload.items() if key != "generation_sha256"},
    )
    mutated_payload = canonical_json_bytes(payload).decode()
    with sqlite3.connect(path) as connection:
        trigger = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='trigger' "
            "AND name='calendar_promotion_no_update'"
        ).fetchone()
        assert trigger is not None
        connection.execute("DROP TRIGGER calendar_promotion_no_update")
        try:
            assert (
                connection.execute(
                    "UPDATE calendar_generation_promotion SET generation_sha256=?,payload_json=? "
                    "WHERE sequence=1",
                    (payload["generation_sha256"], mutated_payload),
                ).rowcount
                == 1
            )
        finally:
            connection.execute(trigger[0])
        restored = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='trigger' "
            "AND name='calendar_promotion_no_update'"
        ).fetchone()
        assert restored is not None and restored[0] == trigger[0]
        assert (
            connection.execute(
                "UPDATE calendar_generation_head SET generation_sha256=? WHERE singleton=1",
                (payload["generation_sha256"],),
            ).rowcount
            == 1
        )
        connection.commit()
    _assert_repeated_unavailable(store, tmp_path)


def test_reader_rejects_generation_row_promoted_at_drift(tmp_path) -> None:
    path, store, _source, _now = _promote_old_2026_authority(tmp_path)
    with sqlite3.connect(path) as connection:
        row = connection.execute(
            "SELECT promoted_at FROM calendar_generation_promotion WHERE sequence=1"
        ).fetchone()
    assert row is not None
    changed_at = "2026-12-22T03:00:00.000000Z"
    assert changed_at != row[0]
    _tamper_triggered_update(
        path,
        "calendar_promotion_no_update",
        "UPDATE calendar_generation_promotion SET promoted_at=? WHERE sequence=1",
        (changed_at,),
    )
    with sqlite3.connect(path) as connection:
        assert (
            connection.execute(
                "SELECT promoted_at FROM calendar_generation_promotion WHERE sequence=1"
            ).fetchone()[0]
            == changed_at
        )
    _assert_repeated_unavailable(store, tmp_path)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("started_at", "2026-12-22T01:30:00.000000Z"),
        ("finished_at", "2026-12-22T00:30:00.000000Z"),
    ],
)
def test_reader_rejects_attempt_time_not_bound_to_machine_and_promotion(
    tmp_path, field, value
) -> None:
    path, store, _source, _now = _promote_old_2026_authority(tmp_path)
    with sqlite3.connect(path) as connection:
        before = connection.execute(
            "SELECT target_year,slot_date,source_sha256,expected_parent_sha256,started_at,"
            "finished_at FROM calendar_maintenance_attempt"
        ).fetchone()
    assert before is not None
    _tamper_triggered_update(
        path,
        "calendar_attempt_terminal_only",
        f"UPDATE calendar_maintenance_attempt SET {field}=? WHERE target_year=? AND slot_date=?",
        (value, before[0], before[1]),
    )
    with sqlite3.connect(path) as connection:
        after = connection.execute(
            "SELECT target_year,slot_date,source_sha256,expected_parent_sha256,started_at,"
            "finished_at FROM calendar_maintenance_attempt"
        ).fetchone()
    assert after is not None
    assert after[:4] == before[:4]
    assert after[4 if field == "started_at" else 5] == value
    _assert_repeated_unavailable(store, tmp_path)


def test_reader_rejects_failed_attempt_finished_before_started(tmp_path) -> None:
    now = datetime(2026, 12, 22, 1, tzinfo=UTC)
    source = _valid_source()
    path = tmp_path / "calendar_generations.sqlite3"
    store = CalendarGenerationStore(path)
    store.stage_execute(source, now)
    attempt = store.reserve_attempt(source, now)
    store._spend_attempt(attempt, "OFFICIAL_UNAVAILABLE", 2, 0, finished_at=now)
    earlier = (now - timedelta(minutes=1)).isoformat(timespec="microseconds").replace("+00:00", "Z")
    _tamper_triggered_update(
        path,
        "calendar_attempt_terminal_only",
        "UPDATE calendar_maintenance_attempt SET finished_at=? WHERE target_year=? AND slot_date=?",
        (earlier, attempt.target_year, attempt.slot_date.isoformat()),
    )
    _assert_repeated_unavailable(store, tmp_path)


def test_reader_rejects_attempt_started_before_candidate_admission(tmp_path) -> None:
    now = datetime(2026, 12, 22, 1, tzinfo=UTC)
    source = _valid_source()
    path = tmp_path / "calendar_generations.sqlite3"
    store = CalendarGenerationStore(path)
    store.stage_execute(source, now)
    attempt = store.reserve_attempt(source, now)
    later = (now + timedelta(minutes=1)).isoformat(timespec="microseconds").replace("+00:00", "Z")
    _tamper_triggered_update(
        path,
        "calendar_candidate_no_update",
        "UPDATE calendar_generation_candidate SET staged_at=? WHERE source_sha256=?",
        (later, source.source_sha256),
    )
    assert attempt.outcome == "RUNNING"
    _assert_repeated_unavailable(store, tmp_path)


def test_reader_rejects_running_attempt_with_official_request_count(tmp_path) -> None:
    now = datetime(2026, 12, 22, 1, tzinfo=UTC)
    source = _valid_source()
    path = tmp_path / "calendar_generations.sqlite3"
    store = CalendarGenerationStore(path)
    store.stage_execute(source, now)
    attempt = store.reserve_attempt(source, now)
    connection = sqlite3.connect(path)
    trigger = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type='trigger' "
        "AND name='calendar_attempt_terminal_only'"
    ).fetchone()
    assert trigger is not None
    connection.execute("DROP TRIGGER calendar_attempt_terminal_only")
    try:
        connection.execute("PRAGMA ignore_check_constraints=ON")
        connection.execute(
            "UPDATE calendar_maintenance_attempt SET official_requests=1 "
            "WHERE target_year=? AND slot_date=?",
            (attempt.target_year, attempt.slot_date.isoformat()),
        )
    finally:
        connection.execute(trigger[0])
        connection.commit()
        connection.close()
    _assert_repeated_unavailable(store, tmp_path)


def test_reader_rejects_generation_promoted_in_disallowed_future_year(tmp_path) -> None:
    # A new 2027 year has no earlier map: the rejection must be the promotion
    # year's contract, not a side effect of rewriting an old 2026 closure.
    now = datetime(2026, 12, 22, 1, tzinfo=UTC)
    source = _valid_source()
    path = tmp_path / "calendar_generations.sqlite3"
    store = CalendarGenerationStore(path)
    assert store.stage_execute(source, now).outcome == "STAGED"
    attempt = store.reserve_attempt(source, now)
    assert (
        store.promote(
            attempt,
            source,
            (b"sse body", b"szse body"),
            _machine_for(source, now),
            now + timedelta(hours=1),
        ).outcome
        == "PROMOTED"
    )
    assert store.read().status == "ready"
    with sqlite3.connect(path) as connection:
        row = connection.execute(
            "SELECT payload_json FROM calendar_generation_promotion WHERE sequence=1"
        ).fetchone()
    assert row is not None
    payload = json.loads(row[0])
    payload["promoted_at"] = "2030-01-01T00:00:00Z"
    payload["generation_sha256"] = "0" * 64
    payload["generation_sha256"] = domain_sha256(
        "stock-eva/r2f4.1/calendar-generation/v1",
        {key: value for key, value in payload.items() if key != "generation_sha256"},
    )
    _tamper_triggered_update(
        path,
        "calendar_promotion_no_update",
        "UPDATE calendar_generation_promotion SET generation_sha256=?,payload_json=?,promoted_at=? "
        "WHERE sequence=1",
        (
            payload["generation_sha256"],
            canonical_json_bytes(payload).decode(),
            "2030-01-01T00:00:00.000000Z",
        ),
    )
    _tamper_triggered_update(
        path,
        "calendar_attempt_terminal_only",
        "UPDATE calendar_maintenance_attempt SET finished_at=? WHERE target_year=2027",
        ("2030-01-01T00:00:00.000000Z",),
    )
    with sqlite3.connect(path) as connection:
        assert (
            connection.execute(
                "UPDATE calendar_generation_head SET generation_sha256=? WHERE singleton=1",
                (payload["generation_sha256"],),
            ).rowcount
            == 1
        )
        connection.commit()
    _assert_repeated_unavailable(store, tmp_path)


def _history_changed_2026_source() -> CalendarSourceBundleV1:
    base = _bundled_2026_revision()
    changed_date = date(2026, 12, 21)
    schedules = tuple(
        schedule.model_copy(
            update={
                "closed_dates": tuple(sorted((*schedule.closed_dates, changed_date))),
                "schedule_sha256": "0" * 64,
            }
        )
        for schedule in base.schedules
    )
    schedules = tuple(
        schedule.model_copy(update={"schedule_sha256": build_schedule_sha256(schedule)})
        for schedule in schedules
    )
    source = base.model_copy(update={"schedules": schedules, "source_sha256": "0" * 64})
    return source.model_copy(update={"source_sha256": build_source_sha256(source)})


def test_reader_rejects_self_consistent_skip_from_bundle_to_2028(tmp_path) -> None:
    # 2028 is a legal next-year target here; missing 2027 is the sole transition
    # failure rather than the independent current/next-year rule.
    now = datetime(2027, 1, 4, 1, tzinfo=UTC)
    source = _source_for_year(2028, (date(2028, 1, 3),))
    path, _generation = _manual_promotion(
        tmp_path,
        source,
        now,
        target_year=2028,
        promoted_at=now + timedelta(hours=1),
    )
    store = CalendarGenerationStore(path)
    _assert_repeated_unavailable(store, tmp_path)


def test_reader_rejects_self_consistent_completed_history_change(tmp_path) -> None:
    now = datetime(2026, 12, 22, 1, tzinfo=UTC)
    source = _history_changed_2026_source()
    path, _generation = _manual_promotion(
        tmp_path,
        source,
        now,
        target_year=2026,
        promoted_at=now + timedelta(hours=1),
        bodies=(b"bundled-2026-sse-revision", b"bundled-2026-szse-revision"),
    )
    store = CalendarGenerationStore(path)
    _assert_repeated_unavailable(store, tmp_path)


def test_invalid_bypassed_source_does_not_leak_payload_in_warnings_or_logs(
    tmp_path, caplog
) -> None:
    canary = "D2_PAYLOAD_CANARY_NOT_PUBLIC"
    raw = _valid_source().model_dump(mode="json")
    raw_schedule = {"title": canary, "official_url": f"https://www.sse.com.cn/{canary}"}
    raw_schedule.update(
        {k: v for k, v in raw["schedules"][0].items() if k not in ("title", "official_url")}
    )
    raw_schedule["reviewed_on"] = "2026-12-21T01:00:00"
    raw_schedule["schedule_sha256"] = domain_sha256(
        "stock-eva/r2f4.1/calendar-schedule/v1",
        {key: value for key, value in raw_schedule.items() if key != "schedule_sha256"},
    )
    raw["schedules"] = [raw_schedule, raw["schedules"][1]]
    raw["source_sha256"] = "0" * 64
    raw["source_sha256"] = domain_sha256(
        "stock-eva/r2f4.1/calendar-source/v1",
        {key: value for key, value in raw.items() if key != "source_sha256"},
    )
    bypassed = BaseModel.model_construct.__func__(CalendarSourceBundleV1, **raw)
    store = CalendarGenerationStore(tmp_path / "calendar_generations.sqlite3")
    caplog.set_level(logging.WARNING)
    with warnings.catch_warnings(record=True) as caught:
        with pytest.raises(ValueError):
            store.plan_stage(bypassed, datetime(2026, 12, 22, 1, tzinfo=UTC))
    assert not caught, "invalid authority must not emit serializer warnings containing input"
    assert all(canary not in record.getMessage() for record in caplog.records)
    assert not (tmp_path / "calendar_generations.sqlite3").exists()


def _d3_tree(root: Path) -> dict[str, tuple[bytes, int]]:
    return {
        str(path.relative_to(root)): (path.read_bytes(), path.stat().st_mtime_ns)
        for path in root.rglob("*")
        if path.is_file() and not path.is_symlink()
    }


def test_d3_existing_zero_byte_database_is_not_initialized(tmp_path) -> None:
    path = tmp_path / "calendar_generations.sqlite3"
    path.touch(mode=0o600)
    path.with_name(path.name + ".lock").touch(mode=0o600)
    before = _d3_tree(tmp_path)
    with pytest.raises(CalendarStoreUnavailable):
        CalendarGenerationStore(path).stage_execute(
            _valid_source(), datetime(2026, 12, 22, 1, tzinfo=UTC)
        )
    assert _d3_tree(tmp_path) == before


def test_d3_reader_rejects_writable_by_other_control_directory(tmp_path) -> None:
    path, store = _staged_store(tmp_path)
    path.parent.chmod(0o777)
    before = _d3_tree(path.parent)
    assert store.read().status == "unavailable"
    assert _d3_tree(path.parent) == before


def test_d3_reader_detects_path_replacement_after_connect(tmp_path, monkeypatch) -> None:
    path, store = _staged_store(tmp_path)
    foreign = path.with_name("replacement.sqlite3")
    foreign.write_bytes(path.read_bytes())
    foreign.chmod(0o600)
    original = store._connect
    injected = False
    after_swap: dict[str, tuple[bytes, int]] | None = None

    def connect(*, readonly=False):
        nonlocal injected, after_swap
        connection = original(readonly=readonly)
        if readonly and not injected:
            path.rename(path.with_name("original.sqlite3"))
            os.replace(foreign, path)
            injected = True
            after_swap = _d3_tree(path.parent)
        return connection

    monkeypatch.setattr(store, "_connect", connect)
    result = store.read()
    assert injected
    assert result.status == "unavailable"
    assert after_swap is not None
    assert _d3_tree(path.parent) == after_swap


def test_d3_source_reader_does_not_follow_mid_open_symlink(tmp_path, monkeypatch) -> None:
    source = _valid_source()
    path = tmp_path / "reviewed.json"
    foreign = tmp_path / "foreign.json"
    path.write_text(source.model_dump_json())
    foreign.write_text(source.model_dump_json())
    original_io_open = io.open
    original_os_open = os.open
    injected = False

    def swap_if_target(value):
        nonlocal injected
        if isinstance(value, (str, os.PathLike)) and Path(value) == path and not injected:
            path.rename(path.with_name("reviewed-before.json"))
            path.symlink_to(foreign)
            injected = True

    def patched_io_open(value, *args, **kwargs):
        swap_if_target(value)
        return original_io_open(value, *args, **kwargs)

    def patched_os_open(value, *args, **kwargs):
        swap_if_target(value)
        return original_os_open(value, *args, **kwargs)

    monkeypatch.setattr(io, "open", patched_io_open)
    monkeypatch.setattr(os, "open", patched_os_open)
    with pytest.raises((ValueError, OSError)):
        load_source(path)
    assert injected


def test_d3_reader_rejects_non_normative_index(tmp_path) -> None:
    path, store = _staged_store(tmp_path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE INDEX unexpected_candidate_index ON calendar_generation_candidate(admission)"
        )
    before = _d3_tree(path.parent)
    assert store.read().status == "unavailable"
    assert _d3_tree(path.parent) == before


def test_d3_snapshot_retains_complete_bundled_config_metadata(tmp_path) -> None:
    _path, store = _staged_store(tmp_path)
    result = store.read()
    snapshot = result.calendar
    assert snapshot is not None and result.bundled is not None
    assert snapshot.bundled_sha256 == bundled_sha256(result.bundled)
    assert snapshot.generation_sha256 is None
    for config in result.bundled:
        assert snapshot.configs[config["year"]].model_dump(mode="json") == config
    sources = snapshot.sources_for(2026)
    assert [item.model_dump(mode="json") for item in sources] == next(
        config["sources"] for config in result.bundled if config["year"] == 2026
    )


def test_d3_snapshot_has_complete_calendar_operations(tmp_path) -> None:
    _path, store = _staged_store(tmp_path)
    snapshot = store.read().calendar
    assert snapshot is not None
    assert snapshot.snapshot() is snapshot
    assert snapshot.previous_session(date(2026, 12, 22)) == date(2026, 12, 21)
    assert snapshot.latest_expected_session(datetime(2026, 12, 22, 1, tzinfo=UTC)) == date(
        2026, 12, 21
    )
    assert snapshot.market_phase(datetime(2026, 12, 22, 1, tzinfo=UTC)) == "pre_market"
    assert not snapshot.sources_for(2028)


def test_d3_snapshot_matches_trading_calendar_time_and_unknown_boundaries(tmp_path) -> None:
    _path, store = _staged_store(tmp_path)
    snapshot = store.read().calendar
    assert snapshot is not None

    shanghai = ZoneInfo("Asia/Shanghai")
    assert snapshot.market_phase(datetime(2026, 12, 22, 1, tzinfo=UTC)) == "pre_market"
    assert snapshot.market_phase(datetime(2026, 12, 22, 10, 0, tzinfo=shanghai)) == "market_open"
    assert snapshot.market_phase(datetime(2026, 12, 22, 7, tzinfo=UTC)) == "after_close_waiting"
    assert snapshot.market_phase(datetime(2026, 1, 1, 1, tzinfo=UTC)) == "closed"
    assert snapshot.previous_session(date(2026, 1, 2)) == date(2025, 12, 31)
    assert snapshot.latest_expected_session(datetime(2028, 1, 1, 1, tzinfo=UTC)) is None


def test_reader_fails_closed_under_sqlite_exclusive_lock(tmp_path) -> None:
    path = tmp_path / "calendar_generations.sqlite3"
    store = CalendarGenerationStore(path)
    store.stage_execute(_valid_source(), datetime(2026, 12, 22, 1, tzinfo=UTC))
    before = (body_sha256(path.read_bytes()), path.stat().st_mtime_ns)
    blocker = sqlite3.connect(path, timeout=0)
    try:
        blocker.execute("BEGIN EXCLUSIVE")
        assert not path.with_name(path.name + "-journal").exists()
        assert store.read().status == "unavailable"
        assert (body_sha256(path.read_bytes()), path.stat().st_mtime_ns) == before
    finally:
        blocker.rollback()
        blocker.close()


def test_d3_reader_does_not_materialize_unbounded_control_rows(tmp_path, monkeypatch) -> None:
    store = CalendarGenerationStore(tmp_path / "calendar_generations.sqlite3")
    store.stage_execute(_valid_source(), datetime(2026, 12, 22, 1, tzinfo=UTC))
    original_connect = store._connect
    checked: set[str] = set()
    tables = {
        "calendar_generation_candidate": 1024,
        "calendar_generation_promotion": 256,
    }

    class Cursor:
        def __init__(self, inner, sql):
            self.inner = inner
            self.sql = " ".join(sql.lower().split())

        def fetchone(self):
            row = self.inner.fetchone()
            for table, cap in tables.items():
                if self.sql == f"select count(*) from {table}" and row[0] <= cap:
                    checked.add(table)
            return row

        def fetchall(self):
            for table in tables:
                if f"from {table}" in self.sql and "count(*)" not in self.sql:
                    assert " limit " in f" {self.sql} " or table in checked, (
                        "must count/bound or stream untrusted rows before materializing them"
                    )
            return self.inner.fetchall()

        def __iter__(self):
            return iter(self.inner)

        def __getattr__(self, name):
            return getattr(self.inner, name)

    class Connection:
        def __init__(self, connection):
            self._connection = connection

        def execute(self, sql, parameters=()):
            return Cursor(self._connection.execute(sql, parameters), sql)

        def close(self):
            store._close_connection(self._connection)

        def __getattr__(self, name):
            return getattr(self._connection, name)

    def connect(*, readonly=False):
        return Connection(original_connect(readonly=readonly))

    monkeypatch.setattr(store, "_connect", connect)
    assert store.read().status == "ready"


@pytest.mark.parametrize("blank", [" ", "\t\n", "\u3000"])
def test_d3_extraction_review_identity_must_not_be_blank(blank) -> None:
    with pytest.raises(ValueError):
        _valid_source().schedules[0].model_copy(update={"review_id": blank})


@pytest.mark.parametrize("digest", ["not-a-sha256", "F" * 64])
def test_d3_public_generation_official_hashes_require_lowercase_sha256(digest) -> None:
    source = _valid_source()
    now = datetime(2026, 12, 22, 1, tzinfo=UTC)
    with pytest.raises(ValueError):
        CalendarGenerationV1(
            sequence=1,
            parent_sha256=None,
            bundled_sha256=bundled_sha256(),
            source_sha256=source.source_sha256,
            attempt_target_year=source.year,
            attempt_slot_date=now.date(),
            official_body_hashes=(digest, source.schedules[1].body_sha256),
            machine=_machine_for(source, now),
            promoted_at=now,
            generation_sha256="0" * 64,
        )


def test_d3_schedule_factory_hashes_the_validated_utc_projection() -> None:
    original = _valid_source().schedules[0]
    values = original.model_dump(mode="python", exclude={"schedule_sha256"})
    values["reviewed_on"] = values["reviewed_on"].astimezone(ZoneInfo("Asia/Shanghai"))
    rebuilt = build_schedule(**values)
    assert rebuilt.reviewed_on == original.reviewed_on
    assert rebuilt.schedule_sha256 == original.schedule_sha256


def test_d3_observation_factory_hashes_the_validated_utc_projection() -> None:
    original = _machine_for(_valid_source(), datetime(2026, 12, 22, 1, tzinfo=UTC))
    values = original.model_dump(mode="python", exclude={"observation_sha256", "days"})
    values["days"] = original.days
    values["observed_at"] = values["observed_at"].astimezone(ZoneInfo("Asia/Shanghai"))
    rebuilt = build_observation(**values)
    assert rebuilt.observed_at == original.observed_at
    assert rebuilt.observation_sha256 == original.observation_sha256


@pytest.mark.parametrize("field", ["source_sha256", "started_at"])
def test_d3_forged_attempt_cannot_terminalize_a_different_reserved_identity(
    tmp_path, field
) -> None:
    now = datetime(2026, 12, 22, 1, tzinfo=UTC)
    source = _valid_source()
    path = tmp_path / "calendar_generations.sqlite3"
    store = CalendarGenerationStore(path)
    assert store.stage_execute(source, now).outcome == "STAGED"
    attempt = store.reserve_attempt(source, now)
    values = attempt.model_dump(mode="python")
    values[field] = "f" * 64 if field == "source_sha256" else now + timedelta(hours=1)
    forged = BaseModel.model_construct.__func__(type(attempt), **values)
    before = body_sha256(path.read_bytes())
    result = store.promote(
        forged,
        source,
        (b"sse body", b"szse body"),
        _machine_for(source, now),
        now + timedelta(hours=1),
    )
    assert result.outcome != "PROMOTED"
    assert body_sha256(path.read_bytes()) == before
    with sqlite3.connect(path) as connection:
        assert (
            connection.execute("SELECT outcome FROM calendar_maintenance_attempt").fetchone()[0]
            == "RUNNING"
        )


@pytest.mark.parametrize("bad_input", ["machine_digest", "official_body"])
def test_d3_failure_audit_does_not_mutate_an_unproven_store(tmp_path, bad_input) -> None:
    now = datetime(2026, 12, 22, 1, tzinfo=UTC)
    source = _valid_source()
    path = tmp_path / "calendar_generations.sqlite3"
    store = CalendarGenerationStore(path)
    assert store.stage_execute(source, now).outcome == "STAGED"
    attempt = store.reserve_attempt(source, now)
    with sqlite3.connect(path) as connection:
        trigger = connection.execute(
            "SELECT sql FROM sqlite_master WHERE name='calendar_meta_no_update'"
        ).fetchone()
        assert trigger is not None
        connection.execute("DROP TRIGGER calendar_meta_no_update")
        try:
            assert (
                connection.execute(
                    "UPDATE calendar_generation_meta SET bundled_sha256=?", ("f" * 64,)
                ).rowcount
                == 1
            )
        finally:
            connection.execute(trigger[0])
        restored = connection.execute(
            "SELECT sql FROM sqlite_master WHERE name='calendar_meta_no_update'"
        ).fetchone()
        assert restored is not None and restored[0] == trigger[0]
        connection.commit()
    assert store.read().status == "unavailable"
    before = body_sha256(path.read_bytes())
    machine = _machine_for(source, now)
    bodies = (b"sse body", b"szse body")
    if bad_input == "machine_digest":
        machine = machine.model_copy(update={"observation_sha256": "0" * 64})
    else:
        bodies = (b"wrong", b"szse body")
    result = store.promote(attempt, source, bodies, machine, now + timedelta(hours=1))
    assert result.outcome != "PROMOTED"
    assert body_sha256(path.read_bytes()) == before
    with sqlite3.connect(path) as connection:
        assert (
            connection.execute("SELECT outcome FROM calendar_maintenance_attempt").fetchone()[0]
            == "RUNNING"
        )


class _FaultConnection:
    """Connection proxy used only to inject faults in the promotion transaction."""

    def __init__(self, connection: sqlite3.Connection, mode: str, state: dict[str, bool]):
        self._connection = connection
        self._mode = mode
        self._state = state

    def execute(self, sql, parameters=()):
        normalized = " ".join(str(sql).split()).lower()
        if self._mode == "official_insert_after" and (
            normalized.startswith("insert or ignore into calendar_official_object")
        ):
            before_changes = self._connection.total_changes
            self._connection.execute(sql, parameters)
            self._state["official_inserted"] = self._connection.total_changes == before_changes + 1
            self._state["injected"] = True
            raise sqlite3.OperationalError("injected after official object insert")
        if self._mode == "generation_insert_after" and (
            normalized.startswith("insert into calendar_generation_promotion")
        ):
            before_changes = self._connection.total_changes
            self._connection.execute(sql, parameters)
            self._state["generation_inserted"] = (
                self._connection.total_changes == before_changes + 1
            )
            self._state["injected"] = True
            raise sqlite3.OperationalError("injected after generation insert")
        if self._mode == "head_update_before" and normalized.startswith(
            "update calendar_generation_head set"
        ):
            self._state["injected"] = True
            raise sqlite3.OperationalError("injected before head update")
        return self._connection.execute(sql, parameters)

    def commit(self):
        if self._mode == "commit_before":
            self._state["injected"] = True
            raise sqlite3.OperationalError("injected before commit")
        return self._connection.commit()

    def __getattr__(self, name):
        return getattr(self._connection, name)


@pytest.mark.parametrize(
    "mode",
    [
        "official_insert_after",
        "generation_insert_after",
        "head_update_before",
        "commit_before",
    ],
)
def test_promotion_transaction_faults_rollback_without_orphan_or_canonical_change(
    tmp_path, monkeypatch, mode
) -> None:
    path, store, old_source, now = _promote_old_2026_authority(tmp_path)
    new_source = _valid_source()
    old_hashes = {schedule.body_sha256 for schedule in old_source.schedules}
    new_bodies = (b"sse body", b"szse body")
    new_hashes = {body_sha256(body) for body in new_bodies}
    assert old_hashes.isdisjoint(new_hashes)
    assert store.stage_execute(new_source, now).outcome == "STAGED"
    attempt = store.reserve_attempt(new_source, now)
    old_read = store.read()
    assert old_read.generation is not None
    old_head = (old_read.generation.sequence, old_read.generation.generation_sha256)

    canonical_root = tmp_path / "synthetic-canonical"
    (canonical_root / "nested").mkdir(parents=True)
    (canonical_root / "manifest.json").write_bytes(b"immutable manifest")
    (canonical_root / "nested" / "part-0.parquet").write_bytes(b"immutable canonical")
    canonical_before = _canonical_tree_snapshot(canonical_root)

    state = {"injected": False, "official_inserted": False, "generation_inserted": False}
    original_connect = store._connect

    def connect_with_fault(*, readonly=False):
        connection = original_connect(readonly=readonly)
        return _FaultConnection(connection, mode, state)

    monkeypatch.setattr(store, "_connect", connect_with_fault)
    result = store.promote(
        attempt,
        new_source,
        new_bodies,
        _machine_for(new_source, now),
        now + timedelta(hours=2),
    )
    assert state["injected"], f"fault injector did not trigger for {mode}"
    assert state["official_inserted"] == (mode == "official_insert_after")
    assert state["generation_inserted"] == (mode == "generation_insert_after")
    assert result.outcome != "PROMOTED"
    assert store.read().status == "ready"
    after = store.read()
    assert after.generation is not None
    assert (after.generation.sequence, after.generation.generation_sha256) == old_head
    assert _canonical_tree_snapshot(canonical_root) == canonical_before

    with sqlite3.connect(path) as connection:
        assert (
            connection.execute("SELECT sequence FROM calendar_generation_head").fetchone()[0] == 1
        )
        assert (
            connection.execute("SELECT COUNT(*) FROM calendar_generation_promotion").fetchone()[0]
            == 1
        )
        assert (
            connection.execute("SELECT COUNT(*) FROM calendar_official_object").fetchone()[0] == 2
        )
        objects = connection.execute(
            "SELECT body_sha256,body_bytes FROM calendar_official_object"
        ).fetchall()
        assert {row[0] for row in objects} == old_hashes
        assert {body_sha256(bytes(row[1])) for row in objects} == old_hashes
        assert not new_hashes.intersection({row[0] for row in objects})
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM calendar_maintenance_attempt WHERE target_year=2027"
            ).fetchone()[0]
            == 1
        )
    assert old_source.year == 2026


class _TamperedRow:
    def __init__(self, row):
        self._row = row

    def __getitem__(self, key):
        value = self._row[key]
        if key == "payload_json":
            payload = json.loads(value)
            payload["sequence"] = int(payload["sequence"]) + 100
            return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        return value

    def __iter__(self):
        return iter(self._row)


class _TamperedCursor:
    def __init__(self, cursor, state):
        self._cursor = cursor
        self._state = state

    def fetchone(self):
        row = self._cursor.fetchone()
        if row is not None:
            self._state["readback"] = True
            return _TamperedRow(row)
        return row

    def fetchall(self):
        rows = self._cursor.fetchall()
        if rows:
            self._state["readback"] = True
        return [_TamperedRow(row) for row in rows]

    def __iter__(self):
        for row in self._cursor:
            self._state["readback"] = True
            yield _TamperedRow(row)

    def __getattr__(self, name):
        return getattr(self._cursor, name)


class _ReadbackTamperConnection:
    def __init__(self, connection, state):
        self._connection = connection
        self._state = state
        self._generation_inserted = False

    def execute(self, sql, parameters=()):
        normalized = " ".join(str(sql).split()).lower()
        cursor = self._connection.execute(sql, parameters)
        if normalized.startswith("insert into calendar_generation_promotion"):
            self._generation_inserted = True
        if (
            self._generation_inserted
            and normalized.startswith("select")
            and "calendar_generation_promotion" in normalized
        ):
            return _TamperedCursor(cursor, self._state)
        return cursor

    def __getattr__(self, name):
        return getattr(self._connection, name)


def test_promotion_readback_tamper_is_observed_and_cannot_move_head(tmp_path, monkeypatch) -> None:
    path, store, _old_source, now = _promote_old_2026_authority(tmp_path)
    new_source = _valid_source()
    assert store.stage_execute(new_source, now).outcome == "STAGED"
    attempt = store.reserve_attempt(new_source, now)
    old = store.read().generation
    assert old is not None
    state = {"readback": False}
    original_connect = store._connect

    def connect_with_tamper(*, readonly=False):
        return _ReadbackTamperConnection(original_connect(readonly=readonly), state)

    monkeypatch.setattr(store, "_connect", connect_with_tamper)
    result = store.promote(
        attempt,
        new_source,
        (b"sse body", b"szse body"),
        _machine_for(new_source, now),
        now + timedelta(hours=2),
    )
    assert state["readback"], "promotion must read back its inserted generation before head update"
    assert result.outcome != "PROMOTED"
    monkeypatch.setattr(store, "_connect", original_connect)
    after = store.read()
    assert after.status == "ready"
    assert after.generation is not None
    assert after.generation.generation_sha256 == old.generation_sha256
    with sqlite3.connect(path) as connection:
        assert (
            connection.execute("SELECT sequence FROM calendar_generation_head").fetchone()[0] == 1
        )


def test_stale_parent_across_2026_revision_and_2027_slot_is_rejected(tmp_path) -> None:
    now = datetime(2026, 12, 22, 1, tzinfo=UTC)
    source_2026 = _bundled_2026_revision()
    source_2027 = _valid_source()
    path = tmp_path / "calendar_generations.sqlite3"
    store = CalendarGenerationStore(path)
    assert store.stage_execute(source_2026, now).outcome == "STAGED"
    assert store.stage_execute(source_2027, now).outcome == "STAGED"
    attempt_2026 = store.reserve_attempt(source_2026, now)
    attempt_2027 = store.reserve_attempt(source_2027, now)
    assert attempt_2026.expected_parent_sha256 is None
    assert attempt_2027.expected_parent_sha256 is None

    winner = store.promote(
        attempt_2026,
        source_2026,
        (b"bundled-2026-sse-revision", b"bundled-2026-szse-revision"),
        _machine_for(source_2026, now),
        now + timedelta(hours=1),
    )
    assert winner.outcome == "PROMOTED"
    stale = store.promote(
        attempt_2027,
        source_2027,
        (b"sse body", b"szse body"),
        _machine_for(source_2027, now),
        now + timedelta(hours=2),
    )
    assert stale.outcome == "PARENT_CHANGED"
    result = store.read()
    assert result.status == "ready"
    assert result.generation is not None
    assert result.generation.sequence == 1
    assert result.source is not None
    assert result.source.year == 2026
    with sqlite3.connect(path) as connection:
        assert (
            connection.execute("SELECT COUNT(*) FROM calendar_generation_promotion").fetchone()[0]
            == 1
        )
        attempts = connection.execute(
            "SELECT target_year,slot_date,source_sha256,expected_parent_sha256,outcome "
            "FROM calendar_maintenance_attempt "
            "ORDER BY target_year"
        ).fetchall()
        assert attempts[0][:4] == (2026, "2026-12-22", source_2026.source_sha256, None)
        assert attempts[0][4] == "PROMOTED"
        assert attempts[1][:4] == (2027, "2026-12-22", source_2027.source_sha256, None)
        assert attempts[1][4] in {"RUNNING", "PARENT_CHANGED"}


def test_writer_path_substitution_after_precheck_is_fail_closed_and_foreign_unchanged(
    tmp_path, monkeypatch
) -> None:
    path, store, _old_source, now = _promote_old_2026_authority(tmp_path)
    new_source = _valid_source()
    assert store.stage_execute(new_source, now).outcome == "STAGED"
    attempt = store.reserve_attempt(new_source, now)
    original_target = path.read_bytes()

    foreign = tmp_path / "foreign.sqlite3"
    real_connect = sqlite3.connect
    # The foreign DB is a complete byte-for-byte clone, including the reserved
    # candidate/attempt identity.  A schema mismatch would only test an early
    # error path and could hide an unsafe pathname swap.
    foreign.write_bytes(original_target)
    foreign_before = foreign.read_bytes()
    foreign_sentinel = tmp_path / "foreign.sentinel"
    foreign_sentinel.write_bytes(b"must remain untouched")
    foreign_sentinel_before = foreign_sentinel.read_bytes()
    backup = tmp_path / "original-target.sqlite3"
    state = {"opened_target": False}
    original_sqlite_connect = sqlite3.connect
    original_os_open = os.open

    def swap_target_path() -> None:
        if not state["opened_target"]:
            state["opened_target"] = True
            path.rename(backup)
            path.symlink_to(foreign)

    def substitute_before_open(database, *args, **kwargs):
        if Path(database) == path:
            swap_target_path()
        return original_sqlite_connect(database, *args, **kwargs)

    def substitute_after_fd_open(file, flags, *args, **kwargs):
        fd = original_os_open(file, flags, *args, **kwargs)
        if isinstance(file, (str, bytes, os.PathLike)) and Path(file) == path:
            swap_target_path()
        return fd

    monkeypatch.setattr(sqlite3, "connect", substitute_before_open)
    monkeypatch.setattr(os, "open", substitute_after_fd_open)
    result = store.promote(
        attempt,
        new_source,
        (b"sse body", b"szse body"),
        _machine_for(new_source, now),
        now + timedelta(hours=2),
    )
    assert state["opened_target"], "target pathname substitution did not reach sqlite open"
    assert result.outcome != "PROMOTED"
    assert backup.read_bytes() == original_target
    assert foreign.read_bytes() == foreign_before
    assert foreign_sentinel.read_bytes() == foreign_sentinel_before
    with real_connect(backup) as connection:
        assert (
            connection.execute("SELECT sequence FROM calendar_generation_head").fetchone()[0] == 1
        )
    assert store.read().status == "unavailable"


@pytest.mark.parametrize("operation", ["stage", "reserve", "promote"])
@pytest.mark.parametrize("corrupt_state", ["bundled_base", "unrelated_candidate"])
def test_writer_refuses_unproven_control_graph_without_mutation(
    tmp_path, operation, corrupt_state
) -> None:
    now = datetime(2026, 12, 22, 1, tzinfo=UTC)
    source = _valid_source()
    path = tmp_path / "calendar_generations.sqlite3"
    store = CalendarGenerationStore(path)
    assert store.stage_execute(source, now).outcome == "STAGED"
    unrelated = _bundled_2026_revision()
    if corrupt_state == "unrelated_candidate":
        assert store.stage_execute(unrelated, now).outcome == "STAGED"
    attempt = store.reserve_attempt(source, now) if operation == "promote" else None
    assert store.read().status == "ready"
    if corrupt_state == "bundled_base":
        _tamper_triggered_update(
            path,
            "calendar_meta_no_update",
            "UPDATE calendar_generation_meta SET bundled_sha256=?",
            ("f" * 64,),
        )
    else:
        damaged = unrelated.model_dump(mode="json")
        damaged["schedules"][0]["schedule_sha256"] = "f" * 64
        damaged["source_sha256"] = domain_sha256(
            "stock-eva/r2f4.1/calendar-source/v1",
            {key: value for key, value in damaged.items() if key != "source_sha256"},
        )
        _tamper_triggered_update(
            path,
            "calendar_candidate_no_update",
            "UPDATE calendar_generation_candidate SET source_sha256=?,payload_json=? "
            "WHERE source_sha256=?",
            (
                damaged["source_sha256"],
                canonical_json_bytes(damaged).decode(),
                unrelated.source_sha256,
            ),
        )
    assert store.read().status == "unavailable"
    before = (body_sha256(path.read_bytes()), path.stat().st_mtime_ns)
    try:
        if operation == "stage":
            schedules = tuple(
                schedule.model_copy(update={"review_id": "new-reviewed-identity"})
                for schedule in source.schedules
            )
            schedules = tuple(
                schedule.model_copy(update={"schedule_sha256": build_schedule_sha256(schedule)})
                for schedule in schedules
            )
            revised = source.model_copy(update={"schedules": schedules})
            revised = revised.model_copy(update={"source_sha256": build_source_sha256(revised)})
            result = store.stage_execute(revised, now)
        elif operation == "reserve":
            result = store.reserve_attempt(source, now)
        else:
            result = store.promote(
                attempt,
                source,
                (b"sse body", b"szse body"),
                _machine_for(source, now),
                now + timedelta(hours=1),
            )
    except CalendarStoreUnavailable:
        result = None
    assert (body_sha256(path.read_bytes()), path.stat().st_mtime_ns) == before
    assert result is None or result.outcome == "CONTROL_STATE_UNAVAILABLE"


@pytest.mark.parametrize("operation", ["stage", "reserve", "promote"])
def test_writer_holds_transaction_before_verified_state_read(tmp_path, monkeypatch, operation):
    now = datetime(2026, 12, 22, 1, tzinfo=UTC)
    store = CalendarGenerationStore(tmp_path / "calendar_generations.sqlite3")
    source = _valid_source()
    store.stage_execute(source, now)
    attempt = store.reserve_attempt(source, now) if operation == "promote" else None
    original = store._load_verified_state
    calls = []

    def verify(connection):
        calls.append(connection.in_transaction)
        assert connection.in_transaction, "writer proof/selection must run inside its transaction"
        return original(connection)

    monkeypatch.setattr(store, "_load_verified_state", verify)
    if operation == "stage":
        assert store.stage_execute(source, now).outcome == "ALREADY_STAGED"
    elif operation == "reserve":
        assert store.reserve_attempt(source, now).outcome == "RUNNING"
    else:
        assert (
            store.promote(
                attempt,
                source,
                (b"sse body", b"szse body"),
                _machine_for(source, now),
                now + timedelta(hours=1),
            ).outcome
            == "PROMOTED"
        )
    assert calls


def test_promotion_binds_the_transaction_verified_bundled_base(tmp_path, monkeypatch) -> None:
    now = datetime(2026, 12, 22, 1, tzinfo=UTC)
    source = _valid_source()
    path = tmp_path / "calendar_generations.sqlite3"
    store = calendar_module.CalendarGenerationStore(path)
    assert store.stage_execute(source, now).outcome == "STAGED"
    attempt = store.reserve_attempt(source, now)
    original_bundled = calendar_module._bundled_payload
    original = original_bundled()
    changed = copy.deepcopy(original)
    year = next(item for item in changed if item["year"] == 2026)
    assert "2026-12-24" not in year["closed_dates"]
    year["closed_dates"].append("2026-12-24")
    original_load = store._load_verified_state
    injected = False

    def load(connection):
        nonlocal injected
        verified = original_load(connection)
        monkeypatch.setattr(calendar_module, "_bundled_payload", lambda: changed)
        injected = True
        return verified

    monkeypatch.setattr(store, "_load_verified_state", load)
    result = store.promote(
        attempt,
        source,
        (b"sse body", b"szse body"),
        _machine_for(source, now),
        now + timedelta(hours=1),
    )
    assert injected
    with sqlite3.connect(path) as connection:
        meta_base = connection.execute(
            "SELECT bundled_sha256 FROM calendar_generation_meta"
        ).fetchone()[0]
        rows = connection.execute(
            "SELECT payload_json FROM calendar_generation_promotion"
        ).fetchall()
    if result.outcome == "PROMOTED":
        assert len(rows) == 1
        assert json.loads(rows[0][0])["bundled_sha256"] == meta_base
    else:
        assert rows == []
    monkeypatch.setattr(calendar_module, "_bundled_payload", original_bundled)
    monkeypatch.setattr(store, "_load_verified_state", original_load)
    assert store.read().status == "ready"


@pytest.mark.parametrize(
    "field,value",
    [
        ("target_year", True),
        ("official_requests", True),
        ("finished_at", datetime(2026, 12, 22, 2, tzinfo=UTC)),
        ("machine_requests", 1),
        ("official_requests", 3),
        ("started_at", datetime(2026, 12, 22, 1)),
        ("__missing_outcome__", None),
    ],
)
def test_promote_revalidates_construct_bypassed_attempt_before_terminalizing(
    tmp_path, field, value
) -> None:
    now = datetime(2026, 12, 22, 1, tzinfo=UTC)
    source = _valid_source()
    path = tmp_path / "calendar_generations.sqlite3"
    store = CalendarGenerationStore(path)
    assert store.stage_execute(source, now).outcome == "STAGED"
    actual = store.reserve_attempt(source, now)
    values = actual.model_dump(mode="python")
    if field == "__missing_outcome__":
        del values["outcome"]
    else:
        values[field] = value
    forged = BaseModel.model_construct.__func__(CalendarMaintenanceAttempt, **values)
    before = path.read_bytes()
    with warnings.catch_warnings(record=True) as caught:
        with pytest.raises((ValueError, TypeError, CalendarStoreUnavailable)):
            store.promote(
                forged,
                source,
                (b"sse body", b"szse body"),
                _machine_for(source, now),
                now + timedelta(hours=1),
            )
    assert not caught, "public attempt revalidation must not emit serializer warnings"
    assert path.read_bytes() == before
    with sqlite3.connect(path) as connection:
        row = connection.execute(
            "SELECT outcome,finished_at,official_requests,machine_requests "
            "FROM calendar_maintenance_attempt"
        ).fetchone()
    assert row == ("RUNNING", None, 0, 0)


@pytest.mark.parametrize("failure", ["official", "machine"])
def test_failure_audit_uses_promoted_at_and_real_request_counts(tmp_path, failure) -> None:
    now = datetime(2026, 12, 22, 1, tzinfo=UTC)
    promoted_at = now + timedelta(hours=1)
    source = _valid_source()
    path = tmp_path / "calendar_generations.sqlite3"
    store = CalendarGenerationStore(path)
    assert store.stage_execute(source, now).outcome == "STAGED"
    attempt = store.reserve_attempt(source, now)
    machine = _machine_for(source, now)
    bodies = (b"sse body", b"szse body")
    expected = ("MACHINE_CONFLICT", 2, 1)
    if failure == "official":
        bodies = (b"mismatched body", b"szse body")
        expected = ("OFFICIAL_HASH_MISMATCH", 2, 0)
    else:
        days = list(machine.days)
        days[0] = CalendarMachineDayV1(date=days[0].date, is_open=not days[0].is_open)
        machine = build_observation(
            provider=machine.provider,
            contract_version=machine.contract_version,
            range_start=machine.range_start,
            range_end=machine.range_end,
            observed_at=machine.observed_at,
            days=tuple(days),
        )
    result = store.promote(attempt, source, bodies, machine, promoted_at)
    assert result.outcome == expected[0]
    with sqlite3.connect(path) as connection:
        row = connection.execute(
            "SELECT outcome,finished_at,official_requests,machine_requests "
            "FROM calendar_maintenance_attempt"
        ).fetchone()
    assert row == (
        expected[0],
        promoted_at.isoformat(timespec="microseconds").replace("+00:00", "Z"),
        expected[1],
        expected[2],
    )
    assert store.read().status == "ready"


def test_failure_audit_rejects_finished_before_started_without_writing(tmp_path) -> None:
    now = datetime(2026, 12, 22, 1, tzinfo=UTC)
    source = _valid_source()
    path = tmp_path / "calendar_generations.sqlite3"
    store = CalendarGenerationStore(path)
    assert store.stage_execute(source, now).outcome == "STAGED"
    attempt = store.reserve_attempt(source, now)
    before = path.read_bytes()
    result = store.promote(
        attempt,
        source,
        (b"mismatched body", b"szse body"),
        _machine_for(source, now),
        now - timedelta(minutes=1),
    )
    assert result.outcome != "PROMOTED"
    assert path.read_bytes() == before
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT outcome,finished_at FROM calendar_maintenance_attempt"
        ).fetchone() == ("RUNNING", None)


def test_reserving_a_spent_slot_raises_typed_attempt_error(tmp_path) -> None:
    spent_type = getattr(calendar_module, "CalendarAttemptAlreadySpent", None)
    assert spent_type is not None, "the spent-slot domain error must be public and typed"
    assert issubclass(spent_type, CalendarStoreUnavailable)
    now = datetime(2026, 12, 22, 1, tzinfo=UTC)
    source = _valid_source()
    store = CalendarGenerationStore(tmp_path / "calendar_generations.sqlite3")
    assert store.stage_execute(source, now).outcome == "STAGED"
    first = store.reserve_attempt(source, now)
    assert first.outcome == "RUNNING"
    with pytest.raises(spent_type):
        store.reserve_attempt(source, now)
    with sqlite3.connect(store.path) as connection:
        assert connection.execute(
            "SELECT COUNT(*),outcome FROM calendar_maintenance_attempt"
        ).fetchone() == (1, "RUNNING")


def test_changed_review_identity_keeps_immutable_prior_snapshots(tmp_path) -> None:
    first_now = datetime(2026, 12, 22, 1, tzinfo=UTC)
    second_now = datetime(2026, 12, 23, 1, tzinfo=UTC)
    original = _valid_source()
    revised_schedules = tuple(
        schedule.model_copy(update={"review_id": "d3c-second-independent-review"})
        for schedule in original.schedules
    )
    revised_schedules = tuple(
        schedule.model_copy(update={"schedule_sha256": build_schedule_sha256(schedule)})
        for schedule in revised_schedules
    )
    revised = original.model_copy(
        update={"schedules": revised_schedules, "source_sha256": "0" * 64}
    )
    revised = revised.model_copy(update={"source_sha256": build_source_sha256(revised)})
    store = CalendarGenerationStore(tmp_path / "calendar_generations.sqlite3")
    assert store.stage_execute(original, first_now).outcome == "STAGED"
    first_attempt = store.reserve_attempt(original, first_now)
    assert (
        store.promote(
            first_attempt,
            original,
            (b"sse body", b"szse body"),
            _machine_for(original, first_now),
            first_now + timedelta(hours=1),
        ).outcome
        == "PROMOTED"
    )
    first_read = store.read()
    assert first_read.status == "ready" and first_read.calendar is not None
    first_snapshot = first_read.calendar
    first_digest = first_snapshot.generation_sha256
    assert store.stage_execute(revised, second_now).outcome == "STAGED"
    second_attempt = store.reserve_attempt(revised, second_now)
    assert (
        store.promote(
            second_attempt,
            revised,
            (b"sse body", b"szse body"),
            _machine_for(revised, second_now),
            second_now + timedelta(hours=1),
        ).outcome
        == "PROMOTED"
    )
    second_read = store.read()
    assert second_read.status == "ready" and second_read.calendar is not None
    second_snapshot = second_read.calendar
    assert first_digest != second_snapshot.generation_sha256
    assert first_snapshot.generation_sha256 == first_digest
    assert first_snapshot.bundled_sha256 == second_snapshot.bundled_sha256
    assert first_snapshot.configs[2025] == second_snapshot.configs[2025]
    assert first_snapshot.configs[2026] == second_snapshot.configs[2026]
    assert first_snapshot.configs[2027].closed_dates == second_snapshot.configs[2027].closed_dates
    assert first_snapshot.configs == second_snapshot.configs


def _read_fifo_source(path: Path, result_queue) -> None:
    try:
        calendar_module.load_source(path)
    except calendar_module.CalendarGenerationError:
        result_queue.put("rejected")


def test_fifo_source_is_rejected_without_blocking_before_open(tmp_path) -> None:
    fifo = tmp_path / "source.json"
    os.mkfifo(fifo, 0o600)
    context = multiprocessing.get_context("spawn")
    results = context.Queue()
    child = context.Process(target=_read_fifo_source, args=(fifo, results))
    child.start()
    child.join(timeout=1)
    blocked = child.is_alive()
    if blocked:
        child.terminate()
        child.join(timeout=1)
    try:
        assert not blocked, "source open must not block on a FIFO"
        assert child.exitcode == 0
        assert results.get(timeout=1) == "rejected"
    finally:
        child.close()
        results.close()


def test_fifo_lock_is_rejected_without_changing_store(tmp_path) -> None:
    store = calendar_module.CalendarGenerationStore(tmp_path / "calendar_generations.sqlite3")
    source = _valid_source()
    now = datetime(2026, 12, 22, 1, tzinfo=UTC)
    assert store.stage_execute(source, now).outcome == "STAGED"
    assert store.read().status == "ready"
    lock_path = store.path.with_name(store.path.name + ".lock")
    lock_path.unlink()
    os.mkfifo(lock_path, 0o600)
    before = store.path.read_bytes()
    result = store.read()
    assert result.status == "unavailable"
    assert not result.calendar.configs
    assert store.path.read_bytes() == before


def test_initialization_metadata_and_head_are_atomic(tmp_path, monkeypatch) -> None:
    store = calendar_module.CalendarGenerationStore(tmp_path / "calendar_generations.sqlite3")
    original = store._connect
    injected = False

    class Connection:
        def __init__(self, connection):
            self._connection = connection

        def execute(self, sql, parameters=()):
            nonlocal injected
            if str(sql).startswith("INSERT INTO calendar_generation_head"):
                assert (
                    self._connection.execute(
                        "SELECT COUNT(*) FROM calendar_generation_meta"
                    ).fetchone()[0]
                    == 1
                )
                injected = True
                raise sqlite3.OperationalError("private initialization fault")
            return self._connection.execute(sql, parameters)

        def __getattr__(self, name):
            return getattr(self._connection, name)

    monkeypatch.setattr(store, "_connect", lambda **kwargs: Connection(original(**kwargs)))
    with pytest.raises(CalendarStoreUnavailable):
        store.stage_execute(_valid_source(), datetime(2026, 12, 22, 1, tzinfo=UTC))
    assert injected
    if store.path.exists():
        with sqlite3.connect(f"file:{store.path}?mode=ro", uri=True) as connection:
            tables = {
                row[0]
                for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
            meta_count = (
                connection.execute("SELECT COUNT(*) FROM calendar_generation_meta").fetchone()[0]
                if "calendar_generation_meta" in tables
                else 0
            )
            head_count = (
                connection.execute("SELECT COUNT(*) FROM calendar_generation_head").fetchone()[0]
                if "calendar_generation_head" in tables
                else 0
            )
        assert (meta_count, head_count) == (0, 0)


def test_new_store_does_not_adopt_concurrently_created_empty_file(tmp_path, monkeypatch) -> None:
    store = calendar_module.CalendarGenerationStore(tmp_path / "calendar_generations.sqlite3")
    original_open = os.open
    injected = False

    def open_with_creation_race(path, flags, *args, **kwargs):
        nonlocal injected
        if os.fspath(path) == os.fspath(store.path) and flags & os.O_CREAT and not injected:
            injected = True
            created_fd = original_open(store.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            os.close(created_fd)
        return original_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(calendar_module.os, "open", open_with_creation_race)
    try:
        store.stage_execute(_valid_source(), datetime(2026, 12, 22, 1, tzinfo=UTC))
    except (OSError, CalendarStoreUnavailable):
        pass
    assert injected
    assert store.path.read_bytes() == b""


@pytest.mark.parametrize("kind", ["metadata_key", "head_key", "extra_head"])
def test_reader_rejects_non_normative_singleton_rows(tmp_path, kind) -> None:
    store = calendar_module.CalendarGenerationStore(tmp_path / "calendar_generations.sqlite3")
    assert (
        store.stage_execute(_valid_source(), datetime(2026, 12, 22, 1, tzinfo=UTC)).outcome
        == "STAGED"
    )
    assert store.read().status == "ready"
    with sqlite3.connect(store.path) as connection:
        connection.execute("PRAGMA ignore_check_constraints=ON")
        if kind == "metadata_key":
            trigger = connection.execute(
                "SELECT sql FROM sqlite_master WHERE name='calendar_meta_no_update'"
            ).fetchone()
            assert trigger is not None
            connection.execute("DROP TRIGGER calendar_meta_no_update")
            try:
                connection.execute("UPDATE calendar_generation_meta SET singleton=2")
            finally:
                connection.execute(trigger[0])
            assert (
                connection.execute(
                    "SELECT sql FROM sqlite_master WHERE name='calendar_meta_no_update'"
                ).fetchone()[0]
                == trigger[0]
            )
        elif kind == "head_key":
            connection.execute("UPDATE calendar_generation_head SET singleton=2")
        else:
            connection.execute("INSERT INTO calendar_generation_head VALUES (2,0,NULL)")
    before = store.path.read_bytes()
    result = store.read()
    assert result.status == "unavailable"
    assert not result.calendar.configs
    assert store.path.read_bytes() == before


@pytest.mark.parametrize("swap_phase", ["at_open", "after_validated_check"])
def test_r1_reader_rejects_valid_foreign_database_replaced_during_open(
    tmp_path, monkeypatch, swap_phase
):
    now = datetime(2026, 12, 22, 1, tzinfo=UTC)
    source = _valid_source()
    path = tmp_path / "calendar_generations.sqlite3"
    store = calendar_module.CalendarGenerationStore(path)
    store.stage_execute(source, now)
    foreign_path = tmp_path / "foreign.sqlite3"
    foreign = calendar_module.CalendarGenerationStore(foreign_path)
    foreign.stage_execute(source, now)
    attempt = foreign.reserve_attempt(source, now)
    assert (
        foreign.promote(
            attempt,
            source,
            (b"sse body", b"szse body"),
            _machine_for(source, now),
            now + timedelta(hours=1),
        ).outcome
        == "PROMOTED"
    )
    original_bytes, foreign_bytes = path.read_bytes(), foreign_path.read_bytes()
    backup = tmp_path / "original.sqlite3"
    real_open = os.open
    swapped = False
    real_check = store._check_path
    checks = 0

    def swap_database():
        nonlocal swapped
        swapped = True
        path.rename(backup)
        shutil.copyfile(foreign_path, path)
        path.chmod(0o600)

    def open_with_swap(file, flags, *args, **kwargs):
        nonlocal swapped
        if os.fspath(file) == os.fspath(path) and not swapped:
            swap_database()
        return real_open(file, flags, *args, **kwargs)

    def check_then_swap(*, allow_missing):
        nonlocal checks
        proof = real_check(allow_missing=allow_missing)
        checks += 1
        if checks == 2:
            swap_database()
        return proof

    if swap_phase == "at_open":
        monkeypatch.setattr(calendar_module.os, "open", open_with_swap)
    else:
        monkeypatch.setattr(store, "_check_path", check_then_swap)
    result = store.read()
    assert swapped
    assert backup.read_bytes() == original_bytes
    assert foreign_path.read_bytes() == foreign_bytes
    assert path.read_bytes() == foreign_bytes
    assert result.status == "unavailable"
    assert not result.calendar.configs


def _r1_read_database_with_fifo_swap(path, backup, results):
    real_open = os.open
    swapped = False

    def open_with_swap(file, flags, *args, **kwargs):
        nonlocal swapped
        if os.fspath(file) == os.fspath(path) and not swapped:
            swapped = True
            path.rename(backup)
            os.mkfifo(path, 0o600)
        return real_open(file, flags, *args, **kwargs)

    calendar_module.os.open = open_with_swap
    try:
        result = calendar_module.CalendarGenerationStore(path).read()
        results.put((swapped, result.status, bool(result.calendar.configs)))
    finally:
        calendar_module.os.open = real_open


def test_r1_reader_database_fifo_swap_is_nonblocking(tmp_path):
    path = tmp_path / "calendar_generations.sqlite3"
    backup = tmp_path / "original.sqlite3"
    store = calendar_module.CalendarGenerationStore(path)
    store.stage_execute(_valid_source(), datetime(2026, 12, 22, 1, tzinfo=UTC))
    original_bytes = path.read_bytes()
    context = multiprocessing.get_context("spawn")
    results = context.Queue()
    child = context.Process(target=_r1_read_database_with_fifo_swap, args=(path, backup, results))
    child.start()
    child.join(5)
    blocked = child.is_alive()
    if blocked:
        child.terminate()
        child.join(2)
    try:
        assert backup.read_bytes() == original_bytes
        assert path.is_fifo()
        assert not blocked, "database open blocks before descriptor rejection"
        assert child.exitcode == 0
        assert results.get(timeout=1) == (True, "unavailable", False)
    finally:
        child.close()
        results.close()


@pytest.mark.parametrize("kind", ["head_blob", "object_text"])
@pytest.mark.parametrize("operation", ["read", "stage", "reserve", "promote"])
def test_r1_sqlite_storage_type_corruption_is_safe(tmp_path, kind, operation):
    path, store, _, now = _promote_old_2026_authority(tmp_path)
    source = _valid_source()
    store.stage_execute(source, now)
    attempt = store.reserve_attempt(source, now)
    if kind == "head_blob":
        with sqlite3.connect(path) as connection:
            digest = connection.execute(
                "SELECT generation_sha256 FROM calendar_generation_head"
            ).fetchone()[0]
            connection.execute(
                "UPDATE calendar_generation_head SET generation_sha256=?",
                (sqlite3.Binary(digest.encode("ascii")),),
            )
    else:
        _tamper_triggered_update(
            path,
            "calendar_object_no_update",
            "UPDATE calendar_official_object SET body_bytes=CAST(body_bytes AS TEXT)",
            (),
        )
    before = path.read_bytes()
    if operation == "read":
        result = store.read()
        assert result.status == "unavailable"
        assert not result.calendar.configs
    else:
        try:
            if operation == "stage":
                store.stage_execute(source, now)
            elif operation == "reserve":
                store.reserve_attempt(source, now + timedelta(days=1))
            else:
                result = store.promote(
                    attempt,
                    source,
                    (b"sse body", b"szse body"),
                    _machine_for(source, now),
                    now + timedelta(hours=1),
                )
                assert result.outcome == "CONTROL_STATE_UNAVAILABLE"
        except calendar_module.CalendarStoreUnavailable:
            pass
        else:
            assert operation == "promote", "corrupt store was accepted by a writer"
    assert path.read_bytes() == before
