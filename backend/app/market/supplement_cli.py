"""Explicit command line entry point for optional supplemental ingestion."""

import argparse
import json
from datetime import date

from pydantic import ValidationError

from backend.app.config import Settings
from backend.app.market.akshare_supplemental import (
    AKShareSupplementalProvider,
    SupplementalDataError,
    SupplementalSourceUnavailableError,
)
from backend.app.market.supplement_ingestion import (
    SupplementalDatasetError,
    SupplementalDatasetInitializer,
    SupplementalIngestionRequest,
    SupplementalIngestionService,
    SupplementalStore,
    supplemental_dataset_root,
)
from backend.app.storage.preflight import StoragePreflight


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="stock-eva-supplemental",
        description=(
            "Plan or explicitly execute the audited AKShare supplemental canary. "
            "A plan performs no network requests."
        ),
    )
    parser.add_argument("--through-date", type=date.fromisoformat)
    parser.add_argument("--market-flow", action="store_true")
    parser.add_argument("--classification", action="store_true")
    parser.add_argument(
        "--sector",
        action="append",
        default=[],
        help="one explicit industry name; repeat for at most 10 industries",
    )
    parser.add_argument(
        "--initialize",
        action="store_true",
        help="explicitly create only the empty sentinel and manifest",
    )
    parser.add_argument(
        "--canary",
        action="store_true",
        help="confirm this is a small, controlled provider canary",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="perform provider requests and publish only after full validation",
    )
    return parser


def _service(settings: Settings) -> SupplementalIngestionService:
    root = supplemental_dataset_root(settings)
    if settings.nas_market_dataset_root is not None:
        readiness = StoragePreflight(settings).inspect()
        if not readiness.market_data_available:
            raise SupplementalDatasetError("configured NAS market dataset is unavailable")
        if not root.exists():
            raise SupplementalDatasetError("supplemental dataset must be explicitly initialized")
        SupplementalDatasetInitializer(
            root,
            require_smb=True,
        ).initialize()
    store = SupplementalStore(
        root,
        settings.local_control_dir / settings.supplemental_audit_database_name,
        staging_root=settings.local_staging_dir / "supplemental",
        lock_path=settings.local_lock_dir / "supplemental-ingestion.lock",
    )
    store.validate_initialized()
    return SupplementalIngestionService(store, AKShareSupplementalProvider())


def run(argv: list[str] | None = None, *, settings: Settings | None = None) -> int:
    args = build_parser().parse_args(argv)
    active_settings = settings or Settings()
    if args.initialize:
        if (
            args.execute
            or args.canary
            or args.through_date is not None
            or args.market_flow
            or args.classification
            or args.sector
        ):
            print(
                json.dumps(
                    {
                        "status": "rejected",
                        "error_code": "INITIALIZE_SCOPE_MUST_BE_EMPTY",
                    },
                    separators=(",", ":"),
                )
            )
            return 2
        try:
            if active_settings.nas_market_dataset_root is not None:
                readiness = StoragePreflight(active_settings).inspect()
                if not readiness.market_data_available:
                    raise SupplementalDatasetError("configured NAS market dataset is unavailable")
            root = SupplementalDatasetInitializer(
                supplemental_dataset_root(active_settings),
                require_smb=active_settings.nas_market_dataset_root is not None,
            ).initialize()
        except SupplementalDatasetError:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "error_code": "SUPPLEMENTAL_INITIALIZATION_FAILED",
                    },
                    separators=(",", ":"),
                )
            )
            return 4
        print(
            json.dumps(
                {
                    "status": "ready",
                    "dataset": "stock-eva-supplemental",
                    "root": str(root),
                },
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )
        return 0
    if args.through_date is None:
        print(
            json.dumps(
                {"status": "rejected", "error_code": "THROUGH_DATE_REQUIRED"},
                separators=(",", ":"),
            )
        )
        return 2
    try:
        request = SupplementalIngestionRequest(
            through_date=args.through_date,
            include_market_flow=args.market_flow,
            include_classification=args.classification,
            sector_names=args.sector,
            canary=args.canary,
        )
    except ValidationError:
        print(
            json.dumps(
                {"status": "rejected", "error_code": "INVALID_CANARY_SCOPE"},
                separators=(",", ":"),
            )
        )
        return 2

    if not args.execute:
        print(
            json.dumps(
                SupplementalIngestionService.plan(request),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 0
    if not args.canary:
        print(
            json.dumps(
                {
                    "status": "rejected",
                    "error_code": "EXPLICIT_CANARY_REQUIRED",
                },
                separators=(",", ":"),
            )
        )
        return 2
    try:
        service = _service(active_settings)
        result = service.execute(request)
    except SupplementalSourceUnavailableError:
        payload = {
            "status": "error",
            "error_code": "SUPPLEMENTAL_SOURCE_UNAVAILABLE",
        }
        print(json.dumps(payload, separators=(",", ":")))
        return 5
    except SupplementalDataError:
        payload = {"status": "error", "error_code": "SUPPLEMENTAL_SOURCE_INVALID"}
        print(json.dumps(payload, separators=(",", ":")))
        return 3
    except (SupplementalDatasetError, OSError):
        payload = {"status": "error", "error_code": "SUPPLEMENTAL_PUBLISH_FAILED"}
        print(json.dumps(payload, separators=(",", ":")))
        return 4
    print(
        json.dumps(
            result.model_dump(mode="json"),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
