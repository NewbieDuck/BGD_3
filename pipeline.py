import argparse

from src.config import configure_java_environment


configure_java_environment()

from src.orchestrator import run_pipeline


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="F1 ELT pipeline")
    parser.add_argument("--smoke", action="store_true", help="Run a quick test on the selected season only")
    parser.add_argument("--smoke-year", type=int, default=2025, help="Season used by --smoke")
    parser.add_argument("--smoke-round", type=int, help="Optional race round used by --smoke")
    parser.add_argument("--smoke-session", choices=["R", "Q"], help="Optional session type used by --smoke")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    return run_pipeline(
        smoke=args.smoke,
        smoke_year=args.smoke_year,
        smoke_round=args.smoke_round,
        smoke_session=args.smoke_session,
    )


if __name__ == "__main__":
    raise SystemExit(main())
