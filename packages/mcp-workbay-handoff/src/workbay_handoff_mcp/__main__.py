import sys


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "errors-record":
        from .errors_record_cli import main as errors_record_main

        return errors_record_main(sys.argv[2:])
    from .cli import main as cli_main

    cli_main()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
