"""Read-only Actions connection check. This command cannot execute trading."""
from zerotrader.cloud_preflight import main


if __name__ == "__main__":
    raise SystemExit(main())
