"""Explicit GET-only PAPER migration reconciliation; never starts a trader."""
from zerotrader.cloud_reconcile import main


if __name__ == "__main__":
    raise SystemExit(main())
