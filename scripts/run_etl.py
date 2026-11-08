"""Run the ETL from a checkout without installing the package. Same arguments as etl.main."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from etl.main import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
