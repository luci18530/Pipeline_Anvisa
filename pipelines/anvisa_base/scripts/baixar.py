#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Compat: encaminha para pipelines.anvisa_base.workflows.baixar."""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.anvisa_base.workflows.baixar import *  # noqa: F401,F403
from pipelines.anvisa_base.workflows.baixar import main


if __name__ == "__main__":
    main()
