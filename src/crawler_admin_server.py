# SPDX-FileCopyrightText: OEDS Contributors
#
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations

import os

import uvicorn
from crawler_core.runtime_env import load_local_crawler_env


def main() -> None:
    load_local_crawler_env()

    host = os.getenv("OEDS_ADMIN_HOST", "127.0.0.1")
    port = int(os.getenv("OEDS_ADMIN_PORT", "3010"))
    reload_enabled = os.getenv("OEDS_ADMIN_RELOAD", "").lower() in {"1", "true", "yes"}

    uvicorn.run(
        "crawler_admin.app:app",
        host=host,
        port=port,
        reload=reload_enabled,
    )


if __name__ == "__main__":
    main()
