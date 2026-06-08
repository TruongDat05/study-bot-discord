from __future__ import annotations

from services.database import DatabaseService


def initialize_core_database(database: DatabaseService) -> None:
    """Run additive core migrations.

    The bot already owns the full schema in ``services.database``. This helper
    exists so plugin/config/ACL startup code can clearly state its dependency
    without knowing which backend or migration implementation is active.
    """
    database.initialize()

