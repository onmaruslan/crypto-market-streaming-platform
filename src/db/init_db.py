from sqlalchemy import create_engine, text

from src.config.settings import POSTGRES_URI
from src.db.ddl import MARKET_TRADES_DDL, LOADED_STAGING_FILES_DDL, TRADES_1M_AGG_DDL


def init_db():
    engine = create_engine(POSTGRES_URI)

    with engine.begin() as conn:
        conn.execute(text(MARKET_TRADES_DDL))
        conn.execute(text(LOADED_STAGING_FILES_DDL))
        conn.execute(text(TRADES_1M_AGG_DDL))
