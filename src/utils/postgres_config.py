from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

JDBC_DRIVER = PROJECT_ROOT / "drivers" / "postgresql-42.7.12.jar"

POSTGRES_CONFIG = {
    "host": "localhost",
    "port": "5434",
    "database": "retail_dw",
    "user": "postgres",
    "password": ""  
}

JDBC_URL = (
    f"jdbc:postgresql://"
    f"{POSTGRES_CONFIG['host']}:"
    f"{POSTGRES_CONFIG['port']}/"
    f"{POSTGRES_CONFIG['database']}"
)