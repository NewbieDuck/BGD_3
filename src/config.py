from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class PipelineConfig:
    db_host: str
    db_port: str
    db_name: str
    db_user: str
    db_password: str
    jdbc_driver: str
    jdbc_jar: Path
    bronze_dir: Path
    fastf1_cache_dir: Path
    java_home: str

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.db_host}:{self.db_port}/{self.db_name}"

    @property
    def jdbc_props(self) -> dict[str, str]:
        return {
            "user": self.db_user,
            "password": self.db_password,
            "driver": self.jdbc_driver,
        }

    @property
    def sqlalchemy_url(self) -> str:
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"


def load_config() -> PipelineConfig:
    _load_env_file(PROJECT_ROOT / ".env")

    jdbc_jar = Path(os.getenv("F1_JDBC_JAR", str(PROJECT_ROOT / "drivers" / "postgresql-42.7.3.jar")))
    bronze_dir = Path(os.getenv("F1_BRONZE_DIR", str(PROJECT_ROOT / "f1_raw_data")))
    cache_dir = Path(os.getenv("F1_FASTF1_CACHE_DIR", str(PROJECT_ROOT / "ff1cache")))

    return PipelineConfig(
        db_host=os.getenv("F1_DB_HOST", "localhost"),
        db_port=os.getenv("F1_DB_PORT", "5432"),
        db_name=os.getenv("F1_DB_NAME", "f1db"),
        db_user=os.getenv("F1_DB_USER", "f1user"),
        db_password=os.getenv("F1_DB_PASSWORD", "admin"),
        jdbc_driver=os.getenv("F1_JDBC_DRIVER", "org.postgresql.Driver"),
        jdbc_jar=jdbc_jar if jdbc_jar.is_absolute() else PROJECT_ROOT / jdbc_jar,
        bronze_dir=bronze_dir if bronze_dir.is_absolute() else PROJECT_ROOT / bronze_dir,
        fastf1_cache_dir=cache_dir if cache_dir.is_absolute() else PROJECT_ROOT / cache_dir,
        java_home=os.getenv("JAVA_HOME", "/usr/lib/jvm/java-17-temurin-jdk"),
    )


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def configure_java_environment(config: PipelineConfig | None = None) -> None:
    config = config or load_config()
    os.environ["JAVA_HOME"] = config.java_home
    os.environ["PATH"] = f"{config.java_home}/bin:{os.environ.get('PATH', '')}"
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    os.environ.setdefault(
        "JAVA_TOOL_OPTIONS",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
        "--add-opens=java.base/javax.security.auth=ALL-UNNAMED",
    )
