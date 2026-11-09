"""서비스 차원은 원본 계좌와 거래 참조를 디스크에서 정확히 연결해 만듭니다."""

import shutil
import sqlite3
from pathlib import Path

import pyarrow as pa
from dbcheck.data_contract import account_match_key, bank_alias, service_account_key

from .exporting import DatasetWriter
from .service_contract import SERVICE_ACCOUNT_SCHEMA, SERVICE_ENTITY_SCHEMA
from .staged_contract import Progress, TextInput, require


class Dimensions:
    def __init__(self, folder: Path):
        self.folder = folder
        folder.mkdir(parents=True, exist_ok=False)
        self.path = folder / "dimensions.sqlite"
        with self.path.open("xb"):
            pass
        self.connection = sqlite3.connect(self.path)
        try:
            self.connection.execute("PRAGMA cache_size=-16384")
            self.temp = folder / "sqlite_temp"
            self.temp.mkdir()
            quoted = str(self.temp.resolve()).replace("'", "''")
            self.connection.execute("PRAGMA temp_store_directory='" + quoted + "'")
            actual = self.connection.execute("PRAGMA temp_store_directory").fetchone()
            require(
                actual is not None and Path(actual[0]).resolve() == self.temp.resolve(),
                "Cannot confirm isolated SQLite temporary directory",
            )
            self.connection.execute("PRAGMA temp_store=FILE")
            self.connection.execute(
                "CREATE TABLE master(key TEXT COLLATE BINARY PRIMARY KEY,bank TEXT NOT NULL,acct TEXT NOT NULL,eid TEXT NOT NULL,raw_bank TEXT NOT NULL,internal TEXT NOT NULL,bname TEXT NOT NULL,ename TEXT NOT NULL) WITHOUT ROWID"
            )
            self.connection.execute(
                "CREATE TABLE refs(key TEXT COLLATE BINARY PRIMARY KEY) WITHOUT ROWID"
            )
            self.connection.commit()
        except BaseException:
            self.connection.close()
            raise
        self.observed_bytes = 0

    def budget(self) -> None:
        paths = [
            self.path,
            Path(str(self.path) + "-journal"),
            Path(str(self.path) + "-wal"),
            Path(str(self.path) + "-shm"),
        ]
        paths.extend(self.temp.iterdir())
        size = sum(path.stat().st_size for path in paths if path.is_file())
        self.observed_bytes = max(self.observed_bytes, size)
        require(
            size < 2 * 1024**3 and shutil.disk_usage(self.folder).free > 5 * 1024**3,
            "Service dimension staging/free-space budget exceeded",
        )

    def load_master(self, source: TextInput, release: str, progress: Progress) -> None:
        count = 0
        for index in range(len(source.manifest["parts"])):
            table = source.read(index)
            rows = []
            for row in table.to_pylist():
                require(
                    all(
                        type(row[name]) is str
                        for name in (
                            "account_key",
                            "bank_raw",
                            "account_raw",
                            "entity_id_raw",
                            "entity_name_raw",
                            "bank_name_raw",
                        )
                    ),
                    "Original account master must preserve string values",
                )
                require(
                    row["account_key"]
                    == account_match_key(release, row["bank_raw"], row["account_raw"]),
                    "Original account master key differs",
                )
                rows.append(
                    (
                        service_account_key(row["bank_raw"], row["account_raw"]),
                        bank_alias(row["bank_raw"]),
                        row["account_raw"],
                        row["entity_id_raw"],
                        row["bank_raw"],
                        row["account_key"],
                        row["bank_name_raw"],
                        row["entity_name_raw"],
                    )
                )
            self.connection.executemany("INSERT INTO master VALUES (?,?,?,?,?,?,?,?)", rows)
            self.connection.commit()
            count += len(rows)
            self.budget()
            progress.emit("hi_service/master", count, source.manifest["rows"], "accounts")
        require(count == source.manifest["rows"], "Service master coverage differs")
        source.verify_metadata()

    def add_references(self, transactions: pa.Table) -> None:
        values = transactions.select(["from_account_key", "to_account_key"]).to_pydict()
        rows = [
            (value,) for name in ("from_account_key", "to_account_key") for value in values[name]
        ]
        require(all(type(row[0]) is str for row in rows), "Service reference key must be a string")
        self.connection.executemany("INSERT OR IGNORE INTO refs VALUES (?)", rows)
        self.connection.commit()
        self.budget()

    def export(self, folder: Path, metadata: dict, progress: Progress) -> tuple[dict, dict]:
        missing = self.connection.execute(
            "SELECT count(*) FROM refs r WHERE NOT EXISTS(SELECT 1 FROM master m WHERE m.key=r.key)"
        ).fetchone()[0]
        require(missing == 0, "Service reference absent from original master")
        conflict = self.connection.execute(
            "SELECT count(*) FROM (SELECT m.eid FROM master m JOIN refs r ON r.key=m.key GROUP BY m.eid HAVING count(DISTINCT m.ename)>1)"
        ).fetchone()[0]
        require(conflict == 0, "Referenced entity has inconsistent original names")
        schema = SERVICE_ACCOUNT_SCHEMA
        fields = schema.names
        writer = DatasetWriter(
            folder / "accounts", schema, dict(metadata, table="accounts"), formats=("csv", "json")
        )
        cursor = self.connection.execute(
            "SELECT m.key,m.bank,m.acct,m.eid,m.raw_bank,m.internal,m.bname FROM master m JOIN refs r ON r.key=m.key ORDER BY m.bank,m.acct"
        )
        count = 0
        total = self.connection.execute("SELECT count(*) FROM refs").fetchone()[0]
        try:
            while True:
                values = cursor.fetchmany(10000)
                if not values:
                    break
                writer.write(
                    pa.Table.from_pylist([dict(zip(fields, row)) for row in values], schema=schema)
                )
                count += len(values)
                self.budget()
                progress.emit("hi_service/accounts", count, total, "accounts")
        finally:
            cursor.close()
        require(count == total, "Service account/reference population differs")
        accounts = writer.finish()
        entity_schema = SERVICE_ENTITY_SCHEMA
        entity_writer = DatasetWriter(
            folder / "entities",
            entity_schema,
            dict(metadata, table="entities"),
            formats=("csv", "json"),
        )
        cursor = self.connection.execute(
            "SELECT DISTINCT m.eid,m.ename FROM master m JOIN refs r ON r.key=m.key ORDER BY m.eid"
        )
        entities = 0
        try:
            while True:
                values = cursor.fetchmany(10000)
                if not values:
                    break
                entity_writer.write(
                    pa.Table.from_pylist(
                        [dict(entity_id=row[0], entity_name=row[1]) for row in values],
                        schema=entity_schema,
                    )
                )
                entities += len(values)
                self.budget()
                progress.emit("hi_service/entities", entities, None, "entities")
        finally:
            cursor.close()
        entity_manifest = entity_writer.finish()
        self.budget()
        datasets = {
            "service_accounts": {"path": "service/accounts", "rows": accounts["rows"]},
            "service_entities": {"path": "service/entities", "rows": entity_manifest["rows"]},
        }
        counts = dict(
            accounts=count,
            entities=entities,
            references_complete=True,
            synthetic_customer_attributes_created=False,
            bank_alias_resolution="normalized service bank ID; original bank spelling in raw fields; internal ML keys preserved",
            staging_observed_bytes=self.observed_bytes,
            staging_cache_kib=16384,
            staging_budget_is_not_rss_or_transient_disk_hard_limit=True,
        )
        return datasets, counts

    def close(self) -> None:
        self.connection.close()
