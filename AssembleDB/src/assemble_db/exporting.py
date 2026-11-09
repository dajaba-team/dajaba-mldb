"""선언한 형식을 쓰고 타입·값과 원문 금액 표현을 각각 재검증합니다."""

import csv
import hashlib
import json
import re
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.compute as _compute
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from dbcheck.distribution import sha256, write_json

compute = cast(Any, _compute)
FORMATS = ("csv", "json", "parquet")
SELECTED_CODEC = "typed-selected-formats-v1"


def from_wire(rows: list[dict], schema: pa.Schema, *, csv_mode=False) -> pa.Table:
    for row in rows:
        if set(row) != set(schema.names):
            raise ValueError("Reloaded columns differ from declared schema")
    if not rows:
        return pa.Table.from_batches([], schema=schema)
    return restore_table(pa.Table.from_pylist(rows), schema, csv_mode=csv_mode)


def restore_table(raw: pa.Table, schema: pa.Schema, *, csv_mode=False) -> pa.Table:
    arrays = []
    for field in schema:
        col = raw.column(field.name)
        if csv_mode:
            col = compute.if_else(
                compute.starts_with(col, "\\\\"), compute.utf8_slice_codeunits(col, start=1), col
            )
        elif not pa.types.is_null(col.type):
            expected_string = (
                pa.types.is_string(field.type)
                or pa.types.is_decimal(field.type)
                or pa.types.is_timestamp(field.type)
            )
            if expected_string and not pa.types.is_string(col.type):
                raise ValueError(f"JSON must preserve an exact string: {field.name}")
            if pa.types.is_integer(field.type) and not pa.types.is_integer(col.type):
                raise ValueError(f"JSON integer type mismatch: {field.name}")
            if pa.types.is_boolean(field.type) and not pa.types.is_boolean(col.type):
                raise ValueError(f"JSON boolean type mismatch: {field.name}")
        arrays.append(compute.cast(col, field.type, safe=True))
    return pa.Table.from_arrays(arrays, schema=schema)


def wire_rows(table: pa.Table) -> list[dict]:
    arrays = []
    for field in table.schema:
        column = table.column(field.name)
        if pa.types.is_decimal(field.type):
            column = compute.cast(column, pa.string())
        elif pa.types.is_timestamp(field.type):
            column = compute.strftime(column, format="%Y-%m-%dT%H:%M:%S")
        arrays.append(column)
    return pa.Table.from_arrays(arrays, names=table.schema.names).to_pylist()


def logical_hash(table: pa.Table, rows: list[dict] | None = None) -> str:
    # 원문 자릿수와 별개인 typed 논리 해시를 유지하여 과거 산출물도 검증합니다.
    rows = wire_rows(table) if rows is None else rows
    digest = hashlib.sha256(str(table.schema).encode("utf-8"))
    digest.update(b"\n")
    digest.update(
        json.dumps(rows, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
    )
    return digest.hexdigest()


def decimal_wire_hash(rows: list[dict], fields: list[str]) -> str:
    values = [[row[name] for name in fields] for row in rows]
    return hashlib.sha256(
        json.dumps(values, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode(
            "utf-8"
        )
    ).hexdigest()


def load_payload(
    path: Path, schema: pa.Schema, *, include_wire=False, data: bytes | None = None
) -> tuple[pa.Table, list[dict] | None]:
    if path.suffix == ".parquet":
        return pq.read_table(path if data is None else pa.BufferReader(data)), None
    if path.suffix == ".json":
        rows = json.loads(
            path.read_text(encoding="utf-8") if data is None else data.decode("utf-8")
        )
        if not isinstance(rows, list):
            raise ValueError("JSON file must contain one complete array")
        return from_wire(rows, schema), rows if include_wire else None
    if path.suffix != ".csv":
        raise ValueError("Unsupported dataset format")
    raw = pacsv.read_csv(
        path if data is None else pa.BufferReader(data),
        read_options=pacsv.ReadOptions(use_threads=False),
        convert_options=pacsv.ConvertOptions(
            column_types={f.name: pa.string() for f in schema},
            strings_can_be_null=True,
            quoted_strings_can_be_null=True,
            null_values=["\\N"],
        ),
    )
    if raw.schema.names != schema.names:
        raise ValueError("CSV header/order differs from schema")
    return restore_table(raw, schema, csv_mode=True), raw.to_pylist() if include_wire else None


def reload_file(path: Path, schema: pa.Schema) -> pa.Table:
    return load_payload(path, schema)[0]


def _formats(value) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or not value or any(type(x) is not str for x in value):
        raise ValueError("Formats must be an explicit nonempty list")
    if len(set(value)) != len(value) or set(value) - set(FORMATS):
        raise ValueError("Unknown or repeated dataset format")
    return tuple(value)


def declared_formats(manifest: dict) -> tuple[str, ...]:
    if "serialization_version" not in manifest:
        if "formats" in manifest or "lexical_decimal_fields" in manifest:
            raise ValueError("Selected format metadata requires its codec version")
        return FORMATS
    if manifest["serialization_version"] != SELECTED_CODEC:
        raise ValueError("Unsupported selected format codec")
    return _formats(manifest.get("formats"))


def primary_part_path(folder: Path, part: dict) -> Path:
    # 서비스 파일은 CSV/JSON이고 ML 파일은 Parquet일 수 있습니다.
    for suffix in (".parquet", ".csv", ".json"):
        matches = [item for item in part["files"] if Path(item["path"]).suffix == suffix]
        if matches:
            if len(matches) != 1:
                raise ValueError("Repeated primary format")
            path = (folder / matches[0]["path"]).resolve()
            if path.parent != folder.resolve():
                raise ValueError("Unsafe primary dataset path")
            return path
    raise ValueError("No readable dataset part")


class DatasetWriter:
    def __init__(self, folder: Path, schema: pa.Schema, metadata: dict, *, formats=None):
        self.folder, self.schema = folder, schema
        self.formats = FORMATS if formats is None else _formats(formats)
        folder.mkdir(parents=True, exist_ok=False)
        (folder / "schema.arrow").write_bytes(schema.serialize().to_pybytes())
        self.manifest: dict[str, Any] = dict(
            status="RUNNING",
            **metadata,
            rows=0,
            parts=[],
            schema=[dict(name=f.name, type=str(f.type), nullable=f.nullable) for f in schema],
            serialization=dict(
                csv="UTF-8, RFC4180, NULL=\\N; leading backslash escaped with one extra backslash",
                json="complete JSON array per part; exact decimal and timestamp strings",
                parquet="typed Arrow schema; zstd",
                logical_hash="declared schema plus canonical JSON logical values, stable record order",
            ),
        )
        if formats is not None:
            self.manifest.update(serialization_version=SELECTED_CODEC, formats=list(self.formats))

    def _rows(self, table: pa.Table, decimal_raw: dict[str, list[str | None]] | None) -> list[dict]:
        rows = wire_rows(table)
        if decimal_raw is None:
            if self.manifest.get("lexical_decimal_fields"):
                raise ValueError("Original decimal representation missing from a later batch")
            return rows
        names = list(decimal_raw)
        if not names or set(names) - {f.name for f in self.schema if pa.types.is_decimal(f.type)}:
            raise ValueError("Raw decimal fields must refer to declared decimal columns")
        previous = self.manifest.get("lexical_decimal_fields")
        if previous is None and self.manifest["parts"]:
            raise ValueError("Original decimal contract cannot start after an earlier batch")
        if previous is not None and previous != names:
            raise ValueError("Original decimal field order changed")
        self.manifest.update(
            serialization_version=SELECTED_CODEC,
            formats=list(self.formats),
            lexical_decimal_fields=names,
        )
        for name, original in decimal_raw.items():
            if len(original) != len(table):
                raise ValueError("Original decimal row count mismatch")
            values = table.column(name).to_pylist()
            for i, (text, value) in enumerate(zip(original, values)):
                if text is None:
                    if value is not None:
                        raise ValueError("Original decimal null/value mismatch")
                elif (
                    type(text) is not str or re.fullmatch(r"[+-]?[0-9]+(?:\.[0-9]+)?", text) is None
                ):
                    raise ValueError("Original decimal must remain plain exact text")
                elif value is None or Decimal(text) != value:
                    raise ValueError("Original decimal and typed value differ")
                rows[i][name] = text
        if not from_wire(rows, self.schema).equals(table):
            raise ValueError("Original decimal representation changes logical values")
        return rows

    def _emit(self, table: pa.Table, name: str, decimal_raw=None) -> None:
        if decimal_raw is not None and "parquet" in self.formats:
            raise ValueError("Preserve original spelling in text, then derive Parquet")
        rows = self._rows(table, decimal_raw)
        files = []
        fields = self.manifest.get("lexical_decimal_fields", [])
        lexical = decimal_wire_hash(rows, fields) if fields else None
        for fmt in self.formats:
            path = self.folder / f"{name}.{fmt}"
            if path.exists():
                raise FileExistsError("Output data file already exists")
            if fmt == "json":
                path.write_text(
                    json.dumps(rows, ensure_ascii=False, separators=(",", ":"), allow_nan=False),
                    encoding="utf-8",
                )
            elif fmt == "csv":
                with path.open("x", encoding="utf-8", newline="") as stream:
                    writer = csv.writer(stream)
                    writer.writerow(self.schema.names)
                    for row in rows:
                        cells = []
                        for key in self.schema.names:
                            value = row[key]
                            if value is None:
                                value = "\\N"
                            elif type(value) is bool:
                                value = "true" if value else "false"
                            elif isinstance(value, str) and value.startswith("\\"):
                                value = "\\" + value
                            cells.append(value)
                        writer.writerow(cells)
            else:
                pq.write_table(table, path, compression="zstd", version="2.6")
            loaded, payload = load_payload(path, self.schema, include_wire=bool(fields))
            if not loaded.schema.equals(self.schema) or not loaded.equals(table):
                raise ValueError(f"Reloaded logical values/types differ: {path}")
            if fields and fmt != "parquet":
                if payload is None or decimal_wire_hash(payload, fields) != lexical:
                    raise ValueError("Original decimal spelling failed independent read-back")
            files.append(
                dict(
                    path=path.name, sha256=sha256(path), bytes=path.stat().st_size, rows=len(loaded)
                )
            )
        part = dict(rows=len(table), logical_sha256=logical_hash(table), files=files)
        if fields:
            if "parquet" in self.formats:
                raise ValueError(
                    "Original decimal spelling belongs to canonical text; derive Parquet afterward"
                )
            assert isinstance(lexical, str)
            part["lexical_decimal_sha256"] = lexical
        self.manifest["parts"].append(part)
        self.manifest["rows"] += len(table)
        write_json(self.folder / "manifest.json", self.manifest)

    def write(
        self, table: pa.Table, *, decimal_raw: dict[str, list[str | None]] | None = None
    ) -> None:
        if table.schema != self.schema:
            raise ValueError("Output batch schema changed")
        if len(table):
            self._emit(table, f"part-{len(self.manifest['parts']):06d}", decimal_raw)

    def finish(self) -> dict:
        if not self.manifest["parts"]:
            self._emit(pa.Table.from_batches([], schema=self.schema), "empty")
        self.manifest.update(
            status="VERIFIED", three_format_reload_verified=set(self.formats) == set(FORMATS)
        )
        if "serialization_version" in self.manifest:
            self.manifest["format_reload_verified"] = True
        write_json(self.folder / "manifest.json", self.manifest)
        return self.manifest


def verify_dataset(folder: Path) -> dict:
    manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or manifest.get("status") != "VERIFIED":
        raise ValueError("Incomplete dataset manifest")
    schema = pa.ipc.read_schema(pa.BufferReader((folder / "schema.arrow").read_bytes()))
    formats = declared_formats(manifest)
    fields = manifest.get("lexical_decimal_fields", [])
    if (
        not isinstance(fields, list)
        or len(set(fields)) != len(fields)
        or set(fields) - {f.name for f in schema if pa.types.is_decimal(f.type)}
    ):
        raise ValueError("Invalid original-decimal field declaration")
    if fields and "parquet" in formats:
        raise ValueError("Original decimal spelling must be committed in canonical text")
    rows = 0
    for part in manifest["parts"]:
        if {Path(f["path"]).suffix for f in part["files"]} != {"." + fmt for fmt in formats} or len(
            part["files"]
        ) != len(formats):
            raise ValueError("Part files differ from declared format contract")
        for item in part["files"]:
            path = (folder / item["path"]).resolve()
            if path.parent != folder.resolve() or sha256(path) != item["sha256"]:
                raise ValueError("Unsafe or modified output file")
            table, payload = load_payload(path, schema, include_wire=bool(fields))
            if (
                not table.schema.equals(schema)
                or len(table) != part["rows"]
                or logical_hash(table) != part["logical_sha256"]
            ):
                raise ValueError("Output logical value/type/row reconciliation failed")
            if fields and path.suffix != ".parquet":
                if payload is None or decimal_wire_hash(payload, fields) != part.get(
                    "lexical_decimal_sha256"
                ):
                    raise ValueError("Output original decimal spelling differs")
        rows += part["rows"]
    if rows != manifest["rows"]:
        raise ValueError("Dataset row total mismatch")
    return dict(rows=rows, parts=len(manifest["parts"]), status="VERIFIED")
