"""Both assembly paths share the unchanged service dimension column contract."""

import pyarrow as pa

from assemble_db import pipeline, staged_dimensions
from assemble_db.service_contract import SERVICE_ACCOUNT_SCHEMA, SERVICE_ENTITY_SCHEMA


def test_service_account_columns_preserve_order_and_types():
    expected = pa.schema(
        [
            (name, pa.string())
            for name in (
                "account_key",
                "bank_id",
                "account_no",
                "entity_id",
                "master_bank_id_raw",
                "internal_account_key",
                "bank_name",
            )
        ]
    )
    assert SERVICE_ACCOUNT_SCHEMA.equals(expected, check_metadata=True)
    assert pipeline.SERVICE_ACCOUNT_SCHEMA is SERVICE_ACCOUNT_SCHEMA
    assert staged_dimensions.SERVICE_ACCOUNT_SCHEMA is SERVICE_ACCOUNT_SCHEMA


def test_service_entity_columns_preserve_order_and_types():
    expected = pa.schema([("entity_id", pa.string()), ("entity_name", pa.string())])
    assert SERVICE_ENTITY_SCHEMA.equals(expected, check_metadata=True)
    assert pipeline.SERVICE_ENTITY_SCHEMA is SERVICE_ENTITY_SCHEMA
    assert staged_dimensions.SERVICE_ENTITY_SCHEMA is SERVICE_ENTITY_SCHEMA
