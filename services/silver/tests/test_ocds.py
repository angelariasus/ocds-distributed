import json

import pytest
from pyspark.sql import SparkSession

from silver import ocds


@pytest.fixture(scope="session")
def spark():
    s = SparkSession.builder.master("local[2]").appName("silver-tests").config("spark.sql.shuffle.partitions", "2").getOrCreate()
    yield s
    s.stop()


def compiled(ocid="ocds-1", amendments=()):
    return {
        "ocid": ocid,
        "date": "2026-09-05T10:00:00-05:00",
        "tender": {
            "id": "1258637",
            "title": "ADQUISICION DE MEDICAMENTOS",
            "procurementMethod": "direct",
            "procurementMethodDetails": "Contratación Directa",
            "mainProcurementCategory": "goods",
            "datePublished": "2026-09-01T08:00:00-05:00",
            "value": {"amount": 1000.0},
            "items": [
                {"id": "1", "description": "PARACETAMOL 500 MG", "quantity": 10.0, "unit": {"name": "UNIDAD"}},
                {"id": "2", "description": "IBUPROFENO 400 MG", "quantity": 5.0, "unit": {"name": "UNIDAD"}},
            ],
        },
        "awards": [
            {
                "id": "A1",
                "date": "2026-09-10T00:00:00-05:00",
                "status": "active",
                "value": {"amount": 900.0},
                "suppliers": [{"id": "PE-RUC-20100000001", "name": "FARMACIA SAC"}],
                "items": [{"id": "1"}],
            }
        ],
        "contracts": [
            {
                "id": "C1",
                "awardID": "A1",
                "dateSigned": "2026-09-20T00:00:00-05:00",
                "value": {"amount": 950.0},
                "items": [{"id": "1"}],
                "amendments": [{"value": {"amount": a}} for a in amendments],
            },
            # A second contract keeps the single-contract fallback from assigning C1 to every item.
            {"id": "C2", "awardID": None, "dateSigned": None, "value": {"amount": 10.0}, "items": [{"id": "9"}], "amendments": []},
        ],
        "parties": [
            {
                "id": "PE-RUC-20131257750",
                "name": "SEGURO SOCIAL DE SALUD - ESSALUD",
                "roles": ["buyer"],
                "address": {"region": "LIMA"},
                "additionalIdentifiers": [{"id": "20131257750", "scheme": "PE-RUC"}],
            }
        ],
    }


def state_row(ocid, lamport, body, partition=0, offset=0):
    value = {"ocid": ocid, "source": "seace_v3", "lamport": lamport, "compiled": {"date": body["date"], "body": json.dumps(body)}}
    return (ocid.encode(), json.dumps(value).encode(), partition, offset)


def kafka_frame(spark, rows):
    return spark.createDataFrame(rows, "key binary, value binary, partition int, offset long")


def test_flatten_resolves_award_and_contract_per_item(spark):
    states = ocds.latest_states(kafka_frame(spark, [state_row("ocds-1", 5, compiled(amendments=(100.0, 50.0)))]))
    rows = {r["n_item"]: r for r in ocds.flatten(states).collect()}

    assert set(rows) == {"1", "2"}
    item1, item2 = rows["1"], rows["2"]
    assert item1["monto_adjudicado"] == 900.0 and item1["monto_contratado"] == 950.0
    assert item1["monto_adicional"] == 150.0 and item1["tiene_adenda"] is True
    assert item1["ruc_proveedor"] == "20100000001" and item1["nombre_proveedor"] == "FARMACIA SAC"
    assert item1["ruc_comprador"] == "20131257750" and item1["departamento_comprador"] == "LIMA"
    assert item1["es_contratacion_directa"] is True
    assert str(item1["fecha_convocatoria"]) == "2026-09-01" and str(item1["fecha_suscripcion"]) == "2026-09-20"
    assert item1["anio_fiscal"] == 2026
    # Item 2 was never awarded: the award cascade must not borrow item 1's award.
    assert item2["monto_adjudicado"] is None and item2["n_cod_contrato"] is None
    assert item2["monto_adicional"] == 0.0 and item2["tiene_adenda"] is False


def test_latest_states_keeps_highest_lamport_per_key(spark):
    old = compiled()
    old["tender"]["title"] = "OLD"
    new = compiled()
    new["tender"]["title"] = "NEW"
    frame = kafka_frame(
        spark,
        [
            state_row("ocds-1", 9, new, offset=1),
            state_row("ocds-1", 3, old, offset=2),
            state_row("ocds-2", 1, compiled("ocds-2"), offset=3),
        ],
    )
    states = ocds.latest_states(frame).collect()
    by_ocid = {r["ocid"]: r for r in states}
    assert len(states) == 2
    assert by_ocid["ocds-1"]["compiledRelease"]["tender"]["title"] == "NEW"
    assert by_ocid["ocds-1"]["lamport"] == 9


def test_records_without_items_produce_no_rows(spark):
    body = compiled()
    body["tender"]["items"] = []
    states = ocds.latest_states(kafka_frame(spark, [state_row("ocds-1", 1, body)]))
    assert ocds.flatten(states).count() == 0


def test_amounts_are_per_item_not_per_process(spark):
    # One call with two items and one award covering both: values must split, not repeat.
    body = compiled()
    body["tender"]["value"] = {"amount": 3000.0}
    body["tender"]["items"][0]["totalValue"] = {"amount": 1000.0}
    body["tender"]["items"][1]["totalValue"] = {"amount": 2000.0}
    body["awards"][0]["value"] = {"amount": 2700.0}
    body["awards"][0]["items"] = [{"id": "1", "totalValue": {"amount": 900.0}}, {"id": "2", "totalValue": {"amount": 1800.0}}]
    body["contracts"] = []
    states = ocds.latest_states(kafka_frame(spark, [state_row("ocds-1", 1, body)]))
    rows = {r["n_item"]: r for r in ocds.flatten(states).collect()}

    assert rows["1"]["monto_referencial"] == 1000.0 and rows["2"]["monto_referencial"] == 2000.0
    assert rows["1"]["monto_adjudicado"] == 900.0 and rows["2"]["monto_adjudicado"] == 1800.0
    assert sum(r["monto_referencial"] for r in rows.values()) == 3000.0
    assert rows["1"]["monto_referencial_proceso"] == 3000.0
