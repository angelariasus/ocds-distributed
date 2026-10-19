"""OCDS compiled releases → one row per tender item (the Silver grain).

Ported from essalud-pipeline's ocds_flattener: a strict projected schema, explode of
tender items and the award → contract cascade resolved per item with higher-order
array functions, so the whole transformation stays inside Spark's optimizer and
runs in parallel across partitions.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, LongType, StringType, StructField, StructType

_VALUE = StructType([StructField("amount", DoubleType())])
_ID_ONLY = StructType([StructField("id", StringType())])
_ITEM = StructType(
    [
        StructField("id", StringType()),
        StructField("description", StringType()),
        StructField("quantity", DoubleType()),
        StructField("unit", StructType([StructField("name", StringType())])),
        StructField("classification", StructType([StructField("id", StringType()), StructField("description", StringType())])),
    ]
)
_TENDER = StructType(
    [
        StructField("id", StringType()),
        StructField("title", StringType()),
        StructField("description", StringType()),
        StructField("procurementMethod", StringType()),
        StructField("procurementMethodDetails", StringType()),
        StructField("mainProcurementCategory", StringType()),
        StructField("datePublished", StringType()),
        StructField("value", _VALUE),
        StructField("tenderPeriod", StructType([StructField("startDate", StringType())])),
        StructField("items", ArrayType(_ITEM)),
    ]
)
_SUPPLIER = StructType([StructField("id", StringType()), StructField("name", StringType())])
_AWARD = StructType(
    [
        StructField("id", StringType()),
        StructField("date", StringType()),
        StructField("status", StringType()),
        StructField("value", _VALUE),
        StructField("suppliers", ArrayType(_SUPPLIER)),
        StructField("items", ArrayType(_ID_ONLY)),
    ]
)
_CONTRACT = StructType(
    [
        StructField("id", StringType()),
        StructField("awardID", StringType()),
        StructField("dateSigned", StringType()),
        StructField("value", _VALUE),
        StructField("items", ArrayType(_ID_ONLY)),
        StructField("amendments", ArrayType(StructType([StructField("value", _VALUE)]))),
    ]
)
_PARTY = StructType(
    [
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("roles", ArrayType(StringType())),
        StructField("address", StructType([StructField("region", StringType())])),
        StructField("additionalIdentifiers", ArrayType(StructType([StructField("id", StringType()), StructField("scheme", StringType())]))),
    ]
)
COMPILED_SCHEMA = StructType(
    [
        StructField("ocid", StringType()),
        StructField("date", StringType()),
        StructField("tender", _TENDER),
        StructField("awards", ArrayType(_AWARD)),
        StructField("contracts", ArrayType(_CONTRACT)),
        StructField("parties", ArrayType(_PARTY)),
    ]
)

# The compiler's state envelope as published on records.compiled (only what Silver reads).
STATE_SCHEMA = StructType(
    [
        StructField("ocid", StringType()),
        StructField("source", StringType()),
        StructField("lamport", LongType()),
        StructField("compiled", StructType([StructField("date", StringType()), StructField("body", StringType())])),
        StructField("last_seen", StringType()),
    ]
)


def latest_states(kafka_df: DataFrame) -> DataFrame:
    """Reduces a batch read of the compacted topic to the newest state per OCID.

    Compaction is eventual, so a batch read can still return several versions of a key; the highest
    Lamport time (then offset) wins. Returns ocid, lamport, compiled date and the parsed record.
    """
    parsed = kafka_df.select(
        F.col("key").cast("string").alias("key"),
        F.col("partition"),
        F.col("offset"),
        F.from_json(F.col("value").cast("string"), STATE_SCHEMA).alias("state"),
    )
    ranked = parsed.withColumn(
        "rank",
        F.row_number().over(Window.partitionBy("key").orderBy(F.col("state.lamport").desc(), F.col("offset").desc())),
    )
    return ranked.filter(F.col("rank") == 1).select(
        F.col("key").alias("ocid"),
        F.col("state.source").alias("source"),
        F.col("state.lamport").alias("lamport"),
        F.col("state.compiled.date").alias("compiled_date"),
        F.from_json(F.col("state.compiled.body"), COMPILED_SCHEMA).alias("compiledRelease"),
    )


def _clean_ruc(col):
    cleaned = F.regexp_extract(col, r"(\d+)$", 1)
    return F.when(col.isNull(), F.lit(None)).when(cleaned == "", col).otherwise(cleaned)


def _to_date(col):
    return F.to_date(F.substring(col, 1, 10), "yyyy-MM-dd")


def flatten(df: DataFrame) -> DataFrame:
    """One row per tender item with its resolved award, contract, buyer and supplier."""
    tender = F.col("compiledRelease.tender")
    parties = F.col("compiledRelease.parties")

    buyer = F.coalesce(
        F.element_at(F.filter(parties, lambda p: F.array_contains(p["roles"], F.lit("buyer"))), 1),
        F.element_at(F.filter(parties, lambda p: F.array_contains(p["roles"], F.lit("procuringEntity"))), 1),
    )
    peruc = F.element_at(F.filter(buyer["additionalIdentifiers"], lambda x: x["scheme"] == F.lit("PE-RUC")), 1)["id"]

    rec = (
        df.withColumn("_buyer", buyer)
        .withColumn("_awards", F.coalesce(F.col("compiledRelease.awards"), F.array().cast(ArrayType(_AWARD))))
        .withColumn("_contracts", F.coalesce(F.col("compiledRelease.contracts"), F.array().cast(ArrayType(_CONTRACT))))
        .withColumn("item", F.explode_outer(tender["items"]))
        .filter(F.col("item").isNotNull())
        .withColumn("_item_id", F.col("item.id"))
    )

    awards_matching = F.filter(F.col("_awards"), lambda a: F.exists(a["items"], lambda i: i["id"] == F.col("_item_id")))
    any_award_items = F.coalesce(F.exists(F.col("_awards"), lambda a: F.size(a["items"]) > 0), F.lit(False))
    rec = rec.withColumn(
        "_award",
        F.when(F.size(awards_matching) > 0, F.element_at(awards_matching, 1))
        .when((~any_award_items) & (F.size("_awards") > 0), F.element_at("_awards", 1))
        .otherwise(F.lit(None)),
    )

    by_award = F.filter(F.col("_contracts"), lambda c: c["awardID"].isNotNull() & (c["awardID"] == F.col("_award")["id"]))
    by_item = F.filter(F.col("_contracts"), lambda c: F.exists(c["items"], lambda i: i["id"] == F.col("_item_id")))
    rec = rec.withColumn(
        "_contract",
        F.when(F.size("_contracts") <= 0, F.lit(None))
        .when(F.col("_award").isNotNull() & (F.size(by_award) > 0), F.element_at(by_award, 1))
        .when(F.size(by_item) > 0, F.element_at(by_item, 1))
        .when(F.size("_contracts") == 1, F.element_at("_contracts", 1))
        .otherwise(F.lit(None)),
    )

    supplier = F.element_at(F.col("_award")["suppliers"], 1)
    monto_adicional = F.coalesce(
        F.aggregate(
            F.col("_contract")["amendments"],
            F.lit(0.0),
            lambda acc, am: acc + F.coalesce(am["value"]["amount"], F.lit(0.0)),
        ),
        F.lit(0.0),
    )
    fecha_convocatoria = _to_date(F.coalesce(tender["datePublished"], tender["tenderPeriod"]["startDate"]))

    flat = rec.select(
        F.col("ocid"),
        F.col("source"),
        F.col("lamport"),
        tender["id"].alias("codigo_convocatoria"),
        F.col("_contract")["id"].alias("n_cod_contrato"),
        F.col("_item_id").alias("n_item"),
        F.col("item")["description"].alias("descripcion_item"),
        F.col("item")["classification"]["id"].alias("clasificacion_item"),
        F.col("item")["quantity"].alias("cantidad"),
        F.col("item")["unit"]["name"].alias("unidad_medida"),
        tender["title"].alias("titulo"),
        tender["mainProcurementCategory"].alias("categoria"),
        fecha_convocatoria.alias("fecha_convocatoria"),
        _to_date(F.col("_award")["date"]).alias("fecha_buena_pro"),
        _to_date(F.col("_contract")["dateSigned"]).alias("fecha_suscripcion"),
        tender["value"]["amount"].alias("monto_referencial"),
        F.col("_award")["value"]["amount"].alias("monto_adjudicado"),
        F.col("_contract")["value"]["amount"].alias("monto_contratado"),
        monto_adicional.alias("monto_adicional"),
        F.coalesce(peruc, _clean_ruc(F.col("_buyer")["id"])).alias("ruc_comprador"),
        F.col("_buyer")["name"].alias("nombre_comprador"),
        F.col("_buyer")["address"]["region"].alias("departamento_comprador"),
        _clean_ruc(supplier["id"]).alias("ruc_proveedor"),
        supplier["name"].alias("nombre_proveedor"),
        tender["procurementMethod"].alias("metodo_contratacion"),
        tender["procurementMethodDetails"].alias("detalles_metodo"),
        F.coalesce(tender["procurementMethod"] == F.lit("direct"), F.lit(False)).alias("es_contratacion_directa"),
        F.col("_award")["status"].alias("estado_adjudicacion"),
        F.coalesce(F.size(F.col("_contract")["amendments"]) > 0, F.lit(False)).alias("tiene_adenda"),
    )
    return flat.withColumn(
        "anio_fiscal",
        F.coalesce(F.year("fecha_convocatoria"), F.year("fecha_buena_pro"), F.year("fecha_suscripcion")),
    )
