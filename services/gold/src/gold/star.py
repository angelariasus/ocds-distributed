"""Silver items → star-schema frames (dimensions and fact), in pandas.

Dimension rows are derived from the items themselves; surrogate keys are assigned by the database
on upsert, and the fact's foreign keys are resolved by joining on the natural keys inside the swap
transaction, so the whole load is one consistent snapshot.
"""

import re
from dataclasses import dataclass

import pandas as pd

ESSALUD_RUC = "20131257750"


@dataclass
class StarFrames:
    tiempo: pd.DataFrame
    entidad: pd.DataFrame
    proveedor: pd.DataFrame
    item: pd.DataFrame
    tipo_proceso: pd.DataFrame
    fact: pd.DataFrame


def _norm(text: pd.Series) -> pd.Series:
    return text.fillna("").astype(str).str.upper().str.replace(r"\s+", " ", regex=True).str.strip()


def _supplier_type(ruc: pd.Series) -> pd.Series:
    r = ruc.fillna("").astype(str)
    return pd.Series(
        [
            "PERSONA NATURAL"
            if x.startswith("10")
            else "PERSONA JURIDICA"
            if x.startswith("20")
            else "CONSORCIO"
            if re.match(r"^\d{11}", x) is None and x
            else "DESCONOCIDO"
            for x in r
        ],
        index=ruc.index,
    )


def _sk_tiempo(dates: pd.Series) -> pd.Series:
    d = pd.to_datetime(dates, errors="coerce")
    return (d.dt.year * 10000 + d.dt.month * 100 + d.dt.day).astype("Int64")


def build(items: pd.DataFrame) -> StarFrames:
    df = items.copy()
    for col in ("fecha_convocatoria", "fecha_buena_pro", "fecha_suscripcion"):
        df[col] = pd.to_datetime(df[col], errors="coerce")

    dates = pd.concat([df["fecha_convocatoria"], df["fecha_buena_pro"], df["fecha_suscripcion"]]).dropna().drop_duplicates()
    tiempo = pd.DataFrame({"fecha": dates.dt.normalize()}).drop_duplicates()
    tiempo["sk_tiempo"] = _sk_tiempo(tiempo["fecha"])
    tiempo["anio"] = tiempo["fecha"].dt.year
    tiempo["trimestre"] = tiempo["fecha"].dt.quarter
    tiempo["mes"] = tiempo["fecha"].dt.month
    tiempo["dia"] = tiempo["fecha"].dt.day
    tiempo["dia_semana"] = tiempo["fecha"].dt.dayofweek + 1
    tiempo["fecha"] = tiempo["fecha"].dt.date

    entidad = (
        df[["ruc_comprador", "nombre_comprador", "departamento_comprador"]]
        .dropna(subset=["ruc_comprador"])
        .rename(columns={"ruc_comprador": "ruc", "nombre_comprador": "nombre", "departamento_comprador": "departamento"})
        .assign(nombre=lambda d: _norm(d["nombre"]), departamento=lambda d: _norm(d["departamento"]))
        .drop_duplicates("ruc")
    )
    entidad["es_essalud"] = entidad["ruc"] == ESSALUD_RUC

    proveedor = (
        df[["ruc_proveedor", "nombre_proveedor"]]
        .dropna(subset=["ruc_proveedor"])
        .rename(columns={"ruc_proveedor": "ruc", "nombre_proveedor": "nombre"})
        .assign(nombre=lambda d: _norm(d["nombre"]))
        .drop_duplicates("ruc")
    )
    proveedor["tipo"] = _supplier_type(proveedor["ruc"])

    item = pd.DataFrame(
        {
            "clasificacion": df["clasificacion_item"].fillna("SIN-CLASIFICACION").astype(str),
            "descripcion": _norm(df["descripcion_item"]).str.slice(0, 300),
        }
    ).drop_duplicates()
    item = item[item["descripcion"] != ""]

    tipo_proceso = pd.DataFrame(
        {
            "metodo": df["metodo_contratacion"].fillna("unknown").astype(str),
            "detalles": _norm(df["detalles_metodo"]).replace("", "SIN DETALLE"),
        }
    ).drop_duplicates()

    fact = pd.DataFrame(
        {
            "ocid": df["ocid"],
            "codigo_convocatoria": df["codigo_convocatoria"],
            "n_item": df["n_item"],
            "anio_fiscal": df["anio_fiscal"].astype("Int64"),
            "fk_tiempo_convocatoria": _sk_tiempo(df["fecha_convocatoria"]),
            "fk_tiempo_buena_pro": _sk_tiempo(df["fecha_buena_pro"]),
            "fk_tiempo_suscripcion": _sk_tiempo(df["fecha_suscripcion"]),
            "ruc_comprador": df["ruc_comprador"],
            "ruc_proveedor": df["ruc_proveedor"],
            "clasificacion": df["clasificacion_item"].fillna("SIN-CLASIFICACION").astype(str),
            "descripcion": _norm(df["descripcion_item"]).str.slice(0, 300),
            "metodo": df["metodo_contratacion"].fillna("unknown").astype(str),
            "detalles": _norm(df["detalles_metodo"]).replace("", "SIN DETALLE"),
            "cantidad": df["cantidad"],
            "monto_referencial": df["monto_referencial"],
            "monto_adjudicado": df["monto_adjudicado"],
            "monto_contratado": df["monto_contratado"],
            "monto_adicional": df["monto_adicional"].fillna(0.0),
            "flag_contratacion_directa": df["es_contratacion_directa"].fillna(False).astype(bool),
            "flag_tiene_adenda": df["tiene_adenda"].fillna(False).astype(bool),
        }
    )
    contratado = fact["monto_contratado"].where(fact["monto_contratado"] > 0)
    fact["ratio_sobrecosto_pct"] = (fact["monto_adicional"] / contratado * 100).round(4)
    fact["lead_time_dias"] = (df["fecha_suscripcion"] - df["fecha_convocatoria"]).dt.days.astype("Int64")
    fact = fact[fact["anio_fiscal"].notna()]
    return StarFrames(tiempo=tiempo, entidad=entidad, proveedor=proveedor, item=item, tipo_proceso=tipo_proceso, fact=fact)
