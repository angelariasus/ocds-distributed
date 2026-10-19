import pandas as pd

from gold.star import build


def items():
    return pd.DataFrame(
        {
            "ocid": ["o1", "o1", "o2"],
            "codigo_convocatoria": ["1", "1", "2"],
            "n_item": ["1", "2", "1"],
            "anio_fiscal": [2026, 2026, 2025],
            "fecha_convocatoria": ["2026-09-01", "2026-09-01", "2025-03-10"],
            "fecha_buena_pro": ["2026-09-10", None, "2025-03-20"],
            "fecha_suscripcion": ["2026-09-20", None, None],
            "ruc_comprador": ["20131257750", "20131257750", "20100000002"],
            "nombre_comprador": ["Seguro Social  de Salud", "Seguro Social de Salud", "MUNICIPALIDAD X"],
            "departamento_comprador": ["Lima", "LIMA", None],
            "ruc_proveedor": ["20100000001", None, "10456789012"],
            "nombre_proveedor": ["Farmacia SAC", None, "JUAN PEREZ"],
            "clasificacion_item": ["51101500", "51101500", None],
            "descripcion_item": ["PARACETAMOL 500 MG", "IBUPROFENO 400 MG", "IBUPROFENO 400 MG"],
            "metodo_contratacion": ["direct", "direct", "open"],
            "detalles_metodo": ["Contratación Directa", "Contratación Directa", "Licitación Pública"],
            "cantidad": [10.0, 5.0, 1.0],
            "monto_referencial": [1000.0, 1000.0, 50.0],
            "monto_adjudicado": [900.0, None, 45.0],
            "monto_contratado": [950.0, None, None],
            "monto_adicional": [95.0, 0.0, 0.0],
            "es_contratacion_directa": [True, True, False],
            "tiene_adenda": [True, False, False],
        }
    )


def test_dimensions_are_deduplicated_and_normalised():
    star = build(items())
    assert list(star.entidad["ruc"]) == ["20131257750", "20100000002"]
    assert star.entidad.iloc[0]["nombre"] == "SEGURO SOCIAL DE SALUD" and bool(star.entidad.iloc[0]["es_essalud"])
    assert dict(zip(star.proveedor["ruc"], star.proveedor["tipo"], strict=True)) == {"20100000001": "PERSONA JURIDICA", "10456789012": "PERSONA NATURAL"}
    assert len(star.item) == 3 and set(star.item["clasificacion"]) == {"51101500", "SIN-CLASIFICACION"}
    assert len(star.tipo_proceso) == 2
    assert sorted(star.tiempo["sk_tiempo"]) == [20250310, 20250320, 20260901, 20260910, 20260920]


def test_fact_measures_and_keys():
    star = build(items())
    fact = star.fact.set_index(["ocid", "n_item"])
    row = fact.loc[("o1", "1")]
    assert row["fk_tiempo_convocatoria"] == 20260901 and row["fk_tiempo_suscripcion"] == 20260920
    assert row["ratio_sobrecosto_pct"] == 10.0 and row["lead_time_dias"] == 19
    assert bool(row["flag_tiene_adenda"]) and bool(row["flag_contratacion_directa"])
    unawarded = fact.loc[("o1", "2")]
    assert pd.isna(unawarded["ratio_sobrecosto_pct"]) and pd.isna(unawarded["fk_tiempo_buena_pro"])
    assert sorted(star.fact["anio_fiscal"].unique()) == [2025, 2026]
