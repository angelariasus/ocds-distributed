"""PostgreSQL side of the load: staging tables, the atomic swap and the outbox."""

import io
import json
import logging
from pathlib import Path

import pandas as pd
import psycopg

from .star import StarFrames

log = logging.getLogger(__name__)

SCHEMA_SQL = Path(__file__).resolve().parent / "sql" / "schema.sql"

DIMS = {
    "tiempo": ("gold.dim_tiempo", ["sk_tiempo", "fecha", "anio", "trimestre", "mes", "dia", "dia_semana"], "sk_tiempo"),
    "entidad": ("gold.dim_entidad_compradora", ["ruc", "nombre", "departamento", "es_essalud"], "ruc"),
    "proveedor": ("gold.dim_proveedor", ["ruc", "nombre", "tipo"], "ruc"),
    "item": (
        "gold.dim_item",
        ["clasificacion", "descripcion", "es_medicamento", "petitorio_codigo", "petitorio_dci", "petitorio_match", "petitorio_score"],
        "clasificacion, descripcion",
    ),
    "tipo_proceso": ("gold.dim_tipo_proceso", ["metodo", "detalles"], "metodo, detalles"),
}
FACT_STAGING_COLUMNS = [
    "ocid", "codigo_convocatoria", "n_item", "anio_fiscal",
    "fk_tiempo_convocatoria", "fk_tiempo_buena_pro", "fk_tiempo_suscripcion",
    "ruc_comprador", "ruc_proveedor", "clasificacion", "descripcion", "metodo", "detalles",
    "cantidad", "monto_referencial", "monto_adjudicado", "monto_contratado", "monto_adicional",
    "flag_contratacion_directa", "flag_tiene_adenda", "ratio_sobrecosto_pct", "lead_time_dias",
]  # fmt: skip


def _copy(cur: psycopg.Cursor, table: str, df: pd.DataFrame, columns: list[str]) -> None:
    buf = io.StringIO()
    df[columns].to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)
    with cur.copy(f"COPY {table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')") as copy:
        while chunk := buf.read(1 << 16):
            copy.write(chunk)


class PostgresWarehouse:
    def __init__(self, dsn: str):
        self.dsn = dsn

    def connect(self) -> psycopg.Connection:
        return psycopg.connect(self.dsn, connect_timeout=10)

    def ensure_schema(self) -> None:
        with self.connect() as conn:
            conn.execute(SCHEMA_SQL.read_text(encoding="utf-8"))
            conn.commit()

    def load_staging(self, token: int, frames: StarFrames) -> None:
        with self.connect() as conn, conn.cursor() as cur:
            for name, (table, columns, _) in DIMS.items():
                stg = f"staging.{name}_{token}"
                cur.execute(f"DROP TABLE IF EXISTS {stg}")
                cur.execute(f"CREATE TABLE {stg} (LIKE {table} INCLUDING DEFAULTS)")
                _copy(cur, stg, getattr(frames, name), columns)
            fact_stg = f"staging.fact_{token}"
            cur.execute(f"DROP TABLE IF EXISTS {fact_stg}")
            cur.execute(
                f"""CREATE TABLE {fact_stg} (
                    ocid text, codigo_convocatoria text, n_item text, anio_fiscal int,
                    fk_tiempo_convocatoria int, fk_tiempo_buena_pro int, fk_tiempo_suscripcion int,
                    ruc_comprador text, ruc_proveedor text, clasificacion text, descripcion text, metodo text, detalles text,
                    cantidad double precision, monto_referencial double precision, monto_adjudicado double precision,
                    monto_contratado double precision, monto_adicional double precision,
                    flag_contratacion_directa boolean, flag_tiene_adenda boolean,
                    ratio_sobrecosto_pct double precision, lead_time_dias int)"""
            )
            _copy(cur, fact_stg, frames.fact, FACT_STAGING_COLUMNS)
            conn.commit()
        log.info("staging loaded for token %d: fact rows=%d", token, len(frames.fact))

    def swap(self, token: int, holder: str, years: list[int]) -> int:
        """One transaction: fence check first, then dimensions, then the fact partitions, then the outbox."""
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT gold.assert_fence('gold-loader', %s, %s)", (token, holder))
            for name, (table, columns, conflict) in DIMS.items():
                stg = f"staging.{name}_{token}"
                updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c not in conflict.replace(" ", "").split(","))
                action = f"DO UPDATE SET {updates}" if updates else "DO NOTHING"
                cur.execute(f"INSERT INTO {table} ({', '.join(columns)}) SELECT {', '.join(columns)} FROM {stg} ON CONFLICT ({conflict}) {action}")
            for year in years:
                cur.execute(f"CREATE TABLE IF NOT EXISTS gold.fact_items_{year} PARTITION OF gold.fact_items FOR VALUES IN ({year})")
                cur.execute("DELETE FROM gold.fact_items WHERE anio_fiscal = %s", (year,))
            cur.execute(
                f"""INSERT INTO gold.fact_items (
                        ocid, codigo_convocatoria, n_item, anio_fiscal,
                        fk_tiempo_convocatoria, fk_tiempo_buena_pro, fk_tiempo_suscripcion,
                        fk_entidad, fk_proveedor, fk_item, fk_tipo_proceso,
                        cantidad, monto_referencial, monto_adjudicado, monto_contratado, monto_adicional,
                        ratio_sobrecosto_pct, lead_time_dias, flag_contratacion_directa, flag_tiene_adenda, load_token)
                    SELECT s.ocid, s.codigo_convocatoria, s.n_item, s.anio_fiscal,
                           s.fk_tiempo_convocatoria, s.fk_tiempo_buena_pro, s.fk_tiempo_suscripcion,
                           e.sk_entidad, p.sk_proveedor, i.sk_item, t.sk_tipo_proceso,
                           s.cantidad, s.monto_referencial, s.monto_adjudicado, s.monto_contratado, s.monto_adicional,
                           s.ratio_sobrecosto_pct, s.lead_time_dias, s.flag_contratacion_directa, s.flag_tiene_adenda, %s
                      FROM staging.fact_{token} s
                      LEFT JOIN gold.dim_entidad_compradora e ON e.ruc = s.ruc_comprador
                      LEFT JOIN gold.dim_proveedor p ON p.ruc = s.ruc_proveedor
                      LEFT JOIN gold.dim_item i ON i.clasificacion = s.clasificacion AND i.descripcion = s.descripcion
                      LEFT JOIN gold.dim_tipo_proceso t ON t.metodo = s.metodo AND t.detalles = s.detalles""",
                (token,),
            )
            rows = cur.rowcount
            # Content digest of exactly what this load wrote; the audit ledger attests it.
            cur.execute(
                """SELECT encode(sha256(convert_to(coalesce(string_agg(
                       ocid || '|' || coalesce(n_item, '') || '|' || coalesce(monto_adjudicado::text, '') || '|' || coalesce(monto_contratado::text, ''),
                       E'
' ORDER BY ocid, n_item), ''), 'UTF8')), 'hex')
                     FROM gold.fact_items WHERE load_token = %s""",
                (token,),
            )
            digest = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO gold.outbox (event_type, payload) VALUES ('gold.loaded', %s)",
                (json.dumps({"token": token, "holder": holder, "years": years, "rows_fact": rows, "digest": digest}),),
            )
            conn.commit()
        log.info("swap committed for token %d: years=%s fact rows=%d", token, years, rows)
        return rows

    def drop_staging(self, token: int) -> None:
        with self.connect() as conn:
            for name in list(DIMS) + ["fact"]:
                conn.execute(f"DROP TABLE IF EXISTS staging.{name}_{token}")
            conn.commit()

    def record_load(self, token: int, holder: str, status: str, years: list[int], rows: int | None) -> None:
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO gold.loads (token, holder, status, years, rows_fact, finished_at)
                   VALUES (%s, %s, %s, %s, %s, now())
                   ON CONFLICT (token) DO UPDATE SET status = EXCLUDED.status, rows_fact = EXCLUDED.rows_fact, finished_at = now()""",
                (token, holder, status, years, rows),
            )
            conn.commit()
