-- Gold star schema for EsSalud procurement analysis, ported from essalud-pipeline's
-- SQL Server DDL to PostgreSQL. Idempotent: safe to run on every loader start.

CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS staging;

-- Fencing: every loader carries a token that only grows (the etcd revision of its lease).
-- A write with a token lower than the last accepted one comes from a loader whose lease
-- expired while it was paused; it must be rejected even though it believes it holds the lock.
CREATE TABLE IF NOT EXISTS gold.fence (
    name       text PRIMARY KEY,
    token      bigint NOT NULL DEFAULT 0,
    holder     text,
    updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO gold.fence (name) VALUES ('gold-loader') ON CONFLICT DO NOTHING;

CREATE OR REPLACE FUNCTION gold.assert_fence(p_name text, p_token bigint, p_holder text)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE accepted bigint;
BEGIN
    UPDATE gold.fence
       SET token = p_token, holder = p_holder, updated_at = now()
     WHERE name = p_name AND token <= p_token
    RETURNING token INTO accepted;
    IF accepted IS NULL THEN
        RAISE EXCEPTION 'stale fencing token % for % (a newer loader already wrote)', p_token, p_name
            USING ERRCODE = 'serialization_failure';
    END IF;
    RETURN accepted;
END $$;

CREATE TABLE IF NOT EXISTS gold.loads (
    token       bigint PRIMARY KEY,
    holder      text NOT NULL,
    started_at  timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    status      text NOT NULL,
    years       int[],
    rows_fact   bigint
);

-- Transactional outbox: written in the same transaction as the data it describes, relayed
-- to Kafka afterwards. Consumers deduplicate on id.
CREATE TABLE IF NOT EXISTS gold.outbox (
    id           bigserial PRIMARY KEY,
    event_type   text NOT NULL,
    payload      jsonb NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_unpublished ON gold.outbox (id) WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS gold.dim_tiempo (
    sk_tiempo  int PRIMARY KEY,          -- yyyymmdd
    fecha      date NOT NULL UNIQUE,
    anio       int NOT NULL,
    trimestre  int NOT NULL,
    mes        int NOT NULL,
    dia        int NOT NULL,
    dia_semana int NOT NULL
);

CREATE TABLE IF NOT EXISTS gold.dim_entidad_compradora (
    sk_entidad   serial PRIMARY KEY,
    ruc          text NOT NULL UNIQUE,
    nombre       text,
    departamento text,
    es_essalud   boolean NOT NULL DEFAULT false
);

CREATE TABLE IF NOT EXISTS gold.dim_proveedor (
    sk_proveedor serial PRIMARY KEY,
    ruc          text NOT NULL UNIQUE,
    nombre       text,
    tipo         text NOT NULL          -- PERSONA NATURAL | PERSONA JURIDICA | CONSORCIO | DESCONOCIDO
);

CREATE TABLE IF NOT EXISTS gold.dim_item (
    sk_item       serial PRIMARY KEY,
    clasificacion text NOT NULL,
    descripcion   text NOT NULL,
    UNIQUE (clasificacion, descripcion)
);

CREATE TABLE IF NOT EXISTS gold.dim_tipo_proceso (
    sk_tipo_proceso serial PRIMARY KEY,
    metodo          text NOT NULL,
    detalles        text NOT NULL,
    UNIQUE (metodo, detalles)
);

-- Declarative partitioning by fiscal year: each year's slice is loaded and replaced on its own,
-- which is what lets the loader re-run for a subset of years without touching the rest.
CREATE TABLE IF NOT EXISTS gold.fact_items (
    sk_hecho                 bigserial,
    ocid                     text NOT NULL,
    codigo_convocatoria      text,
    n_item                   text,
    anio_fiscal              int NOT NULL,
    fk_tiempo_convocatoria   int,
    fk_tiempo_buena_pro      int,
    fk_tiempo_suscripcion    int,
    fk_entidad               int,
    fk_proveedor             int,
    fk_item                  int,
    fk_tipo_proceso          int,
    cantidad                 double precision,
    monto_referencial        double precision,
    monto_adjudicado         double precision,
    monto_contratado         double precision,
    monto_adicional          double precision,
    ratio_sobrecosto_pct     double precision,
    lead_time_dias           int,
    flag_contratacion_directa boolean,
    flag_tiene_adenda        boolean,
    load_token               bigint NOT NULL,
    PRIMARY KEY (anio_fiscal, sk_hecho)
) PARTITION BY LIST (anio_fiscal);

CREATE INDEX IF NOT EXISTS fact_items_entidad ON gold.fact_items (fk_entidad);
CREATE INDEX IF NOT EXISTS fact_items_proveedor ON gold.fact_items (fk_proveedor);
