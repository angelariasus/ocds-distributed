// Package store runs the read queries against Gold through HAProxy's replica
// port, so the API scales reads across standbys and never touches the primary.
package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/angelariasus/ocds-distributed/services/api/gen/queryv1"
)

type Store struct {
	pool *pgxpool.Pool
}

func Open(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 8
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

const topBuyersSQL = `
SELECT e.ruc, coalesce(e.nombre, ''), coalesce(e.departamento, ''), e.es_essalud,
       count(*), coalesce(sum(f.monto_referencial), 0), coalesce(sum(f.monto_adjudicado), 0)
  FROM gold.fact_items f
  JOIN gold.dim_entidad_compradora e ON e.sk_entidad = f.fk_entidad
 WHERE ($1 = 0 OR f.anio_fiscal = $1)
 GROUP BY e.ruc, e.nombre, e.departamento, e.es_essalud
 ORDER BY sum(f.monto_referencial) DESC NULLS LAST
 LIMIT $2`

func (s *Store) TopBuyers(ctx context.Context, year, limit int32) ([]*queryv1.Buyer, error) {
	rows, err := s.pool.Query(ctx, topBuyersSQL, year, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*queryv1.Buyer
	for rows.Next() {
		b := &queryv1.Buyer{}
		if err := rows.Scan(&b.Ruc, &b.Nombre, &b.Departamento, &b.EsEssalud, &b.Items, &b.MontoReferencial, &b.MontoAdjudicado); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

const buyerSummarySQL = `
SELECT e.ruc, coalesce(e.nombre, ''), coalesce(e.departamento, ''), e.es_essalud,
       count(*), coalesce(sum(f.monto_referencial), 0), coalesce(sum(f.monto_adjudicado), 0),
       sum(CASE WHEN f.flag_tiene_adenda THEN 1 ELSE 0 END),
       coalesce(avg(f.lead_time_dias), 0), coalesce(avg(f.ratio_sobrecosto_pct), 0),
       count(DISTINCT f.fk_proveedor)
  FROM gold.fact_items f
  JOIN gold.dim_entidad_compradora e ON e.sk_entidad = f.fk_entidad
 WHERE e.ruc = $1 AND ($2 = 0 OR f.anio_fiscal = $2)
 GROUP BY e.ruc, e.nombre, e.departamento, e.es_essalud`

func (s *Store) BuyerSummary(ctx context.Context, ruc string, year int32) (*queryv1.BuyerSummaryResponse, error) {
	r := &queryv1.BuyerSummaryResponse{Buyer: &queryv1.Buyer{}}
	b := r.Buyer
	err := s.pool.QueryRow(ctx, buyerSummarySQL, ruc, year).Scan(
		&b.Ruc, &b.Nombre, &b.Departamento, &b.EsEssalud, &b.Items, &b.MontoReferencial, &b.MontoAdjudicado,
		&r.ConAdenda, &r.LeadTimePromedioDias, &r.SobrecostoPromedioPct, &r.Proveedores,
	)
	if err != nil {
		return nil, fmt.Errorf("buyer %s: %w", ruc, err)
	}
	return r, nil
}

const searchItemsSQL = `
SELECT f.ocid, i.descripcion, coalesce(e.nombre, ''), coalesce(p.nombre, ''),
       coalesce(f.cantidad, 0), coalesce(f.monto_adjudicado, 0), coalesce(t.fecha::text, '')
  FROM gold.fact_items f
  JOIN gold.dim_item i ON i.sk_item = f.fk_item
  JOIN gold.dim_entidad_compradora e ON e.sk_entidad = f.fk_entidad
  LEFT JOIN gold.dim_proveedor p ON p.sk_proveedor = f.fk_proveedor
  LEFT JOIN gold.dim_tiempo t ON t.sk_tiempo = f.fk_tiempo_convocatoria
 WHERE i.descripcion ILIKE '%' || $1 || '%' AND ($2 = 0 OR f.anio_fiscal = $2)
 ORDER BY f.monto_adjudicado DESC NULLS LAST
 LIMIT $3`

func (s *Store) SearchItems(ctx context.Context, text string, year, limit int32) ([]*queryv1.Item, error) {
	rows, err := s.pool.Query(ctx, searchItemsSQL, text, year, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*queryv1.Item
	for rows.Next() {
		it := &queryv1.Item{}
		if err := rows.Scan(&it.Ocid, &it.Descripcion, &it.Comprador, &it.Proveedor, &it.Cantidad, &it.MontoAdjudicado, &it.FechaConvocatoria); err != nil {
			return nil, err
		}
		out = append(out, it)
	}
	return out, rows.Err()
}
