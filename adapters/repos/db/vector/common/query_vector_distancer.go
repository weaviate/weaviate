// File: adapters/repos/db/vector/common/query_vector_distancer.go
package common

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"
)

type QueryVectorDistancer struct {
	db *sql.DB
}

func NewQueryVectorDistancer(db *sql.DB) *QueryVectorDistancer {
	if db == nil {
		panic("nil db")
	}
	return &QueryVectorDistancer{db: db}
}

func (q *QueryVectorDistancer) Distance(ctx context.Context, vectorID uuid.UUID) (float64, error) {
	if ctx == nil {
		return 0, fmt.Errorf("nil context")
	}

	query := "SELECT distance FROM vector_distance WHERE vector_id = $1"
	var distance sql.NullFloat64
	err := q.db.QueryRowContext(ctx, query, vectorID).Scan(&distance)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("vector with ID %s not found: %w", vectorID, sql.ErrNoRows)
		}
		return 0, fmt.Errorf("failed to query vector distance for ID %s: %w", vectorID, err)
	}
	if !distance.Valid {
		return 0, fmt.Errorf("vector with ID %s has null distance", vectorID)
	}
	return distance.Float64, nil
}