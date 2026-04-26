// File: adapters/repos/db/vector/common/query_vector_distancer.go
//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
// 
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
	return &QueryVectorDistancer{db: db}
}

func (q *QueryVectorDistancer) Distance(ctx context.Context, vectorID uuid.UUID) (float64, error) {
	query := "SELECT distance FROM vector_distance WHERE vector_id = ?"
	var distance float64
	err := q.db.QueryRowContext(ctx, query, vectorID).Scan(&distance)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("vector not found: %w", err)
		}
		return 0, fmt.Errorf("failed to query vector distance: %w", err)
	}
	return distance, nil
}