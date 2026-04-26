// File: adapters/repos/db/vector/common/query_vector_distancer.go
//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
// 
// Orca Security Scan Summary
// | Status  | Check | Issues by priority |   |
// | ------- | ----- | ------------------ | - |
// | <img width="16" alt="Passed" src="https://raw.githubusercontent.com/orcasecurity/orca-cli/main/resources/images/prcomment/status_v1/passed.png" title="Passed"> Passed | Infrastructure as Code | <img width="12" alt="high" src="https://raw.githubusercontent.com/orcasecurity/orca-cli/main/resources/images/prcomment/priority/high.png" title="High"> 0 &emsp; <img width="12" alt="medium" src="https://raw.githubusercontent.com/orcasecurity/orca-cli/main/resources/images/prcomment/priority/medium.png" title="Medium"> 0 &emsp; <img width="12" alt="low" src="https://raw.githubusercontent.com/orcasecurity/orca-cli/main/resources/images/prcomment/priority/low.png" title="Low"> 0 
//
package common

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/go-sql-driver/mysql"
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
		if errors.Is(err, sql.ErrNoRows) {
			return 0, fmt.Errorf("vector not found: %w", err)
		}
		return 0, err
	}
	return distance, nil
}