package models

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/models"
)

func errPayloadFromSingleErr(err error) *models.ErrorResponse {
	return &models.ErrorResponse{Error: []*models.ErrorResponseErrorItems0{{
		Message: fmt.Sprintf("%s", err),
	}}}
}
