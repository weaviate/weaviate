package lib

import (
	"fmt"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
)

func HandleBatchResponse(res []models.ObjectsGetResponse) error {
	msgs := []string{}

	for i, obj := range res {
		if obj.Result.Errors == nil {
			continue
		}

		if len(obj.Result.Errors.Error) == 0 {
			continue
		}

		msg := fmt.Sprintf("at pos %d: %s", i, obj.Result.Errors.Error[0].Message)
		msgs = append(msgs, msg)
	}

	if len(msgs) == 0 {
		return nil
	}

	msg := strings.Join(msgs, ", ")
	return fmt.Errorf("%s", msg)
}
