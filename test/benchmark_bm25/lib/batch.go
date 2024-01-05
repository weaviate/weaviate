//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

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
