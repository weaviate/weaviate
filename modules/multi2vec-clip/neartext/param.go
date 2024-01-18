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

package neartext

import (
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/usecases/modulecomponents/nearText"
)

func (g *GraphQLArgumentsProvider) validateNearTextFn(param interface{}) error {
	nearTextParams, ok := param.(*nearText.NearTextParams)
	if !ok {
		return errors.New("'nearText' invalid parameter")
	}
	return nearTextParams.Validate()
}
