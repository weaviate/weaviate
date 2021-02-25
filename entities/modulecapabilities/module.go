//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package modulecapabilities

import (
	"net/http"

	"github.com/semi-technologies/weaviate/entities/moduletools"
)

type Module interface {
	Name() string
	Init(params moduletools.ModuleInitParams) error
	RootHandler() http.Handler // TODO: remove from overall module, this is a capability
}
