//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
// 
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package filters

// Pagination for now only contains a limit paramter, but might be extended in
// the future
type Pagination struct {
	Limit int
}

// ExtractPaginationFromArgs gets the limit key out of a map. Not specific to
// GQL, but can be used from GQL
func ExtractPaginationFromArgs(args map[string]interface{}) (*Pagination, error) {
	limit, ok := args["limit"]
	if !ok {
		return nil, nil
	}

	return &Pagination{
		Limit: limit.(int),
	}, nil
}
