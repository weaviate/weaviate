//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package usagelimits

import "testing"

func TestRenderTemplate(t *testing.T) {
	tests := []struct {
		name     string
		template string
		limit    LimitName
		value    int64
		want     string
	}{
		{
			name:     "default template (empty input)",
			template: "",
			limit:    LimitObjects,
			value:    10000,
			want:     "objects count limit of 10000 reached for this instance.",
		},
		{
			name:     "default template (explicit)",
			template: DefaultErrorMessageTemplate,
			limit:    LimitTenants,
			value:    2,
			want:     "tenants count limit of 2 reached for this instance.",
		},
		{
			name:     "cloud upgrade override",
			template: "You've hit the Free-tier limit of {value} {limit}. Upgrade at https://console.weaviate.cloud/upgrade",
			limit:    LimitObjects,
			value:    10000,
			want:     "You've hit the Free-tier limit of 10000 objects. Upgrade at https://console.weaviate.cloud/upgrade",
		},
		{
			name:     "no placeholders is allowed (template stays as-is)",
			template: "free-tier limit reached, please upgrade",
			limit:    LimitCollections,
			value:    3,
			want:     "free-tier limit reached, please upgrade",
		},
		{
			name:     "value placeholder appears multiple times",
			template: "{value} is the limit; you tried to exceed {value}",
			limit:    LimitObjects,
			value:    100,
			want:     "100 is the limit; you tried to exceed 100",
		},
		{
			name:     "all four limit names render",
			template: "{limit}",
			limit:    LimitShards,
			value:    1,
			want:     "shards",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RenderTemplate(tt.template, tt.limit, tt.value)
			if got != tt.want {
				t.Errorf("RenderTemplate() = %q, want %q", got, tt.want)
			}
		})
	}
}
