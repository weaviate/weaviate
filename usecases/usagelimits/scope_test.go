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

import (
	"strings"
	"testing"
)

func TestScopeValidate(t *testing.T) {
	tests := []struct {
		name     string
		input    Scope
		wantErr  bool
		wantMsgs []string
	}{
		{name: "empty defaults to node", input: "", wantErr: false},
		{name: "node is implemented", input: ScopeNode, wantErr: false},
		{
			name:     "cluster is reserved",
			input:    ScopeCluster,
			wantErr:  true,
			wantMsgs: []string{"USAGE_LIMITS_SCOPE=cluster", "not yet supported"},
		},
		{
			name:     "namespace is reserved",
			input:    ScopeNamespace,
			wantErr:  true,
			wantMsgs: []string{"USAGE_LIMITS_SCOPE=namespace", "not yet supported"},
		},
		{
			name:     "garbage rejected",
			input:    Scope("instance"),
			wantErr:  true,
			wantMsgs: []string{"not a recognized value", "node|cluster|namespace"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.input.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() err = %v, wantErr = %v", err, tt.wantErr)
			}
			for _, m := range tt.wantMsgs {
				if err == nil || !strings.Contains(err.Error(), m) {
					t.Errorf("error %q must contain %q", err, m)
				}
			}
		})
	}
}

func TestValidateScopeString(t *testing.T) {
	if err := ValidateScopeString("node"); err != nil {
		t.Fatalf("node should validate, got %v", err)
	}
	if err := ValidateScopeString(""); err != nil {
		t.Fatalf("empty should validate (defaults to node), got %v", err)
	}
	if err := ValidateScopeString("cluster"); err == nil {
		t.Fatalf("cluster should be rejected as not yet supported")
	}
	if err := ValidateScopeString("garbage"); err == nil {
		t.Fatalf("garbage should be rejected as unrecognized")
	}
}
