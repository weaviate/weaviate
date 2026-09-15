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

package license

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/monitoring"
)

func TestRegisterMetrics(t *testing.T) {
	t.Run("1 for the current status, 0 for the others", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		RegisterMetrics(reg, State{Status: StatusValid, LicenseID: "lic_01ARZ3NDEKTSV4RRFFQ69G5FAV"})

		families, err := reg.Gather()
		require.NoError(t, err)
		require.Len(t, families, 1)
		require.Equal(t, "weaviate_license_status", families[0].GetName())

		got := map[string]float64{}
		for _, m := range families[0].GetMetric() {
			for _, label := range m.GetLabel() {
				if label.GetName() == "status" {
					got[label.GetValue()] = m.GetGauge().GetValue()
				}
			}
		}
		require.Equal(t, map[string]float64{"valid": 1, "unlicensed": 0}, got)
	})

	t.Run("no-op registerer does nothing and does not panic", func(t *testing.T) {
		RegisterMetrics(monitoring.NoopRegisterer, State{Status: StatusUnlicensed})
	})
}
