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
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/monitoring"
)

type mutableStateProvider struct {
	state State
	calls int
}

func (p *mutableStateProvider) Current() State {
	p.calls++
	return p.state
}

func TestRegisterMetrics(t *testing.T) {
	for _, current := range allStatuses {
		t.Run(string(current), func(t *testing.T) {
			reg := prometheus.NewRegistry()
			RegisterMetrics(reg, State{Status: current, LicenseID: "lic_01ARZ3NDEKTSV4RRFFQ69G5FAV"})

			statuses, expiresAt := scrapeLicenseMetrics(t, reg)
			edition := EditionEnterprise
			if current == StatusUnlicensed {
				edition = EditionCommunity
			}
			require.Len(t, statuses, len(allStatuses))
			active := 0
			for _, status := range allStatuses {
				want := float64(0)
				if status == current {
					want = 1
				}
				key := string(edition) + "/" + string(status)
				value, ok := statuses[key]
				require.True(t, ok, "missing status series %q", key)
				require.Equal(t, want, value)
				if value == 1 {
					active++
				}
			}
			require.Equal(t, 1, active)
			require.Equal(t, []float64{0}, expiresAt)
		})
	}
}

func TestRegisterMetricsDynamic(t *testing.T) {
	firstExpiry := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	secondExpiry := firstExpiry.Add(48 * time.Hour)
	provider := &mutableStateProvider{state: State{Status: StatusValid, ExpiresAt: firstExpiry}}
	reg := prometheus.NewRegistry()
	RegisterMetrics(reg, provider)

	statuses, expiresAt := scrapeLicenseMetrics(t, reg)
	require.Equal(t, float64(1), statuses["enterprise/valid"])
	require.Equal(t, []float64{float64(firstExpiry.Unix())}, expiresAt)
	require.Equal(t, 1, provider.calls)

	provider.state.ExpiresAt = secondExpiry
	statuses, expiresAt = scrapeLicenseMetrics(t, reg)
	require.Equal(t, float64(1), statuses["enterprise/valid"])
	require.Equal(t, []float64{float64(secondExpiry.Unix())}, expiresAt)
	require.Equal(t, 2, provider.calls)

	provider.state = State{Status: StatusExpired}
	statuses, expiresAt = scrapeLicenseMetrics(t, reg)
	require.Equal(t, float64(0), statuses["enterprise/valid"])
	require.Equal(t, float64(1), statuses["enterprise/expired"])
	require.Equal(t, []float64{0}, expiresAt)
	require.Equal(t, 3, provider.calls)
}

func TestRegisterMetricsNoop(t *testing.T) {
	provider := &mutableStateProvider{state: State{Status: StatusUnlicensed}}
	RegisterMetrics(monitoring.NoopRegisterer, provider)
	require.Zero(t, provider.calls)
}

func scrapeLicenseMetrics(t *testing.T, reg *prometheus.Registry) (map[string]float64, []float64) {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	require.Len(t, families, 2)

	statuses := map[string]float64{}
	var expiresAt []float64
	for _, family := range families {
		switch family.GetName() {
		case "weaviate_license_status":
			require.Len(t, family.GetMetric(), len(allStatuses))
			for _, metric := range family.GetMetric() {
				var edition, status string
				for _, label := range metric.GetLabel() {
					switch label.GetName() {
					case "edition":
						edition = label.GetValue()
					case "status":
						status = label.GetValue()
					}
				}
				statuses[edition+"/"+status] = metric.GetGauge().GetValue()
			}
		case "weaviate_license_expires_at_seconds":
			for _, metric := range family.GetMetric() {
				expiresAt = append(expiresAt, metric.GetGauge().GetValue())
			}
		default:
			t.Fatalf("unexpected metric family %q", family.GetName())
		}
	}
	return statuses, expiresAt
}
