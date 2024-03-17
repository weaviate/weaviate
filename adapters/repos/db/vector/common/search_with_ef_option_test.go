package common

import "testing"

func TestWithEFOptions(t *testing.T) {
	tests := []struct {
		name           string
		dynamicMin     int
		dynamicMax     int
		dynamicFactor  int
		expectedResult SearchWithEFOptions
	}{
		{
			name:          "The specified parameters can be assigned to options.",
			dynamicMin:    1,
			dynamicMax:    10,
			dynamicFactor: 2,
			expectedResult: SearchWithEFOptions{
				DynamicMin:    1,
				DynamicMax:    10,
				DynamicFactor: 2,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualResult := SearchWithEFOptions{}
			option := WithEFOptions(tt.dynamicMin, tt.dynamicMax, tt.dynamicFactor)
			option(&actualResult)

			if actualResult.DynamicMin != tt.expectedResult.DynamicMin ||
				actualResult.DynamicMax != tt.expectedResult.DynamicMax ||
				actualResult.DynamicFactor != tt.expectedResult.DynamicFactor {
				t.Errorf("WithEFOptions() = %v, want %v", actualResult, tt.expectedResult)
			}
		})
	}
}
