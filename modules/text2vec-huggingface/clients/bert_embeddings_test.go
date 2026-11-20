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

package clients

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_bertEmbeddingsDecoder_calculateVector(t *testing.T) {
	tests := []struct {
		name       string
		embeddings [][]float32
		want       []float32
		wantErr    string
	}{
		{
			name:       "nil",
			embeddings: nil,
			wantErr:    "missing embeddings",
		},
		{
			name:       "empty",
			embeddings: [][]float32{},
			wantErr:    "missing embeddings",
		},
		{
			name:       "second token vector shorter",
			embeddings: [][]float32{{1, 2}, {1}},
			wantErr:    "token vector 1 has 1 dimensions, expected 2",
		},
		{
			name:       "second token vector longer",
			embeddings: [][]float32{{1}, {1, 2}},
			wantErr:    "token vector 1 has 2 dimensions, expected 1",
		},
		{
			name:       "empty token vector after a valid one",
			embeddings: [][]float32{{1, 2}, {}},
			wantErr:    "token vector 1 has 0 dimensions, expected 2",
		},
		{
			name:       "zero-length token vectors",
			embeddings: [][]float32{{}, {}},
			wantErr:    "token vector 0 has no dimensions",
		},
		{
			name:       "nil token vector",
			embeddings: [][]float32{nil},
			wantErr:    "token vector 0 has no dimensions",
		},
		{
			name:       "just one vector",
			embeddings: [][]float32{{-0.17978577315807343}},
			want:       []float32{-0.17978577315807343},
		},
		{
			name: "distilbert-base-uncased",
			embeddings: [][]float32{
				{-0.17978577315807343, -0.0678672045469284, 0.1706605851650238, -0.1639413982629776, -0.12804915010929108, 0.017568372189998627, 0.1610901951789856, 0.19909054040908813, -0.26103103160858154, -0.14505508542060852},
				{-0.25516796112060547, -0.054695576429367065, 0.13527897000312805, -0.3919253945350647, 0.1900954395532608, 0.5994636416435242, 0.5798457264900208, 0.6522972583770752, -0.08617493510246277, -0.35053199529647827},
				{0.930827260017395, 0.3315476179122925, -0.323006272315979, 0.18198077380657196, -0.3299236297607422, -0.5998684763908386, 0.3299814462661743, -0.6352149844169617, 0.5154204368591309, 0.11740084737539291},
			},
			want: []float32{0.1652911752462387, 0.06966160982847214, -0.005688905715942383, -0.12462866306304932, -0.08929244428873062, 0.005721171852201223, 0.35697245597839355, 0.07205760478973389, 0.05607149004936218, -0.1260620802640915},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := bertEmbeddingsDecoder{}
			got, err := d.calculateVector(tt.embeddings)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				require.Nil(t, got)
				return
			}
			require.NoError(t, err)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("bertEmbeddingsDecoder.calculateVector() = %v, want %v", got, tt.want)
			}
		})
	}
}
