package vectorizer

import (
	"context"
	"testing"

	"github.com/weaviate/weaviate/entities/moduletools"
	c "github.com/weaviate/weaviate/modules/text2vec-kserve/clients"
)

func TestVectorizer_Text(t *testing.T) {
	type fields struct {
		client c.Client
	}
	type args struct {
		input []string
		cfg   moduletools.ClassConfig
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *[]float32
		wantErr bool
	}{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &Vectorizer{
				client: tt.fields.client,
			}
			settings := NewClassSettings(tt.args.cfg)
			_, err := v.Texts(context.TODO(), tt.args.input, settings)
			if (err != nil) != tt.wantErr {
				t.Errorf("Vectorizer.Text() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
