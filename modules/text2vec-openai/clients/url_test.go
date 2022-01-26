//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package clients

import (
	"testing"
)

func Test_openAIUrlBuilder(t *testing.T) {
	t.Run("documentUrl", func(t *testing.T) {
		type args struct {
			docType string
			model   string
		}
		tests := []struct {
			name string
			args args
			want string
		}{
			{
				name: "Document type: text model: ada vectorizationType: document",
				args: args{
					docType: "text",
					model:   "ada",
				},
				want: "https://api.openai.com/v1/engines/text-search-ada-doc-001/embeddings",
			},
			{
				name: "Document type: text model: babbage vectorizationType: document",
				args: args{
					docType: "text",
					model:   "babbage",
				},
				want: "https://api.openai.com/v1/engines/text-search-babbage-doc-001/embeddings",
			},
			{
				name: "Document type: text model: curie vectorizationType: document",
				args: args{
					docType: "text",
					model:   "curie",
				},
				want: "https://api.openai.com/v1/engines/text-search-curie-doc-001/embeddings",
			},
			{
				name: "Document type: text model: davinci vectorizationType: document",
				args: args{
					docType: "text",
					model:   "davinci",
				},
				want: "https://api.openai.com/v1/engines/text-search-davinci-doc-001/embeddings",
			},
			{
				name: "Document type: code model: ada vectorizationType: code",
				args: args{
					docType: "code",
					model:   "ada",
				},
				want: "https://api.openai.com/v1/engines/code-search-ada-code-001/embeddings",
			},
			{
				name: "Document type: code model: babbage vectorizationType: code",
				args: args{
					docType: "code",
					model:   "babbage",
				},
				want: "https://api.openai.com/v1/engines/code-search-babbage-code-001/embeddings",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				o := newOpenAIUrlBuilder()
				if got := o.documentUrl(tt.args.docType, tt.args.model); got != tt.want {
					t.Errorf("openAIUrlBuilder.documentUrl() = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("queryUrl", func(t *testing.T) {
		type args struct {
			docType string
			model   string
		}
		tests := []struct {
			name string
			args args
			want string
		}{
			{
				name: "Document type: text model: ada vectorizationType: query",
				args: args{
					docType: "text",
					model:   "ada",
				},
				want: "https://api.openai.com/v1/engines/text-search-ada-query-001/embeddings",
			},
			{
				name: "Document type: text model: babbage vectorizationType: query",
				args: args{
					docType: "text",
					model:   "babbage",
				},
				want: "https://api.openai.com/v1/engines/text-search-babbage-query-001/embeddings",
			},
			{
				name: "Document type: text model: curie vectorizationType: query",
				args: args{
					docType: "text",
					model:   "curie",
				},
				want: "https://api.openai.com/v1/engines/text-search-curie-query-001/embeddings",
			},
			{
				name: "Document type: text model: davinci vectorizationType: query",
				args: args{
					docType: "text",
					model:   "davinci",
				},
				want: "https://api.openai.com/v1/engines/text-search-davinci-query-001/embeddings",
			},

			{
				name: "Document type: code model: ada vectorizationType: text",
				args: args{
					docType: "code",
					model:   "ada",
				},
				want: "https://api.openai.com/v1/engines/code-search-ada-text-001/embeddings",
			},
			{
				name: "Document type: code model: babbage vectorizationType: text",
				args: args{
					docType: "code",
					model:   "babbage",
				},
				want: "https://api.openai.com/v1/engines/code-search-babbage-text-001/embeddings",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				o := newOpenAIUrlBuilder()
				if got := o.queryUrl(tt.args.docType, tt.args.model); got != tt.want {
					t.Errorf("openAIUrlBuilder.queryUrl() = %v, want %v", got, tt.want)
				}
			})
		}
	})
}
