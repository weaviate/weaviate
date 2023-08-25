//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package clients

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetMeta(t *testing.T) {
	t.Run("when the server is providing meta", func(t *testing.T) {
		server := httptest.NewServer(&testMetaHandler{t: t})
		defer server.Close()
		c := New(server.URL, nullLogger())
		meta, err := c.MetaInfo()

		assert.Nil(t, err)
		assert.NotNil(t, meta)
		metaModel := meta["model"]
		assert.True(t, metaModel != nil)
		model, modelOK := metaModel.(map[string]interface{})
		assert.True(t, modelOK)
		assert.True(t, model["_name_or_path"] != nil)
		assert.True(t, model["architectures"] != nil)
	})
}

type testMetaHandler struct {
	t *testing.T
	// the test handler will report as not ready before the time has passed
	readyTime time.Time
}

func (f *testMetaHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, "/meta", r.URL.String())
	assert.Equal(f.t, http.MethodGet, r.Method)

	if time.Since(f.readyTime) < 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	w.Write([]byte(f.metaInfo()))
}

func (f *testMetaHandler) metaInfo() string {
	return `{
		"model": {
			"vocab_size": 32128,
			"d_model": 768,
			"d_kv": 64,
			"d_ff": 2048,
			"num_layers": 12,
			"num_decoder_layers": 12,
			"num_heads": 12,
			"relative_attention_num_buckets": 32,
			"relative_attention_max_distance": 128,
			"dropout_rate": 0.1,
			"layer_norm_epsilon": 1e-06,
			"initializer_factor": 1.0,
			"feed_forward_proj": "gated-gelu",
			"use_cache": true,
			"dense_act_fn": "gelu_new",
			"is_gated_act": true,
			"return_dict": true,
			"output_hidden_states": false,
			"output_attentions": false,
			"torchscript": false,
			"torch_dtype": null,
			"use_bfloat16": false,
			"tf_legacy_loss": false,
			"pruned_heads": {},
			"tie_word_embeddings": false,
			"is_encoder_decoder": true,
			"is_decoder": false,
			"cross_attention_hidden_size": null,
			"add_cross_attention": false,
			"tie_encoder_decoder": false,
			"max_length": 20,
			"min_length": 0,
			"do_sample": false,
			"early_stopping": false,
			"num_beams": 1,
			"num_beam_groups": 1,
			"diversity_penalty": 0.0,
			"temperature": 1.0,
			"top_k": 50,
			"top_p": 1.0,
			"typical_p": 1.0,
			"repetition_penalty": 1.0,
			"length_penalty": 1.0,
			"no_repeat_ngram_size": 0,
			"encoder_no_repeat_ngram_size": 0,
			"bad_words_ids": null,
			"num_return_sequences": 1,
			"chunk_size_feed_forward": 0,
			"output_scores": false,
			"return_dict_in_generate": false,
			"forced_bos_token_id": null,
			"forced_eos_token_id": null,
			"remove_invalid_values": false,
			"exponential_decay_length_penalty": null,
			"suppress_tokens": null,
			"begin_suppress_tokens": null,
			"architectures": [
				"T5ForConditionalGeneration"
			],
			"finetuning_task": null,
			"id2label": {
				"0": "LABEL_0",
				"1": "LABEL_1"
			},
			"label2id": {
				"LABEL_0": 0,
				"LABEL_1": 1
			},
			"tokenizer_class": null,
			"prefix": null,
			"bos_token_id": null,
			"pad_token_id": 0,
			"eos_token_id": 1,
			"sep_token_id": null,
			"decoder_start_token_id": 0,
			"task_specific_params": {
				"summarization": {
					"early_stopping": true,
					"length_penalty": 2.0,
					"max_length": 200,
					"min_length": 30,
					"no_repeat_ngram_size": 3,
					"num_beams": 4,
					"prefix": "summarize: "
				},
				"translation_en_to_de": {
					"early_stopping": true,
					"max_length": 300,
					"num_beams": 4,
					"prefix": "translate English to German: "
				},
				"translation_en_to_fr": {
					"early_stopping": true,
					"max_length": 300,
					"num_beams": 4,
					"prefix": "translate English to French: "
				},
				"translation_en_to_ro": {
					"early_stopping": true,
					"max_length": 300,
					"num_beams": 4,
					"prefix": "translate English to Romanian: "
				}
			},
			"problem_type": null,
			"_name_or_path": "./models/model",
			"transformers_version": "4.26.1",
			"model_type": "t5",
			"n_positions": 512,
			"output_past": true
		}
	}`
}
