//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
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
	t.Run("when common server is providing meta", func(t *testing.T) {
		server := httptest.NewServer(&testMetaHandler{t: t})
		defer server.Close()
		v := New(server.URL, server.URL, 0, nullLogger())
		meta, err := v.MetaInfo()

		assert.Nil(t, err)
		assert.NotNil(t, meta)

		model := extractChildMap(t, meta, "model")
		assert.NotNil(t, model["_name_or_path"])
		assert.NotNil(t, model["architectures"])
		assert.Contains(t, model["architectures"], "DistilBertModel")
		ID2Label := extractChildMap(t, model, "id2label")
		assert.NotNil(t, ID2Label["0"])
		assert.NotNil(t, ID2Label["1"])
	})

	t.Run("when passage and query servers are providing meta", func(t *testing.T) {
		serverPassage := httptest.NewServer(&testMetaHandler{t: t, modelType: "passage"})
		serverQuery := httptest.NewServer(&testMetaHandler{t: t, modelType: "query"})
		defer serverPassage.Close()
		defer serverQuery.Close()
		v := New(serverPassage.URL, serverQuery.URL, 0, nullLogger())
		meta, err := v.MetaInfo()

		assert.Nil(t, err)
		assert.NotNil(t, meta)

		passage := extractChildMap(t, meta, "passage")
		passageModel := extractChildMap(t, passage, "model")
		assert.NotNil(t, passageModel["_name_or_path"])
		assert.NotNil(t, passageModel["architectures"])
		assert.Contains(t, passageModel["architectures"], "DPRContextEncoder")
		passageID2Label := extractChildMap(t, passageModel, "id2label")
		assert.NotNil(t, passageID2Label["0"])
		assert.NotNil(t, passageID2Label["1"])

		query := extractChildMap(t, meta, "query")
		queryModel := extractChildMap(t, query, "model")
		assert.NotNil(t, queryModel["_name_or_path"])
		assert.NotNil(t, queryModel["architectures"])
		assert.Contains(t, queryModel["architectures"], "DPRQuestionEncoder")
		queryID2Label := extractChildMap(t, queryModel, "id2label")
		assert.NotNil(t, queryID2Label["0"])
		assert.NotNil(t, queryID2Label["1"])
	})

	t.Run("when passage and query servers are unavailable", func(t *testing.T) {
		rt := time.Now().Add(time.Hour)
		serverPassage := httptest.NewServer(&testMetaHandler{t: t, modelType: "passage", readyTime: rt})
		serverQuery := httptest.NewServer(&testMetaHandler{t: t, modelType: "query", readyTime: rt})
		defer serverPassage.Close()
		defer serverQuery.Close()
		v := New(serverPassage.URL, serverQuery.URL, 0, nullLogger())
		meta, err := v.MetaInfo()

		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "[passage] unexpected status code '503' of meta request")
		assert.Contains(t, err.Error(), "[query] unexpected status code '503' of meta request")
		assert.Nil(t, meta)
	})
}

type testMetaHandler struct {
	t *testing.T
	// the test handler will report as not ready before the time has passed
	readyTime time.Time
	modelType string
}

func (h *testMetaHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(h.t, "/meta", r.URL.String())
	assert.Equal(h.t, http.MethodGet, r.Method)

	if time.Since(h.readyTime) < 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	w.Write([]byte(h.metaInfo()))
}

func (h *testMetaHandler) metaInfo() string {
	switch h.modelType {
	case "passage":
		return `{
              "model": {
                "return_dict": true,
                "output_hidden_states": false,
                "output_attentions": false,
                "torchscript": false,
                "torch_dtype": "float32",
                "use_bfloat16": false,
                "pruned_heads": {},
                "tie_word_embeddings": true,
                "is_encoder_decoder": false,
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
                "diversity_penalty": 0,
                "temperature": 1,
                "top_k": 50,
                "top_p": 1,
                "repetition_penalty": 1,
                "length_penalty": 1,
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
                "architectures": [
                  "DPRContextEncoder"
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
                "eos_token_id": null,
                "sep_token_id": null,
                "decoder_start_token_id": null,
                "task_specific_params": null,
                "problem_type": null,
                "_name_or_path": "./models/model",
                "transformers_version": "4.16.2",
                "gradient_checkpointing": false,
                "model_type": "dpr",
                "vocab_size": 30522,
                "hidden_size": 768,
                "num_hidden_layers": 12,
                "num_attention_heads": 12,
                "hidden_act": "gelu",
                "intermediate_size": 3072,
                "hidden_dropout_prob": 0.1,
                "attention_probs_dropout_prob": 0.1,
                "max_position_embeddings": 512,
                "type_vocab_size": 2,
                "initializer_range": 0.02,
                "layer_norm_eps": 1e-12,
                "projection_dim": 0,
                "position_embedding_type": "absolute"
              }
            }`
	case "query":
		return `{
              "model": {
                "return_dict": true,
                "output_hidden_states": false,
                "output_attentions": false,
                "torchscript": false,
                "torch_dtype": "float32",
                "use_bfloat16": false,
                "pruned_heads": {},
                "tie_word_embeddings": true,
                "is_encoder_decoder": false,
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
                "diversity_penalty": 0,
                "temperature": 1,
                "top_k": 50,
                "top_p": 1,
                "repetition_penalty": 1,
                "length_penalty": 1,
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
                "architectures": [
                  "DPRQuestionEncoder"
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
                "eos_token_id": null,
                "sep_token_id": null,
                "decoder_start_token_id": null,
                "task_specific_params": null,
                "problem_type": null,
                "_name_or_path": "./models/model",
                "transformers_version": "4.16.2",
                "gradient_checkpointing": false,
                "model_type": "dpr",
                "vocab_size": 30522,
                "hidden_size": 768,
                "num_hidden_layers": 12,
                "num_attention_heads": 12,
                "hidden_act": "gelu",
                "intermediate_size": 3072,
                "hidden_dropout_prob": 0.1,
                "attention_probs_dropout_prob": 0.1,
                "max_position_embeddings": 512,
                "type_vocab_size": 2,
                "initializer_range": 0.02,
                "layer_norm_eps": 1e-12,
                "projection_dim": 0,
                "position_embedding_type": "absolute"
              }
            }`
	default:
		return `{
              "model": {
                "_name_or_path": "distilbert-base-uncased",
                "activation": "gelu",
                "add_cross_attention": false,
                "architectures": [
                  "DistilBertModel"
                ],
                "attention_dropout": 0.1,
                "bad_words_ids": null,
                "bos_token_id": null,
                "chunk_size_feed_forward": 0,
                "decoder_start_token_id": null,
                "dim": 768,
                "diversity_penalty": 0,
                "do_sample": false,
                "dropout": 0.1,
                "early_stopping": false,
                "encoder_no_repeat_ngram_size": 0,
                "eos_token_id": null,
                "finetuning_task": null,
                "hidden_dim": 3072,
                "id2label": {
                  "0": "LABEL_0",
                  "1": "LABEL_1"
                },
                "initializer_range": 0.02,
                "is_decoder": false,
                "is_encoder_decoder": false,
                "label2id": {
                  "LABEL_0": 0,
                  "LABEL_1": 1
                },
                "length_penalty": 1,
                "max_length": 20,
                "max_position_embeddings": 512,
                "min_length": 0,
                "model_type": "distilbert",
                "n_heads": 12,
                "n_layers": 6,
                "no_repeat_ngram_size": 0,
                "num_beam_groups": 1,
                "num_beams": 1,
                "num_return_sequences": 1,
                "output_attentions": false,
                "output_hidden_states": false,
                "output_scores": false,
                "pad_token_id": 0,
                "prefix": null,
                "pruned_heads": {},
                "qa_dropout": 0.1,
                "repetition_penalty": 1,
                "return_dict": true,
                "return_dict_in_generate": false,
                "sep_token_id": null,
                "seq_classif_dropout": 0.2,
                "sinusoidal_pos_embds": false,
                "task_specific_params": null,
                "temperature": 1,
                "tie_encoder_decoder": false,
                "tie_weights_": true,
                "tie_word_embeddings": true,
                "tokenizer_class": null,
                "top_k": 50,
                "top_p": 1,
                "torchscript": false,
                "transformers_version": "4.3.2",
                "use_bfloat16": false,
                "vocab_size": 30522,
                "xla_device": null
              }
            }`
	}
}

func extractChildMap(t *testing.T, parent map[string]interface{}, name string) map[string]interface{} {
	assert.NotNil(t, parent[name])
	child, ok := parent[name].(map[string]interface{})
	assert.True(t, ok)
	assert.NotNil(t, child)

	return child
}
