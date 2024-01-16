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
	t.Run("when the server is providing meta", func(t *testing.T) {
		server := httptest.NewServer(&testMetaHandler{t: t})
		defer server.Close()
		c := New(server.URL, 0, nullLogger())
		meta, err := c.MetaInfo()

		assert.Nil(t, err)
		assert.NotNil(t, meta)
		metaModel := meta["model"]
		assert.True(t, metaModel != nil)
		model, modelOK := metaModel.(map[string]interface{})
		assert.True(t, modelOK)
		assert.True(t, model["_name_or_path"] != nil)
		assert.True(t, model["architectures"] != nil)
		modelID2label, modelID2labelOK := model["id2label"].(map[string]interface{})
		assert.True(t, modelID2labelOK)
		assert.True(t, modelID2label["0"] != nil)
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
        "_name_or_path": "bert-large-uncased-whole-word-masking-finetuned-squad",
        "add_cross_attention": false,
        "architectures": [
            "BertForQuestionAnswering"
        ],
        "attention_probs_dropout_prob": 0.1,
        "bad_words_ids": null,
        "bos_token_id": null,
        "chunk_size_feed_forward": 0,
        "decoder_start_token_id": null,
        "diversity_penalty": 0.0,
        "do_sample": false,
        "early_stopping": false,
        "encoder_no_repeat_ngram_size": 0,
        "eos_token_id": null,
        "finetuning_task": null,
        "gradient_checkpointing": false,
        "hidden_act": "gelu",
        "hidden_dropout_prob": 0.1,
        "hidden_size": 1024,
        "id2label": {
            "0": "LABEL_0",
            "1": "LABEL_1"
        },
        "initializer_range": 0.02,
        "intermediate_size": 4096,
        "is_decoder": false,
        "is_encoder_decoder": false,
        "label2id": {
            "LABEL_0": 0,
            "LABEL_1": 1
        },
        "layer_norm_eps": 1e-12,
        "length_penalty": 1.0,
        "max_length": 20,
        "max_position_embeddings": 512,
        "min_length": 0,
        "model_type": "bert",
        "no_repeat_ngram_size": 0,
        "num_attention_heads": 16,
        "num_beam_groups": 1,
        "num_beams": 1,
        "num_hidden_layers": 24,
        "num_return_sequences": 1,
        "output_attentions": false,
        "output_hidden_states": false,
        "output_scores": false,
        "pad_token_id": 0,
        "position_embedding_type": "absolute",
        "prefix": null,
        "pruned_heads": {},
        "repetition_penalty": 1.0,
        "return_dict": true,
        "return_dict_in_generate": false,
        "sep_token_id": null,
        "task_specific_params": null,
        "temperature": 1.0,
        "tie_encoder_decoder": false,
        "tie_word_embeddings": true,
        "tokenizer_class": null,
        "top_k": 50,
        "top_p": 1.0,
        "torchscript": false,
        "transformers_version": "4.3.2",
        "type_vocab_size": 2,
        "use_bfloat16": false,
        "use_cache": true,
        "vocab_size": 30522,
        "xla_device": null
    }
}`
}
