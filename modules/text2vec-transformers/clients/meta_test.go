//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
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
