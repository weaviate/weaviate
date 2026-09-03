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

package banner

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

func TestEmbeddedArtGlyphs(t *testing.T) {
	// No backslash: Grafana's newline unescaping leaves \\ doubled.
	for i, line := range embeddedArt {
		assert.NotContains(t, line, `\`, "line %d", i)
		assert.LessOrEqual(t, len([]rune(line)), maxArtColumns, "line %d", i)
	}
	assert.LessOrEqual(t, len(embeddedArt), maxArtLines)
}

func TestRender(t *testing.T) {
	got := render([]string{"  ██", "  ▁▁"}, nil, landingURL()+"?clusterid=abc")

	assert.True(t, strings.HasPrefix(got, "\n╔══"), "top border first: %q", got)
	assert.Contains(t, got, "║░░      ██\n║░░      ▁▁\n", "art inside the frame")
	assert.Contains(t, got, "║░░        ⇒ Version: ")
	assert.Contains(t, got, "║░░        ⇒ Docs:    "+landingURL()+"?clusterid=abc\n")
	assert.NotContains(t, got, "News", "no news line without a message")

	// One frame: the borders and the two fill rows all span the widest line.
	rows := strings.Split(strings.TrimSuffix(got, "\n"), "\n")
	top, bottom := rows[1], rows[len(rows)-1]
	assert.True(t, strings.HasPrefix(top, "╔═"), "got %q", top)
	assert.True(t, strings.HasPrefix(bottom, "╚═"), "got %q", bottom)
	assert.Equal(t, len([]rune(top)), len([]rune(bottom)))
	assert.Equal(t, 2, strings.Count(got, "║░░░"), "one fill row under each border")
}

func TestRenderNews(t *testing.T) {
	news := "Weaviate 1.39 is out: https://weaviate.io/blog/weaviate-1-39-release"
	got := render(nil, []string{news}, landingURL())
	assert.Contains(t, got, "║░░        ⇒ News:    "+news+"\n", "got %q", got)

	// A second line starts in the column the first line's text starts in.
	got = render(nil, []string{"first", "second"}, landingURL())
	indent := strings.Repeat(" ", len([]rune("        ⇒ News:    ")))
	assert.Contains(t, got, "⇒ News:    first\n║░░"+indent+"second\n", "got %q", got)
}

func TestRenderDropsControlCharacters(t *testing.T) {
	got := render(nil, []string{"new\nlevel=error msg=forged"}, "https://d\nlevel=error msg=forged\r\x1b[31m")

	assert.Contains(t, got, "⇒ Docs:    https://dlevel=error msg=forged[31m\n")
	assert.Contains(t, got, "⇒ News:    newlevel=error msg=forged\n")
	assert.NotContains(t, got, "\nlevel=error")
}

func TestSanitize(t *testing.T) {
	wide := strings.Repeat("█", maxArtColumns+5)

	tests := []struct {
		name    string
		in      []string
		want    []string
		wantErr bool
	}{
		{name: "kept as is", in: []string{"  ██▁", "  ▁██"}, want: []string{"  ██▁", "  ▁██"}},
		{name: "control characters dropped", in: []string{"██\n\x1b[31m▁\r"}, want: []string{"██[31m▁"}},
		// U+009B is a one-byte CSI some terminals honor like ESC [.
		{name: "C1 controls dropped", in: []string{"██\u009b[31m▁\u0085"}, want: []string{"██[31m▁"}},
		{name: "lines cut to the column limit", in: []string{wide}, want: []string{strings.Repeat("█", maxArtColumns)}},
		{
			name: "line count capped",
			in:   []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"},
			want: []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"},
		},
		{name: "trailing blank lines dropped", in: []string{"██", "", "   "}, want: []string{"██"}},
		{name: "nothing visible", in: []string{"", "\n", "\x00"}, wantErr: true},
		{name: "empty", in: nil, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := sanitize(tt.in, maxArtLines)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFetch(t *testing.T) {
	serve := func(status int, contentType, body string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "application/json", r.Header.Get("Accept"))
			assert.True(t, strings.HasPrefix(r.Header.Get("User-Agent"), "weaviate/"))
			w.Header().Set("Content-Type", contentType)
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
		}))
	}

	tests := []struct {
		name        string
		status      int
		contentType string
		body        string
		want        *Content
		wantErr     string
	}{
		{
			name: "art", status: 200, contentType: "application/json; charset=utf-8",
			body: `{"schema_version":1,"art":["  ██","  ▁▁"],"_comment":"ignored"}`,
			want: &Content{Art: []string{"  ██", "  ▁▁"}},
		},
		{
			name: "art with news", status: 200, contentType: "application/json",
			body: `{"schema_version":1,"art":["  ██"],"message":["Weaviate 1.39 is out: https://weaviate.io/blog/weaviate-1-39-release","","more","a fourth line is cut"]}`,
			want: &Content{Art: []string{"  ██"}, Message: []string{"Weaviate 1.39 is out: https://weaviate.io/blog/weaviate-1-39-release", "", "more"}},
		},
		{
			name: "blank news is dropped", status: 200, contentType: "application/json",
			body: `{"schema_version":1,"art":["  ██"],"message":[""," ","\u0000"]}`,
			want: &Content{Art: []string{"  ██"}},
		},
		{
			name: "text/plain as raw file hosts serve it", status: 200, contentType: "text/plain; charset=utf-8",
			body: `{"schema_version":1,"art":["  ██"]}`, want: &Content{Art: []string{"  ██"}},
		},
		{name: "not found", status: 404, contentType: "application/json", body: `{}`, wantErr: "unexpected status 404"},
		{name: "html error page", status: 200, contentType: "text/html", body: `<html>`, wantErr: "unexpected content type"},
		{name: "not json", status: 200, contentType: "application/json", body: `nope`, wantErr: "decode"},
		{name: "unknown schema", status: 200, contentType: "application/json", body: `{"schema_version":2,"art":["x"]}`, wantErr: "unsupported schema_version 2"},
		{name: "no art", status: 200, contentType: "application/json", body: `{"schema_version":1,"art":[]}`, wantErr: "no visible lines"},
		{
			name: "oversized body", status: 200, contentType: "application/json",
			body: `{"schema_version":1,"art":["` + strings.Repeat("x", maxBodyBytes) + `"]}`, wantErr: "exceeds",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := serve(tt.status, tt.contentType, tt.body)
			defer srv.Close()

			got, err := Fetch(context.Background(), srv.Client(), srv.URL)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFetchHonoursContextTimeout(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-block }))
	defer func() { close(block); srv.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := Fetch(ctx, srv.Client(), srv.URL)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestRepeater(t *testing.T) {
	t.Cleanup(func() { enterrors.SetClusterIDSource(nil) })
	const id = "0198c0de-dead-beef-8000-000000000001"
	var committed atomic.Bool
	clusterID := func() string {
		if committed.Load() {
			return id
		}
		return ""
	}
	enterrors.SetClusterIDSource(clusterID)

	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)

	fetched := &Content{Art: []string{"  ██ remote"}, Message: []string{"Weaviate 1.39 is out: https://weaviate.io/blog/weaviate-1-39-release"}}
	calls := 0
	fetch := func(context.Context) (*Content, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("offline")
		}
		return fetched, nil
	}

	r := NewRepeater(logger, clusterID, 5*time.Millisecond, fetch)
	assert.Equal(t, &Content{Art: embeddedArt}, r.Content(), "nothing fetched yet")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()

	// No id yet: nothing is logged, however long we wait.
	time.Sleep(30 * time.Millisecond)
	assert.Empty(t, hook.AllEntries(), "no banner before the cluster has an id")
	committed.Store(true)

	var banners []*logrus.Entry
	require.Eventually(t, func() bool {
		banners = nil
		for _, e := range hook.AllEntries() {
			if e.Level == logrus.InfoLevel && e.Data["action"] == Action {
				banners = append(banners, e)
			}
		}
		return len(banners) >= 2
	}, 2*time.Second, time.Millisecond)

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}

	// The failed first fetch is a debug entry, never an error, and never
	// carries the banner action: its message embeds the fetch error, which
	// must stay on the formatter's quoted single-line path.
	var failedFetch int
	for _, e := range hook.AllEntries() {
		if strings.Contains(e.Message, "banner art not fetched") {
			assert.Equal(t, logrus.DebugLevel, e.Level)
			assert.Equal(t, "banner_fetch", e.Data["action"])
			failedFetch++
		}
	}
	assert.Equal(t, 1, failedFetch)

	last := banners[len(banners)-1]
	docsURL := landingURL() + "?clusterid=0198c0de-dead-beef-8000-000000000001"
	assert.Equal(t, docsURL, last.Data[enterrors.DocsLinkField])
	assert.Contains(t, last.Message, "  ██ remote\n", "the fetched art replaces the embedded art")
	assert.Contains(t, last.Message, "⇒ Docs:    "+docsURL)
	assert.Contains(t, last.Message, "⇒ News:    Weaviate 1.39 is out: https://weaviate.io/blog/weaviate-1-39-release\n")
	assert.Equal(t, fetched, r.Content())
}

func TestRepeaterStopsWhileWaitingForClusterID(t *testing.T) {
	logger, hook := test.NewNullLogger()
	r := NewRepeater(logger, func() string { return "" }, time.Millisecond, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
	assert.Empty(t, hook.AllEntries())
}

func TestNewRepeaterDefaults(t *testing.T) {
	r := NewRepeater(logrus.New(), func() string { return "" }, 0, nil)
	assert.Equal(t, defaultInterval, r.interval)
	assert.NotNil(t, r.fetch)
}
