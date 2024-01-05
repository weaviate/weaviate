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

package swagger_middleware

import (
	"fmt"
	"html/template"
	"net/http"
	"strings"
)

const swaggerUIVersion = "3.19.4"

type templateData struct {
	Prefix   string
	APIKey   string
	APIToken string
}

func AddMiddleware(swaggerJSON []byte, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v1/swagger.json") && r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			w.Write(swaggerJSON)
		} else if strings.HasPrefix(r.URL.Path, "/v1/swagger") && r.Method == http.MethodGet {
			renderSwagger(w, r)
		} else {
			next.ServeHTTP(w, r)
		}
	})
}

// renderswagger renders the swagger GUI
func renderSwagger(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("WWW-Authenticate", `Basic realm="Provide your key and token (as username as password respectively)"`)

	user, password, authOk := r.BasicAuth()
	if !authOk {
		http.Error(w, "Not authorized", 401)
		return
	}

	t := template.New("Swagger")
	t, err := t.Parse(swaggerTemplate)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Create result string
	d := templateData{
		Prefix:   fmt.Sprintf("https://cdn.jsdelivr.net/npm/swagger-ui-dist@%s", swaggerUIVersion),
		APIKey:   user,
		APIToken: password,
	}

	err = t.ExecuteTemplate(w, "index", d)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// tmpl is the page template to render GraphiQL
const swaggerTemplate = `
{{ define "index" }}
<!-- HTML for static distribution bundle build -->
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8">
    <title>Weaviate API</title>
    <link rel="stylesheet" type="text/css" href="{{ .Prefix }}/swagger-ui.css" >
    <link rel="icon" type="image/png" href="{{ .Prefix }}/favicon-32x32.png" sizes="32x32" />
    <link rel="icon" type="image/png" href="{{ .Prefix }}/favicon-16x16.png" sizes="16x16" />
    <style>
      html
      {
        box-sizing: border-box;
        overflow: -moz-scrollbars-vertical;
        overflow-y: scroll;
      }

      *,
      *:before,
      *:after
      {
        box-sizing: inherit;
      }

      body
      {
        margin:0;
        background: #fafafa;
      }
    </style>
  </head>

  <body>
    <div id="swagger-ui"></div>

    <script src="{{ .Prefix }}/swagger-ui-bundle.js"> </script>
    <script src="{{ .Prefix }}/swagger-ui-standalone-preset.js"> </script>
    <script>
    window.onload = function() {

      // Build a system
      const ui = SwaggerUIBundle({
        url: "/v1/swagger.json",
        dom_id: '#swagger-ui',
        deepLinking: true,
        presets: [
          SwaggerUIBundle.presets.apis,
          SwaggerUIStandalonePreset
        ],
        requestInterceptor: function(request) {
          request.headers["X-API-KEY"] = "{{ .APIKey }}"
          request.headers["X-API-TOKEN"] = "{{ .APIToken }}"

          var requestUrl = null
          try {
            requestUrl = new URL(request.url)
          } catch (err) {
            console.log("Could not parse url in request", request, "; not checking if we need to change the protocol.")
          }

          if (requestUrl != null && requestUrl.protocol != location.protocol) {
            requestUrl.protocol = location.protocol
            request.url = requestUrl.toString()
          }

          return request
        },
        plugins: [
          SwaggerUIBundle.plugins.DownloadUrl
        ],
        layout: "StandaloneLayout"
      })

      window.ui = ui
    }
  </script>
  </body>
</html>
{{ end }}
`
