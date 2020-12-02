module github.com/semi-technologies/weaviate

require (
	github.com/TylerBrock/colorjson v0.0.0-20180527164720-95ec53f28296
	github.com/bmatcuk/doublestar v1.1.3
	github.com/boltdb/bolt v1.3.1
	github.com/coreos/etcd v3.3.18+incompatible
	github.com/coreos/go-oidc v2.0.0+incompatible
	github.com/coreos/go-systemd v0.0.0-20190321100706-95778dfbb74e // indirect
	github.com/danaugrs/go-tsne v0.0.0-20200708172100-6b7d1d577fd3
	github.com/davecgh/go-spew v1.1.1
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/elastic/go-elasticsearch/v5 v5.6.0
	github.com/fatih/camelcase v1.0.0
	github.com/fatih/color v1.9.0 // indirect
	github.com/go-openapi/errors v0.19.9
	github.com/go-openapi/loads v0.19.5
	github.com/go-openapi/runtime v0.19.24
	github.com/go-openapi/spec v0.19.8
	github.com/go-openapi/strfmt v0.19.11
	github.com/go-openapi/swag v0.19.12
	github.com/go-openapi/validate v0.19.10
	github.com/golang/protobuf v1.4.2 // indirect
	github.com/google/uuid v1.1.1
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/graphql-go/graphql v0.7.7
	github.com/hokaccha/go-prettyjson v0.0.0-20190818114111-108c894c2c0e // indirect
	github.com/jessevdk/go-flags v1.4.0
	github.com/json-iterator/go v1.1.8 // indirect
	github.com/mattn/go-colorable v0.1.7 // indirect
	github.com/mitchellh/mapstructure v1.4.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.1 // indirect
	github.com/nyaruka/phonenumbers v1.0.54
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v0.9.3 // indirect
	github.com/rs/cors v1.5.0
	github.com/satori/go.uuid v0.0.0-20180103174451-36e9d2ebbde5
	github.com/semi-technologies/contextionary v0.0.0-20200701085343-13c11a568705
	github.com/sirupsen/logrus v1.6.0
	github.com/square/go-jose v2.3.0+incompatible
	github.com/stretchr/testify v1.6.1
	go.mongodb.org/mongo-driver v1.4.4 // indirect
	golang.org/x/exp v0.0.0-20191030013958-a1ab85dbe136 // indirect
	golang.org/x/net v0.0.0-20201021035429-f5854403a974
	golang.org/x/oauth2 v0.0.0-20190604053449-0f29369cfe45 // indirect
	golang.org/x/tools v0.0.0-20201201210846-92771a23d8e3 // indirect
	gonum.org/v1/gonum v0.7.0
	google.golang.org/appengine v1.6.1 // indirect
	google.golang.org/genproto v0.0.0-20191108220845-16a3f7862a1a // indirect
	google.golang.org/grpc v1.24.0
	gopkg.in/square/go-jose.v2 v2.5.1 // indirect
	gopkg.in/yaml.v2 v2.4.0
	honnef.co/go/tools v0.0.1-2020.1.5 // indirect
	sigs.k8s.io/yaml v1.1.0 // indirect
)

replace github.com/coreos/go-systemd => github.com/coreos/go-systemd/v22 v22.0.0

go 1.14
