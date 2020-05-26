module github.com/semi-technologies/weaviate

require (
	github.com/TylerBrock/colorjson v0.0.0-20180527164720-95ec53f28296
	github.com/asaskevich/govalidator v0.0.0-20200428143746-21a406dcc535 // indirect
	github.com/bmatcuk/doublestar v1.1.3
	github.com/boltdb/bolt v1.3.1
	github.com/coreos/etcd v3.3.18+incompatible
	github.com/coreos/go-oidc v2.0.0+incompatible
	github.com/davecgh/go-spew v1.1.1
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/elastic/go-elasticsearch/v5 v5.6.0
	github.com/fatih/camelcase v1.0.0
	github.com/fatih/color v1.7.0 // indirect
	github.com/go-openapi/errors v0.19.4
	github.com/go-openapi/loads v0.19.3
	github.com/go-openapi/runtime v0.19.15
	github.com/go-openapi/spec v0.19.3
	github.com/go-openapi/strfmt v0.19.5
	github.com/go-openapi/swag v0.19.9
	github.com/go-openapi/validate v0.19.3
	github.com/gorilla/mux v1.7.0
	github.com/graphql-go/graphql v0.7.7
	github.com/hokaccha/go-prettyjson v0.0.0-20190818114111-108c894c2c0e // indirect
	github.com/jessevdk/go-flags v1.4.0
	github.com/json-iterator/go v1.1.8 // indirect
	github.com/mailru/easyjson v0.7.1 // indirect
	github.com/mattn/go-colorable v0.1.4 // indirect
	github.com/mattn/go-isatty v0.0.11 // indirect
	github.com/mitchellh/mapstructure v1.3.1 // indirect
	github.com/modern-go/reflect2 v1.0.1 // indirect
	github.com/nyaruka/phonenumbers v1.0.54
	github.com/pkg/errors v0.8.1
	github.com/rs/cors v1.5.0
	github.com/satori/go.uuid v0.0.0-20180103174451-36e9d2ebbde5
	github.com/semi-technologies/contextionary v0.0.0-20200424084255-29fe88e56c12
	github.com/sirupsen/logrus v1.4.2
	github.com/square/go-jose v2.3.0+incompatible
	github.com/stretchr/testify v1.4.0
	github.com/ugorji/go/codec v0.0.0-20190309163734-c4a1c341dc93
	go.mongodb.org/mongo-driver v1.3.3 // indirect
	golang.org/x/net v0.0.0-20200226121028-0de0cce0169b
	golang.org/x/tools v0.0.0-20200522201501-cb1345f3a375 // indirect
	google.golang.org/grpc v1.24.0
	gopkg.in/yaml.v2 v2.3.0
	sigs.k8s.io/yaml v1.1.0 // indirect
)

replace github.com/coreos/go-systemd => github.com/coreos/go-systemd/v22 v22.0.0

go 1.14
