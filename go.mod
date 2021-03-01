module github.com/semi-technologies/weaviate

require (
	github.com/bmatcuk/doublestar v1.1.3
	github.com/coreos/etcd v3.3.18+incompatible
	github.com/coreos/go-oidc v2.0.0+incompatible
	github.com/danaugrs/go-tsne v0.0.0-20200708172100-6b7d1d577fd3
	github.com/davecgh/go-spew v1.1.1
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/fatih/camelcase v1.0.0
	github.com/go-openapi/errors v0.20.0
	github.com/go-openapi/loads v0.19.5
	github.com/go-openapi/runtime v0.19.26
	github.com/go-openapi/spec v0.19.8
	github.com/go-openapi/strfmt v0.20.0
	github.com/go-openapi/swag v0.19.14
	github.com/go-openapi/validate v0.19.10
	github.com/google/uuid v1.1.1
	github.com/graphql-go/graphql v0.7.9
	github.com/jessevdk/go-flags v1.4.0
	github.com/nyaruka/phonenumbers v1.0.54
	github.com/pkg/errors v0.9.1
	github.com/rs/cors v1.5.0
	github.com/satori/go.uuid v0.0.0-20180103174451-36e9d2ebbde5
	github.com/semi-technologies/contextionary v0.0.0-20210212161414-8c1316389775
	github.com/sirupsen/logrus v1.6.0
	github.com/square/go-jose v2.3.0+incompatible
	github.com/stretchr/testify v1.6.1
	go.etcd.io/bbolt v1.3.3
	golang.org/x/net v0.0.0-20201021035429-f5854403a974
	gonum.org/v1/gonum v0.7.0
	google.golang.org/grpc v1.24.0
	gopkg.in/yaml.v2 v2.4.0
)

replace github.com/coreos/go-systemd => github.com/coreos/go-systemd/v22 v22.0.0

go 1.15
