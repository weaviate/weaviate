FROM golang:1.26-alpine

WORKDIR /app

# go.mod/go.sum are committed so the build is hash-pinned and reproducible:
# the previous `go get github.com/weaviate/mockoidc@latest` made every CI
# run depend on whatever was tagged last — the v0.0.1/v0.0.2 tags
# (2026-10-29) require go >= 1.26 and broke all acceptance jobs still
# building with go 1.24.
COPY go.mod go.sum mockoidc.go ./
RUN go mod download && go build -o mockoidc .

CMD ["/app/mockoidc"]
