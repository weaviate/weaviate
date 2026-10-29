FROM golang:1.26-alpine

WORKDIR /app

RUN go mod init mockoidc
# pinned: @latest made every CI run depend on whatever was tagged last —
# the v0.0.1/v0.0.2 tags (2026-10-29) require go >= 1.26 and broke all
# acceptance jobs still building with go 1.24. Bump the pin and the base
# image together.
RUN go get github.com/weaviate/mockoidc@v0.0.2
COPY mockoidc.go .
RUN go build -o mockoidc .

CMD ["/app/mockoidc"]
