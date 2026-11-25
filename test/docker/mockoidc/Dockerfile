FROM golang:1.26-alpine

WORKDIR /app

RUN go mod init mockoidc
RUN go get github.com/weaviate/mockoidc@latest
COPY mockoidc.go .
RUN go build -o mockoidc .

CMD ["/app/mockoidc"]
