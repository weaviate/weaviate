FROM golang:alpine

WORKDIR /app

RUN go mod init mockoidc
RUN go get github.com/oauth2-proxy/mockoidc@latest
COPY mockoidc.go .

CMD ["go", "run", "/app"]
