FROM golang:alpine

WORKDIR /app

RUN go mod init mockoidc_helper
COPY mockoidc_helper.go .
RUN go build -o mockoidc_helper .

CMD ["/app/mockoidc_helper"]
