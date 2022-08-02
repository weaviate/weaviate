FROM golang:1.16-alpine

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY app.go .

RUN go build -o importer .

CMD ["/app/importer"]
