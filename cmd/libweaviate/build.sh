go build -buildmode c-shared -o libweaviate.so .
go build -buildmode c-shared -o libweaviate.dylib .
CGO_ENABLED=1 GOOS=darwin GOARCH=amd64 go build -buildmode c-shared -o libweaviateamd.dylib .
CGO_ENABLED=1 GOOS=darwin GOARCH=x86_64 go build -buildmode c-shared -o libweaviatex86_64.dylib .

python bindings/pythonctypes.py
