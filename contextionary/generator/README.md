# Contextionary generator
The contextionary generator

How to build:
```
go build ./contextionary/generator/cmd/generator.go
```

Usage:

```
 ./generator --help
Usage:
  generator [OPTIONS]

Application Options:
  -c, --vector-csv-path= Path to the output file of Glove
  -t, --temp-db-path=    Location for the temporary database (default: .tmp_import)
  -p, --output-prefix=   The prefix of the names of the files
  -k=                    number of forrests to generate (default: 20)

Help Options:
  -h, --help             Show this help message
```
