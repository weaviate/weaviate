# Contextionary generator
The contextionary generator takes as input a Glove file and produces two output files:
1. a `$PREFIX.knn` file, which is the output of the Annoy building process
2. a `$PREFIX.idx` file, which contains the word list.

Combined, these form a Contextionary that can be loaded from disk (see the [README.md](../README.md)) for the `contextionary` package.

## Build & Usage
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

The generated Annoy index can be tuned by setting `k` to a different value.
Increasing `k` will increase the on-disk space used.

## File formats

### Annoy file format
There is no documentation apart from [this](https://github.com/spotify/annoy/blob/d1ba6a9405e06203b842e6ebbbea58deda388631/src/annoylib.h#L245-L258) comment in the source code.

### Wordlist File Format

Start                                                              | Length (bytes)       |  Type                             | Description
-------------------------------------------------------------------|----------------------|-----------------------------------|-------------
0                                                                  | 8                    | little endian uint64              | number of words
8                                                                  | 8                    | little endian uint64              | vector length
16                                                                 | 8                    | little endian uint64              | length of metadata
24                                                                 | `len(metadata)`      | bytes (not null terminated)       | JSON encoded metadata
`24 + len(metadata)`                                               | 0-4                  | padding until 4 byte-aligned      |  4 - ((24 + len(metadata)) % 4)`
`24 + len(metadata) + 0-4 + 8 * $idx`                              | 8                    | little endian uint64              | pointer to word location since beginning of file
`24 + len(metadata) + 0-4 + 8 * ($nrwords+1)`                      | `len($words[0]) + 1` | C-style string; `\x00` terminated | First word
`24 + len(metadata) + 0-4 + 8 * ($nrwords+1) + len($words[0]) + 1` | `len($words[1]) + 1` | C-style string; `\x00` terminated | Second word
...                                                                | ...                  | ...                               | repeating `$nrWords` times.

Note that the words are also padded until they are 4-byte aligned.
