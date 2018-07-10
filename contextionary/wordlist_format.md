# Wordlist File Format

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
