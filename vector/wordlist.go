package vector

//// #include <string.h>
//import "C"

import (
  "bytes"
  "encoding/binary"
  "fmt"
  "os"
  "syscall"
)

type Wordlist struct {
  vectorWidth uint64
  numberOfWords uint64
  file os.File
  mmap []byte
}

func LoadWordlist(path string) (*Wordlist, error) {
  file, err := os.Open(path)
  if err != nil {
    return nil, fmt.Errorf("Can't open the wordlist at %s: %+v", path, err)
  }

	file_info, err := file.Stat()
	if err != nil {
    return nil, fmt.Errorf("Can't stat the wordlist at %s: %+v", path, err)
	}

	mmap, err := syscall.Mmap(int(file.Fd()), 0, int(file_info.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
  if err != nil {
    return nil, fmt.Errorf("Can't mmap the file %s: %+v", path, err)
	}

  nrWordsBytes     := mmap[0:8]
  vectorWidthBytes := mmap[8:16]

  nrWords := binary.LittleEndian.Uint64(nrWordsBytes)
  vectorWidth := binary.LittleEndian.Uint64(vectorWidthBytes)

  return &Wordlist {
    vectorWidth: vectorWidth,
    numberOfWords: nrWords,
    mmap: mmap,
  }, nil
}

func (w *Wordlist) FindIndexByWord(_needle string) ItemIndex {
  var needle = string([]byte(_needle))
  needle += "\x00"

  var bytes_needle= []byte(needle)

  var low ItemIndex = 0
  var high ItemIndex = ItemIndex(w.numberOfWords)

  for low <= high {
    var midpoint ItemIndex = (low + high) / 2
    word_ptr := w.getWordPtr(midpoint)[0:len(bytes_needle)]

    var cmp = bytes.Compare(bytes_needle, word_ptr)

    if cmp == 0 {
      return midpoint
    } else if cmp < 0 {
      high = midpoint - 1
    } else {
      low = midpoint + 1
    }
  }

  return -1
}

func (w *Wordlist) getWordPtr(index ItemIndex) []byte {
  entry_addr := (2 + index) * 8
  word_address_bytes := w.mmap[entry_addr:entry_addr+8]
  word_address := binary.LittleEndian.Uint64(word_address_bytes)
  print("idx ", index, " getting addr ", entry_addr, "\n")
  return w.mmap[word_address:]
}

func (w *Wordlist) getWord(index ItemIndex) string {
  ptr := w.getWordPtr(index)
  for i := 0; i < len(ptr); i ++ {
    if ptr[i] == '\x00' {
      return string(ptr[0:i])
    }
  }

  return ""
}
