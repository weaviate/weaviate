// Package secconf implements secconf encoding as specified in the following
// format:
//
//   base64(gpg(gzip(data)))
//
package secconf

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"io"
	"io/ioutil"

	"golang.org/x/crypto/openpgp"
)

// Deocde decodes data using the secconf codec.
func Decode(data []byte, secertKeyring io.Reader) ([]byte, error) {
	decoder := base64.NewDecoder(base64.StdEncoding, bytes.NewBuffer(data))
	entityList, err := openpgp.ReadArmoredKeyRing(secertKeyring)
	if err != nil {
		return nil, err
	}
	md, err := openpgp.ReadMessage(decoder, entityList, nil, nil)
	if err != nil {
		return nil, err
	}
	gzReader, err := gzip.NewReader(md.UnverifiedBody)
	if err != nil {
		return nil, err
	}
	defer gzReader.Close()
	bytes, err := ioutil.ReadAll(gzReader)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

// Encode encodes data to a base64 encoded using the secconf codec.
// data is encrypted with all public keys found in the supplied keyring.
func Encode(data []byte, keyring io.Reader) ([]byte, error) {
	entityList, err := openpgp.ReadArmoredKeyRing(keyring)
	if err != nil {
		return nil, err
	}
	buffer := new(bytes.Buffer)
	encoder := base64.NewEncoder(base64.StdEncoding, buffer)
	pgpWriter, err := openpgp.Encrypt(encoder, entityList, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	gzWriter := gzip.NewWriter(pgpWriter)
	if _, err := gzWriter.Write(data); err != nil {
		return nil, err
	}
	if err := gzWriter.Close(); err != nil {
		return nil, err
	}
	if err := pgpWriter.Close(); err != nil {
		return nil, err
	}
	if err := encoder.Close(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}
