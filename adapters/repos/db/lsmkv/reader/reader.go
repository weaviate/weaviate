//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package reader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type ContentReader interface {
	ReadAt(p []byte, off int64) (n int, err error)
	Size() (int64, error)
	Close() error
}

var _ ContentReader = (*LocalContentReader)(nil)

type S3ContentReader struct {
	path       string
	remoteFile *minio.Object
	size       int64
	// TODO: bucket config
}

func NewS3ContentReader(path string) (*S3ContentReader, error) {
	spath := strings.Split(path, "/")
	bucket := strings.ToLower(spath[len(spath)-4])

	key := strings.Join(spath[len(spath)-4:], "/")

	endpoint := os.Getenv("BACKUP_S3_ENDPOINT")
	useSSL := os.Getenv("BACKUP_S3_USE_SSL")

	region := os.Getenv("AWS_REGION")
	if len(region) == 0 {
		region = os.Getenv("AWS_DEFAULT_REGION")
	}

	var creds *credentials.Credentials
	if (os.Getenv("AWS_ACCESS_KEY_ID") != "" || os.Getenv("AWS_ACCESS_KEY") != "") &&
		(os.Getenv("AWS_SECRET_ACCESS_KEY") != "" || os.Getenv("AWS_SECRET_KEY") != "") {
		creds = credentials.NewEnvAWS()
	} else {
		creds = credentials.NewIAM("")
		// .Get() got deprecated with 7.0.83
		// and passing nil will use default context,
		if _, err := creds.GetWithContext(nil); err != nil {
			// can be anonymous access
			creds = credentials.NewEnvAWS()
		}
	}

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  creds,
		Region: region,
		Secure: useSSL == "true",
	})
	if err != nil {
		panic(err)
	}
	ok, err := client.BucketExists(context.Background(), bucket)
	if err != nil {
		panic(fmt.Errorf("check bucket %q: %w", bucket, err))
	}
	if !ok {
		panic(fmt.Errorf("bucket %q does not exist", bucket))
	}

	obj, err := client.GetObject(context.Background(), bucket, key, minio.GetObjectOptions{})
	if err != nil {
		panic(fmt.Errorf("get object %q: %w", key, err))
	}

	objStat, err := obj.Stat()
	if err != nil {
		panic(fmt.Errorf("stat object %q: %w", key, err))
	}
	if objStat.Size < 0 {
		panic(fmt.Errorf("object %q has negative size %d", key, objStat.Size))
	}
	if objStat.Size == 0 {
		panic(fmt.Errorf("object %q has size 0", key))
	}

	s3Reader := &S3ContentReader{
		path:       path,
		remoteFile: obj,
		size:       objStat.Size,
	}

	return s3Reader, nil
}

func (r *S3ContentReader) ReadAt(p []byte, off int64) (n int, err error) {
	read, err := r.remoteFile.ReadAt(p, int64(off))
	if err != nil && !errors.Is(err, io.EOF) {
		err = fmt.Errorf("get object %q: %w", r.path, err)
		return 0, err
	}

	return read, nil
}

func (r *S3ContentReader) Size() (int64, error) {
	return r.size, nil
}

func (r *S3ContentReader) Close() error {
	return r.remoteFile.Close()
}

type LocalContentReader struct {
	f    *os.File
	size int64
}

func NewLocalContentReader(path string) (*LocalContentReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat file: %w", err)
	}

	return &LocalContentReader{f: f, size: fi.Size()}, nil
}

func (r *LocalContentReader) ReadAt(p []byte, off int64) (n int, err error) {
	return r.f.ReadAt(p, off)
}

func (r *LocalContentReader) Size() (int64, error) {
	return r.size, nil
}

func (r *LocalContentReader) Close() error {
	return r.f.Close()
}
