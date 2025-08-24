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

package modstgazure

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/entities/backup"
	ubak "github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
)

const (
	defaultBlockSize   = int64(40 * 1024 * 1024)
	defaultConcurrency = 1
)

type azureClient struct {
	client     *azblob.Client
	config     clientConfig
	serviceURL string
	dataPath   string
}

func newClient(ctx context.Context, config *clientConfig, dataPath string) (*azureClient, error) {
	connectionString := os.Getenv("AZURE_STORAGE_CONNECTION_STRING")
	if connectionString != "" {
		client, err := azblob.NewClientFromConnectionString(connectionString, nil)
		if err != nil {
			return nil, errors.Wrap(err, "create client using connection string")
		}
		serviceURL := ""
		connectionStrings := strings.Split(connectionString, ";")
		for _, str := range connectionStrings {
			if strings.HasPrefix(str, "BlobEndpoint") {
				blobEndpoint := strings.Split(str, "=")
				if len(blobEndpoint) > 1 {
					serviceURL = blobEndpoint[1]
					if !strings.HasSuffix(serviceURL, "/") {
						serviceURL = serviceURL + "/"
					}
				}
			}
		}
		return &azureClient{client, *config, serviceURL, dataPath}, nil
	}

	// Your account name and key can be obtained from the Azure Portal.
	accountName := os.Getenv("AZURE_STORAGE_ACCOUNT")
	accountKey := os.Getenv("AZURE_STORAGE_KEY")

	if accountName == "" {
		return nil, errors.New("AZURE_STORAGE_ACCOUNT must be set")
	}

	// The service URL for blob endpoints is usually in the form: http(s)://<account>.blob.core.windows.net/
	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)

	if accountKey != "" {
		cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
		if err != nil {
			return nil, err
		}

		client, err := azblob.NewClientWithSharedKeyCredential(serviceURL, cred, nil)
		if err != nil {
			return nil, err
		}
		return &azureClient{client, *config, serviceURL, dataPath}, nil
	}

	options := &azblob.ClientOptions{
		ClientOptions: policy.ClientOptions{
			Retry: policy.RetryOptions{
				MaxRetries:    3,
				RetryDelay:    4 * time.Second,
				MaxRetryDelay: 120 * time.Second,
			},
		},
	}

	client, err := azblob.NewClientWithNoCredential(serviceURL, options)
	if err != nil {
		return nil, err
	}
	return &azureClient{client, *config, serviceURL, dataPath}, nil
}

func (a *azureClient) HomeDir(backupID, overrideBucket, overridePath string) string {
	if overrideBucket == "" {
		overrideBucket = a.config.Container
	}

	return a.serviceURL + path.Join(overrideBucket, a.makeObjectName(overridePath, []string{backupID}))
}

func (g *azureClient) makeObjectName(overridePath string, parts []string) string {
	if overridePath != "" {
		base := path.Join(parts...)
		return path.Join(overridePath, base)
	} else {
		base := path.Join(parts...)
		return path.Join(g.config.BackupPath, base)
	}
}

func (a *azureClient) AllBackups(ctx context.Context) ([]*backup.DistributedBackupDescriptor, error) {
	var meta []*backup.DistributedBackupDescriptor

	blobs := a.client.NewListBlobsFlatPager(a.config.Container, &azblob.ListBlobsFlatOptions{Prefix: to.Ptr(a.config.BackupPath)})
	for {
		if !blobs.More() {
			break
		}
		blob, err := blobs.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("get next blob: %w", err)
		}

		if blob.ListBlobsFlatSegmentResponse.Segment != nil {
			for _, item := range blob.ListBlobsFlatSegmentResponse.Segment.BlobItems {
				if item.Name == nil || !strings.Contains(*item.Name, ubak.GlobalBackupFile) {
					continue
				}

				// now we have ubak.GlobalBackupFile
				contents, err := a.getObject(ctx, a.config.Container, *item.Name)
				if err != nil {
					return nil, fmt.Errorf("get blob item %q: %w", *item.Name, err)
				}
				var desc backup.DistributedBackupDescriptor
				if err := json.Unmarshal(contents, &desc); err != nil {
					return nil, fmt.Errorf("unmarshal blob item %q: %w", *item.Name, err)
				}
				meta = append(meta, &desc)
			}
		}
	}

	return meta, nil
}

func (a *azureClient) GetObject(ctx context.Context, backupID, key, overrideBucket, overridePath string) ([]byte, error) {
	objectName := a.makeObjectName(overridePath, []string{backupID, key})

	containerName := a.config.Container
	if overrideBucket != "" {
		containerName = overrideBucket
	}

	return a.getObject(ctx, containerName, objectName)
}

func (a *azureClient) getObject(ctx context.Context, containerName, objectName string) ([]byte, error) {
	blobDownloadResponse, err := a.client.DownloadStream(ctx, containerName, objectName, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return nil, backup.NewErrNotFound(errors.Wrapf(err, "get object %s", objectName))
		}
		return nil, backup.NewErrInternal(errors.Wrapf(err, "download stream for object %s", objectName))
	}

	reader := blobDownloadResponse.Body
	downloadData, err := io.ReadAll(reader)
	errClose := reader.Close()
	if errClose != nil {
		return nil, backup.NewErrInternal(errors.Wrapf(errClose, "close stream for object %s", objectName))
	}
	if err != nil {
		return nil, backup.NewErrInternal(errors.Wrapf(err, "read stream for object %s", objectName))
	}

	return downloadData, nil
}

func (a *azureClient) PutObject(ctx context.Context, backupID, key, overrideBucket, overridePath string, data []byte) error {
	objectName := a.makeObjectName(overridePath, []string{backupID, key})

	containerName := a.config.Container
	if overrideBucket != "" {
		containerName = overrideBucket
	}

	reader := bytes.NewReader(data)
	_, err := a.client.UploadStream(ctx,
		containerName,
		objectName,
		reader,
		&azblob.UploadStreamOptions{
			Metadata:    map[string]*string{"backupid": to.Ptr(backupID)},
			Tags:        map[string]string{"backupid": backupID},
			BlockSize:   a.getBlockSize(ctx),
			Concurrency: a.getConcurrency(ctx),
		})
	if err != nil {
		return backup.NewErrInternal(errors.Wrapf(err, "upload stream for object %s", objectName))
	}

	return nil
}

func (a *azureClient) Initialize(ctx context.Context, backupID, overrideBucket, overridePath string) error {
	key := "access-check"

	if err := a.PutObject(ctx, backupID, key, overrideBucket, overridePath, []byte("")); err != nil {
		return errors.Wrap(err, "failed to access-check Azure backup module")
	}

	containerName := a.config.Container
	if overrideBucket != "" {
		containerName = overrideBucket
	}

	objectName := a.makeObjectName(overridePath, []string{backupID, key})
	if _, err := a.client.DeleteBlob(ctx, containerName, objectName, nil); err != nil {
		return errors.Wrap(err, "failed to remove access-check Azure backup module at"+objectName)
	}

	return nil
}

func (a *azureClient) WriteToFile(ctx context.Context, backupID, key, destPath, overrideBucket, overridePath string) error {
	dir := path.Dir(destPath)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return errors.Wrapf(err, "make dir %s", dir)
	}

	file, err := os.Create(destPath)
	if err != nil {
		return backup.NewErrInternal(errors.Wrapf(err, "create file: %q", destPath))
	}
	defer file.Close()

	containerName := a.config.Container
	if overrideBucket != "" {
		containerName = overrideBucket
	}

	objectName := a.makeObjectName(overridePath, []string{backupID, key})
	_, err = a.client.DownloadFile(ctx, containerName, objectName, file, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return backup.NewErrNotFound(errors.Wrapf(err, "get object %s", objectName))
		}
		return backup.NewErrInternal(errors.Wrapf(err, "download file for object %s", objectName))
	}

	return nil
}

func (a *azureClient) getBlockSize(ctx context.Context) int64 {
	blockSize := defaultBlockSize
	blockSizeStr := modulecomponents.GetValueFromContext(ctx, "X-Azure-Block-Size")

	if blockSizeStr == "" {
		blockSizeStr = os.Getenv("AZURE_BLOCK_SIZE")
	}

	if blockSizeStr != "" {
		bs, err := strconv.ParseInt(blockSizeStr, 10, 64)
		if err != nil {
			return defaultBlockSize
		}
		blockSize = bs
	}
	return blockSize
}

func (a *azureClient) getConcurrency(ctx context.Context) int {
	concurrency := defaultConcurrency
	concurrencyStr := modulecomponents.GetValueFromContext(ctx, "X-Azure-Concurrency")

	if concurrencyStr == "" {
		concurrencyStr = os.Getenv("AZURE_CONCURRENCY")
	}

	if concurrencyStr != "" {
		cc, err := strconv.Atoi(concurrencyStr)
		if err != nil {
			return defaultConcurrency
		}
		concurrency = cc
	}
	return concurrency
}

func (a *azureClient) Write(ctx context.Context, backupID, key, overrideBucket, overridePath string, r io.ReadCloser) (written int64, err error) {
	path := a.makeObjectName(overridePath, []string{backupID, key})
	reader := &reader{src: r}
	defer func() {
		r.Close()
		written = int64(reader.count)
	}()

	containerName := a.config.Container
	if overrideBucket != "" {
		containerName = overrideBucket
	}

	if _, err = a.client.UploadStream(ctx,
		containerName,
		path,
		reader,
		&azblob.UploadStreamOptions{
			Metadata:    map[string]*string{"backupid": to.Ptr(backupID)},
			Tags:        map[string]string{"backupid": backupID},
			BlockSize:   a.getBlockSize(ctx),
			Concurrency: a.getConcurrency(ctx),
		}); err != nil {
		err = fmt.Errorf("upload stream %q: %w", path, err)
	}

	return
}

func (a *azureClient) Read(ctx context.Context, backupID, key, overrideBucket, overridePath string, w io.WriteCloser) (int64, error) {
	defer w.Close()

	containerName := a.config.Container
	if overrideBucket != "" {
		containerName = overrideBucket
	}

	path := a.makeObjectName(overridePath, []string{backupID, key})
	resp, err := a.client.DownloadStream(ctx, containerName, path, nil)
	if err != nil {
		err = fmt.Errorf("find object %q: %w", path, err)
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			err = backup.NewErrNotFound(err)
		}
		return 0, err
	}
	defer resp.Body.Close()

	read, err := io.Copy(w, resp.Body)
	if err != nil {
		return read, fmt.Errorf("io.copy %q: %w", path, err)
	}

	return read, nil
}

func (a *azureClient) SourceDataPath() string {
	return a.dataPath
}

// reader is a wrapper used to count number of written bytes
// Unlike GCS and S3 Azure Interface does not provide this information
type reader struct {
	src   io.Reader
	count int
}

func (r *reader) Read(p []byte) (n int, err error) {
	n, err = r.src.Read(p)
	r.count += n
	return
}
