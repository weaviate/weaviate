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
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/backup"
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

func (a *azureClient) HomeDir(backupID string) string {
	return a.serviceURL + path.Join(a.config.Container, a.makeObjectName(backupID))
}

func (a *azureClient) makeObjectName(parts ...string) string {
	base := path.Join(parts...)
	return path.Join(a.config.BackupPath, base)
}

func (a *azureClient) GetObject(ctx context.Context, backupID, key string) ([]byte, error) {
	objectName := a.makeObjectName(backupID, key)

	blobDownloadResponse, err := a.client.DownloadStream(ctx, a.config.Container, objectName, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return nil, backup.NewErrNotFound(errors.Wrapf(err, "get object '%s'", objectName))
		}
		return nil, backup.NewErrInternal(errors.Wrapf(err, "download stream for object '%s'", objectName))
	}

	reader := blobDownloadResponse.Body
	downloadData, err := io.ReadAll(reader)
	errClose := reader.Close()
	if errClose != nil {
		return nil, backup.NewErrInternal(errors.Wrapf(errClose, "close stream for object '%s'", objectName))
	}
	if err != nil {
		return nil, backup.NewErrInternal(errors.Wrapf(err, "read stream for object '%s'", objectName))
	}

	return downloadData, nil
}

func (a *azureClient) PutFile(ctx context.Context, backupID, key, srcPath string) error {
	filePath := path.Join(a.dataPath, srcPath)
	file, err := os.Open(filePath)
	if err != nil {
		return backup.NewErrInternal(errors.Wrapf(err, "open file: %q", filePath))
	}
	defer file.Close()

	objectName := a.makeObjectName(backupID, key)
	_, err = a.client.UploadFile(ctx,
		a.config.Container,
		objectName,
		file,
		&azblob.UploadFileOptions{
			Metadata: map[string]*string{"backupid": to.Ptr(backupID)},
			Tags:     map[string]string{"backupid": backupID},
		})
	if err != nil {
		return backup.NewErrInternal(errors.Wrapf(err, "upload file for object '%s'", objectName))
	}

	return nil
}

func (a *azureClient) PutObject(ctx context.Context, backupID, key string, data []byte) error {
	objectName := a.makeObjectName(backupID, key)

	reader := bytes.NewReader(data)
	_, err := a.client.UploadStream(ctx,
		a.config.Container,
		objectName,
		reader,
		&azblob.UploadStreamOptions{
			Metadata: map[string]*string{"backupid": to.Ptr(backupID)},
			Tags:     map[string]string{"backupid": backupID},
		})
	if err != nil {
		return backup.NewErrInternal(errors.Wrapf(err, "upload stream for object '%s'", objectName))
	}

	return nil
}

func (a *azureClient) Initialize(ctx context.Context, backupID string) error {
	key := "access-check"

	if err := a.PutObject(ctx, backupID, key, []byte("")); err != nil {
		return errors.Wrap(err, "failed to access-check Azure backup module")
	}

	objectName := a.makeObjectName(backupID, key)
	if _, err := a.client.DeleteBlob(ctx, a.config.Container, objectName, nil); err != nil {
		return errors.Wrap(err, "failed to remove access-check Azure backup module")
	}

	return nil
}

func (a *azureClient) WriteToFile(ctx context.Context, backupID, key, destPath string) error {
	dir := path.Dir(destPath)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return errors.Wrapf(err, "make dir '%s'", dir)
	}

	file, err := os.Create(destPath)
	if err != nil {
		return backup.NewErrInternal(errors.Wrapf(err, "create file: %q", destPath))
	}
	defer file.Close()

	objectName := a.makeObjectName(backupID, key)
	_, err = a.client.DownloadFile(ctx, a.config.Container, objectName, file, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return backup.NewErrNotFound(errors.Wrapf(err, "get object '%s'", objectName))
		}
		return backup.NewErrInternal(errors.Wrapf(err, "download file for object '%s'", objectName))
	}

	return nil
}

func (a *azureClient) Write(ctx context.Context, backupID, key string, r io.ReadCloser) (written int64, err error) {
	path := a.makeObjectName(backupID, key)
	reader := &reader{src: r}
	defer func() {
		r.Close()
		written = int64(reader.count)
	}()

	if _, err = a.client.UploadStream(ctx,
		a.config.Container,
		path,
		reader,
		&azblob.UploadStreamOptions{
			Metadata: map[string]*string{"backupid": to.Ptr(backupID)},
			Tags:     map[string]string{"backupid": backupID},
		}); err != nil {
		err = fmt.Errorf("upload stream %q: %w", path, err)
	}

	return
}

func (a *azureClient) Read(ctx context.Context, backupID, key string, w io.WriteCloser) (int64, error) {
	defer w.Close()

	path := a.makeObjectName(backupID, key)
	resp, err := a.client.DownloadStream(ctx, a.config.Container, path, nil)
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
