package copier

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
)

// TODO
type Copier struct {
	n          nodeResolver
	t          targetNode
	rootPath   string
	shardAdder shardAdder
}

// using targetNode interface here to avoid circular dependency
type targetNode interface {
	PauseAndListFiles(ctx context.Context,
		hostName, indexName, shardName string) ([]string, error)
	GetFile(ctx context.Context,
		hostName, indexName, shardName, fileName string) (io.ReadCloser, error)
}

type nodeResolver interface {
	NodeHostname(nodeName string) (string, bool)
}

type shardAdder interface {
	AddShard(ctx context.Context, shardName string) error
}

// TODO
func New(t targetNode, nodeResolver nodeResolver, rootPath string, shardAdder shardAdder) *Copier {
	return &Copier{
		t:          t,
		n:          nodeResolver,
		rootPath:   rootPath,
		shardAdder: shardAdder,
	}
}

// TODO
func (c *Copier) Run(srcNodeId, destNodeId, collectionName, shardName string) error {
	// TODO context
	sourceNodeHostname, ok := c.n.NodeHostname(srcNodeId)
	if !ok {
		return fmt.Errorf("sourceNodeName not found for node %s", srcNodeId)
	}
	filePaths, err := c.t.PauseAndListFiles(context.Background(), sourceNodeHostname, collectionName, shardName)
	if err != nil {
		return err
	}
	fmt.Println("NATEE Copier.Run files", filePaths)
	// TODO parallel
	for _, filePath := range filePaths {
		reader, err := c.t.GetFile(context.Background(), sourceNodeHostname, collectionName, shardName, filePath)
		if err != nil {
			return err
		}
		defer reader.Close()
		fmt.Println("NATEE Copier.Run GetFile", filePath, err)
		// TODO write to destination

		finalPath := filepath.Join(c.rootPath, filePath)
		fmt.Println("NATEE Copier.Run finalPath", finalPath)
		dir := path.Dir(finalPath)
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return fmt.Errorf("create parent folder for %s: %w", filePath, err)
		}

		f, err := os.Create(finalPath)
		if err != nil {
			return fmt.Errorf("open file %q for writing: %w", filePath, err)
		}

		defer f.Close()
		n, err := io.Copy(f, reader)
		if err != nil {
			return err
		}
		fmt.Println("NATEE Copier.Run io.Copy", n)
	}

	// TODO need to understand more about hwo to create/add shard, do i need to coordinate with the
	// fsm changes to sharding state stuff too here? locks/etc?
	err = c.shardAdder.AddShard(context.Background(), shardName)
	fmt.Println("NATEE Copier.Run AddShard", err)
	if err != nil {
		return err
	}

	return nil
}
