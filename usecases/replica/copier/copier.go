package copier

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
)

// Copier for shard replicas, can copy a shard replica from one node to another.
type Copier struct {
	n          nodeResolver
	t          targetNode
	rootPath   string
	shardAdder shardAdder
}

// using targetNode interface here to avoid circular dependency
type targetNode interface {
	// See adapters/clients.RemoteIndex.PauseAndListFiles
	PauseAndListFiles(ctx context.Context,
		hostName, indexName, shardName string) ([]string, error)
	// See adapters/clients.RemoteIndex.GetFile
	GetFile(ctx context.Context,
		hostName, indexName, shardName, fileName string) (io.ReadCloser, error)
}

type nodeResolver interface {
	// See TODO
	NodeHostname(nodeName string) (string, bool)
}

type shardAdder interface {
	// See TODO
	AddShard(ctx context.Context, shardName string) error
}

// New creates a new shard replica Copier.
func New(t targetNode, nodeResolver nodeResolver, rootPath string, shardAdder shardAdder) *Copier {
	return &Copier{
		t:          t,
		n:          nodeResolver,
		rootPath:   rootPath,
		shardAdder: shardAdder,
	}
}

// Run copies a shard replica from the source node to this node.
func (c *Copier) Run(srcNodeId, collectionName, shardName string) error {
	// TODO context
	sourceNodeHostname, ok := c.n.NodeHostname(srcNodeId)
	if !ok {
		return fmt.Errorf("sourceNodeName not found for node %s", srcNodeId)
	}
	relativeFilePaths, err := c.t.PauseAndListFiles(context.Background(), sourceNodeHostname, collectionName, shardName)
	if err != nil {
		return err
	}
	fmt.Println("NATEE Copier.Run files", relativeFilePaths)
	// TODO parallel? worker pool?
	for _, relativeFilePath := range relativeFilePaths {
		reader, err := c.t.GetFile(context.Background(), sourceNodeHostname, collectionName, shardName, relativeFilePath)
		if err != nil {
			return err
		}
		defer reader.Close()
		fmt.Println("NATEE Copier.Run GetFile", relativeFilePath, err)

		finalPath := filepath.Join(c.rootPath, relativeFilePath)
		fmt.Println("NATEE Copier.Run finalPath", finalPath)
		dir := path.Dir(finalPath)
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return fmt.Errorf("create parent folder for %s: %w", relativeFilePath, err)
		}

		f, err := os.Create(finalPath)
		if err != nil {
			return fmt.Errorf("open file %q for writing: %w", relativeFilePath, err)
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
