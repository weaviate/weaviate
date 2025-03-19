package copier

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/cluster"
)

// Copier for shard replicas, can copy a shard replica from one node to another.
type Copier struct {
	// nodeSelector converts node IDs to hostnames
	nodeSelector cluster.NodeSelector
	// remoteIndex allows you to "call" methods on other nodes, in this case, we'll be "calling"
	// methods on the source node to perform the copy
	remoteIndex remoteIndex
	// rootDataPath is the local path to the root data directory for the shard, we'll copy files
	// to this path
	rootDataPath string
	// indexGetter is used to load the index for the collection so that we can create/interact
	// with the shard on this node
	indexGetter ShardLoaderAdapter
}

// remoteIndex interface here is just to avoid a circular dependency introduced by
// adapters/clients.RemoteIndex
type remoteIndex interface {
	// See adapters/clients.RemoteIndex.PauseAndListFiles
	PauseAndListFiles(ctx context.Context,
		hostName, indexName, shardName string) ([]string, error)
	// See adapters/clients.RemoteIndex.GetFile
	GetFile(ctx context.Context,
		hostName, indexName, shardName, fileName string) (io.ReadCloser, error)
}

// ShardLoaderAdapter is a type that can get an index as a ShardLoader, this is used to avoid a circular
// dependency between the copier and the db package.
type ShardLoaderAdapter interface {
	// See adapters/repos/db.DB.GetIndexAsShardLoader
	GetAsShardLoader(name schema.ClassName) ShardLoader
}

// ShardLoader is a type that can load a shard from disk files, this is used to avoid a circular
// dependency between the copier and the db package.
type ShardLoader interface {
	// See adapters/repos/db.Index.LoadShard
	LoadShard(ctx context.Context, name string) error
}

// New creates a new shard replica Copier.
func New(t remoteIndex, nodeSelector cluster.NodeSelector, rootPath string, indexGetter ShardLoaderAdapter) *Copier {
	return &Copier{
		remoteIndex:  t,
		nodeSelector: nodeSelector,
		rootDataPath: rootPath,
		indexGetter:  indexGetter,
	}
}

// CopyReplica copies a shard replica from the source node to this node.
func (c *Copier) CopyReplica(srcNodeId, collectionName, shardName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
	defer cancel()
	sourceNodeHostname, ok := c.nodeSelector.NodeHostname(srcNodeId)
	if !ok {
		return fmt.Errorf("sourceNodeName not found for node %s", srcNodeId)
	}
	relativeFilePaths, err := c.remoteIndex.PauseAndListFiles(ctx, sourceNodeHostname, collectionName, shardName)
	if err != nil {
		return err
	}
	for _, relativeFilePath := range relativeFilePaths {
		reader, err := c.remoteIndex.GetFile(ctx, sourceNodeHostname, collectionName, shardName, relativeFilePath)
		if err != nil {
			return err
		}
		defer reader.Close()

		finalPath := filepath.Join(c.rootDataPath, relativeFilePath)
		dir := path.Dir(finalPath)
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return fmt.Errorf("create parent folder for %s: %w", relativeFilePath, err)
		}

		f, err := os.Create(finalPath)
		if err != nil {
			return fmt.Errorf("open file %q for writing: %w", relativeFilePath, err)
		}

		defer f.Close()
		_, err = io.Copy(f, reader)
		if err != nil {
			return err
		}
	}

	// TODO need to understand more about hwo to create/add shard, do i need to coordinate with the
	// fsm changes to sharding state stuff too here? do i need the shard transfer locks/etc?
	err = c.indexGetter.GetAsShardLoader(schema.ClassName(collectionName)).LoadShard(ctx, shardName)
	if err != nil {
		return err
	}

	return nil
}
