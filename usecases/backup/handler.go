//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package backup

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/config"
)

// classifyCanCommitErr maps a free-form canCommit error to a
// [CanCommitErrorKind]. nil err returns the empty kind so callers can keep
// using empty-string semantics when nothing went wrong.
//
// Classification reads the error chain, never the words: Backupable() wraps the
// [backup.ErrBackupBlockedByInFlightReindex] sentinel inside an errors.Join.
func classifyCanCommitErr(err error) CanCommitErrorKind {
	if err == nil {
		return ""
	}
	if !allReindexRefusals(err) {
		return CanCommitErrCannotCommit
	}
	// Backupable joins one refusal per class and an outage can answer some differently.
	// An observed migration outranks: it is the half with something to wait for.
	if errors.Is(err, backup.ErrBackupBlockedByInFlightReindex) {
		return CanCommitErrInFlightReindex
	}
	return CanCommitErrCreateReindexUndetermined
}

// classifyRestoreGateErr keeps the gate's two answers apart across the RPC boundary: a migration it observed, and a check it could not complete.
func classifyRestoreGateErr(err error) CanCommitErrorKind {
	if errors.Is(err, backup.ErrReindexActivityUndetermined) {
		return CanCommitErrRestoreReindexUndetermined
	}
	return CanCommitErrRestoreBlockedByReindex
}

// Version of backup structure
const (
	// "2.1" support restore on 2 phases
	Version = "2.1"
	// "2.0" support compression
	// Version = "2.0"
	// version1 is the newest structure version that is no longer restorable
	version1 = "1.0"
)

var (
	errLegacySingleNode = legacyRestoreErr("by Weaviate older than v1.17, which stored a single " +
		"top-level backup.json instead of per-node metadata")
	errLegacyUncompressed = legacyRestoreErr("by Weaviate older than v1.21, which stored files uncompressed")
	errLegacyFlatFS       = legacyRestoreErr("by Weaviate older than v1.23, which stored shard files " +
		"in a flat directory instead of one directory per shard")
)

// legacyRestoreErr builds the refusal for a backup format this build no longer restores.
func legacyRestoreErr(origin string) error {
	return fmt.Errorf("backup was created %s, and can no longer be restored: restore it on a "+
		"release that still supports it and create a new backup", origin)
}

// maxMajorVersion is the newest backup-structure major version this build restores.
var maxMajorVersion, _ = parseMajor(Version)

// checkRestorableVersion refuses backups this build cannot restore, either because their
// format is too old or because a later Weaviate produced them. version is the
// backup-structure version; serverVersion is the Weaviate version that wrote it.
func checkRestorableVersion(version, serverVersion string) error {
	// An empty version means a corrupt descriptor rather than an old one; Validate reports it.
	if version != "" && version <= version1 {
		return errLegacyUncompressed
	}
	if serverVersionOlderThan(serverVersion, 1, 23) {
		return errLegacyFlatFS
	}
	// A structure version may omit the minor, so compare majors only.
	if major, ok := parseMajor(version); ok && major > maxMajorVersion {
		return fmt.Errorf("%s: %s > %s", errMsgHigherVersion, version, Version)
	}
	return nil
}

// parseMajor reads the leading number of a "major[.minor[.patch]]" version. ok is false
// when it is missing or unparseable.
func parseMajor(version string) (major int, ok bool) {
	major, err := strconv.Atoi(strings.Split(version, ".")[0])
	return major, err == nil
}

// parseVersion splits a "major.minor[.patch]" version. ok is false when either number is
// missing or unparseable.
func parseVersion(version string) (major, minor int, ok bool) {
	parts := strings.Split(version, ".")
	if len(parts) < 2 {
		return 0, 0, false
	}
	major, ok = parseMajor(version)
	if !ok {
		return 0, 0, false
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, false
	}
	return major, minor, true
}

// serverVersionOlderThan reports whether serverVersion, formatted "major.minor[.patch]",
// is older than major.minor. An unparseable version is not treated as older.
func serverVersionOlderThan(serverVersion string, major, minor int) bool {
	gotMajor, gotMinor, ok := parseVersion(serverVersion)
	if !ok {
		return false
	}
	if gotMajor != major {
		return gotMajor < major
	}
	return gotMinor < minor
}

// TODO error handling need to be implemented properly.
// Current error handling is not idiomatic and relays on string comparisons which makes testing very brittle.

var regExpID = regexp.MustCompile("^[a-z0-9_-]+$")

type BackupBackendProvider interface {
	BackupBackend(backend string, useCase modulecapabilities.BackendUseCase) (modulecapabilities.BackupBackend, error)
	EnabledBackupBackends() []modulecapabilities.BackupBackend
}

type schemaManger interface {
	RestoreClass(ctx context.Context, d *backup.ClassDescriptor, nodeMapping map[string]string, overwriteAlias bool, stripNamespaces bool) error
	NodeName() string
	NamespacesEnabled() bool
	ClassEqual(name string) string
}

type NodeResolver interface {
	NodeHostname(nodeName string) (string, bool)
	AllNames() []string
	NodeCount() int

	// LeaderID is used to return the current leader ID
	// It may return empty strings if there is no current leader or the leader is unknown.
	LeaderID() string
}

// dynUserSnapshotter is the backup-side contract for the dynamic-user FSM. The variadic
// Snapshot filters to a user subset for backups; zero args is the full snapshot.
type dynUserSnapshotter interface {
	Snapshot(userIDs ...string) ([]byte, error)
	Restore(snapshot []byte, stripNamespaces bool) error
}

// RBACSnapshotter is the backup-side contract for the RBAC FSM. The variadic
// Snapshot filters to a role subset for backups; zero args is the full snapshot.
// It is exported so the wiring can hold one as a genuinely nil interface when
// RBAC is off, rather than boxing a nil *rbac.Manager into a non-nil interface.
type RBACSnapshotter interface {
	Snapshot(roles ...string) ([]byte, error)
	Restore(snapshot []byte, stripNamespaces bool) error
}

type Status struct {
	Path         string
	StartedAt    time.Time
	CompletedAt  time.Time
	Status       backup.Status
	Err          string
	Size         float64
	BaseBackupID string
}

type Handler struct {
	node string
	// deps
	logger     logrus.FieldLogger
	authorizer authorization.Authorizer
	backupper  *backupper
	restorer   *restorer
	backends   BackupBackendProvider
}

func NewHandler(
	logger logrus.FieldLogger,
	cfg config.Backup,
	authorizer authorization.Authorizer,
	schema schemaManger,
	sourcer Sourcer,
	backends BackupBackendProvider,
	rbacSourcer RBACSnapshotter,
	dynUserSourcer dynUserSnapshotter,
) *Handler {
	node := schema.NodeName()
	m := &Handler{
		node:       node,
		logger:     logger,
		authorizer: authorizer,
		backends:   backends,
		backupper: newBackupper(node, logger, cfg,
			sourcer, rbacSourcer, dynUserSourcer,
			backends),
		restorer: newRestorer(node, logger,
			sourcer, rbacSourcer, dynUserSourcer,
			backends, schema.NamespacesEnabled(),
		),
	}
	return m
}

// Compression is the compression configuration.
type Compression struct {
	// Level is one of GzipDefaultCompression, GzipBestSpeed, GzipBestCompression
	Level CompressionLevel

	// CPUPercentage desired CPU core utilization (1%-80%), default: 50%
	CPUPercentage int
}

// BackupRequest a transition request from API to Backend.
type BackupRequest struct {
	// Compression is the compression configuration.
	Compression

	// ID is the backup ID
	ID string
	// Backend specify on which backend to store backups (gcs, s3, ..)
	Backend string

	// Include is list of class which need to be backed up
	// The same class cannot appear in both Include and Exclude in the same request
	Include []string
	// Exclude means include all classes but those specified in Exclude
	// The same class cannot appear in both Include and Exclude in the same request
	Exclude []string

	// Non-empty switches the backup to a filtered dynamic-user snapshot.
	// Empty keeps the whole-cluster snapshot. Same '*'/'?' wildcards as Include.
	IncludeUsers []string

	// Non-empty filters the RBAC snapshot to the matching roles. Empty keeps the
	// whole-cluster snapshot. Same '*'/'?' wildcards as Include; built-ins rejected.
	IncludeRoles []string

	// NodeMapping is a map of node name replacement where key is the old name and value is the new name
	// No effect if the map is empty
	NodeMapping map[string]string

	// Override bucket (optional) - replaces environement variable for one call
	Bucket string

	// Override path (optional) - replaces environement variable for one call
	Path string

	RbacRestoreOption string
	UserRestoreOption string

	BaseBackupID string
}

// OnCanCommit will be triggered when coordinator asks the node to participate
// in a distributed backup operation
func (m *Handler) OnCanCommit(ctx context.Context, req *Request) *CanCommitResponse {
	ret := &CanCommitResponse{Method: req.Method, ID: req.ID}

	nodeName := m.node
	// If we are doing a restore and have a nodeMapping specified, ensure we use the "old" node name from the backup to retrieve/store the
	// backup information.
	if req.Method == OpRestore {
		for oldNodeName, newNodeName := range req.NodeMapping {
			if nodeName == newNodeName {
				nodeName = oldNodeName
				break
			}
		}
	}
	store, err := nodeBackend(nodeName, m.backends, req.Backend, req.ID, req.Bucket, req.Path)
	if err != nil {
		ret.Err = fmt.Sprintf("no backup backend %q, did you enable the right module?", req.Backend)
		ret.ErrKind = CanCommitErrCannotCommit
		return ret
	}

	switch req.Method {
	case OpCreate:
		if err := m.backupper.sourcer.Backupable(ctx, req.Classes); err != nil {
			ret.Err = err.Error()
			ret.ErrKind = classifyCanCommitErr(err)
			return ret
		}
		if err = store.Initialize(ctx, req.Bucket, req.Path); err != nil {
			ret.Err = fmt.Sprintf("init uploader: %v", err)
			ret.ErrKind = CanCommitErrCannotCommit
			return ret
		}
		res, err := m.backupper.backup(store, req)
		if err != nil {
			ret.Err = err.Error()
			ret.ErrKind = classifyCanCommitErr(err)
			return ret
		}
		ret.Timeout = res.Timeout
	case OpRestore:
		// Ahead of validate(), which fetches the descriptor from the backend. An empty
		// class list means no collection only when the coordinator marked the scope
		// exact; without that mark it is the legacy "this node's whole descriptor",
		// which validate() reads the same way, so the gate still covers everything.
		if narrowedToNothing := req.ClassScopeExact && len(req.Classes) == 0; !narrowedToNothing {
			if err := m.restorer.sourcer.RefuseIfAnyReindexInFlight(ctx, req.Classes); err != nil {
				ret.Err = err.Error()
				ret.ErrKind = classifyRestoreGateErr(err)
				return ret
			}
		}
		meta, _, err := m.restorer.validate(ctx, &store, req)
		if err != nil {
			ret.Err = err.Error()
			ret.ErrKind = CanCommitErrCannotCommit
			return ret
		}
		res, err := m.restorer.restore(req, meta, store)
		if err != nil {
			ret.Err = err.Error()
			ret.ErrKind = CanCommitErrCannotCommit
			return ret
		}
		ret.Timeout = res.Timeout
	default:
		ret.Err = fmt.Sprintf("unknown backup operation: %s", req.Method)
		ret.ErrKind = CanCommitErrCannotCommit
		return ret
	}

	return ret
}

// OnCommit will be triggered when the coordinator confirms the execution of a previous operation
func (m *Handler) OnCommit(ctx context.Context, req *StatusRequest) (err error) {
	switch req.Method {
	case OpCreate:
		return m.backupper.OnCommit(ctx, req)
	case OpRestore:
		return m.restorer.OnCommit(ctx, req)
	default:
		return fmt.Errorf("%w: %s", errUnknownOp, req.Method)
	}
}

// OnAbort will be triggered when the coordinator abort the execution of a previous operation
func (m *Handler) OnAbort(ctx context.Context, req *AbortRequest) error {
	switch req.Method {
	case OpCreate:
		return m.backupper.OnAbort(ctx, req)
	case OpRestore:
		return m.restorer.OnAbort(ctx, req)
	default:
		return fmt.Errorf("%w: %s", errUnknownOp, req.Method)

	}
}

func (m *Handler) OnStatus(ctx context.Context, req *StatusRequest) *StatusResponse {
	ret := StatusResponse{
		Method: req.Method,
		ID:     req.ID,
	}
	switch req.Method {
	case OpCreate:
		st, err := m.backupper.OnStatus(ctx, req)
		ret.Status = st.Status
		ret.Err = st.Err
		if err != nil {
			ret.Status = backup.Failed
			ret.Err = err.Error()
		}
	case OpRestore:
		st, err := m.restorer.status(req.Backend, req.ID)
		ret.Status = st.Status
		ret.Err = st.Err
		if err != nil {
			ret.Status = backup.Failed
			ret.Err = err.Error()
		}
	default:
		ret.Status = backup.Failed
		ret.Err = fmt.Sprintf("%v: %s", errUnknownOp, req.Method)
	}

	return &ret
}

func validateID(backupID string) error {
	if !regExpID.MatchString(backupID) {
		return fmt.Errorf("invalid backup id: '%v' allowed characters are lowercase, 0-9, _, -", backupID)
	}
	return nil
}

func nodeBackend(node string, provider BackupBackendProvider, backend, id, bucket, path string) (nodeStore, error) {
	caps, err := provider.BackupBackend(backend, modulecapabilities.BackendUseCaseBackup)
	if err != nil {
		return nodeStore{}, err
	}
	ns := nodeStore{objectStore{backend: caps, backupId: fmt.Sprintf("%s/%s", id, node), bucket: bucket, path: path, node: node}}
	return ns, nil
}

// basePath of the backup
func basePath(backendType, backupID string) string {
	return fmt.Sprintf("%s/%s", backendType, backupID)
}

func filterClasses(classes, excludes []string) []string {
	if len(excludes) == 0 {
		return classes
	}
	m := make(map[string]struct{}, len(classes))
	for _, c := range classes {
		m[c] = struct{}{}
	}
	for _, x := range excludes {
		delete(m, x)
	}
	if len(classes) != len(m) {
		classes = classes[:len(m)]
		i := 0
		for k := range m {
			classes[i] = k
			i++
		}
	}

	return classes
}
