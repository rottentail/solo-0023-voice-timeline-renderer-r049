package checkflow

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// CommitFault injects a failure at the real production publication boundary of
// the storage layer. It never modifies production code: it sabotages the
// on-disk destination that storage.SaveSource / storage.CommitRenderArtifacts
// rename their staged, fully written directory into.
//
// Two modes are used so the same boundary is exercised under any runtime
// identity:
//
//   - permission: the destination index directory becomes non-writable, so the
//     final atomic rename fails with EACCES. This is the preferred mode when
//     the check does not run as the superuser.
//   - blocker: the index directory is moved aside and replaced by a regular
//     file of the same name, so every destination operation fails with ENOTDIR.
//     It works even when permission bits are bypassed (root), and all content
//     is restored byte-for-byte by restore.
//
// The staging area (tmp/) and the job metadata path stay writable, so the
// failure lands specifically at commit time, after decode, render, and staging
// have already succeeded.
type CommitFault struct {
	dataDir    string
	indexName  string
	backupName string
	blocker    string
	mode       string
	original   os.FileMode
	armed      bool
}

var faultIndexNames = map[string]bool{
	"sources": true,
	"renders": true,
}

// armCommitFault arranges the named index directory to reject the final
// publication rename for the given run data directory.
func armCommitFault(dataDir, indexName string) (*CommitFault, error) {
	if !faultIndexNames[indexName] {
		return nil, fmt.Errorf("commit fault target %q is not a recognized publication index", indexName)
	}
	target := filepath.Join(dataDir, indexName)
	if _, err := os.Stat(target); err != nil {
		return nil, fmt.Errorf("inspect commit fault target %s: %w", target, err)
	}
	fault := &CommitFault{dataDir: dataDir, indexName: indexName}

	probe, err := os.MkdirTemp(target, ".probe-*")
	if err == nil {
		_ = os.Remove(probe)
	}
	if err == nil {
		// Probe proved a chmod-denied rename is observable for this identity.
		return fault, fault.armPermission(target)
	}
	if !errors.Is(err, os.ErrPermission) {
		return nil, fmt.Errorf("unexpected probe error arming commit fault: %w", err)
	}

	// Identity bypasses permission bits (typical for root): replace the
	// directory with a regular file while preserving every stored entry.
	return fault, fault.armBlocker(target)
}

func (fault *CommitFault) armPermission(target string) error {
	info, err := os.Stat(target)
	if err != nil {
		return fmt.Errorf("stat commit fault target: %w", err)
	}
	fault.original = info.Mode().Perm()
	if err := os.Chmod(target, 0o500); err != nil {
		return fmt.Errorf("arm permission commit fault: %w", err)
	}
	fault.mode = "permission"
	fault.armed = true
	return nil
}

func (fault *CommitFault) armBlocker(target string) error {
	backup := filepath.Join(fault.dataDir, "."+fault.indexName+"-fault-backup")
	if err := os.Rename(target, backup); err != nil {
		return fmt.Errorf("move commit fault target aside: %w", err)
	}
	handle, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		_ = os.Rename(backup, target)
		return fmt.Errorf("create commit fault blocker: %w", err)
	}
	_ = handle.Close()
	fault.mode = "blocker"
	fault.backupName = backup
	fault.blocker = target
	fault.armed = true
	return nil
}

// Restore returns the repository to a fully writable state so a retained run
// can be inspected and a later successful run never inherits the sabotage.
func (fault *CommitFault) Restore() error {
	if fault == nil || !fault.armed {
		return nil
	}
	fault.armed = false
	if fault.mode == "permission" {
		target := filepath.Join(fault.dataDir, fault.indexName)
		if err := os.Chmod(target, fault.original); err != nil {
			return fmt.Errorf("restore %s permissions: %w", fault.indexName, err)
		}
		return nil
	}
	if err := os.Remove(fault.blocker); err != nil {
		return fmt.Errorf("remove %s blocker: %w", fault.indexName, err)
	}
	if err := os.Rename(fault.backupName, filepath.Join(fault.dataDir, fault.indexName)); err != nil {
		return fmt.Errorf("restore %s directory: %w", fault.indexName, err)
	}
	return nil
}

// Mode reports the active sabotage mechanism for the run report.
func (fault *CommitFault) Mode() string {
	if fault == nil {
		return ""
	}
	return fault.mode
}
