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

package db

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
)

// legacyRecord is the part of a v1.39.3-17 migration record file that this frozen step reads.
type legacyRecord struct {
	file    string
	State   string
	Subject struct {
		Key                struct{ TaskVersion uint64 }
		MigrationType      ReindexMigrationType
		TargetTokenization string
		Props              map[string]struct{ Staged, Canonical string }
	}
}

type legacyRecordProp struct {
	rec          *legacyRecord
	prop, staged string
}

var legacyRecordsDir = filepath.Join(migrationsDir, "records")

// convertLegacyRecords applies the v1.39.3-17 records before the view loads, the newest per folder.
// Each step is safe to repeat, and a failure fails the load before any record is deleted.
func (s *Shard) convertLegacyRecords(class *models.Class) error {
	recs, err := s.readLegacyRecords()
	if err != nil || len(recs) == 0 {
		return err
	}
	byFolder := map[string][]legacyRecordProp{}
	for _, rec := range recs {
		for prop, d := range rec.Subject.Props {
			byFolder[d.Canonical] = append(byFolder[d.Canonical], legacyRecordProp{rec, prop, d.Staged})
		}
	}
	for canonical, props := range byFolder {
		if err := s.convertLegacyFolder(class, canonical, props); err != nil {
			return fmt.Errorf("convert legacy migration of folder %q: %w", canonical, err)
		}
	}
	for _, rec := range recs {
		if err := os.Remove(filepath.Join(s.pathLSM(), legacyRecordsDir, rec.file)); err != nil {
			return fmt.Errorf("remove legacy migration record %q: %w", rec.file, err)
		}
	}
	return nil
}

// Every record decodes before any acts, so an unreadable newest one never lets an older one act.
func (s *Shard) readLegacyRecords() ([]*legacyRecord, error) {
	dir := filepath.Join(s.pathLSM(), legacyRecordsDir)
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list legacy migration records: %w", err)
	}
	var recs []*legacyRecord
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, fmt.Errorf("read legacy migration record %q: %w", e.Name(), err)
		}
		rec := &legacyRecord{file: e.Name()}
		if err := json.Unmarshal(raw, rec); err != nil {
			return nil, fmt.Errorf("decode legacy migration record %q: %w", e.Name(), err)
		}
		recs = append(recs, rec)
	}
	return recs, nil
}

// convertLegacyFolder leaves every folder it does not move to the load sweep. A promoted
// record's data is already in the plain folder.
func (s *Shard) convertLegacyFolder(class *models.Class, canonical string, props []legacyRecordProp) error {
	newest := slices.MaxFunc(props, func(a, b legacyRecordProp) int {
		if c := cmp.Compare(a.rec.Subject.Key.TaskVersion, b.rec.Subject.Key.TaskVersion); c != 0 {
			return c
		}
		return strings.Compare(a.rec.file, b.rec.file)
	})
	switched := newest.rec.State == "swapped" || newest.rec.State == "merged"
	if !switched || newest.staged == "" || !legacyEffectShown(class, newest.rec, newest.prop) {
		return nil
	}
	for _, name := range []string{canonical, newest.staged} {
		if filepath.Base(name) != name || !strings.HasPrefix(name, "property_") {
			return fmt.Errorf("record names folder %q, which is not a property index", name)
		}
	}
	staged := filepath.Join(s.pathLSM(), newest.staged)
	if _, err := os.Stat(staged); errors.Is(err, os.ErrNotExist) {
		return nil // moved before a crash
	} else if err != nil {
		return err
	}
	if err := os.RemoveAll(filepath.Join(s.pathLSM(), canonical)); err != nil {
		return err
	}
	return os.Rename(staged, filepath.Join(s.pathLSM(), canonical))
}

func legacyEffectShown(class *models.Class, rec *legacyRecord, propName string) bool {
	i := slices.IndexFunc(class.Properties, func(p *models.Property) bool { return p.Name == propName })
	if i < 0 {
		return false
	}
	prop := class.Properties[i]
	switch rec.Subject.MigrationType {
	case ReindexTypeRebuildSearchable, ReindexTypeRepairFilterable, ReindexTypeRepairRangeable:
		return true
	case ReindexTypeChangeTokenization, ReindexTypeChangeTokenizationFilterable:
		return prop.Tokenization == rec.Subject.TargetTokenization
	case ReindexTypeEnableFilterable, ReindexTypeEnableSearchable, ReindexTypeEnableRangeable:
		types, _ := ReindexIndexTypes(rec.Subject.MigrationType)
		return classIndexOn(prop, types[0])
	case ReindexTypeChangeAlgorithm:
		return prop.SearchableBlockmax != nil && *prop.SearchableBlockmax
	default:
		return false
	}
}
