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

package inverted

// DeltaMerger can be used to condense the number of single writes into one big
// one. Additionally it removes overlaps between additions and deletions. It is
// meant to be used in batch situation, where 5 ref objects in a row might each
// increase the doc count by one. Instead of writing 5 additions and 4
// deletions, this can be condensed to write just one addition
type DeltaMerger struct {
	additions propsByName
	deletions propsByName
}

func NewDeltaMerger() *DeltaMerger {
	return &DeltaMerger{
		additions: propsByName{},
		deletions: propsByName{},
	}
}

func (dm *DeltaMerger) AddAdditions(props []Property, docID uint64) {
	for _, prop := range props {
		storedProp := dm.additions.getOrCreate(prop.Name)
		storedProp.hasFilterableIndex = prop.HasFilterableIndex
		storedProp.hasSearchableIndex = prop.HasSearchableIndex
		for _, item := range prop.Items {
			storedItem := storedProp.getOrCreateItem(item.Data)
			storedItem.addDocIDAndFrequency(docID, item.TermFrequency)
		}
	}
}

func (dm *DeltaMerger) AddDeletions(props []Property, docID uint64) {
	for _, prop := range props {
		additionProp := dm.additions.getOrCreate(prop.Name)
		for _, item := range prop.Items {
			additionItem := additionProp.getOrCreateItem(item.Data)
			ok := additionItem.deleteIfPresent(docID)
			if ok {
				// we are done with this prop, no need to register an explicit deletion
				continue
			}

			// this was not added by us, we need to remove it
			deletionProp := dm.deletions.getOrCreate(prop.Name)
			deletionProp.hasFilterableIndex = prop.HasFilterableIndex
			deletionProp.hasSearchableIndex = prop.HasSearchableIndex
			deletionItem := deletionProp.getOrCreateItem(item.Data)
			deletionItem.addDocIDAndFrequency(docID, 0) // frequency does not matter on deletion
		}
	}
}

func (dm *DeltaMerger) Merge() DeltaMergeResult {
	return DeltaMergeResult{
		Additions: dm.additions.merge(),
		Deletions: dm.deletions.merge(),
	}
}

type DeltaMergeResult struct {
	Additions []MergeProperty
	Deletions []MergeProperty
}

type MergeProperty struct {
	Name               string
	MergeItems         []MergeItem
	HasFilterableIndex bool
	HasSearchableIndex bool
}

type MergeItem struct {
	Data   []byte
	DocIDs []MergeDocIDWithFrequency
}

// IDs is meant for cases such as deletion, where the frequency is irrelevant,
// but the expected format is a []docID
func (mi MergeItem) IDs() []uint64 {
	out := make([]uint64, len(mi.DocIDs))
	for i, tuple := range mi.DocIDs {
		out[i] = tuple.DocID
	}

	return out
}

// Countable converts the merge item to a regular (non-merge) Countable. Note
// that this loses the IDs and Frequency information, so IDs have to be passed
// separately using .IDs()
func (mi MergeItem) Countable() Countable {
	return Countable{
		Data: mi.Data,
	}
}

type MergeDocIDWithFrequency struct {
	DocID     uint64
	Frequency float32
}

type propsByName map[string]*propWithDocIDs

func (pbn propsByName) getOrCreate(name string) *propWithDocIDs {
	prop, ok := pbn[name]
	if ok {
		return prop
	}
	prop = &propWithDocIDs{name: name, items: map[string]*countableWithDocIDs{}}
	pbn[name] = prop
	return prop
}

func (pbn propsByName) merge() []MergeProperty {
	out := make([]MergeProperty, len(pbn))
	i := 0
	for _, prop := range pbn {
		mergedProp := prop.merge()
		if mergedProp == nil {
			continue
		}
		out[i] = *mergedProp
		i++
	}

	if i == 0 {
		return nil
	}

	return out[:i]
}

type propWithDocIDs struct {
	name               string
	items              map[string]*countableWithDocIDs
	hasFilterableIndex bool
	hasSearchableIndex bool
}

func (pwd *propWithDocIDs) getOrCreateItem(data []byte) *countableWithDocIDs {
	name := string(data)
	item, ok := pwd.items[name]
	if ok {
		return item
	}
	item = &countableWithDocIDs{
		value:  data,
		docIDs: map[uint64]float32{},
	}
	pwd.items[name] = item
	return item
}

func (pwd *propWithDocIDs) merge() *MergeProperty {
	items := make([]MergeItem, len(pwd.items))

	i := 0
	for _, item := range pwd.items {
		mergedItem := item.merge()
		if mergedItem == nil {
			continue
		}

		items[i] = *mergedItem
		i++
	}

	if i == 0 {
		return nil
	}

	return &MergeProperty{
		Name:               pwd.name,
		MergeItems:         items[:i],
		HasFilterableIndex: pwd.hasFilterableIndex,
		HasSearchableIndex: pwd.hasSearchableIndex,
	}
}

type countableWithDocIDs struct {
	value  []byte
	docIDs map[uint64]float32 // map[docid]frequency
}

func (cwd *countableWithDocIDs) addDocIDAndFrequency(docID uint64, freq float32) {
	cwd.docIDs[docID] = freq
}

func (cwd *countableWithDocIDs) deleteIfPresent(docID uint64) bool {
	_, ok := cwd.docIDs[docID]
	if !ok {
		return false
	}

	delete(cwd.docIDs, docID)
	return true
}

func (cwd *countableWithDocIDs) merge() *MergeItem {
	if len(cwd.docIDs) == 0 {
		return nil
	}

	ids := make([]MergeDocIDWithFrequency, len(cwd.docIDs))
	i := 0
	for docID, freq := range cwd.docIDs {
		ids[i] = MergeDocIDWithFrequency{DocID: docID, Frequency: freq}
		i++
	}

	return &MergeItem{
		Data:   cwd.value,
		DocIDs: ids,
	}
}
