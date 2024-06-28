package storage

import (
	"fmt"
	"path"
	"sync"
)

type LSMTree struct {
	sync.RWMutex
	path           string    // That path to the tree's directory
	memtable       *Memtable // The current active memtable
	frozenMemtable *Memtable // A memtable being compacted
	levels         []*Level  // Handles to the levels
}

type NewLSMTreeConf struct {
	Path string // The path to the tree's directory
}

func NewLSMTree(conf NewLSMTreeConf) (*LSMTree, error) {
	return nil, fmt.Errorf("not implemented")
}

type LoadLSMTreeConf struct {
	Path string // The path to the tree's directory
}

func LoadLSMTree(conf LoadLSMTreeConf) (*LSMTree, error) {
	return nil, fmt.Errorf("not implemented")
}

func (t *LSMTree) Get(k string) (map[string]any, error) {
	return nil, fmt.Errorf("not implemented")
}

func (t *LSMTree) Put(k string, v map[string]any) error {
	return fmt.Errorf("not implemented")
}

func (t *LSMTree) Del(k string) error {
	return fmt.Errorf("not implemented")
}

func (t *LSMTree) Close() error {
	return fmt.Errorf("not implemented")
}

func (t *LSMTree) Compact() error {
	// Is the last level full? Or are there no levels yet?
	// ...then add a new level at the end
	if len(t.levels) == 0 || t.lastLevelIsFull() {
		if err := t.addLevel(); err != nil {
			return fmt.Errorf("failed to add level: %w", err)
		}
	}

	// Iterate in reverse order, compacting each level
	//
	// Note that we don't need to compact the last level
	// because it's either not full or new (and empty)
	for i := len(t.levels) - 2; i >= 0; i-- {
		level := t.levels[i]

		// Is this level full?
		if !level.Full() {
			continue
		}

		// Compact the level
		nextLevelPath := fmtLevelPath(t.levelDir(), uint16(i+1))
		table, ids, err := level.Compact(nextLevelPath)
		if err != nil {
			return fmt.Errorf("failed to compact level %d: %w", i+1, err)
		}

		// Add the new table to the next level
		nextLevel := t.levels[i+1]
		if err := nextLevel.AddTable(table); err != nil {
			return fmt.Errorf("failed to add compacted table from level %d to level %d: %w", i+1, i+2, err)
		}

		// Delete the old tables
		if err := level.DeleteTables(ids); err != nil {
			return fmt.Errorf("failed to delete old tables from level %d: %w", i+1, err)
		}
	}

	// Now check if the memtable is full
	if !t.memtable.Full() {
		// If it isn't stop early
		return nil
	}

	// If it is, compact it

	table, err := t.compactMemtable()
	if err != nil {
		return fmt.Errorf("failed to compact memtable: %w", err)
	}

	// Add the new table to the first level
	level := t.levels[0]
	if err := level.AddTable(table); err != nil {
		return fmt.Errorf("failed to add compacted table from memtable to level 1: %w", err)
	}

	// Done
	return nil
}

func (t *LSMTree) compactMemtable() (*SSTable, error) {
	// Create a new memtable
	mt := NewMemtable()

	// Lock the tree
	t.Lock()

	// Freeze the current memtable
	t.memtable.Freeze()

	// Swap the memtables
	t.frozenMemtable = t.memtable
	t.memtable = mt

	// Unlock the tree
	t.Unlock()

	// Read lock the tree
	t.RLock()
	defer t.RUnlock()

	// Compact the frozen memtable
	levelNum := 1
	levelPath := t.levelDir()
	table, err := t.frozenMemtable.Compact(levelPath, levelNum)
	if err != nil {
		return nil, err
	}

	// Delete the frozen memtable
	if err := t.frozenMemtable.Close(); err != nil {
		return nil, err
	}
	t.frozenMemtable = nil

	// Done
	return table, nil
}

func (t *LSMTree) lastLevelIsFull() bool {
	// Ensure that there is at least one level
	if len(t.levels) == 0 {
		return false
	}
	return t.levels[len(t.levels)-1].Full()
}

func (t *LSMTree) levelDir() string {
	return path.Join(t.path, "levels")
}

func (t *LSMTree) addLevel() error {
	// Get the next level's number
	ln := uint16(len(t.levels) + 1)

	// Get the level directory
	dp := t.levelDir()

	// Create the new level
	level, err := CreateLevel(ln, dp)
	if err != nil {
		return err
	}

	// Add the level to the tree
	t.levels = append(t.levels, level)

	// Done
	return nil
}

type LSMTreeMeta struct {
}

func fmtLevelPath(levelPath string, level uint16) string {
	d := fmt.Sprintf("level-%04d", level)
	return path.Join(levelPath, d)
}
