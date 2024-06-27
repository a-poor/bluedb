package storage

import (
	"os"
	"testing"
)

func TestSSTBuilder(t *testing.T) {
	t.Run("should build a new sstable", func(t *testing.T) {
		d, err := os.MkdirTemp("", "sstable")
		if err != nil {
			t.Fatalf("failed to create tmp dir: %s", err)
		}
		defer os.RemoveAll(d)

		// Define the builder
		builder := &SSTBuilder{
			Path:  d,
			Level: 1,
		}

		// Run the setup
		if err := builder.SetUp(); err != nil {
			t.Fatalf("failed to set up the builder: %s", err)
		}

		// Define the records to add
		//
		// Note that in the map, if any of the values are ints they
		// will be converted to floats (in JSON marshal/unmarshal)
		// and the following test will fail.
		minKey, maxKey := "001", "999"
		records := []Record{
			{
				Key: minKey,
				Value: map[string]any{
					"foo": 3.14,
				},
			},
			{
				Key:  "002",
				Tomb: true,
			},
			{
				Key: maxKey,
				Value: map[string]any{
					"baz": true,
				},
			},
		}

		// Add the records to the builder
		for _, r := range records {
			if err := builder.Add(r); err != nil {
				t.Fatalf("failed to add record: %s", err)
			}
		}

		// Finish the builder
		table, err := builder.Finish()
		if err != nil {
			t.Fatalf("failed to finish the builder: %s", err)
		}

		// Check that the table metadata is correct
		expectedMeta := SSTMeta{
			ID:          table.meta.ID,
			Level:       builder.Level,
			MinKey:      minKey,
			MaxKey:      maxKey,
			RecordCount: uint64(len(records)),
			CreatedAt:   table.meta.CreatedAt,
		}
		if table.meta != expectedMeta {
			t.Logf("expected: %+v", expectedMeta)
			t.Logf("got:      %+v", table.meta)
			t.Fatalf("unexpected table metadata: %+v", table.meta)
		}

		// Check that all of the records *might* be
		// in the bloom filter
		for _, r := range records {
			maybe := table.bloom.Test([]byte(r.Key))
			if !maybe {
				t.Fatalf("key %s should be in bloom filter", r.Key)
			}
		}

		// Scan the table and get the records
		var gotRecords []Record
		if err := table.scan(func(r Record) (bool, error) {
			gotRecords = append(gotRecords, r)
			return false, nil
		}); err != nil {
			t.Fatalf("failed to scan table: %s", err)
		}

		// Check that the records are correct
		if len(gotRecords) != len(records) {
			t.Fatalf(
				"expected %d records, got %d",
				len(records),
				len(gotRecords),
			)
		}

		// Use reflect to compare the records
		for i := 0; i < len(records); i++ {
			rexp := records[i]
			rgot := gotRecords[i]

			// Compare the keys
			if rexp.Key != rgot.Key {
				t.Fatalf("expected key %s, got %s", rexp.Key, rgot.Key)
			}

			// Compare the tombstones
			if rexp.Tomb != rgot.Tomb {
				t.Fatalf("expected tombstone %t, got %t", rexp.Tomb, rgot.Tomb)
			}

			// Compare the values
			if len(rexp.Value) != len(rgot.Value) {
				t.Fatalf("unexpected value")
			}
			for k, ve := range rexp.Value {
				vg, ok := rgot.Value[k]
				if !ok {
					t.Fatalf("expected value %v, got nothing", ve)
				}
				if vg != ve {
					t.Fatalf("expected key=%q value to be %v, got %v", k, ve, vg)
				}
			}
		}
	})
}

func TestSSTable(t *testing.T) {}

func TestReadSSTable(t *testing.T) {}

func TestSSTable_Get(t *testing.T) {}

func TestSSTable_scan(t *testing.T) {}
