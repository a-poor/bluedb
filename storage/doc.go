// Package storage provides constructs for storing database records on disk.
//
// The main storage primative is the LSMTree, which is built on top of a
// Memtable and Levels (which is made up of SSTables).
//
// # LSMTree Disk Layout
//
// An LSMTree is stored with the following general structure:
//
//	path/to/tree/
//	├── _meta.json
//	├── _wal.log
//	├── levels/
//	│   ├── {{ LEVEL_NUM }}/
//	│   │   ├── _meta.json
//	│   │   ├── {{ ID_OF_SST }}.data
//	│   │   ├── {{ ID_OF_SST }}.meta
//	│   │   ├── {{ ID_OF_SST }}.bloom
//
// Where in the above, LEVEL_NUM is the level number, width-4, zero-padded.
// There are zero or more levels per tree.
//
// And ID_OF_SST is the ID of the SSTable, which is a UUID. There is (generally)
// at least one table per level. The tables for a given level are stored in the
// same directory -- where each table has a data file, a meta file, and a bloom
// filter file.
//
// Done
package storage
