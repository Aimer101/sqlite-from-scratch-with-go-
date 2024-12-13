package main

import (
	"encoding/binary"
	"fmt"
)

type DatabaseHeader struct {
	HeaderString           [16]byte // The header string: "SQLite format 3\000"
	PageSize               uint16   // The database page size in bytes. Must be a power of two between 512 and 32768 inclusive, or the value 1 representing a page size of 65536.
	WriteVersion           uint8    // File format write version. 1 for legacy; 2 for WAL.
	ReadVersion            uint8    // File format read version. 1 for legacy; 2 for WAL.
	ReservedSpace          uint8    // Bytes of unused "reserved" space at the end of each page. Usually 0.
	MaxPayloadFraction     uint8    // Maximum embedded payload fraction. Must be 64.
	MinPayloadFraction     uint8    // Minimum embedded payload fraction. Must be 32.
	LeafPayloadFraction    uint8    // Leaf payload fraction. Must be 32
	FileChangeCounter      uint32
	DatabaseSize           uint32 // Size of the database file in pages. The "in-header database size".
	FirstFreelistTrunkPage uint32 // Page number of the first freelist trunk page.
	TotalFreelistPages     uint32
	SchemaCookie           uint32
	SchemaFormatNumber     uint32 // The schema format number. Supported schema formats are 1, 2, 3, and 4.
	DefaultPageCacheSize   uint32
	LargestRootBTree       uint32   // The page number of the largest root b-tree page when in auto-vacuum or incremental-vacuum modes, or zero otherwise.
	TextEncoding           uint32   // The database text encoding. A value of 1 means UTF-8. A value of 2 means UTF-16le. A value of 3 means UTF-16be.
	UserVersion            uint32   // The "user version" as read and set by the user_version pragma.
	IncrementalVacuum      uint32   // True (non-zero) for incremental-vacuum mode. False (zero) otherwise.
	ApplicationId          uint32   // The "Application ID" set by PRAGMA application_id.
	Reserved               [20]byte // Reserved for expansion. Must be zero.
	VersionValidFor        uint32   // 	The version-valid-for number.
	SQLiteVersionNumber    uint32
}

func unmarshalDbHeader(data []byte) (DatabaseHeader, error) {
	if len(data) != 100 {
		return DatabaseHeader{}, fmt.Errorf("database header must be 100 bytes, got %d", len(data))
	}

	res := &DatabaseHeader{}
	copy(res.HeaderString[:], data[0:16])

	res.PageSize = binary.BigEndian.Uint16(data[16:18])
	res.WriteVersion = data[18]
	res.ReadVersion = data[19]
	res.ReservedSpace = data[20]
	res.MaxPayloadFraction = data[21]
	res.MinPayloadFraction = data[22]
	res.LeafPayloadFraction = data[23]
	res.FileChangeCounter = binary.BigEndian.Uint32(data[24:28])
	res.DatabaseSize = binary.BigEndian.Uint32(data[28:32])
	res.FirstFreelistTrunkPage = binary.BigEndian.Uint32(data[32:36])
	res.TotalFreelistPages = binary.BigEndian.Uint32(data[36:40])
	res.SchemaCookie = binary.BigEndian.Uint32(data[40:44])
	res.SchemaFormatNumber = binary.BigEndian.Uint32(data[44:48])
	res.DefaultPageCacheSize = binary.BigEndian.Uint32(data[48:52])
	res.LargestRootBTree = binary.BigEndian.Uint32(data[52:56])
	res.TextEncoding = binary.BigEndian.Uint32(data[56:60])
	res.UserVersion = binary.BigEndian.Uint32(data[60:64])
	res.IncrementalVacuum = binary.BigEndian.Uint32(data[64:68])
	res.ApplicationId = binary.BigEndian.Uint32(data[68:72])
	copy(res.HeaderString[:], data[72:92])
	res.VersionValidFor = binary.BigEndian.Uint32(data[92:96])
	res.SQLiteVersionNumber = binary.BigEndian.Uint32(data[96:100])

	return *res, nil
}
