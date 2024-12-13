package main

import (
	"encoding/binary"
	"fmt"
)

// The b-tree page header is 8 bytes in size for leaf pages and 12 bytes for interior pages.

// 0th byte
// The one-byte flag at offset 0 indicating the b-tree page type.
// A value of 2 (0x02) means the page is an interior index b-tree page.
// A value of 5 (0x05) means the page is an interior table b-tree page.
// A value of 10 (0x0a) means the page is a leaf index b-tree page.
// A value of 13 (0x0d) means the page is a leaf table b-tree page.
type SqliteSchemaPageHeader struct {
	PageType            uint8
	FirstFreeblock      uint16
	CellCount           uint16
	CellContentPointer  uint16 // The two-byte integer at offset 5 designates the start of the cell content area. A zero value for this integer is interpreted as 65536.
	FragmantedFreeBytes uint8  // The one-byte integer at offset 7 gives the number of fragmented free bytes within the cell content area.
	// RightmostPointer    uint32 // The four-byte page number at offset 8 is the right-most pointer. This value appears in the header of interior b-tree pages only and is omitted from all other pages.
}

func unmarshalSQliteSchemaPageHeader(data []byte) (SqliteSchemaPageHeader, error) {
	if len(data) != 8 {
		return SqliteSchemaPageHeader{}, fmt.Errorf("database header must be 8 bytes, got %d", len(data))
	}

	res := &SqliteSchemaPageHeader{}

	res.PageType = data[0]
	res.FirstFreeblock = binary.BigEndian.Uint16(data[1:3])
	res.CellCount = binary.BigEndian.Uint16(data[3:5])
	res.CellContentPointer = binary.BigEndian.Uint16(data[5:7]) // The two-byte integer at offset 5 designates the start of the cell content area. A zero value for this integer is interpreted as 65536.
	res.FragmantedFreeBytes = data[7]
	// res.RightmostPointer = binary.BigEndian.Uint32(data[8:12])

	return *res, nil

}

type SqliteSchemaPage struct {
	Header SqliteSchemaPageHeader
}

func unmarshalBtree(data []byte) (SqliteSchemaPage, error) {
	header, err := unmarshalSQliteSchemaPageHeader(data[0:8])

	if err != nil {
		return SqliteSchemaPage{}, err
	}

	return SqliteSchemaPage{Header: header}, nil
}

type Cell struct {
	Size        uint64
	RowID       uint64
	HeaderSize  uint64
	ColumnSizes []uint64
	Body        [][]byte
}
