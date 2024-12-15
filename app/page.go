package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
)

// The b-tree page header is 8 bytes in size for leaf pages and 12 bytes for interior pages.

// 0th byte
// The one-byte flag at offset 0 indicating the b-tree page type.
// A value of 2 (0x02) means the page is an interior index b-tree page.
// A value of 5 (0x05) means the page is an interior table b-tree page.
// A value of 10 (0x0a) means the page is a leaf index b-tree page.
// A value of 13 (0x0d) means the page is a leaf table b-tree page.
type PageHeader struct {
	PageType            uint8
	FirstFreeblock      uint16
	CellCount           uint16
	CellContentPointer  uint16 // The two-byte integer at offset 5 designates the start of the cell content area. A zero value for this integer is interpreted as 65536.
	FragmantedFreeBytes uint8  // The one-byte integer at offset 7 gives the number of fragmented free bytes within the cell content area.
	// RightmostPointer    uint32 // The four-byte page number at offset 8 is the right-most pointer. This value appears in the header of interior b-tree pages only and is omitted from all other pages.
}

func unmarshalPageHeader(data []byte) (PageHeader, error) {
	if len(data) != 8 {
		return PageHeader{}, fmt.Errorf("database header must be 8 bytes, got %d", len(data))
	}

	res := &PageHeader{}

	res.PageType = data[0]
	res.FirstFreeblock = binary.BigEndian.Uint16(data[1:3])
	res.CellCount = binary.BigEndian.Uint16(data[3:5])
	res.CellContentPointer = binary.BigEndian.Uint16(data[5:7]) // The two-byte integer at offset 5 designates the start of the cell content area. A zero value for this integer is interpreted as 65536.
	res.FragmantedFreeBytes = data[7]
	// res.RightmostPointer = binary.BigEndian.Uint32(data[8:12])

	return *res, nil

}

type Page struct {
	Header PageHeader
	Rows   []Cell
}

type Cell struct {
	Size        uint64
	RowID       uint64
	HeaderSize  uint64
	ColumnSizes []uint64
	Columns     [][]byte
}

func parsePointers(data []byte) []uint16 {
	pointersBuffSize := len(data) / 2

	cellPointers := make([]uint16, pointersBuffSize)

	for i := 0; i < pointersBuffSize; i++ {
		cellPointers[i] = binary.BigEndian.Uint16(data[i*2 : (i*2)+2])
	}

	return cellPointers

}

func readPage(file *os.File, pageNumber int, pageSize int) (Page, error) {
	// info, _ := file.Stat() // Size in bytes
	// fmt.Printf("The size of the file is %d bytes.\n", info.Size())

	buff := make([]byte, pageSize)

	readerOffset := pageSize * (pageNumber - 1)

	_, err := file.ReadAt(buff, int64(readerOffset))

	if err != nil {
		return Page{}, err
	}

	offset := 0

	if pageNumber == 1 {
		offset = 100
	}

	header, err := unmarshalPageHeader(buff[offset : offset+8])
	if pageNumber != 1 {
		fmt.Println(header.CellCount)
	}
	offset += 8

	if err != nil {
		return Page{}, err
	}

	// parse pointers
	pointersBuff := buff[offset : offset+int(header.CellCount*2)]

	pointers := parsePointers(pointersBuff)

	var cells []Cell

	for _, pointer := range pointers {
		cell := readCell(file, int(pointer))
		cells = append(cells, cell)
	}

	return Page{Header: header, Rows: cells}, nil
}

func readCell(file *os.File, readerOffset int) Cell {

	varintBytes := make([]byte, 8)

	if _, err := file.ReadAt(varintBytes, int64(readerOffset)); err != nil {
		log.Fatal(err)
	}

	// Size of the record
	rowLength, size := parseVarIntBtes(varintBytes)

	readerOffset += size

	buff := make([]byte, rowLength-uint64(size))

	if _, err := file.ReadAt(buff, int64(readerOffset)); err != nil {
		log.Fatal(err)
	}

	offset := 0

	// The rowid (safe to ignore)
	rowID, size := parseVarIntBtes(buff[offset:])

	offset += size

	columnSizes, size := parseCellHeader(buff[offset:])

	offset += size

	var body [][]byte

	for index, columnSize := range columnSizes {
		body = append(body, buff[offset:offset+int(columnSize)])

		if index == 3 {
			break
		}

		offset += int(columnSize)
	}

	return Cell{
		Size:        rowLength,
		RowID:       rowID,
		HeaderSize:  uint64(size),
		ColumnSizes: columnSizes,
		Columns:     body,
	}
}
