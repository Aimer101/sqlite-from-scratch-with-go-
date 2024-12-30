package main

import (
	"encoding/binary"
	"fmt"
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
	RightmostPointer    uint32 // The four-byte page number at offset 8 is the right-most pointer. This value appears in the header of interior b-tree pages only and is omitted from all other pages.
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

	return *res, nil
}

type Page struct {
	Header PageHeader
	Rows   []LeafPageCell
}

type LeafPageCell struct {
	Size        uint64
	RowID       uint64
	HeaderSize  uint64
	ColumnSizes []uint64
	Columns     [][]byte
}

type InteriorPageCell struct {
	LeftChildPageNumber uint32
	IntegerKey          uint64
}

type InteriorCell struct {
	ChildPageNum uint32 // 4-byte pointer to child page
	RowID        uint64 // varint rowid that acts as key
}

func parsePointers(data []byte) []uint16 {
	pointersBuffSize := len(data) / 2

	cellPointers := make([]uint16, pointersBuffSize)

	for i := 0; i < pointersBuffSize; i++ {
		cellPointers[i] = binary.BigEndian.Uint16(data[i*2 : (i*2)+2])
	}

	return cellPointers

}

func peakPageHeader(file *os.File, pageNumber int, pageSize int) (PageHeader, error) {
	buff := make([]byte, pageSize)

	readerOffset := pageSize * (pageNumber - 1)

	_, err := file.ReadAt(buff, int64(readerOffset))

	if err != nil {
		return PageHeader{}, err
	}

	offset := 0

	if pageNumber == 1 {
		offset = 100
	}

	header, err := unmarshalPageHeader(buff[offset : offset+8])

	if err != nil {
		return PageHeader{}, err
	}

	return header, nil

}

func readPage(file *os.File, pageNumber int, pageSize int) (Page, error) {
	// info, _ := file.Stat() // Size in bytes
	// fmt.Printf("The size of the file is %d bytes.\n", info.Size())
	// fmt.Println("target page index is ", pageNumber)
	// fmt.Println("page size is ", pageSize)

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

	// The b-tree page header is 8 bytes in size for leaf pages and 12 bytes for interior pages.
	// A value of 2 (0x02) means the page is an interior index b-tree page.
	// A value of 5 (0x05) means the page is an interior table b-tree page.
	// A value of 10 (0x0a) means the page is a leaf index b-tree page.
	// A value of 13 (0x0d) means the page is a leaf table b-tree page.

	header, err := unmarshalPageHeader(buff[offset : offset+8])

	offset += 8

	if header.PageType == 0x05 {
		header.RightmostPointer = binary.BigEndian.Uint32(buff[offset : offset+4])
		offset += 4

	}

	if err != nil {
		return Page{}, err
	}

	result := Page{Header: header}
	pointersBuff := buff[offset : offset+int(header.CellCount*2)]
	pointers := parsePointers(pointersBuff)
	var leafPageCells []LeafPageCell

	// interior page
	if header.RightmostPointer != 0 {
		var interiorPageCells []InteriorPageCell
		for _, pointer := range pointers {

			cell := readInteriorPageCell(buff, int(pointer))
			interiorPageCells = append(interiorPageCells, cell)
		}

		var parsedPages []Page

		for _, interiorPageCell := range interiorPageCells {
			parsedPage, _ := readPage(file, int(interiorPageCell.LeftChildPageNumber), pageSize)
			parsedPages = append(parsedPages, parsedPage)
		}

		rightMostPage, _ := readPage(file, int(header.RightmostPointer), pageSize)
		parsedPages = append(parsedPages, rightMostPage)

		for _, parsedPage := range parsedPages {
			leafPageCells = append(leafPageCells, parsedPage.Rows...)
		}

	} else {
		for _, pointer := range pointers {
			cell := readLeafPageCell(buff, int(pointer))
			leafPageCells = append(leafPageCells, cell)
		}
	}

	result.Rows = leafPageCells

	return result, nil
}

func readInteriorPageCell(data []byte, offset int) InteriorPageCell {

	leftChildPageNumber := binary.BigEndian.Uint32(data[offset : offset+4])

	offset += 4

	integerKey, _ := decodeVarint(&data, int64(offset))

	return InteriorPageCell{
		LeftChildPageNumber: leftChildPageNumber,
		IntegerKey:          integerKey,
	}
}

func readLeafPageCell(data []byte, offset int) LeafPageCell {

	rowLength, size := decodeVarint(&data, int64(offset))

	offset += size

	rowID, size := decodeVarint(&data, int64(offset))

	offset += size

	headerLength, size := decodeVarint(&data, int64(offset))
	headerByteEnd := offset + int(headerLength)
	offset += size

	var columnSizes []uint64

	for offset < headerByteEnd {
		serialType, bytesRead := decodeVarint(&data, int64(offset))
		columnSize := getContentSizeFromSerialType(serialType)
		columnSizes = append(columnSizes, columnSize)
		offset += bytesRead
	}

	var columns [][]byte

	for _, columnSize := range columnSizes {
		content := data[offset : offset+int(columnSize)]
		columns = append(columns, content)
		offset += int(columnSize)
	}

	return LeafPageCell{
		Size:       rowLength,
		RowID:      rowID,
		HeaderSize: uint64(size),
		Columns:    columns,
	}
}
