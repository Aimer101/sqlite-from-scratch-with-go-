package page

import (
	"encoding/binary"
	btreecells "github/com/codecrafters-io/sqlite-starter-go/app/btree_cells"
	"github/com/codecrafters-io/sqlite-starter-go/app/helper"
	"os"
	"strings"
)

const (
	InteriorIndexPage uint8 = 0x02
	InteriorTablePage uint8 = 0x05
	LeafIndexPage     uint8 = 0x0a
	LeafTablePage     uint8 = 0x0d
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

	res := &PageHeader{}

	res.PageType = data[0]
	res.FirstFreeblock = binary.BigEndian.Uint16(data[1:3])
	res.CellCount = binary.BigEndian.Uint16(data[3:5])
	res.CellContentPointer = binary.BigEndian.Uint16(data[5:7]) // The two-byte integer at offset 5 designates the start of the cell content area. A zero value for this integer is interpreted as 65536.
	res.FragmantedFreeBytes = data[7]

	return *res, nil
}

type TablePage struct {
	Header PageHeader
	Rows   []btreecells.LeafTableCell
}

func parsePointers(data []byte) []uint16 {
	pointersBuffSize := len(data) / 2

	cellPointers := make([]uint16, pointersBuffSize)

	for i := 0; i < pointersBuffSize; i++ {
		cellPointers[i] = binary.BigEndian.Uint16(data[i*2 : (i*2)+2])
	}

	return cellPointers

}

func PeakPageHeader(file *os.File, pageNumber int, pageSize int) (PageHeader, error) {
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

func ReadPage(file *os.File, pageNumber int, pageSize int) (Page, error) {
	buff := make([]byte, pageSize)

	readerOffset := pageSize * (pageNumber - 1)

	_, err := file.ReadAt(buff, int64(readerOffset))

	if err != nil {
		return Page{}, err
	}
	offset := 0

	if pageNumber == 1 {
		offset = 100 // first page contains 100 bytes of file header
	}

	header, err := unmarshalPageHeader(buff[offset : offset+8])

	if err != nil {
		return Page{}, err
	}

	offset += 8

	if header.PageType == InteriorTablePage || header.PageType == InteriorIndexPage {
		header.RightmostPointer = binary.BigEndian.Uint32(buff[offset : offset+4])
		offset += 4
	}

	pointersBuff := buff[offset : offset+int(header.CellCount*2)]
	pointers := parsePointers(pointersBuff)

	var cells []Cell

	for _, pointer := range pointers {
		var cell Cell

		switch header.PageType {
		case InteriorIndexPage:
			cell = readIndexInteriorCell(buff, int(pointer))

		case InteriorTablePage:
			cell = readTableInteriorCell(buff, int(pointer))

		case LeafIndexPage:
			cell = readIndexLeafCell(buff, int(pointer))

		case LeafTablePage:
			cell = readTableLeafCell(buff, int(pointer))
		}

		cells = append(cells, cell)
	}

	return Page{
		Header: header,
		Cells:  cells,
	}, nil
}

func ReadIndex(file *os.File, pageNumber int, pageSize int, filter []string, ids *[]int64) {
	page, _ := ReadPage(file, pageNumber, pageSize)

	switch page.Header.PageType {
	case InteriorIndexPage:
		for i, cell := range page.Cells {

			switch strings.Compare(filter[1], string(cell.Columns[0])) {
			// lexically smaller, so we want to go left
			case -1:
				ReadIndex(file, int(cell.LeftChildPageNumber), pageSize, filter, ids)
				return
			// equal, so we include current cell and proceed to go with left child
			case 0:
				ReadIndex(file, int(cell.LeftChildPageNumber), pageSize, filter, ids)
				// ids = append(ids, results...)
				*ids = append(*ids, int64(cell.CellIdx))
				return

			// lexically greater, so we want to go right
			case 1:
				if i == len(page.Cells)-1 {
					ReadIndex(file, int(page.Header.RightmostPointer), pageSize, filter, ids)
					return
				}

			}

		}
	case LeafIndexPage:
		for _, cell := range page.Cells {
			if string(cell.Columns[0]) == filter[1] {
				*ids = append(*ids, helper.DecodeTwosCompliment(cell.Columns[1]))
			}
		}
	}

}

func ReadFullTree(file *os.File, pageNumber int, pageSize int) []Cell {
	buff := make([]byte, pageSize)

	readerOffset := pageSize * (pageNumber - 1)

	file.ReadAt(buff, int64(readerOffset))

	offset := 0

	if pageNumber == 1 {
		offset = 100
	}

	header, _ := unmarshalPageHeader(buff[offset : offset+8])

	offset += 8

	if header.PageType == InteriorTablePage {
		header.RightmostPointer = binary.BigEndian.Uint32(buff[offset : offset+4])
		offset += 4
	}

	pointersBuff := buff[offset : offset+int(header.CellCount*2)]

	pointers := parsePointers(pointersBuff)

	var results []Cell

	if header.PageType == InteriorTablePage {
		var cells []Cell
		for _, pointer := range pointers {
			cell := readTableInteriorCell(buff, int(pointer))
			cells = append(cells, cell)
		}

		for _, cell := range cells {
			result := ReadFullTree(file, int(cell.LeftChildPageNumber), pageSize)
			results = append(results, result...)
		}

		rightMostPage := ReadFullTree(file, int(header.RightmostPointer), pageSize)
		results = append(results, rightMostPage...)

	} else {
		for _, pointer := range pointers {
			result := readTableLeafCell(buff, int(pointer))
			results = append(results, result)
		}

	}

	return results

}

func ReadTree(file *os.File, pageNumber int, pageSize int, ids *[]int64) []Cell {
	page, _ := ReadPage(file, pageNumber, pageSize)

	var result []Cell

	if page.Header.PageType == LeafTablePage {
		for _, cell := range page.Cells {
			if int64(cell.CellIdx) != (*ids)[0] {
				continue
			}

			*ids = (*ids)[1:]
			result = append(result, cell)

			if len(*ids) == 0 {
				break
			}
		}

	} else {
		for _, cell := range page.Cells {
			if len(*ids) > 0 && (*ids)[0] <= int64(cell.CellIdx) {
				tranversedResult := ReadTree(file, int(cell.LeftChildPageNumber), pageSize, ids)
				result = append(result, tranversedResult...)
			}

			if len(*ids) == 0 {
				break
			}
		}

		if len(*ids) > 0 {
			tranversedResult := ReadTree(file, int(page.Header.RightmostPointer), pageSize, ids)
			result = append(result, tranversedResult...)
		}

	}

	return result

}
