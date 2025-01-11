package page

import (
	"encoding/binary"
	"github/com/codecrafters-io/sqlite-starter-go/app/helper"
)

func readTableLeafCell(data []byte, offset int) Cell {
	_, size := helper.DecodeVarint(&data, int64(offset))

	offset += size

	rowID, size := helper.DecodeVarint(&data, int64(offset))

	offset += size

	// payload
	headerLength, size := helper.DecodeVarint(&data, int64(offset))
	headerByteEnd := offset + int(headerLength)
	offset += size

	var columnSizes []uint64

	for offset < headerByteEnd {
		serialType, bytesRead := helper.DecodeVarint(&data, int64(offset))
		columnSize := helper.GetContentSizeFromSerialType(serialType)
		columnSizes = append(columnSizes, columnSize)
		offset += bytesRead
	}

	var columns [][]byte

	for _, columnSize := range columnSizes {
		content := data[offset : offset+int(columnSize)]
		columns = append(columns, content)
		offset += int(columnSize)
	}

	return Cell{
		CellIdx: rowID,
		Columns: columns,
	}
}

func readTableInteriorCell(data []byte, offset int) Cell {
	leftChildPageNumber := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	integerKey, _ := helper.DecodeVarint(&data, int64(offset))

	return Cell{
		LeftChildPageNumber: leftChildPageNumber,
		CellIdx:             integerKey,
	}
}

func readIndexLeafCell(data []byte, offset int) Cell {
	_, size := helper.DecodeVarint(&data, int64(offset))

	offset += size

	// payload
	headerLength, size := helper.DecodeVarint(&data, int64(offset))
	headerByteEnd := offset + int(headerLength)
	offset += size

	var columnSizes []uint64

	for offset < headerByteEnd {
		serialType, bytesRead := helper.DecodeVarint(&data, int64(offset))
		columnSize := helper.GetContentSizeFromSerialType(serialType)
		columnSizes = append(columnSizes, columnSize)
		offset += bytesRead
	}

	var columns [][]byte

	for _, columnSize := range columnSizes {
		content := data[offset : offset+int(columnSize)]
		columns = append(columns, content)
		offset += int(columnSize)
	}

	return Cell{
		Columns: columns,
	}
}

func readIndexInteriorCell(data []byte, offset int) Cell {
	leftChildPageNumber := binary.BigEndian.Uint32(data[offset : offset+4])

	offset += 4

	_, size := helper.DecodeVarint(&data, int64(offset))

	offset += size

	// payload
	headerLength, size := helper.DecodeVarint(&data, int64(offset))
	headerByteEnd := offset + int(headerLength)
	offset += size

	var columnSizes []uint64

	for offset < headerByteEnd {
		serialType, bytesRead := helper.DecodeVarint(&data, int64(offset))
		offset += bytesRead

		columnSize := helper.GetContentSizeFromSerialType(serialType)
		columnSizes = append(columnSizes, columnSize)
	}

	var columns [][]byte

	for _, columnSize := range columnSizes {
		content := data[offset : offset+int(columnSize)]
		columns = append(columns, content)
		offset += int(columnSize)
	}

	return Cell{
		LeftChildPageNumber: leftChildPageNumber,
		Columns:             columns,
	}

}
