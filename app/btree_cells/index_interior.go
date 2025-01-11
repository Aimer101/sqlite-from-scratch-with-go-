package btreecells

import (
	"encoding/binary"
	"github/com/codecrafters-io/sqlite-starter-go/app/helper"
)

type InteriorIndexCell struct {
	LeftChildPageNumber uint32
	IntegerKey          uint64
	Columns             [][]byte
}

func UnmarshalInteriorIndexCell(data []byte, offset int) InteriorIndexCell {
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

	return InteriorIndexCell{
		LeftChildPageNumber: leftChildPageNumber,
		Columns:             columns,
	}
}
