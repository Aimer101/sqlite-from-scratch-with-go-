package btreecells

import (
	"github/com/codecrafters-io/sqlite-starter-go/app/helper"
)

type LeafIndexCell struct {
	Columns [][]byte
}

func UnmarshalLeafIndexCell(data []byte, offset int) LeafIndexCell {
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

	return LeafIndexCell{
		Columns: columns,
	}
}
