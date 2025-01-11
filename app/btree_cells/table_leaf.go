package btreecells

import (
	"github/com/codecrafters-io/sqlite-starter-go/app/helper"
)

type LeafTableCell struct {
	RowID   uint64
	Columns [][]byte
}

func UnmarshalLeafTableCell(data []byte, offset int) LeafTableCell {
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

	return LeafTableCell{
		RowID:   rowID,
		Columns: columns,
	}

}
