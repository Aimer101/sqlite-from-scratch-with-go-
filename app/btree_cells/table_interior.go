package btreecells

import (
	"encoding/binary"
	"github/com/codecrafters-io/sqlite-starter-go/app/helper"
)

type InteriorTableCell struct {
	LeftChildPageNumber uint32
	IntegerKey          uint64
}

func UnmarshalInteriorTableCell(data []byte, offset int) InteriorTableCell {
	leftChildPageNumber := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	integerKey, _ := helper.DecodeVarint(&data, int64(offset))

	return InteriorTableCell{
		LeftChildPageNumber: leftChildPageNumber,
		IntegerKey:          integerKey,
	}

}
