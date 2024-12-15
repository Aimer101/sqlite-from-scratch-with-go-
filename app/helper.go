package main

func parseVarIntBtes(data []byte) (uint64, int) {
	var result uint64

	offset := 0

	for offset < len(data) {
		currentByte := data[offset]
		result |= uint64(currentByte&0x7F) << (8 * (offset))
		offset++
		if currentByte&0x80 == 0 {
			break
		}
	}

	return result, offset
}

func getContentSizeFromSerialType(serialType uint64) uint64 {
	switch {
	case serialType <= 4:
		return serialType
	case serialType == 5:
		return 6
	case serialType == 6 || serialType == 7:
		return 8
	case serialType >= 8 && serialType <= 11:
		return 0
	case serialType >= 12 && serialType%2 == 0:
		return (serialType - 12) / 2
	default:
		return (serialType - 13) / 2
	}
}

func parseCellHeader(data []byte) ([]uint64, int) {
	offset := 0

	// Size of record header (varint) including it self:
	// any byte that comes after offset + headerLength is the actual content
	headerLength, size := parseVarIntBtes(data[offset:])

	offset += size

	var columnSizes []uint64

	for offset < int(headerLength) {
		// 1. get serial type
		columnType, size := parseVarIntBtes(data[offset:])

		// 2. get size
		columnSize := getContentSizeFromSerialType(columnType)

		columnSizes = append(columnSizes, columnSize)

		offset += size
	}

	return columnSizes, int(headerLength)

}
