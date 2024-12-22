package main

import (
	"regexp"
	"strings"
)

func decodeVarint(data *[]byte, offset int64) (uint64, int) {
	var result uint64
	var i int64

	for {
		currentByte := (*data)[offset+i]

		if i == 8 {
			result <<= 8
			result |= uint64(currentByte)
			break
		} else {
			result <<= 7
		}

		result |= uint64(currentByte & 0x7f)

		if currentByte>>7 == 0 {
			break
		}

		i++
	}
	return result, int(i + 1)
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

func getColumnIndex(createStatement string, columnNames []string) []int {
	re := regexp.MustCompile(`CREATE TABLE \w+\s*\(([^\)]+)\)`)
	matches := re.FindStringSubmatch(createStatement)
	columns := strings.Split(matches[1], ",")

	var columnIndex []int

	for _, columnName := range columnNames {
		for i, c := range columns {
			if strings.Split(strings.TrimSpace(c), " ")[0] == columnName {
				columnIndex = append(columnIndex, i)
				break
			}
		}
	}

	return columnIndex
}
