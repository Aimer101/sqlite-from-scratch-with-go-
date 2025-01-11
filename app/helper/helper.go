package helper

import (
	"regexp"
	"strings"
)

func ArrayContain[T comparable](target T, elements []T) bool {

	for _, element := range elements {
		if element == target {
			return true
		}
	}

	return false
}

func DecodeTwosCompliment(bytes []byte) int64 {
	size := len(bytes)
	if size > 8 {
		panic("Max 8 bytes allowed")
	}
	var result int64
	var mask int64
	for i := 0; i < size; i++ {
		shift := 8 * (size - i - 1)
		// Combine bytes
		result |= int64(bytes[i]) << shift
		// 11111111 = 255
		mask |= 255 << shift
	}

	// 10000000 = 128
	if bytes[0]&128 == 1 {
		flippedMask := ^mask
		result |= flippedMask
	}
	return result
}

func DecodeVarint(data *[]byte, offset int64) (uint64, int) {
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

func GetContentSizeFromSerialType(serialType uint64) uint64 {
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

func ParseCreateIndexStatement(indexStatement string) []string {
	var result []string
	re := regexp.MustCompile(`(?i)CREATE INDEX \"?\w+\"?\s*ON\s+(\w+)\s+\((\w+)\)`)
	matches := re.FindStringSubmatch(indexStatement)
	columns := strings.Split(matches[2], ",")
	result = append(result, columns...)

	return result
}

func GetTableColumnIndex(createStatement string, columnNames []string) []int {
	// re := regexp.MustCompile(`CREATE TABLE \w+\s*\(([^\)]+)\)`)
	re := regexp.MustCompile(`CREATE TABLE ["']?\w+["']?\s*\(([^\)]+)\)`)
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

func ParseWhereStatement(whereStatement string) []string {
	result := strings.Split(whereStatement, "=")
	result[1] = strings.ReplaceAll(result[1], "'", "")
	result[1] = strings.Trim(result[1], " ")
	result[0] = strings.ReplaceAll(result[0], " ", "")

	return result
}
