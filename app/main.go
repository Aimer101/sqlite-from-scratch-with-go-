package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strings"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

// Usage: your_program.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	switch command {
	case ".dbinfo":
		databaseFile, err := os.Open(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}

		header := make([]byte, 100)

		_, err = databaseFile.Read(header)
		if err != nil {
			log.Fatal(err)
		}

		databaseHeader, err := unmarshalDbHeader(header)

		if err != nil {
			log.Fatal(err)
		}

		_ = databaseHeader

		var pageSize uint16
		// Offset	|Size	|Description
		// 16		|2		|The database page size in bytes. Must be a power of two between 512 and 32768 inclusive, or the value 1 representing a page size of 65536.
		if err := binary.Read(bytes.NewReader(header[16:18]), binary.BigEndian, &pageSize); err != nil {
			fmt.Println("Failed to read integer:", err)
			return
		}

		// Prepare to read page 1 (sqlite_schema root page)
		page := make([]byte, pageSize-100)
		databaseFile.Read(page) // Go back to start of file

		btree, err := unmarshalBtree(page)

		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Printf("database page size: %v", pageSize)
		fmt.Printf("number of tables: %v\n", btree.Header.CellCount)
	case ".tables":

		// structure
		// 100 bytes (database header)
		// 8 bytes (page header)
		// subsequent byte (2 bytes for each pointers) where nPointers is in page header cellCount

		databaseFile, err := os.Open(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}

		// Read first 100 bytes (database header)
		header := make([]byte, 100)

		_, err = databaseFile.Read(header)
		if err != nil {
			log.Fatal(err)
		}

		_, err = unmarshalDbHeader(header)

		if err != nil {
			log.Fatal(err)
		}

		// Read page header (8 bytes for leaf page)
		pageHeaderBuffer := make([]byte, 8)

		databaseFile.Read(pageHeaderBuffer)

		pageHeader, err := unmarshalSQliteSchemaPageHeader(pageHeaderBuffer)

		if err != nil {
			log.Fatal(err)
		}

		// Verify it's a leaf table page
		if pageHeader.PageType != 0x0d {
			log.Fatal("Not a leaf table page")
		}

		cellPointers := make([]uint16, pageHeader.CellCount)
		cellPointerBuffer := make([]byte, pageHeader.CellCount*2) // 2 bytes per pointer
		databaseFile.Read(cellPointerBuffer)

		// Parse cell pointers
		for i := 0; i < int(pageHeader.CellCount); i++ {
			cellPointers[i] = binary.BigEndian.Uint16(cellPointerBuffer[i*2 : (i*2)+2])
		}

		var tablesNames []string

		for _, pointer := range cellPointers {
			cell := readCell(databaseFile, int(pointer))
			if string(cell.Body[0]) == "table" {
				tablesNames = append(tablesNames, string(cell.Body[2]))
			}
		}

		fmt.Println(strings.Join(tablesNames, " "))

	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}

func readCell(file *os.File, offset int) Cell {

	varintBytes := make([]byte, 8)

	if _, err := file.ReadAt(varintBytes, int64(offset)); err != nil {
		log.Fatal(err)
	}

	// Size of the record
	rowLength, size := parseVarIntBtes(varintBytes)

	offset += size

	buff := make([]byte, rowLength-uint64(size))

	if _, err := file.ReadAt(buff, int64(offset)); err != nil {
		log.Fatal(err)
	}

	offset = 0

	// The rowid (safe to ignore)
	rowID, size := parseVarIntBtes(buff[offset:])

	offset += size

	columnSizes, size := processCellHeader(buff[offset:])

	offset += size

	// var columnSizes []uint64

	var body [][]byte

	// table name is at index 2,
	// put a break after index 2
	for index, columnSize := range columnSizes {
		body = append(body, buff[offset:offset+int(columnSize)])

		if index == 2 {
			break
		}

		offset += int(columnSize)
	}

	return Cell{
		Size:        rowLength,
		RowID:       rowID,
		HeaderSize:  uint64(size),
		ColumnSizes: columnSizes,
		Body:        body,
	}
}
