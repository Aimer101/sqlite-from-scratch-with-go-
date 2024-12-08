package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
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

		// mevimo commented on codecrafters that we need to
		// tranverse the whole b-tree to count tables
		// But since the question make an assumption that
		// The sqlite_schema table fits on a single page
		// we can assume that the btree on the first page number of cells = number of tables

		// B-tree Page Header Format
		// Offset	Size	Description
		// 3		|2		|The two-byte integer at offset 3 gives the number of cells on the page.
		nTables := binary.BigEndian.Uint16(page[3 : 3+2])

		// if err := binary.Read(bytes.NewReader(leafRow[3:3+2]), binary.BigEndian, &nTables); err != nil {
		// 	fmt.Println("Failed to read integer:", err)
		// 	return
		// }

		fmt.Printf("database page size: %v", pageSize)
		fmt.Printf("number of tables: %v\n", nTables)
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
