package main

import (
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

		parsedPage, err := readPage(databaseFile, 1, int(databaseHeader.PageSize))

		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Printf("database page size: %v", databaseHeader.PageSize)
		fmt.Printf("number of tables: %v\n", parsedPage.Header.CellCount)
	case ".tables":

		// structure

		// ======== file ==========
		// 100 bytes (database header)
		// ======== page ===========
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

		databaseHeader, err := unmarshalDbHeader(header)

		if err != nil {
			log.Fatal(err)
		}

		parsedPage, err := readPage(databaseFile, 1, int(databaseHeader.PageSize))

		if err != nil {
			log.Fatal(err)
		}

		var tablesNames []string

		for _, rows := range parsedPage.Rows {
			if string(rows.Columns[0]) == "table" {
				tablesNames = append(tablesNames, string(rows.Columns[2]))
			}
		}

		fmt.Println(strings.Join(tablesNames, " "))

	default:

		databaseFile, err := os.Open(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}

		command := strings.Split(command, " ")
		tableName := command[len(command)-1]

		header := make([]byte, 100)

		_, err = databaseFile.Read(header)

		if err != nil {
			log.Fatal(err)
		}

		databaseHeader, err := unmarshalDbHeader(header)

		if err != nil {
			log.Fatal(err)
		}

		firstPage, err := readPage(databaseFile, 1, int(databaseHeader.PageSize))

		if err != nil {
			log.Fatal(err)
		}

		// rootPages := make(map[string]uint8)
		for _, row := range firstPage.Rows {
			if string(row.Columns[0]) == "table" && string(row.Columns[1]) == tableName {
				targetPage, err := readPage(databaseFile, int(row.Columns[3][0]), int(databaseHeader.PageSize))

				if err != nil {
					log.Fatal(err)
				}

				fmt.Println(targetPage.Header.CellCount)
				break
			}
		}
	}
}
