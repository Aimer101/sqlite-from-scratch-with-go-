package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	// Available if you need it!
	"github.com/xwb1989/sqlparser"
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

		handleQuery(command, databaseFile)

	}
}

func handleQuery(queries string, databaseFile *os.File) {
	parsedQuery, err := sqlparser.Parse(queries)

	if err != nil {
		log.Fatal(err)
	}

	switch parsedQuery := parsedQuery.(type) {

	case *sqlparser.Select:
		tableName := sqlparser.String(parsedQuery.From[0])
		selectExp := sqlparser.String(parsedQuery.SelectExprs[0])
		whereStatement := sqlparser.String(parsedQuery.Where)
		var whereExp []string

		if whereStatement != "" {
			whereStatement = sqlparser.String(parsedQuery.Where.Expr)
			whereExp = parseWhereStatement(whereStatement)
		}

		var col_names []string

		for _, col_name := range parsedQuery.SelectExprs {
			col_names = append(col_names, sqlparser.String(col_name))
		}

		header := make([]byte, 100)

		_, err := databaseFile.Read(header)

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

		if strings.Contains(strings.ToLower(selectExp), "count") {

			for _, row := range firstPage.Rows {
				if string(row.Columns[0]) == "table" && string(row.Columns[1]) == tableName {
					targetPageHeader, err := peakPageHeader(databaseFile, int(row.Columns[3][0]), int(databaseHeader.PageSize))

					if err != nil {
						log.Fatal(err)
					}

					fmt.Println(targetPageHeader.CellCount)
				}
			}

		} else {

			for _, row := range firstPage.Rows {
				if string(row.Columns[0]) == "table" && string(row.Columns[1]) == tableName {
					columnIndexes := getColumnIndex(string(row.Columns[4]), col_names)
					targetPage, err := readPage(databaseFile, int(row.Columns[3][0]), int(databaseHeader.PageSize))

					if err != nil {
						log.Fatal(err)
					}

					filter_col_index := -1

					if whereStatement != "" {
						filter_col_index = getColumnIndex(string(row.Columns[4]), []string{whereExp[0]})[0]

					}

					var col_results [][]string

					for _, row := range targetPage.Rows {

						if filter_col_index != -1 && string(row.Columns[filter_col_index]) != whereExp[1] {
							continue
						}

						var col_result []string

						for _, index := range columnIndexes {
							col_result = append(col_result, string(row.Columns[index]))
							continue
						}

						col_results = append(col_results, col_result)
					}

					for _, col_result := range col_results {
						result := strings.Join(col_result, "|")
						fmt.Println(result)
					}

				}
			}

		}
	}

}
