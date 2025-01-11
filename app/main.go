package main

import (
	"fmt"
	helper "github/com/codecrafters-io/sqlite-starter-go/app/helper"
	"github/com/codecrafters-io/sqlite-starter-go/app/page"
	"log"
	"os"
	"strconv"
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

		databaseHeader, err := page.UnmarshalDbHeader(header)

		if err != nil {
			log.Fatal(err)
		}

		pageHeader, err := page.PeakPageHeader(databaseFile, 1, int(databaseHeader.PageSize))

		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Printf("database page size: %v", databaseHeader.PageSize)
		fmt.Printf("number of tables: %v\n", pageHeader.CellCount)
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

		databaseHeader, err := page.UnmarshalDbHeader(header)

		if err != nil {
			log.Fatal(err)
		}

		rootPageCells := page.ReadFullTree(databaseFile, 1, int(databaseHeader.PageSize))

		if err != nil {
			log.Fatal(err)
		}

		var tablesNames []string

		for _, row := range rootPageCells {

			pointer := page.UnmarshalRootPagePointer(row.Columns)

			if pointer.PageType == "table" {
				tablesNames = append(tablesNames, pointer.TableName)
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
		var whereExp []string // 0 is for col_name, 1 is for value

		if whereStatement != "" {
			whereStatement = sqlparser.String(parsedQuery.Where.Expr)
			whereExp = helper.ParseWhereStatement(whereStatement)
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

		databaseHeader, err := page.UnmarshalDbHeader(header)

		if err != nil {
			log.Fatal(err)
		}

		rootPageCells := page.ReadFullTree(databaseFile, 1, int(databaseHeader.PageSize))

		if strings.Contains(strings.ToLower(selectExp), "count") {

			for _, row := range rootPageCells {
				if string(row.Columns[0]) == "table" && string(row.Columns[1]) == tableName {
					targetPageHeader, err := page.PeakPageHeader(databaseFile, int(row.Columns[3][0]), int(databaseHeader.PageSize))

					if err != nil {
						log.Fatal(err)
					}

					fmt.Println(targetPageHeader.CellCount)
				}
			}

		} else {
			rootPagePointers := make(map[string]page.RootPagePointer)

			for _, cell := range rootPageCells {
				if string(cell.Columns[2]) != tableName {
					continue
				}
				// 0 is for type
				// 1 is for name of object created
				// 2 is name of table
				// 3 is for page number
				// 4 is for create statement
				var values [][]byte
				values = append(values, cell.Columns...)

				name := string(values[0]) + "-" + string(values[2])

				firstPagePointer := page.UnmarshalRootPagePointer(cell.Columns)

				rootPagePointers[name] = firstPagePointer
			}

			table := "table-" + tableName
			index := "index-" + tableName

			var results []page.Cell

			if _, ok := rootPagePointers[index]; ok {
				pointer := rootPagePointers[index]
				parsedCreateIndexStatemet := helper.ParseCreateIndexStatement(pointer.CreateStatement)

				if helper.ArrayContain(whereExp[0], parsedCreateIndexStatemet) {
					indexPageNum := uint32(rootPagePointers[index].PageNumber)
					tablePageNum := uint32(rootPagePointers[table].PageNumber)

					var ids []int64

					page.ReadIndex(databaseFile, int(indexPageNum), int(databaseHeader.PageSize), whereExp, &ids)

					results = page.ReadTree(databaseFile, int(tablePageNum), int(databaseHeader.PageSize), &ids)
				}

			} else {
				tablePageNum := uint32(rootPagePointers[table].PageNumber)

				results = page.ReadFullTree(databaseFile, int(tablePageNum), int(databaseHeader.PageSize))
			}

			tableColumnIndexes := helper.GetTableColumnIndex(string(rootPagePointers[table].CreateStatement), col_names)

			filter_col_index := -1

			if whereStatement != "" {
				filter_col_index = helper.GetTableColumnIndex(string(rootPagePointers[table].CreateStatement), []string{whereExp[0]})[0]
			}

			var col_results [][]string

			for _, row := range results {
				// if there is no "WHERE" statement
				// or if the row's column data doesnt match the filter
				// we skip it
				if filter_col_index != -1 && string(row.Columns[filter_col_index]) != whereExp[1] {
					continue
				}

				var col_result []string

				for _, index := range tableColumnIndexes {
					if index == 0 {
						col_result = append(col_result, strconv.Itoa(int(row.CellIdx)))
						continue
					}

					col_result = append(col_result, string(row.Columns[index]))
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
