package page

import "github/com/codecrafters-io/sqlite-starter-go/app/helper"

type Cell struct {
	LeftChildPageNumber uint32
	CellIdx             uint64
	Columns             [][]byte
}

type Page struct {
	Header PageHeader
	Cells  []Cell
}

type RootPagePointer struct {
	PageType        string
	ObjName         string
	TableName       string
	PageNumber      int64
	CreateStatement string
}

func UnmarshalRootPagePointer(pointerBuffer [][]byte) RootPagePointer {
	// 0 is for type
	// 1 is for name of object created
	// 2 is name of table
	// 3 is for page number
	// 4 is for create statement
	pageType := string(pointerBuffer[0])
	objName := string(pointerBuffer[1])
	tableName := string(pointerBuffer[2])
	pageNum := helper.DecodeTwosCompliment(pointerBuffer[3])
	createStatement := string(pointerBuffer[4])

	return RootPagePointer{
		PageType:        pageType,
		ObjName:         objName,
		TableName:       tableName,
		PageNumber:      pageNum,
		CreateStatement: createStatement,
	}
}
