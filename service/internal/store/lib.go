package store

/**
 * The schema for parsing rows received as raw Json
 * Note: row without a `ts` field will be dropped.
 * e.g.
 * {"int":{"ts":1641679041,"count":807},"str":{"event":"init_app"}}
 */
type RawJson struct {
	Int map[string]int64  `json:"int"`
	Str map[string]string `json:"str"`
}

type columnId uint16

// Metadata of a column
type ColumnInfo struct {
	Name       string
	ColumnType ColumnType

	id columnId
}

/**
 * Supported data types
 * Note: `ts` column is stored as an IntColumnType.
 */
type ColumnType = uint8

const (
	NoneColumnType ColumnType = iota
	IntColumnType  ColumnType = iota // int64
	StrColumnType  ColumnType = iota // string
)

// Timestamp column is required and always the first column in the table and in all blocks.
const TS_COLUMN_ID int = 0
const TS_COLUMN_NAME string = "ts"
