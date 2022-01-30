package store

import (
	"bapi/internal/common"
	"bapi/internal/pb"
	"bufio"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

var JSON_PATH string = "./fixtures/log.json"

func TestScan(t *testing.T) {
	cur_str := "{\"int\":{\"ts\":1641712510,\"count\":726},\"str\":{\"event\":\"edit\",\"message\":\"hi\"}}\n{\"int\":{\"ts\":1641712510,\"count\":726},\"str\":{\"event\":\"edit\",\"message\":\"hi\"}}"
	scanner := bufio.NewScanner(strings.NewReader(cur_str))
	var rawJson RawJson
	for scanner.Scan() {
		err := json.Unmarshal(scanner.Bytes(), &rawJson)
		assert.Nil(t, err)
	}
}

func TestCreate(t *testing.T) {
	table := NewTable(common.NewBapiCtx(), "asd")
	table.IngestFile(JSON_PATH)
}

func TestQuery(t *testing.T) {
	table := debugNewPrefilledTable([]RawJson{
		{
			Int: map[string]int64{"ts": 1643175607},
			Str: map[string]string{"event": "init_app"},
		},
		{
			Int: map[string]int64{"ts": 1643175609, "count": 1},
			Str: map[string]string{"event": "publish"},
		},
		{
			Int: map[string]int64{"ts": 1643175611, "count": 1},
			Str: map[string]string{"event": "create", "source": "modal"},
		},
	})

	maxTs := int64(1643175611)
	result, _ := table.RowsQuery(
		&pb.RowsQuery{
			MinTs: 1643175607,
			MaxTs: &maxTs,
			IntFilters: []*pb.Filter{
				{
					ColumnName: "count",
					FilterOp:   pb.FilterOp_EQ,
					Value:      &pb.Filter_IntVal{IntVal: 1},
				},
			},
			StrFilters: []*pb.Filter{
				{
					ColumnName: "event",
					FilterOp:   pb.FilterOp_EQ,
					Value:      &pb.Filter_StrVal{StrVal: "create"},
				},
			},
			IntColumnNames: []string{"ts"},
			StrColumnNames: []string{"source"},
		},
	)

	var curStrId uint32
	for k, v := range result.StrIdMap {
		if v == "modal" {
			curStrId = k
		}
	}

	assert.EqualValues(t, &pb.RowsQueryResult{
		Count:          1,
		IntColumnNames: []string{"ts"},
		IntResult:      []int64{1643175611},
		IntHasValue:    []bool{true},

		StrColumnNames: []string{"source"},
		StrIdMap:       map[uint32]string{curStrId: "modal"},
		StrResult:      []uint32{curStrId},
		StrHasValue:    []bool{true},
	}, result)
}

func debugNewPrefilledTable(rawRows []RawJson) *Table {
	table := NewTable(common.NewBapiCtx(), "asd")
	ingester := table.ingesterPool.Get().(*ingester)
	for _, rawRow := range rawRows {
		ingester.ingestRawJson(
			table,
			rawRow,
		)
	}

	pb, _ := ingester.buildPartialBlock()
	table.addPartialBlock(pb)
	return table
}
