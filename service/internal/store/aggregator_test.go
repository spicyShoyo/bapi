package store

import (
	"bapi/internal/pb"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAggregator(t *testing.T) {
	setup := debugAggregatorSetup{
		rows: [][]interface{}{
			{1, 2, "ok"},
			{1, 2, "ok"},
			{2, 3, "ok"},
			{1, 4, "ok2"},
		},
		groupbyIntCols: []string{"groupbyIntCol"},
		groupbyStrCols: []string{"groupbyStrCol"},
		aggIntCols:     []string{"aggCol"},
	}

	assertAggregator(t, pb.AggOp_SUM, setup, [][][]interface{}{
		{{1, "ok"}, {4}},
		{{2, "ok"}, {3}},
		{{1, "ok2"}, {4}},
	})

	assertAggregator(t, pb.AggOp_COUNT, setup, [][][]interface{}{
		{{1, "ok"}, {2}},
		{{2, "ok"}, {1}},
		{{1, "ok2"}, {1}},
	})

	assertAggregator(t, pb.AggOp_COUNT_DISTINCT, setup, [][][]interface{}{
		{{1, "ok"}, {1}},
		{{2, "ok"}, {1}},
		{{1, "ok2"}, {1}},
	})
}

type debugAggregatorSetup struct {
	rows           [][]interface{}
	groupbyIntCols []string
	groupbyStrCols []string
	aggIntCols     []string
}

/**
 * Helper for asserting that the aggregation result matches the expected.
 * The `expected`` is in the format of:
 *	 [
 *	 	([...groupbyIntCols, ...groupbyStrCols], aggIntCols), // first group
 *	 	([...groupbyIntCols, ...groupbyStrCols], aggIntCols), // second group
 *	 	...
 *	 ]
 */
func assertAggregator(
	t *testing.T,
	op pb.AggOp,
	s debugAggregatorSetup,
	expected [][][]interface{}) {
	blockRes, strStore := debugNewBlockQueryResult(s.rows)
	aggregator := newAggregator(&aggCtx{
		op:               op,
		groupbyIntColCnt: len(s.groupbyIntCols),
		intColCnt:        len(s.groupbyIntCols) + len(s.aggIntCols),
		groupbyStrColCnt: len(s.groupbyStrCols),
		strColCnt:        len(s.groupbyStrCols),

		groupbyIntColumnNames: s.groupbyIntCols,
		groupbyStrColumnNames: s.groupbyStrCols,
		aggIntColumnNames:     s.aggIntCols,
		strStore:              strStore,
	})

	res, _ := aggregator.aggregate([]*BlockQueryResult{blockRes})
	actual := make([][][]interface{}, res.Count)

	for i := range actual {
		actual[i] = make([][]interface{}, 2)
		actual[i][0] = make([]interface{}, 0) // groupby cols
		actual[i][1] = make([]interface{}, 0) // agg cols

		// fills groupbyIntCols
		for colIdx := 0; colIdx < len(res.IntColumnNames); colIdx++ {
			valIdx := colIdx*int(res.Count) + i
			val := res.IntResult[valIdx]
			hasVal := res.IntHasValue[valIdx]
			if hasVal {
				actual[i][0] = append(actual[i][0], int(val)) // since it's int64
			} else {
				actual[i][0] = append(actual[i][0], 0)
			}
		}

		// fills groupbyStrCols
		for colIdx := 0; colIdx < len(res.StrColumnNames); colIdx++ {
			valIdx := colIdx*int(res.Count) + i
			val := res.StrResult[valIdx]
			hasVal := res.StrHasValue[valIdx]
			if hasVal {
				str, _ := res.StrIdMap[val]
				actual[i][0] = append(actual[i][0], str)
			} else {
				actual[i][0] = append(actual[i][0], "")
			}

		}

		// fills aggIntCols
		for colIdx := 0; colIdx < len(res.IntColumnNames); colIdx++ {
			valIdx := colIdx*int(res.Count) + i
			val := res.AggIntResult[valIdx]
			hasVal := res.AggIntHasValue[valIdx]
			if hasVal {
				actual[i][1] = append(actual[i][1], int(val))
			} else {
				actual[i][1] = append(actual[i][1], 0)
			}
		}
	}

	assert.ElementsMatch(t, expected, actual)
}
