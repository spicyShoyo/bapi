package store

import (
	"bapi/internal/common"
	"bapi/internal/pb"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAggregator(t *testing.T) {
	setup := debugAggregatorSetup{
		rows: [][]interface{}{
			{1, 2, "ok"},
			{1, 2, "ok"},
			{1, 5, "ok"},
			{2, 3, "ok"},
			{1, 4, "ok2"},
		},
		groupbyIntCols: []string{"groupbyIntCol"},
		groupbyStrCols: []string{"groupbyStrCol"},
		aggIntCols:     []string{"aggCol"},
	}

	assertAggregatorForTableQuery(t, pb.AggOp_SUM, setup, [][][]interface{}{
		{{1, "ok"}, {9}},
		{{2, "ok"}, {3}},
		{{1, "ok2"}, {4}},
	})

	assertAggregatorForTableQuery(t, pb.AggOp_COUNT, setup, [][][]interface{}{
		{{1, "ok"}, {3}},
		{{2, "ok"}, {1}},
		{{1, "ok2"}, {1}},
	})

	assertAggregatorForTableQuery(t, pb.AggOp_COUNT_DISTINCT, setup, [][][]interface{}{
		{{1, "ok"}, {2}},
		{{2, "ok"}, {1}},
		{{1, "ok2"}, {1}},
	})

	assertAggregatorForTableQuery(t, pb.AggOp_AVG, setup, [][][]interface{}{
		{{1, "ok"}, {3.0}},
		{{2, "ok"}, {3.0}},
		{{1, "ok2"}, {4.0}},
	})
}

func TestAggregatorForTimelineQuery(t *testing.T) {
	setup := debugAggregatorSetup{
		rows: [][]interface{}{
			{1, 2},
			{1, 500},
		},
		groupbyIntCols: []string{"groupbyIntCol"},
		groupbyStrCols: []string{},
	}

	assertAggregatorForTimelineQuery(t, pb.TimeGran_MIN_5, setup, [][][]interface{}{
		{{1}, {0, 1, 1, 1}},
	})

	assertAggregatorForTimelineQuery(t, pb.TimeGran_MIN_15, setup, [][][]interface{}{
		{{1}, {0, 2}},
	})
}

type debugAggregatorSetup struct {
	rows           [][]interface{}
	groupbyIntCols []string
	groupbyStrCols []string
	aggIntCols     []string // for tableQuery
	gran           uint64   // for timelineQuery
}

/**
 * Helper for asserting that the timelineQuery aggregation result matches the expected.
 * StartTs is 0.
 * The `expected` is in the format of:
 *	 [
 *	 	([...groupbyIntCols, ...groupbyStrCols], [tsBuckets[0], counts[0], tsBuckets[1], counts[1], ...]),
 *	 	...
 *	 ]
 */
func assertAggregatorForTimelineQuery(
	t *testing.T,
	gran pb.TimeGran,
	s debugAggregatorSetup,
	expected [][][]interface{}) {
	blockRes, strStore := debugNewBlockQueryResult(s.rows)

	aggIntCols := []string{TS_COLUMN_NAME}
	intColCnt := len(s.groupbyIntCols) + len(aggIntCols)

	aggregator := newAggregator(&aggCtx{
		logger:           common.NewTestBapiCtx().Logger,
		op:               pb.AggOp_TIMELINE_COUNT,
		groupbyIntColCnt: len(s.groupbyIntCols),
		intColCnt:        intColCnt,
		groupbyStrColCnt: len(s.groupbyStrCols),
		strColCnt:        len(s.groupbyStrCols),

		groupbyIntColumnNames: s.groupbyIntCols,
		groupbyStrColumnNames: s.groupbyStrCols,
		aggIntColumnNames:     aggIntCols,
		strStore:              strStore,

		isTimelineQuery: true,
		startTs:         0,
		gran:            uint64(gran),
	})

	res, _ := aggregator.aggregateForTimeline([]*BlockQueryResult{blockRes})
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

		for _, timelineGroup := range res.TimelineGroups {
			for tsBucketIdx := 0; tsBucketIdx < len(timelineGroup.TsBuckets); tsBucketIdx++ {
				actual[i][1] = append(actual[i][1], int(timelineGroup.TsBuckets[tsBucketIdx]))
				actual[i][1] = append(actual[i][1], int(timelineGroup.Counts[tsBucketIdx]))
			}
		}
	}

	assert.ElementsMatch(t, expected, actual)
}

/**
 * Helper for asserting that the aggregation result matches the expected.
 * The `expected` is in the format of:
 *	 [
 *	 	([...groupbyIntCols, ...groupbyStrCols], aggIntCols), // first group
 *	 	([...groupbyIntCols, ...groupbyStrCols], aggIntCols), // second group
 *	 	...
 *	 ]
 */
func assertAggregatorForTableQuery(
	t *testing.T,
	op pb.AggOp,
	s debugAggregatorSetup,
	expected [][][]interface{}) {
	blockRes, strStore := debugNewBlockQueryResult(s.rows)
	aggregator := newAggregator(&aggCtx{
		logger:           common.NewTestBapiCtx().Logger,
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

	res, _ := aggregator.aggregateForTableQuery([]*BlockQueryResult{blockRes})
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
		for colIdx := 0; colIdx < len(res.AggIntColumnNames); colIdx++ {
			valIdx := colIdx*int(res.Count) + i
			val := res.AggIntResult[valIdx]
			hasVal := res.AggIntHasValue[valIdx]
			if hasVal {
				actual[i][1] = append(actual[i][1], int(val))
			} else {
				actual[i][1] = append(actual[i][1], 0)
			}
		}

		// fills aggFloatCols
		for colIdx := 0; colIdx < len(res.AggFloatColumnNames); colIdx++ {
			valIdx := colIdx*int(res.Count) + i
			val := res.AggFloatResult[valIdx]
			hasVal := res.AggFloatHasValue[valIdx]
			if hasVal {
				actual[i][1] = append(actual[i][1], val)
			} else {
				actual[i][1] = append(actual[i][1], 0)
			}
		}
	}

	assert.ElementsMatch(t, expected, actual)
}
