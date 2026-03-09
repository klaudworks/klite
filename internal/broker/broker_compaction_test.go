package broker

import (
	"testing"
	"time"

	"github.com/klaudworks/klite/internal/clock"
	"github.com/klaudworks/klite/internal/cluster"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeTopic creates a TopicData with the given configs and one partition per
// entry in the dirtyPerPartition slice.
func makeTopic(name string, configs map[string]string, dirtyPerPartition []int32) *cluster.TopicData {
	partitions := make([]*cluster.PartData, len(dirtyPerPartition))
	for i, dirty := range dirtyPerPartition {
		pd := &cluster.PartData{
			Topic: name,
			Index: int32(i),
		}
		pd.Lock()
		pd.SetDirtyObjects(dirty)
		pd.Unlock()
		partitions[i] = pd
	}
	td := &cluster.TopicData{
		Name:       name,
		Partitions: partitions,
		Configs:    make(map[string]string),
	}
	for k, v := range configs {
		td.Configs[k] = v
	}
	return td
}

// setLastCompacted sets the lastCompacted time on a partition by resetting
// dirty objects to a known time, then restoring the dirty count.
func setLastCompacted(pd *cluster.PartData, t time.Time, dirty int32) {
	pd.Lock()
	pd.ResetDirtyObjects(t)
	pd.SetDirtyObjects(dirty)
	pd.Unlock()
}

func TestSelectDirtyPartition_SkipsDeletePolicy(t *testing.T) {
	td := makeTopic("delete-only", map[string]string{
		"cleanup.policy": "delete",
	}, []int32{100})

	clk := clock.NewFakeClock(time.Now())
	gotTD, gotPD := selectDirtyPartition([]*cluster.TopicData{td}, 1, clk)
	assert.Nil(t, gotTD)
	assert.Nil(t, gotPD)
}

func TestSelectDirtyPartition_SkipsDefaultPolicy(t *testing.T) {
	// No cleanup.policy set at all — defaults to "delete"
	td := makeTopic("no-policy", nil, []int32{100})

	clk := clock.NewFakeClock(time.Now())
	gotTD, gotPD := selectDirtyPartition([]*cluster.TopicData{td}, 1, clk)
	assert.Nil(t, gotTD)
	assert.Nil(t, gotPD)
}

func TestSelectDirtyPartition_CompactDeletePolicyEligible(t *testing.T) {
	td := makeTopic("compact-delete", map[string]string{
		"cleanup.policy": "compact,delete",
	}, []int32{10})

	clk := clock.NewFakeClock(time.Now())
	gotTD, gotPD := selectDirtyPartition([]*cluster.TopicData{td}, 5, clk)
	require.NotNil(t, gotTD)
	require.NotNil(t, gotPD)
	assert.Equal(t, "compact-delete", gotTD.Name)
}

func TestSelectDirtyPartition_BelowMinDirtySkipped(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	td := makeTopic("compacted", map[string]string{
		"cleanup.policy": "compact",
	}, []int32{3})
	// Mark as previously compacted so it's not a never-compacted case
	setLastCompacted(td.Partitions[0], now, 3)

	clk := clock.NewFakeClock(now.Add(time.Second))
	gotTD, gotPD := selectDirtyPartition([]*cluster.TopicData{td}, 10, clk)
	assert.Nil(t, gotTD)
	assert.Nil(t, gotPD)
}

func TestSelectDirtyPartition_StalePartitionEligible(t *testing.T) {
	compactedAt := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	td := makeTopic("stale", map[string]string{
		"cleanup.policy":        "compact",
		"max.compaction.lag.ms": "60000", // 60 seconds
	}, []int32{2})
	setLastCompacted(td.Partitions[0], compactedAt, 2)

	// Clock is 61 seconds after last compaction — exceeds lag
	clk := clock.NewFakeClock(compactedAt.Add(61 * time.Second))
	gotTD, gotPD := selectDirtyPartition([]*cluster.TopicData{td}, 10, clk)
	require.NotNil(t, gotTD)
	require.NotNil(t, gotPD)
	assert.Equal(t, "stale", gotTD.Name)
}

func TestSelectDirtyPartition_StaleButNotExceeded(t *testing.T) {
	compactedAt := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	td := makeTopic("not-stale", map[string]string{
		"cleanup.policy":        "compact",
		"max.compaction.lag.ms": "60000",
	}, []int32{2})
	setLastCompacted(td.Partitions[0], compactedAt, 2)

	// Clock is only 30 seconds after — lag not exceeded
	clk := clock.NewFakeClock(compactedAt.Add(30 * time.Second))
	gotTD, gotPD := selectDirtyPartition([]*cluster.TopicData{td}, 10, clk)
	assert.Nil(t, gotTD)
	assert.Nil(t, gotPD)
}

func TestSelectDirtyPartition_NeverCompactedAlwaysEligible(t *testing.T) {
	td := makeTopic("never-compacted", map[string]string{
		"cleanup.policy": "compact",
	}, []int32{1}) // dirty=1, lastCompacted is zero

	clk := clock.NewFakeClock(time.Now())
	gotTD, gotPD := selectDirtyPartition([]*cluster.TopicData{td}, 100, clk)
	require.NotNil(t, gotTD)
	require.NotNil(t, gotPD)
	assert.Equal(t, "never-compacted", gotTD.Name)
}

func TestSelectDirtyPartition_NeverCompactedZeroDirtySkipped(t *testing.T) {
	td := makeTopic("never-compacted-clean", map[string]string{
		"cleanup.policy": "compact",
	}, []int32{0}) // dirty=0, lastCompacted is zero

	clk := clock.NewFakeClock(time.Now())
	gotTD, gotPD := selectDirtyPartition([]*cluster.TopicData{td}, 100, clk)
	assert.Nil(t, gotTD)
	assert.Nil(t, gotPD)
}

func TestSelectDirtyPartition_HighestDirtySelected(t *testing.T) {
	td := makeTopic("multi", map[string]string{
		"cleanup.policy": "compact",
	}, []int32{5, 20, 10})

	clk := clock.NewFakeClock(time.Now())
	gotTD, gotPD := selectDirtyPartition([]*cluster.TopicData{td}, 1, clk)
	require.NotNil(t, gotTD)
	require.NotNil(t, gotPD)
	assert.Equal(t, int32(1), gotPD.Index, "partition with 20 dirty objects should be selected")
}

func TestSelectDirtyPartition_HighestDirtyAcrossTopics(t *testing.T) {
	td1 := makeTopic("topic-a", map[string]string{
		"cleanup.policy": "compact",
	}, []int32{5})
	td2 := makeTopic("topic-b", map[string]string{
		"cleanup.policy": "compact",
	}, []int32{15})

	clk := clock.NewFakeClock(time.Now())
	gotTD, gotPD := selectDirtyPartition([]*cluster.TopicData{td1, td2}, 1, clk)
	require.NotNil(t, gotTD)
	require.NotNil(t, gotPD)
	assert.Equal(t, "topic-b", gotTD.Name)
	assert.Equal(t, int32(0), gotPD.Index)
}

func TestSelectDirtyPartition_NoTopicsReturnsNil(t *testing.T) {
	clk := clock.NewFakeClock(time.Now())
	gotTD, gotPD := selectDirtyPartition(nil, 1, clk)
	assert.Nil(t, gotTD)
	assert.Nil(t, gotPD)
}
