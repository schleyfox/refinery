package cache

import (
	"fmt"
	"testing"
	"time"

	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeTestTrace(traceID string, sendBy time.Time) *types.Trace {
	return &types.Trace{
		TraceID: traceID,
		SendBy:  sendBy,
		APIKey:  "test-key",
		Dataset: "test-dataset",
	}
}

func TestIndividualSpanBatchSamplingCache_AddAndGet(t *testing.T) {
	m := &metrics.MockMetrics{}
	m.Start()
	c := NewInMemIndividualSpanBatchSamplingCache(100, m, &logger.NullLogger{})

	now := time.Now()
	trace1 := makeTestTrace("trace1", now.Add(time.Minute))
	trace2 := makeTestTrace("trace2", now.Add(time.Minute))

	ok := c.Add(10, "test-reason", "key1", trace1)
	assert.True(t, ok, "should successfully add trace")

	ok = c.Add(20, "test-reason-2", "key2", trace2)
	assert.True(t, ok, "should successfully add second trace")

	kt := c.Get("test-reason", "key1")
	require.NotNil(t, kt)
	require.Len(t, kt.Traces, 1)
	assert.Equal(t, trace1, kt.Traces[0])

	kt = c.Get("test-reason-2", "key2")
	require.NotNil(t, kt)
	require.Len(t, kt.Traces, 1)
	assert.Equal(t, trace2, kt.Traces[0])

	kt = c.Get("nonexistent", "nonexistent")
	assert.Nil(t, kt)
}

func TestIndividualSpanBatchSamplingCache_AddMultipleTracesToSameKey(t *testing.T) {
	m := &metrics.MockMetrics{}
	m.Start()
	c := NewInMemIndividualSpanBatchSamplingCache(100, m, &logger.NullLogger{})

	now := time.Now()
	trace1 := makeTestTrace("trace1", now.Add(time.Minute))
	trace2 := makeTestTrace("trace2", now.Add(time.Minute))
	trace3 := makeTestTrace("trace3", now.Add(time.Minute))

	ok := c.Add(10, "reason1", "key1", trace1)
	assert.True(t, ok)

	ok = c.Add(20, "reason1", "key1", trace2)
	assert.True(t, ok)

	ok = c.Add(15, "reason1", "key1", trace3)
	assert.True(t, ok)

	kt := c.Get("reason1", "key1")
	require.NotNil(t, kt)
	require.Len(t, kt.Traces, 3)
	assert.Contains(t, kt.Traces, trace1)
	assert.Contains(t, kt.Traces, trace2)
	assert.Contains(t, kt.Traces, trace3)
}

func TestIndividualSpanBatchSamplingCache_SampleRateIsMaximum(t *testing.T) {
	m := &metrics.MockMetrics{}
	m.Start()
	c := NewInMemIndividualSpanBatchSamplingCache(100, m, &logger.NullLogger{})

	now := time.Now()
	trace1 := makeTestTrace("trace1", now.Add(time.Minute))
	trace2 := makeTestTrace("trace2", now.Add(time.Minute))
	trace3 := makeTestTrace("trace3", now.Add(time.Minute))

	c.Add(10, "reason", "key1", trace1)
	c.Add(30, "reason", "key1", trace2) // higher sample rate
	c.Add(20, "reason", "key1", trace3)

	all := c.GetAll()
	require.Len(t, all, 1)
	assert.Equal(t, uint(30), all[0].SampleRate, "sample rate should be maximum of all traces")
}

func TestIndividualSpanBatchSamplingCache_ReasonIsFirst(t *testing.T) {
	m := &metrics.MockMetrics{}
	m.Start()
	c := NewInMemIndividualSpanBatchSamplingCache(100, m, &logger.NullLogger{})

	now := time.Now()
	trace1 := makeTestTrace("trace1", now.Add(time.Minute))
	trace2 := makeTestTrace("trace2", now.Add(time.Minute))

	c.Add(10, "first-reason", "key1", trace1)
	c.Add(10, "second-reason", "key1", trace2)

	all := c.GetAll()
	require.Len(t, all, 2, "different reasons create different batches")

	// Check both batches exist
	reasonMap := make(map[string]bool)
	for _, kt := range all {
		reasonMap[kt.BatchKey.Reason] = true
	}
	assert.True(t, reasonMap["first-reason"], "first-reason batch should exist")
	assert.True(t, reasonMap["second-reason"], "second-reason batch should exist")
}

func TestIndividualSpanBatchSamplingCache_GetAll(t *testing.T) {
	m := &metrics.MockMetrics{}
	m.Start()
	c := NewInMemIndividualSpanBatchSamplingCache(100, m, &logger.NullLogger{})

	now := time.Now()
	trace1 := makeTestTrace("trace1", now.Add(time.Minute))
	trace2 := makeTestTrace("trace2", now.Add(time.Minute))
	trace3 := makeTestTrace("trace3", now.Add(time.Minute))

	c.Add(10, "reason", "key1", trace1)
	c.Add(20, "reason", "key2", trace2)
	c.Add(30, "reason", "key1", trace3) // same key as trace1

	all := c.GetAll()
	assert.Len(t, all, 2, "should have 2 unique batch keys")

	// Find key1 and key2 in the results
	var key1Found, key2Found bool
	for _, kt := range all {
		if kt.BatchKey.SamplerKey == "key1" {
			key1Found = true
			assert.Len(t, kt.Traces, 2, "key1 should have 2 traces")
		} else if kt.BatchKey.SamplerKey == "key2" {
			key2Found = true
			assert.Len(t, kt.Traces, 1, "key2 should have 1 trace")
		}
	}
	assert.True(t, key1Found, "key1 should be in results")
	assert.True(t, key2Found, "key2 should be in results")
}

func TestIndividualSpanBatchSamplingCache_Capacity(t *testing.T) {
	m := &metrics.MockMetrics{}
	m.Start()
	capacity := 5
	c := NewInMemIndividualSpanBatchSamplingCache(capacity, m, &logger.NullLogger{})

	assert.Equal(t, capacity, c.GetCacheCapacity())
	assert.Equal(t, 0, c.GetCacheEntryCount())

	now := time.Now()

	// Add 3 traces (under capacity)
	c.Add(10, "reason", "key1", makeTestTrace("trace1", now))
	assert.Equal(t, 1, c.GetCacheEntryCount())
	assert.False(t, c.IsFull())

	c.Add(10, "reason", "key2", makeTestTrace("trace2", now))
	assert.Equal(t, 2, c.GetCacheEntryCount())
	assert.False(t, c.IsFull())

	c.Add(10, "reason", "key3", makeTestTrace("trace3", now))
	assert.Equal(t, 3, c.GetCacheEntryCount())
	assert.False(t, c.IsFull())

	// Add 2 more to reach capacity
	c.Add(10, "reason", "key4", makeTestTrace("trace4", now))
	assert.Equal(t, 4, c.GetCacheEntryCount())
	assert.False(t, c.IsFull())

	c.Add(10, "reason", "key5", makeTestTrace("trace5", now))
	assert.Equal(t, 5, c.GetCacheEntryCount())
	assert.True(t, c.IsFull())

	// Try to add one more - should fail
	ok := c.Add(10, "reason", "key6", makeTestTrace("trace6", now))
	assert.False(t, ok, "adding to full cache should return false")
	assert.Equal(t, 5, c.GetCacheEntryCount())
}

func TestIndividualSpanBatchSamplingCache_CapacityCountsIndividualTraces(t *testing.T) {
	m := &metrics.MockMetrics{}
	m.Start()
	capacity := 5
	c := NewInMemIndividualSpanBatchSamplingCache(capacity, m, &logger.NullLogger{})

	now := time.Now()

	// Add 3 traces to key1 (3 entries total)
	c.Add(10, "reason", "key1", makeTestTrace("trace1", now))
	c.Add(10, "reason", "key1", makeTestTrace("trace2", now))
	c.Add(10, "reason", "key1", makeTestTrace("trace3", now))
	assert.Equal(t, 3, c.GetCacheEntryCount())

	// Add 2 more traces to key2 (5 entries total)
	c.Add(10, "reason", "key2", makeTestTrace("trace4", now))
	c.Add(10, "reason", "key2", makeTestTrace("trace5", now))
	assert.Equal(t, 5, c.GetCacheEntryCount())
	assert.True(t, c.IsFull())

	// Try to add another trace - should fail
	ok := c.Add(10, "reason", "key3", makeTestTrace("trace6", now))
	assert.False(t, ok, "adding to full cache should fail")
}

func TestIndividualSpanBatchSamplingCache_TakeExpiredSpans(t *testing.T) {
	m := &metrics.MockMetrics{}
	m.Start()
	c := NewInMemIndividualSpanBatchSamplingCache(100, m, &logger.NullLogger{})

	now := time.Now()

	// Add traces with different expiration times
	trace1 := makeTestTrace("trace1", now.Add(-2*time.Minute)) // expired
	trace2 := makeTestTrace("trace2", now.Add(-1*time.Minute)) // expired
	trace3 := makeTestTrace("trace3", now.Add(1*time.Minute))  // not expired
	trace4 := makeTestTrace("trace4", now.Add(2*time.Minute))  // not expired

	c.Add(10, "reason", "key1", trace1)
	c.Add(20, "reason", "key2", trace2)
	c.Add(30, "reason", "key3", trace3)
	c.Add(40, "reason", "key4", trace4)

	assert.Equal(t, 4, c.GetCacheEntryCount())

	// Take expired spans
	expired := c.TakeExpiredSpans(now, 0)
	assert.Len(t, expired, 2, "should have 2 expired batches")

	// Check that expired ones are returned
	expiredKeys := make(map[string]bool)
	for _, kt := range expired {
		expiredKeys[kt.BatchKey.SamplerKey] = true
	}
	assert.True(t, expiredKeys["key1"], "key1 should be expired")
	assert.True(t, expiredKeys["key2"], "key2 should be expired")

	// Check that only non-expired remain in cache
	assert.Equal(t, 2, c.GetCacheEntryCount())
	assert.NotNil(t, c.Get("reason", "key3"), "key3 should still be in cache")
	assert.NotNil(t, c.Get("reason", "key4"), "key4 should still be in cache")
	assert.Nil(t, c.Get("reason", "key1"), "key1 should be removed")
	assert.Nil(t, c.Get("reason", "key2"), "key2 should be removed")
}

func TestIndividualSpanBatchSamplingCache_TakeExpiredSpansWithLimit(t *testing.T) {
	m := &metrics.MockMetrics{}
	m.Start()
	c := NewInMemIndividualSpanBatchSamplingCache(100, m, &logger.NullLogger{})

	now := time.Now()

	// Add 3 expired traces
	c.Add(10, "reason", "key1", makeTestTrace("trace1", now.Add(-3*time.Minute)))
	c.Add(20, "reason", "key2", makeTestTrace("trace2", now.Add(-2*time.Minute)))
	c.Add(30, "reason", "key3", makeTestTrace("trace3", now.Add(-1*time.Minute)))

	// Take only 2 expired spans
	expired := c.TakeExpiredSpans(now, 2)
	assert.Len(t, expired, 2, "should return only 2 batches due to limit")

	// One should still remain in cache
	assert.Equal(t, 1, c.GetCacheEntryCount())
}

func TestIndividualSpanBatchSamplingCache_TakeExpiredSpansReturnsOldestFirst(t *testing.T) {
	m := &metrics.MockMetrics{}
	m.Start()
	c := NewInMemIndividualSpanBatchSamplingCache(100, m, &logger.NullLogger{})

	now := time.Now()

	// Add traces with different expiration times (oldest first)
	c.Add(10, "reason", "key1", makeTestTrace("trace1", now.Add(-3*time.Minute)))
	c.Add(20, "reason", "key2", makeTestTrace("trace2", now.Add(-2*time.Minute)))
	c.Add(30, "reason", "key3", makeTestTrace("trace3", now.Add(-1*time.Minute)))

	// Take expired spans one at a time to verify order
	expired := c.TakeExpiredSpans(now, 1)
	require.Len(t, expired, 1)
	assert.Equal(t, "key1", expired[0].BatchKey.SamplerKey, "oldest should be returned first")

	expired = c.TakeExpiredSpans(now, 1)
	require.Len(t, expired, 1)
	assert.Equal(t, "key2", expired[0].BatchKey.SamplerKey, "second oldest should be returned next")

	expired = c.TakeExpiredSpans(now, 1)
	require.Len(t, expired, 1)
	assert.Equal(t, "key3", expired[0].BatchKey.SamplerKey, "third oldest should be returned last")
}

func TestIndividualSpanBatchSamplingCache_TakeExpiredSpansEmptyCache(t *testing.T) {
	m := &metrics.MockMetrics{}
	m.Start()
	c := NewInMemIndividualSpanBatchSamplingCache(100, m, &logger.NullLogger{})

	now := time.Now()
	expired := c.TakeExpiredSpans(now, 0)
	assert.Empty(t, expired, "should return empty slice for empty cache")
}

func TestIndividualSpanBatchSamplingCache_TakeLargestBatch(t *testing.T) {
	m := &metrics.MockMetrics{}
	m.Start()
	c := NewInMemIndividualSpanBatchSamplingCache(100, m, &logger.NullLogger{})

	now := time.Now()

	// Add batches of different sizes
	c.Add(10, "reason", "key1", makeTestTrace("trace1", now))
	c.Add(10, "reason", "key1", makeTestTrace("trace2", now))
	c.Add(10, "reason", "key1", makeTestTrace("trace3", now)) // 3 traces

	c.Add(20, "reason", "key2", makeTestTrace("trace4", now))
	c.Add(20, "reason", "key2", makeTestTrace("trace5", now)) // 2 traces

	c.Add(30, "reason", "key3", makeTestTrace("trace6", now)) // 1 trace

	// Take largest batch
	largest := c.TakeLargestBatch()
	require.NotNil(t, largest)
	assert.Equal(t, "key1", largest.BatchKey.SamplerKey, "key1 has the most traces")
	assert.Len(t, largest.Traces, 3)

	// Verify it's removed from cache
	assert.Nil(t, c.Get("reason", "key1"))
	assert.Equal(t, 3, c.GetCacheEntryCount(), "should have 3 traces left (2 from key2 and 1 from key3)")

	// Take next largest
	largest = c.TakeLargestBatch()
	require.NotNil(t, largest)
	assert.Equal(t, "key2", largest.BatchKey.SamplerKey)
	assert.Len(t, largest.Traces, 2)

	// Take last one
	largest = c.TakeLargestBatch()
	require.NotNil(t, largest)
	assert.Equal(t, "key3", largest.BatchKey.SamplerKey)
	assert.Len(t, largest.Traces, 1)

	// Cache should be empty now
	assert.Equal(t, 0, c.GetCacheEntryCount())
}

func TestIndividualSpanBatchSamplingCache_TakeLargestBatchEmptyCache(t *testing.T) {
	m := &metrics.MockMetrics{}
	m.Start()
	c := NewInMemIndividualSpanBatchSamplingCache(100, m, &logger.NullLogger{})

	largest := c.TakeLargestBatch()
	assert.Nil(t, largest, "should return nil for empty cache")
}

func TestIndividualSpanBatchSamplingCache_TakeLargestBatchUpdatesCacheLen(t *testing.T) {
	m := &metrics.MockMetrics{}
	m.Start()
	c := NewInMemIndividualSpanBatchSamplingCache(100, m, &logger.NullLogger{})

	now := time.Now()

	// Add 5 traces to key1
	for i := 0; i < 5; i++ {
		c.Add(10, "reason", "key1", makeTestTrace(fmt.Sprintf("trace%d", i), now))
	}

	assert.Equal(t, 5, c.GetCacheEntryCount())

	// Take largest batch
	largest := c.TakeLargestBatch()
	require.NotNil(t, largest)
	assert.Len(t, largest.Traces, 5)

	// Cache should be empty and count should be 0
	assert.Equal(t, 0, c.GetCacheEntryCount())
	assert.False(t, c.IsFull())
}

func TestIndividualSpanBatchSamplingCache_TakeExpiredSpansUpdatesCacheLen(t *testing.T) {
	m := &metrics.MockMetrics{}
	m.Start()
	c := NewInMemIndividualSpanBatchSamplingCache(100, m, &logger.NullLogger{})

	now := time.Now()

	// Add 3 traces to key1 (all expired)
	for i := 0; i < 3; i++ {
		c.Add(10, "reason", "key1", makeTestTrace(fmt.Sprintf("trace%d", i), now.Add(-time.Minute)))
	}

	assert.Equal(t, 3, c.GetCacheEntryCount())
	assert.False(t, c.IsFull())

	// Take expired spans
	expired := c.TakeExpiredSpans(now, 0)
	require.Len(t, expired, 1)
	require.Len(t, expired[0].Traces, 3)

	// Cache should be empty and count should be 0
	assert.Equal(t, 0, c.GetCacheEntryCount())
	assert.False(t, c.IsFull())
}

func TestIndividualSpanBatchSamplingCache_KeyedTracesStructure(t *testing.T) {
	m := &metrics.MockMetrics{}
	m.Start()
	c := NewInMemIndividualSpanBatchSamplingCache(100, m, &logger.NullLogger{})

	now := time.Now()
	sendBy := now.Add(time.Minute)
	trace1 := makeTestTrace("trace1", sendBy)

	c.Add(42, "test-reason", "test-key", trace1)

	all := c.GetAll()
	require.Len(t, all, 1)

	kt := all[0]
	assert.Equal(t, "test-key", kt.BatchKey.SamplerKey)
	assert.Equal(t, uint(42), kt.SampleRate)
	assert.Equal(t, "test-reason", kt.BatchKey.Reason)
	assert.Equal(t, sendBy, kt.Expiration)
	assert.Len(t, kt.Traces, 1)
	assert.Equal(t, trace1, kt.Traces[0])
}

func TestIndividualSpanBatchSamplingCache_ExpirationIsFromFirstTrace(t *testing.T) {
	m := &metrics.MockMetrics{}
	m.Start()
	c := NewInMemIndividualSpanBatchSamplingCache(100, m, &logger.NullLogger{})

	now := time.Now()
	firstExpiration := now.Add(time.Minute)
	secondExpiration := now.Add(2 * time.Minute)

	trace1 := makeTestTrace("trace1", firstExpiration)
	trace2 := makeTestTrace("trace2", secondExpiration)

	c.Add(10, "reason", "key1", trace1)
	c.Add(10, "reason", "key1", trace2)

	all := c.GetAll()
	require.Len(t, all, 1)

	// Expiration should be from the first trace
	assert.Equal(t, firstExpiration, all[0].Expiration)
}

// Benchmark tests
func BenchmarkIndividualSpanBatchSamplingCache_Add(b *testing.B) {
	m := &metrics.MockMetrics{}
	m.Start()
	c := NewInMemIndividualSpanBatchSamplingCache(b.N, m, &logger.NullLogger{})

	now := time.Now()
	traces := make([]*types.Trace, b.N)
	for i := 0; i < b.N; i++ {
		traces[i] = makeTestTrace(fmt.Sprintf("trace%d", i), now.Add(time.Duration(i)*time.Second))
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		c.Add(10, "reason", fmt.Sprintf("key%d", i%100), traces[i])
	}
}

func BenchmarkIndividualSpanBatchSamplingCache_Get(b *testing.B) {
	m := &metrics.MockMetrics{}
	m.Start()
	c := NewInMemIndividualSpanBatchSamplingCache(b.N, m, &logger.NullLogger{})

	now := time.Now()
	keys := make([]string, 100)
	for i := 0; i < 100; i++ {
		keys[i] = fmt.Sprintf("key%d", i)
		c.Add(10, "reason", keys[i], makeTestTrace(fmt.Sprintf("trace%d", i), now))
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		c.Get("reason", keys[i%100])
	}
}

func BenchmarkIndividualSpanBatchSamplingCache_TakeExpiredSpans(b *testing.B) {
	m := &metrics.MockMetrics{}
	m.Start()
	c := NewInMemIndividualSpanBatchSamplingCache(b.N, m, &logger.NullLogger{})

	now := time.Now()
	for i := 0; i < 1000; i++ {
		trace := makeTestTrace(fmt.Sprintf("trace%d", i), now.Add(-time.Duration(i)*time.Second))
		c.Add(10, "reason", fmt.Sprintf("key%d", i), trace)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Refill cache periodically
		if c.GetCacheEntryCount() == 0 {
			b.StopTimer()
			for j := 0; j < 1000; j++ {
				trace := makeTestTrace(fmt.Sprintf("trace%d", j), now.Add(-time.Duration(j)*time.Second))
				c.Add(10, "reason", fmt.Sprintf("key%d", j), trace)
			}
			b.StartTimer()
		}
		c.TakeExpiredSpans(now, 10)
	}
}

func BenchmarkIndividualSpanBatchSamplingCache_TakeLargestBatch(b *testing.B) {
	m := &metrics.MockMetrics{}
	m.Start()
	c := NewInMemIndividualSpanBatchSamplingCache(b.N*10, m, &logger.NullLogger{})

	now := time.Now()
	for i := 0; i < 1000; i++ {
		trace := makeTestTrace(fmt.Sprintf("trace%d", i), now)
		c.Add(10, "reason", fmt.Sprintf("key%d", i%100), trace)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Refill cache periodically
		if c.GetCacheEntryCount() == 0 {
			b.StopTimer()
			for j := 0; j < 1000; j++ {
				trace := makeTestTrace(fmt.Sprintf("trace%d", j), now)
				c.Add(10, "reason", fmt.Sprintf("key%d", j%100), trace)
			}
			b.StartTimer()
		}
		c.TakeLargestBatch()
	}
}
