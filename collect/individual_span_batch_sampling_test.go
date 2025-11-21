package collect

import (
	"fmt"
	"testing"
	"time"

	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/health"
	"github.com/honeycombio/refinery/internal/peer"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/pubsub"
	"github.com/honeycombio/refinery/sample"
	"github.com/honeycombio/refinery/sharder"
	"github.com/honeycombio/refinery/transmit"
	"github.com/honeycombio/refinery/types"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
)

// TestKeySampler is a simple sampler for testing that returns a specified field value as the key
type TestKeySampler struct {
	keyField   string
	sampleRate uint
}

func (t *TestKeySampler) Start() error {
	return nil
}

func (t *TestKeySampler) GetSampleRate(trace *types.Trace) (rate uint, keep bool, reason string, key string) {
	// Extract the key from the first span's data
	if len(trace.GetSpans()) > 0 {
		span := trace.GetSpans()[0]
		if val, ok := span.Data[t.keyField]; ok {
			key = fmt.Sprintf("%v", val)
		}
	}
	return t.sampleRate, false, "test-sampler", key
}

func (t *TestKeySampler) GetKeyFields() []string {
	return []string{t.keyField}
}

func TestSampleIndividualSpansBatch_BasicSampling(t *testing.T) {
	tests := []struct {
		name            string
		numTraces       int
		desiredRate     uint
		expectedSamples int
	}{
		{"no traces", 0, 1, 0},
		{"single trace rate 1", 1, 1, 1},
		{"single trace rate 2", 1, 2, 1},
		{"10 traces rate 1", 10, 1, 10},
		{"10 traces rate 2", 10, 2, 5},
		{"10 traces rate 3", 10, 3, 4},
		{"10 traces rate 4", 10, 4, 3},
		{"10 traces rate 5", 10, 5, 2},
		{"10 traces rate 10", 10, 10, 1},
		{"100 traces rate 7", 100, 7, 15},
		{"7 traces rate 3", 7, 3, 3},
		{"3 traces rate 7", 3, 7, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keyedTraces := makeKeyedTraces(tt.numTraces, tt.desiredRate)
			if tt.numTraces == 0 {
				keyedTraces.Traces = nil
			}

			result := sampleIndividualSpansBatch(keyedTraces)

			if tt.numTraces == 0 {
				assert.Nil(t, result, "empty input should return nil")
				return
			}

			assert.Len(t, result, tt.expectedSamples, "should select correct number of samples")

			// Verify sum of sample rates equals original trace count
			sumSampleRates := 0
			for _, trace := range result {
				sumSampleRates += int(trace.SampleRate())
			}
			assert.Equal(t, tt.numTraces, sumSampleRates,
				"sum of sample rates should equal original trace count")

			// Verify all selected traces are unique
			seen := make(map[string]bool)
			for _, trace := range result {
				assert.False(t, seen[trace.TraceID], "each trace should only appear once")
				seen[trace.TraceID] = true
			}

			// Verify all selected traces are from the original set
			for _, trace := range result {
				found := false
				for _, original := range keyedTraces.Traces {
					if trace.TraceID == original.TraceID {
						found = true
						break
					}
				}
				assert.True(t, found, "selected trace should be from original set")
			}
		})
	}
}

func TestSampleIndividualSpansBatch_SampleRateDistribution(t *testing.T) {
	// Test that sample rates are distributed correctly when there's a remainder
	// For n=10, rate=3: m=4 samples, baseSampleRate=2, remainder=2
	// Should get 2 traces with rate 3, and 2 traces with rate 2
	keyedTraces := makeKeyedTraces(10, 3)
	result := sampleIndividualSpansBatch(keyedTraces)

	assert.Len(t, result, 4)

	rateCount := make(map[uint]int)
	sumRates := 0
	for _, trace := range result {
		rateCount[trace.SampleRate()]++
		sumRates += int(trace.SampleRate())
	}

	assert.Equal(t, 10, sumRates, "sum of rates should be 10")
	// We should have rates of 2 and 3
	assert.Contains(t, rateCount, uint(2), "should have traces with rate 2")
	assert.Contains(t, rateCount, uint(3), "should have traces with rate 3")
	// 2 traces with rate 3 (=6) + 2 traces with rate 2 (=4) = 10
	assert.Equal(t, 2, rateCount[3], "should have 2 traces with rate 3")
	assert.Equal(t, 2, rateCount[2], "should have 2 traces with rate 2")
}

func TestSampleIndividualSpansBatch_EdgeCases(t *testing.T) {
	t.Run("rate equals count", func(t *testing.T) {
		keyedTraces := makeKeyedTraces(5, 5)
		result := sampleIndividualSpansBatch(keyedTraces)
		assert.Len(t, result, 1, "should select 1 sample")
		assert.Equal(t, uint(5), result[0].SampleRate(), "rate should be 5")
	})

	t.Run("rate greater than count", func(t *testing.T) {
		keyedTraces := makeKeyedTraces(3, 10)
		result := sampleIndividualSpansBatch(keyedTraces)
		assert.Len(t, result, 1, "should select 1 sample")
		assert.Equal(t, uint(3), result[0].SampleRate(), "rate should equal count")
	})

	t.Run("rate is zero", func(t *testing.T) {
		keyedTraces := makeKeyedTraces(5, 0)
		result := sampleIndividualSpansBatch(keyedTraces)
		assert.Len(t, result, 5, "should select all traces when rate is 0")
		sumRates := 0
		for _, trace := range result {
			sumRates += int(trace.SampleRate())
		}
		assert.Equal(t, 5, sumRates, "sum should equal count")
	})

	t.Run("large numbers", func(t *testing.T) {
		keyedTraces := makeKeyedTraces(1000, 7)
		result := sampleIndividualSpansBatch(keyedTraces)

		sumRates := 0
		for _, trace := range result {
			sumRates += int(trace.SampleRate())
		}
		assert.Equal(t, 1000, sumRates, "sum of rates should equal count")
	})
}

func TestSampleIndividualSpansBatch_FloydAlgorithmCorrectness(t *testing.T) {
	// Run the algorithm many times to verify statistical properties
	const runs = 100
	const n = 20
	const desiredRate = 4
	expectedSamples := 5 // ceil(20/4) = 5

	selectionCounts := make(map[string]int)

	for run := 0; run < runs; run++ {
		keyedTraces := makeKeyedTraces(n, desiredRate)
		result := sampleIndividualSpansBatch(keyedTraces)

		require.Len(t, result, expectedSamples, "each run should select 5 samples")

		sumRates := 0
		for _, trace := range result {
			sumRates += int(trace.SampleRate())
			selectionCounts[trace.TraceID]++
		}
		assert.Equal(t, n, sumRates, "sum should always equal n")
	}

	// Each trace should have been selected roughly runs*expectedSamples/n times
	// For 100 runs, 20 traces, 5 selections each = each trace should be selected ~25 times
	expectedPerTrace := float64(runs*expectedSamples) / float64(n)

	for i := 0; i < n; i++ {
		traceID := fmt.Sprintf("trace-%d", i)
		count := selectionCounts[traceID]
		// Allow 50% variance from expected (very generous for small sample)
		assert.InDelta(t, expectedPerTrace, count, expectedPerTrace*0.5,
			"trace %s selected %d times, expected ~%.1f", traceID, count, expectedPerTrace)
	}
}

func TestSampleIndividualSpansBatch_VerifyNoIndexOutOfBounds(t *testing.T) {
	// Test various sizes to ensure Floyd's algorithm doesn't access invalid indices
	testCases := []struct {
		n    int
		rate uint
	}{
		{1, 1},
		{1, 2},
		{2, 1},
		{2, 2},
		{5, 3},
		{3, 5},
		{100, 1},
		{100, 100},
		{100, 99},
		{100, 101},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("n=%d_rate=%d", tc.n, tc.rate), func(t *testing.T) {
			keyedTraces := makeKeyedTraces(tc.n, tc.rate)

			// This should not panic
			result := sampleIndividualSpansBatch(keyedTraces)

			// Verify results
			if result == nil {
				return
			}

			sumRates := 0
			for _, trace := range result {
				sumRates += int(trace.SampleRate())
			}
			assert.Equal(t, tc.n, sumRates, "sum should equal n")

			// Verify all indices are valid
			for _, trace := range result {
				found := false
				for _, original := range keyedTraces.Traces {
					if trace.TraceID == original.TraceID {
						found = true
						break
					}
				}
				assert.True(t, found, "selected trace must be from original set")
			}
		})
	}
}

func TestProcessIndividualSpan_WithBatchSampling(t *testing.T) {
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(2 * time.Millisecond),
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(60 * time.Second),
			MaxBatchSize: 500,
		},
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 2},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			CacheCapacity:                            10,
			ShutdownDelay:                            config.Duration(1 * time.Millisecond),
			UseIndividualSpanBatchSampling:           true,
			IndividualSpanBatchSamplingCacheCapacity: 100,
			IndividualSpanBatchSamplingWindow:        config.Duration(15 * time.Second),
		},
	}

	transmission := &transmit.MockTransmission{}
	transmission.Start()
	defer transmission.Stop()

	coll := newTestCollectorForIndividualSpan(conf, transmission)

	c := cache.NewInMemCache(100, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c

	individualCache := cache.NewInMemIndividualSpanBatchSamplingCache(100, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.individualSpanBatchSamplingCache = individualCache

	stc, err := newCache()
	require.NoError(t, err)
	coll.sampleTraceCache = stc

	coll.incoming = make(chan *types.Span, 5)
	coll.incomingIndividualSpan = make(chan *types.Span, 20)
	coll.fromPeer = make(chan *types.Span, 5)
	coll.outgoingTraces = make(chan sendableTrace, 20)

	// Use test sampler that groups by "batch_key" field with rate 2
	testSampler := &TestKeySampler{keyField: "batch_key", sampleRate: 2}
	coll.datasetSamplers = map[string]sample.Sampler{"test-dataset": testSampler}

	go coll.collect()
	go coll.sendTraces()
	defer coll.Stop()

	// Add 6 spans: 3 with key "batch-1", 3 with key "batch-2"
	for i := 0; i < 6; i++ {
		span := &types.Span{
			TraceID: fmt.Sprintf("trace-%d", i),
			Event: types.Event{
				Dataset: "test-dataset",
				APIKey:  legacyAPIKey,
				Data:    make(map[string]interface{}),
			},
			IsRoot: true,
		}
		if i < 3 {
			span.Data["batch_key"] = "batch-1"
		} else {
			span.Data["batch_key"] = "batch-2"
		}
		coll.AddIndividualSpan(span)
	}

	// Give it a moment to process
	time.Sleep(10 * time.Millisecond)

	// Verify spans are in cache, grouped into 2 batches, not sent yet
	assert.Equal(t, 6, individualCache.GetCacheEntryCount(), "6 spans should be buffered")

	// Verify they're grouped into 2 distinct batches
	allBatches := individualCache.GetAll()
	assert.Len(t, allBatches, 2, "should have 2 batches (one per key)")

	// Each batch should have 3 spans
	for _, batch := range allBatches {
		assert.Len(t, batch.Traces, 3, "each batch should have 3 spans")
		assert.Contains(t, []string{"batch-1", "batch-2"}, batch.BatchKey.SamplerKey, "key should be batch-1 or batch-2")
	}

	assert.Equal(t, 0, len(transmission.Events), "no spans should be sent yet")
}

func TestProcessIndividualSpan_WithoutBatchSampling(t *testing.T) {
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(2 * time.Millisecond),
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(60 * time.Second),
			MaxBatchSize: 500,
		},
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			CacheCapacity:                  10,
			ShutdownDelay:                  config.Duration(1 * time.Millisecond),
			UseIndividualSpanBatchSampling: false,
		},
	}

	transmission := &transmit.MockTransmission{}
	transmission.Start()
	defer transmission.Stop()

	coll := newTestCollectorForIndividualSpan(conf, transmission)

	c := cache.NewInMemCache(100, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c

	stc, err := newCache()
	require.NoError(t, err)
	coll.sampleTraceCache = stc

	coll.incoming = make(chan *types.Span, 5)
	coll.incomingIndividualSpan = make(chan *types.Span, 5)
	coll.fromPeer = make(chan *types.Span, 5)
	coll.outgoingTraces = make(chan sendableTrace, 5)
	coll.datasetSamplers = make(map[string]sample.Sampler)

	go coll.collect()
	go coll.sendTraces()
	defer coll.Stop()

	// Add individual spans
	for i := 0; i < 5; i++ {
		span := &types.Span{
			TraceID: fmt.Sprintf("individual-trace-%d", i),
			Event: types.Event{
				Dataset: "test-dataset",
				APIKey:  legacyAPIKey,
				Data:    make(map[string]interface{}),
			},
			IsRoot: true,
		}
		coll.AddIndividualSpan(span)
	}

	// Spans should be sent immediately
	events := transmission.GetBlock(5)
	assert.Equal(t, 5, len(events), "all spans should be sent immediately without batch sampling")
}

func TestSendExpiredIndividualSpansBatchSampling(t *testing.T) {
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:       config.Duration(2 * time.Millisecond),
			SendDelay:        config.Duration(1 * time.Millisecond),
			TraceTimeout:     config.Duration(60 * time.Second),
			MaxBatchSize:     500,
			MaxExpiredTraces: 100,
		},
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 2},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			CacheCapacity:                            100,
			ShutdownDelay:                            config.Duration(1 * time.Millisecond),
			UseIndividualSpanBatchSampling:           true,
			IndividualSpanBatchSamplingCacheCapacity: 100,
			IndividualSpanBatchSamplingWindow:        config.Duration(15 * time.Second),
		},
	}

	transmission := &transmit.MockTransmission{}
	transmission.Start()
	defer transmission.Stop()

	clock := clockwork.NewFakeClock()
	coll := newTestCollectorForIndividualSpan(conf, transmission)
	coll.Clock = clock

	c := cache.NewInMemCache(100, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c

	individualCache := cache.NewInMemIndividualSpanBatchSamplingCache(100, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.individualSpanBatchSamplingCache = individualCache

	stc, err := newCache()
	require.NoError(t, err)
	coll.sampleTraceCache = stc

	coll.incoming = make(chan *types.Span, 5)
	coll.incomingIndividualSpan = make(chan *types.Span, 50)
	coll.fromPeer = make(chan *types.Span, 5)
	coll.outgoingTraces = make(chan sendableTrace, 50)

	// Use test sampler that groups by "batch_key" field with rate 2
	testSampler := &TestKeySampler{keyField: "batch_key", sampleRate: 2}
	coll.datasetSamplers = map[string]sample.Sampler{"test-dataset": testSampler}

	go coll.collect()
	go coll.sendTraces()
	defer coll.Stop()

	// Add 10 spans with same key, expect 5 to be kept (rate=2)
	for i := 0; i < 10; i++ {
		span := &types.Span{
			TraceID: fmt.Sprintf("batch-trace-%d", i),
			Event: types.Event{
				Dataset: "test-dataset",
				APIKey:  legacyAPIKey,
				Data:    make(map[string]interface{}),
			},
			IsRoot: true,
		}
		span.Data["batch_key"] = "batch-1"
		coll.AddIndividualSpan(span)
	}

	// Wait for processing
	time.Sleep(10 * time.Millisecond)

	// Verify spans are in cache as single batch
	assert.Equal(t, 10, individualCache.GetCacheEntryCount(), "all 10 spans should be in cache")
	allBatches := individualCache.GetAll()
	require.Len(t, allBatches, 1, "should be single batch")
	assert.Equal(t, "batch-1", allBatches[0].BatchKey.SamplerKey, "batch key should be batch-1")
	assert.Len(t, allBatches[0].Traces, 10, "batch should have 10 traces")

	// Advance time past expiration (15 seconds is the batch window)
	clock.Advance(20 * time.Second)

	// Trigger expiration check
	time.Sleep(10 * time.Millisecond)

	// Wait for spans to be sent
	events := transmission.GetBlock(5)
	assert.Equal(t, 5, len(events), "should send 5 spans (rate=2 means keep half)")

	// Verify cache is empty
	assert.Equal(t, 0, individualCache.GetCacheEntryCount(), "cache should be empty after expiration")

	// Verify sum of sample rates equals original count
	sumRates := 0
	for _, event := range events {
		sumRates += int(event.SampleRate)
	}
	assert.Equal(t, 10, sumRates, "sum of sample rates should equal original span count")
}

func TestCacheFullBehavior(t *testing.T) {
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:       config.Duration(2 * time.Millisecond),
			SendDelay:        config.Duration(1 * time.Millisecond),
			TraceTimeout:     config.Duration(60 * time.Second),
			MaxBatchSize:     500,
			MaxExpiredTraces: 100,
		},
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 2},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			CacheCapacity:                            10,
			ShutdownDelay:                            config.Duration(1 * time.Millisecond),
			UseIndividualSpanBatchSampling:           true,
			IndividualSpanBatchSamplingCacheCapacity: 10,
			IndividualSpanBatchSamplingWindow:        config.Duration(15 * time.Second),
		},
	}

	transmission := &transmit.MockTransmission{}
	transmission.Start()
	defer transmission.Stop()

	coll := newTestCollectorForIndividualSpan(conf, transmission)

	c := cache.NewInMemCache(100, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c

	// Create a small cache that will fill up quickly
	individualCache := cache.NewInMemIndividualSpanBatchSamplingCache(10, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.individualSpanBatchSamplingCache = individualCache

	stc, err := newCache()
	require.NoError(t, err)
	coll.sampleTraceCache = stc

	coll.incoming = make(chan *types.Span, 5)
	coll.incomingIndividualSpan = make(chan *types.Span, 50)
	coll.fromPeer = make(chan *types.Span, 5)
	coll.outgoingTraces = make(chan sendableTrace, 50)

	// Use test sampler with rate 2
	testSampler := &TestKeySampler{keyField: "batch_key", sampleRate: 2}
	coll.datasetSamplers = map[string]sample.Sampler{"test-dataset": testSampler}

	go coll.collect()
	go coll.sendTraces()
	defer coll.Stop()

	// Add 8 spans with key "batch-1"
	for i := 0; i < 8; i++ {
		span := &types.Span{
			TraceID: fmt.Sprintf("batch1-trace-%d", i),
			Event: types.Event{
				Dataset: "test-dataset",
				APIKey:  legacyAPIKey,
				Data:    make(map[string]interface{}),
			},
			IsRoot: true,
		}
		span.Data["batch_key"] = "batch-1"
		coll.AddIndividualSpan(span)
	}

	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, 8, individualCache.GetCacheEntryCount(), "cache should have 8 spans")

	// Verify batch structure
	allBatches := individualCache.GetAll()
	require.Len(t, allBatches, 1, "should have 1 batch")
	assert.Equal(t, "batch-1", allBatches[0].BatchKey.SamplerKey)
	assert.Len(t, allBatches[0].Traces, 8)

	// Add 2 more spans with different key to fill cache
	for i := 0; i < 2; i++ {
		span := &types.Span{
			TraceID: fmt.Sprintf("batch2-trace-%d", i),
			Event: types.Event{
				Dataset: "test-dataset",
				APIKey:  legacyAPIKey,
				Data:    make(map[string]interface{}),
			},
			IsRoot: true,
		}
		span.Data["batch_key"] = "batch-2"
		coll.AddIndividualSpan(span)
	}

	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, 10, individualCache.GetCacheEntryCount(), "cache should be full")
	assert.True(t, individualCache.IsFull(), "cache should report as full")

	// Verify we now have 2 batches
	allBatches = individualCache.GetAll()
	require.Len(t, allBatches, 2, "should have 2 batches")

	// Now add one more span - this should trigger eviction of the largest batch (batch-1)
	span := &types.Span{
		TraceID: "overflow-trace",
		Event: types.Event{
			Dataset: "test-dataset",
			APIKey:  legacyAPIKey,
			Data:    make(map[string]interface{}),
		},
		IsRoot: true,
	}
	span.Data["batch_key"] = "batch-3"
	coll.AddIndividualSpan(span)

	time.Sleep(20 * time.Millisecond)

	// The largest batch (batch-1 with 8 spans) should have been evicted and sent
	// Rate is 2, so we should get 4 spans sent
	events := transmission.GetBlock(4)
	assert.Equal(t, 4, len(events), "largest batch should be sampled and sent")

	// Verify sum of sample rates
	sumRates := 0
	for _, event := range events {
		sumRates += int(event.SampleRate)
	}
	assert.Equal(t, 8, sumRates, "sum of sample rates should equal original batch size")

	// Cache should now have room for the new span and contain batch-2 and batch-3
	assert.Less(t, individualCache.GetCacheEntryCount(), 10, "cache should have space after eviction")

	// Verify batch-1 is gone but batch-2 and batch-3 remain
	allBatches = individualCache.GetAll()
	batchKeys := make(map[string]bool)
	for _, batch := range allBatches {
		batchKeys[batch.BatchKey.SamplerKey] = true
	}
	assert.False(t, batchKeys["batch-1"], "batch-1 should have been evicted")
	assert.True(t, batchKeys["batch-2"], "batch-2 should still be present")
	assert.True(t, batchKeys["batch-3"], "batch-3 should be present")
}

func TestMultipleBatchesWithDifferentKeys(t *testing.T) {
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:       config.Duration(2 * time.Millisecond),
			SendDelay:        config.Duration(1 * time.Millisecond),
			TraceTimeout:     config.Duration(60 * time.Second),
			MaxBatchSize:     500,
			MaxExpiredTraces: 100,
		},
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 3},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			CacheCapacity:                            100,
			ShutdownDelay:                            config.Duration(1 * time.Millisecond),
			UseIndividualSpanBatchSampling:           true,
			IndividualSpanBatchSamplingCacheCapacity: 100,
			IndividualSpanBatchSamplingWindow:        config.Duration(15 * time.Second),
		},
	}

	transmission := &transmit.MockTransmission{}
	transmission.Start()
	defer transmission.Stop()

	clock := clockwork.NewFakeClock()
	coll := newTestCollectorForIndividualSpan(conf, transmission)
	coll.Clock = clock

	c := cache.NewInMemCache(100, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c

	individualCache := cache.NewInMemIndividualSpanBatchSamplingCache(100, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.individualSpanBatchSamplingCache = individualCache

	stc, err := newCache()
	require.NoError(t, err)
	coll.sampleTraceCache = stc

	coll.incoming = make(chan *types.Span, 5)
	coll.incomingIndividualSpan = make(chan *types.Span, 100)
	coll.fromPeer = make(chan *types.Span, 5)
	coll.outgoingTraces = make(chan sendableTrace, 100)

	// Use test sampler with rate 3
	testSampler := &TestKeySampler{keyField: "batch_key", sampleRate: 3}
	coll.datasetSamplers = map[string]sample.Sampler{"test-dataset": testSampler}

	go coll.collect()
	go coll.sendTraces()
	defer coll.Stop()

	// Add 3 different batches with different keys
	// Batch 1: 12 spans (should keep 4 with rate 3)
	for i := 0; i < 12; i++ {
		span := &types.Span{
			TraceID: fmt.Sprintf("batch1-%d", i),
			Event: types.Event{
				Dataset: "test-dataset",
				APIKey:  legacyAPIKey,
				Data:    make(map[string]interface{}),
			},
			IsRoot: true,
		}
		span.Data["batch_key"] = "key-1"
		coll.AddIndividualSpan(span)
	}

	// Batch 2: 9 spans (should keep 3 with rate 3)
	for i := 0; i < 9; i++ {
		span := &types.Span{
			TraceID: fmt.Sprintf("batch2-%d", i),
			Event: types.Event{
				Dataset: "test-dataset",
				APIKey:  legacyAPIKey,
				Data:    make(map[string]interface{}),
			},
			IsRoot: true,
		}
		span.Data["batch_key"] = "key-2"
		coll.AddIndividualSpan(span)
	}

	// Batch 3: 6 spans (should keep 2 with rate 3)
	for i := 0; i < 6; i++ {
		span := &types.Span{
			TraceID: fmt.Sprintf("batch3-%d", i),
			Event: types.Event{
				Dataset: "test-dataset",
				APIKey:  legacyAPIKey,
				Data:    make(map[string]interface{}),
			},
			IsRoot: true,
		}
		span.Data["batch_key"] = "key-3"
		coll.AddIndividualSpan(span)
	}

	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, 27, individualCache.GetCacheEntryCount(), "all 27 spans should be in cache")

	// Verify we have exactly 3 batches with correct keys and counts
	allBatches := individualCache.GetAll()
	require.Len(t, allBatches, 3, "should have 3 distinct batches")

	batchSizes := make(map[string]int)
	for _, batch := range allBatches {
		batchSizes[batch.BatchKey.SamplerKey] = len(batch.Traces)
		assert.Equal(t, uint(3), batch.SampleRate, "all batches should have rate 3")
	}
	assert.Equal(t, 12, batchSizes["key-1"], "batch key-1 should have 12 spans")
	assert.Equal(t, 9, batchSizes["key-2"], "batch key-2 should have 9 spans")
	assert.Equal(t, 6, batchSizes["key-3"], "batch key-3 should have 6 spans")

	// Advance time to expire all batches
	clock.Advance(20 * time.Second)
	time.Sleep(10 * time.Millisecond)

	// Should send 4 + 3 + 2 = 9 spans total
	events := transmission.GetBlock(9)
	assert.Equal(t, 9, len(events), "should send 9 spans from all batches")

	// Verify total sum of sample rates equals original count
	sumRates := 0
	for _, event := range events {
		sumRates += int(event.SampleRate)
	}
	assert.Equal(t, 27, sumRates, "sum of all sample rates should equal 27")

	// Verify cache is empty after expiration
	assert.Equal(t, 0, individualCache.GetCacheEntryCount(), "cache should be empty")
}

// Helper functions

func makeKeyedTraces(n int, desiredRate uint) *cache.KeyedTraces {
	traces := make([]*types.Trace, n)
	now := time.Now()
	for i := 0; i < n; i++ {
		traces[i] = &types.Trace{
			TraceID:     fmt.Sprintf("trace-%d", i),
			APIKey:      legacyAPIKey,
			Dataset:     "test",
			ArrivalTime: now,
			SendBy:      now.Add(time.Minute),
		}
		traces[i].SetSampleRate(1)
	}

	return &cache.KeyedTraces{
		BatchKey: cache.BatchKey{
			Reason:     "test",
			SamplerKey: "test-key",
		},
		SampleRate: desiredRate,
		Traces:     traces,
		Expiration: now.Add(time.Minute),
	}
}

func newTestCollectorForIndividualSpan(conf config.Config, transmission transmit.Transmission) *InMemCollector {
	s := &metrics.MockMetrics{}
	s.Start()
	clock := clockwork.NewRealClock()
	healthReporter := &health.Health{
		Clock: clock,
	}
	healthReporter.Start()
	localPubSub := &pubsub.LocalPubSub{
		Config:  conf,
		Metrics: s,
	}
	localPubSub.Start()

	c := &InMemCollector{
		TestMode:         true,
		Config:           conf,
		Clock:            clock,
		Logger:           &logger.NullLogger{},
		Tracer:           noop.NewTracerProvider().Tracer("test"),
		Health:           healthReporter,
		Transmission:     transmission,
		PeerTransmission: &transmit.MockTransmission{},
		PubSub:           localPubSub,
		Metrics:          &metrics.NullMetrics{},
		StressRelief:     &MockStressReliever{},
		SamplerFactory: &sample.SamplerFactory{
			Config:  conf,
			Metrics: s,
			Logger:  &logger.NullLogger{},
		},
		done: make(chan struct{}),
		Peers: &peer.MockPeers{
			Peers: []string{"api1"},
			ID:    "api1",
		},
		Sharder: &sharder.MockSharder{
			Self: &sharder.TestShard{
				Addr: "api1",
			},
		},
		redistributeTimer:                newRedistributeNotifier(&logger.NullLogger{}, &metrics.NullMetrics{}, clock, 2*time.Millisecond),
		individualSpanBatchSamplingCache: cache.NewInMemIndividualSpanBatchSamplingCache(100, &metrics.NullMetrics{}, &logger.NullLogger{}),
	}

	return c
}
