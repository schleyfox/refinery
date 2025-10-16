package cache

import (
	"time"

	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"
	"github.com/rdleal/go-priorityq/kpq"
	"golang.org/x/exp/maps"
)

// IndividualSpanBatchSamplingCache holds individual spans by sampling.key for
// batch uniform sampling. We accumulate spans for a sampling.key over a time
// period and then randomly select 1 of N spans to satisfy our desired Sample
// Rate. This eliminates error in count estimation compared to the default
// Bernoulli sampling and slightly reduces variance for other dimensions at the
// same sample rate. Non-threadsafe.
type IndividualSpanBatchSamplingCache interface {
	Add(sampleRate uint, reason string, key string, trace *types.Trace) bool
	Get(key string) *KeyedTraces
	GetAll() []*KeyedTraces
	GetCacheCapacity() int
	GetCacheEntryCount() int
	IsFull() bool
	TakeExpiredSpans(now time.Time, max int) []*KeyedTraces
	TakeLargestBatch() *KeyedTraces
}

var _ IndividualSpanBatchSamplingCache = (*DefaultInMemIndividualSpanBatchSamplingCache)(nil)

type KeyedTraces struct {
	Key string
	// Could be handled as max, average, or last, but I think max is best
	SampleRate uint
	// Debugging string for sampler config, just keep the first.
	Reason     string
	Traces     []*types.Trace
	Expiration time.Time
}

type DefaultInMemIndividualSpanBatchSamplingCache struct {
	Metrics metrics.Metrics
	Logger  logger.Logger

	timePQ   *kpq.KeyedPriorityQueue[string, time.Time]
	lenPQ    *kpq.KeyedPriorityQueue[string, int]
	capacity int
	cacheLen int
	cache    map[string]*KeyedTraces
}

var individualSpanBatchSamplingCacheMetrics = []metrics.Metadata{
	{Name: "individual_span_batch_sampling_cache_entries", Type: metrics.Histogram, Unit: metrics.Dimensionless, Description: "the number of traces currently stored in the cache"},
}

func NewInMemIndividualSpanBatchSamplingCache(capacity int, metrics metrics.Metrics, logger logger.Logger) *DefaultInMemIndividualSpanBatchSamplingCache {
	for _, metadata := range individualSpanBatchSamplingCacheMetrics {
		metrics.Register(metadata)
	}

	timeCmp := func(v1, v2 time.Time) bool {
		return v1.Before(v2)
	}
	lenCmp := func(v1, v2 int) bool {
		return v1 > v2 // max-heap: largest batches first
	}

	return &DefaultInMemIndividualSpanBatchSamplingCache{
		Metrics: metrics,
		Logger:  logger,

		timePQ:   kpq.NewKeyedPriorityQueue[string](timeCmp),
		lenPQ:    kpq.NewKeyedPriorityQueue[string](lenCmp),
		capacity: capacity,
		cacheLen: 0,
		cache:    make(map[string]*KeyedTraces),
	}
}

func (c *DefaultInMemIndividualSpanBatchSamplingCache) Add(sampleRate uint, reason string, key string, trace *types.Trace) bool {
	if c.IsFull() {
		return false
	}

	c.cacheLen++
	keyedTraces, ok := c.cache[key]
	if ok {
		keyedTraces.Traces = append(keyedTraces.Traces, trace)
		if sampleRate > keyedTraces.SampleRate {
			keyedTraces.SampleRate = sampleRate
		}
	} else {
		keyedTraces = &KeyedTraces{
			Key:        key,
			Traces:     []*types.Trace{trace},
			Expiration: trace.SendBy,
			SampleRate: sampleRate,
			Reason:     reason,
		}
	}

	c.timePQ.Set(keyedTraces.Key, keyedTraces.Expiration)
	c.lenPQ.Set(keyedTraces.Key, len(keyedTraces.Traces))
	c.cache[key] = keyedTraces

	return true
}

func (c *DefaultInMemIndividualSpanBatchSamplingCache) Get(key string) *KeyedTraces {
	keyedTraces, ok := c.cache[key]
	if !ok {
		return nil
	}
	return keyedTraces
}

func (c *DefaultInMemIndividualSpanBatchSamplingCache) GetAll() []*KeyedTraces {
	return maps.Values(c.cache)
}

func (c *DefaultInMemIndividualSpanBatchSamplingCache) GetCacheCapacity() int {
	return c.capacity
}

func (c *DefaultInMemIndividualSpanBatchSamplingCache) GetCacheEntryCount() int {
	return c.cacheLen
}

func (c *DefaultInMemIndividualSpanBatchSamplingCache) IsFull() bool {
	return c.cacheLen >= c.capacity
}

func (c *DefaultInMemIndividualSpanBatchSamplingCache) remove(keyedTraces *KeyedTraces) {
	delete(c.cache, keyedTraces.Key)
	c.cacheLen -= len(keyedTraces.Traces)
	c.lenPQ.Remove(keyedTraces.Key)
	c.timePQ.Remove(keyedTraces.Key)
}

func (c *DefaultInMemIndividualSpanBatchSamplingCache) TakeExpiredSpans(now time.Time, max int) []*KeyedTraces {
	c.Metrics.Histogram("individual_span_batch_sampling_cache_entries", float64(len(c.cache)))
	var expired []*KeyedTraces
	for !c.timePQ.IsEmpty() && (max <= 0 || len(expired) < max) {
		key, expiration, ok := c.timePQ.Pop()
		if !ok {
			break
		}
		keyedTraces, ok := c.cache[key]
		if !ok {
			continue
		}
		if now.Before(expiration) {
			c.timePQ.Push(key, expiration)
			break
		}
		expired = append(expired, keyedTraces)
		c.remove(keyedTraces)
	}

	return expired
}

func (c *DefaultInMemIndividualSpanBatchSamplingCache) TakeLargestBatch() *KeyedTraces {
	c.Metrics.Histogram("individual_span_batch_sampling_cache_entries", float64(len(c.cache)))

	key, _, ok := c.lenPQ.Pop()
	if !ok {
		return nil
	}

	keyedTraces, ok := c.cache[key]
	if !ok {
		return nil
	}

	c.remove(keyedTraces)
	return keyedTraces
}
