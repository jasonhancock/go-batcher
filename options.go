package batcher

import "time"

type options struct {
	metricQueueLength   Gauge
	metricQueueCapacity Gauge
	queueDepthInterval  time.Duration
	queueDepth          int

	metricBatchesProcessed Counter
	metricItemsProcessed   Counter
}

// Option is used to customize the batcher.
type Option func(*options)

// WithQueueMetrics assign Gauges to be used to monitor queue length and capacity
// and the interval at which to update the length.
func WithMetricQueueDepth(length, capacity Gauge, interval time.Duration) Option {
	return func(o *options) {
		o.metricQueueLength = length
		o.metricQueueCapacity = capacity
		o.queueDepthInterval = interval
	}
}

// WithQueueDepth determines how large of a buffered channel to allocate.
func WithQueueDepth(buffer int) Option {
	return func(o *options) {
		o.queueDepth = buffer
	}
}

// WithProcessedCounters adds metric counters for number of items processed and
// number of batches processed.
func WithProcessedCounters(batches, items Counter) Option {
	return func(o *options) {
		o.metricBatchesProcessed = batches
		o.metricItemsProcessed = items
	}
}
