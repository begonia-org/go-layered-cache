// Copyright 2024 geebytes. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package source

import "time"

type WatchOptions struct {
	// BatchSize is the maximum number of messages to read from the stream per call to XREAD.
	BatchSize int64
	// Block is the maximum amount of time to block for new messages.
	Block time.Duration

	// Channels is the list of streams to read from.
	Channels []interface{}
	// WatcheAt is the id of the redis xstream message to start reading from.
	WatcheAt string
}

type WithWatchOptions func(*WatchOptions)
func WithBatchSize(batchSize int64) WithWatchOptions {
	return func(options *WatchOptions) {
		options.BatchSize = batchSize
	}
}
func WithBlock(block time.Duration) WithWatchOptions {
	return func(options *WatchOptions) {
		options.Block = block
	}

}
func WithChannels(channels []interface{}) WithWatchOptions {
	return func(options *WatchOptions) {
		options.Channels = channels
	}
}
// WatcheAt is the id of the redis xstream message to start reading from.
func WithWatcheAt(watcheAt string) WithWatchOptions {
	return func(options *WatchOptions) {
		options.WatcheAt = watcheAt
	}

}
func NewWatchOptions(channels []interface{}) *WatchOptions {
	return &WatchOptions{
		BatchSize: 10,
		Block:     3 * time.Second,
		Channels: channels,
		WatcheAt: "$",
	}
}