package source

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"
	"unsafe"

	"github.com/redis/go-redis/v9"
)

var (
	ErrTypeExceptString = errors.New("type except string")
)

type OnMessage func(ctx context.Context, from string, message interface{}) error
type OnScan func(context.Context, interface{}) error
type DataSource interface {
	Get(ctx context.Context, key string, args ...interface{}) ([]interface{}, error)
	Set(ctx context.Context, key string, args ...interface{}) error
	Watch(ctx context.Context, onMessage OnMessage) (<-chan error, context.CancelFunc)
	Scan(ctx context.Context, pattern string, onRecv OnScan) <-chan error
	Dump(ctx context.Context, key interface{}, args ...interface{}) <-chan interface{}
}

type KeyValueCacheSource interface {
	DataSource
	GetValue(ctx context.Context, key string) ([]byte, error)
	SetValue(ctx context.Context, key string, value []byte) error
}

type FilterSource interface {
	DataSource
	Check(ctx context.Context, key string, value []byte) bool
	Add(ctx context.Context, key string, value []byte) error
}

type CuckooSource interface {
	FilterSource
	Del(ctx context.Context, key string, value []byte) error
}
type DataSourceFromRedis struct {
	rdb     *redis.Client
	watcher *WatchOptions
}
type RedisDump struct {
	Data []byte
	Iter uint64
}
type WatchOptions struct {
	// BatchSize is the maximum number of messages to read from the stream per call to XREAD.
	BatchSize int64
	// Block is the maximum amount of time to block for new messages.
	Block time.Duration
	//

	// Channels is the list of streams to read from.
	Channels []interface{}
	WatcheAt string
}
type TxHandleKeysOptions struct {
	Channel string
	Cmd     string
	CmdArgs []interface{}

	SendMessages []interface{}
}

func RedisStringToBytes(s string) []byte {
	stringHeader := (*reflect.StringHeader)(unsafe.Pointer(&s))

	var b []byte
	byteHeader := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	byteHeader.Data = stringHeader.Data
	byteHeader.Len = stringHeader.Len
	byteHeader.Cap = stringHeader.Len

	return b
}
func BytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
func (d *DataSourceFromRedis) Get(ctx context.Context, key string, args ...interface{}) (string, error) {
	val, err := d.rdb.Get(ctx, key).Result()
	if err != nil {
		return "", err
	}
	return val, nil
}
func (d *DataSourceFromRedis) Set(ctx context.Context, key string, args ...interface{}) error {
	return fmt.Errorf("not implement")
}
func (d *DataSourceFromRedis) GetExec(ctx context.Context, cmd string, args ...interface{}) ([]interface{}, error) {
	result := make([]interface{}, 0)

	cmds, err := d.exec(ctx, cmd, args...)
	if err != nil {
		return nil, err
	}
	for _, cmd := range cmds {
		if cmd.Err() != nil {
			return nil, cmd.Err()
		}
		if cmd.Val() == nil {
			continue
		}
		result = append(result, cmd.Val())

	}
	return result, nil
}
func (d *DataSourceFromRedis) exec(ctx context.Context, cmd string, args ...interface{}) ([]*redis.Cmd, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("args is empty")
	}

	pipe := d.rdb.Pipeline()
	cmds := make([]*redis.Cmd, len(args))
	for _, arg := range args {
		cmds = append(cmds, pipe.Do(ctx, cmd, arg))

	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, err
	}
	return cmds, nil
}
func (d *DataSourceFromRedis) SetExec(ctx context.Context, cmd string, args ...interface{}) error {

	_, err := d.exec(ctx, cmd, args...)
	if err != nil {
		return err

	}
	return nil
}

func (d *DataSourceFromRedis) DelExec(ctx context.Context, cmd string, args ...interface{}) error {
	_, err := d.exec(ctx, cmd, args...)
	if err != nil {
		return err
	}
	return nil
}

func (d *DataSourceFromRedis) Scan(ctx context.Context, pattern string, onScan OnScan) <-chan error {
	ch := make(chan error, 100)
	go func(ctx context.Context) {
		defer close(ch)
		iter := d.rdb.Scan(ctx, 0, pattern, 0).Iterator()
		for iter.Next(ctx) {
			key := iter.Val()
			err := onScan(ctx, key)
			if err != nil {
				ch <- err
				continue
			}
		}
	}(ctx)
	return ch
}

// Watch redis stream
func (d *DataSourceFromRedis) Watch(ctx context.Context, onMessage OnMessage) (<-chan error, context.CancelFunc) {
	ch := make(chan error, 100)
	ctx, cancel := context.WithCancel(ctx)
	go func(ctx context.Context) {
		defer close(ch)

		for {
			select {
			case <-ctx.Done():
				ch <- ctx.Err()
				return
			default:
				streams := make([]string, len(d.watcher.Channels))
				ids := make([]string, len(d.watcher.Channels))
				for index, channel := range d.watcher.Channels {
					streams[index] = channel.(string)
					// log.Printf("watcher:%v %s", channel, channel.(string))
					ids[index] = d.watcher.WatcheAt
				}
				streams = append(streams, ids...)
				xstream := d.rdb.XRead(ctx, &redis.XReadArgs{
					Streams: streams,
					Count:   d.watcher.BatchSize,
					Block:   d.watcher.Block,
				})
				messages, err := xstream.Result()
				if err != nil && err != redis.Nil {
					ch <- fmt.Errorf("xread:%w", err)
					continue
				}
				for _, message := range messages {

					for _, stream := range message.Messages {
						err := onMessage(ctx, message.Stream, stream)
						if err != nil {
							ch <- err
							continue
						}
					}
				}
			}
		}
	}(ctx)
	return ch, cancel

}

func (d *DataSourceFromRedis) UnWatch(ctx context.Context, cancel context.CancelFunc) error {
	cancel()
	return nil
}
func (d *DataSourceFromRedis) PushMessages(ctx context.Context, channel interface{}, message ...interface{}) error {
	// _, err := d.rdb.Publish(ctx, channel, message).Result()
	// return err
	if key, ok := channel.(string); !ok || key == "" {
		return ErrTypeExceptString

	}
	pipe := d.rdb.Pipeline()
	for _, msg := range message {
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: channel.(string),
			Values: msg,
		})
	}
	_, err := pipe.Exec(ctx)
	return err
}
func (d *DataSourceFromRedis) TxWriteHandle(ctx context.Context, options *TxHandleKeysOptions) error {
	pipe := d.rdb.TxPipeline()
	result := make([]interface{}, 0)
	for _, arg := range options.CmdArgs {
		args := []interface{}{options.Cmd}
		if arg == nil {
			continue
		}
		args = append(args, arg.([]interface{})...)
		result = append(result, pipe.Do(ctx, args...))
	}
	for _, msg := range options.SendMessages {
		result = append(result, pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: options.Channel,
			Values: msg,
		}))
	}
	_, err := pipe.Exec(ctx)
	for _, cmd := range result {
		if ret, ok := cmd.(*redis.Cmd); ok {
			if ret.Err() != nil {
				return ret.Err()
			}
		}
		if ret, ok := cmd.(*redis.StringCmd); ok {
			if ret.Err() != nil {
				return ret.Err()
			}
		}
	}
	return err
}

func (d *DataSourceFromRedis) BFScanDump(ctx context.Context, key string, iter int64) ([]byte, int64, error) {
	scanDump := d.rdb.BFScanDump(ctx, key, iter)
	dump, err := scanDump.Result()
	if err != nil || dump.Iter == 0 {
		return nil, 0, err
	}

	return RedisStringToBytes(dump.Data), dump.Iter, err
}
func (d *DataSourceFromRedis) CFScanDump(ctx context.Context, key string, iter int64) ([]byte, int64, error) {
	dump, err := d.rdb.CFScanDump(ctx, key, iter).Result()
	if err != nil {
		return nil, 0, err
	}
	return RedisStringToBytes(dump.Data), dump.Iter, err
}

func NewDataSourceFromRedis(rdb *redis.Client, watcher *WatchOptions) *DataSourceFromRedis {
	return &DataSourceFromRedis{
		rdb:     rdb,
		watcher: watcher,
	}
}
