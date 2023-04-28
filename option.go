package redis_distributed_lock

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/go-redis/redis"
)

type DistributedLockOption func(dl *DistributedLock)

func WithKey(key string) DistributedLockOption {
	return func(dl *DistributedLock) {
		dl.Key = key
		dl.redisLockKey = fmt.Sprintf(redisLockKeyFormat, dl.Key)
	}
}

func WithExpiration(t time.Duration) DistributedLockOption {
	return func(dl *DistributedLock) {
		dl.Expiration = t
	}
}

func WithNodeLockTimeout(t time.Duration) DistributedLockOption {
	return func(dl *DistributedLock) {
		dl.NodeLockTimeout = t
	}
}

func WithRedisLockTimeout(t time.Duration) DistributedLockOption {
	return func(dl *DistributedLock) {
		dl.RedisLockTimeout = t
	}
}

func WithWatchdogRate(t time.Duration) DistributedLockOption {
	return func(dl *DistributedLock) {
		dl.WatchdogRate = t
	}
}

func WithNodeLockInterval(t time.Duration) DistributedLockOption {
	return func(dl *DistributedLock) {
		dl.NodeLockInterval = t
	}
}

func WithRedisLockInterval(t time.Duration) DistributedLockOption {
	return func(dl *DistributedLock) {
		dl.RedisLockInterval = t
	}
}

func WithIoWriter(w io.Writer) DistributedLockOption {
	return func(dl *DistributedLock) {
		dl.ioWriter = w
	}
}

func WithRedisClient(cli *redis.Client) DistributedLockOption {
	return func(dl *DistributedLock) {
		dl.redisClient = cli
	}
}

func WithIdfa(name string) DistributedLockOption {
	return func(dl *DistributedLock) {
		hostname, _ := os.Hostname()
		dl.idfa = hostname + "." + name
	}
}

func WithDebug() DistributedLockOption {
	return func(dl *DistributedLock) {
		dl.debug = true
	}
}
