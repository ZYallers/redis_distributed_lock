package redis_distributed_lock

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis"
)

// redis分布式锁
type DistributedLock struct {
	Key               string        // redis锁key
	Expiration        time.Duration // redis锁key有效时间
	NodeLockTimeout   time.Duration // 获取节点锁超时时间
	RedisLockTimeout  time.Duration // 获取redis锁超时时间
	NodeLockInterval  time.Duration // 循环获取节点锁间隔时间
	RedisLockInterval time.Duration // 循环获取redis锁间隔时间
	WatchdogRate      time.Duration // 看门狗检测频率，注意：此时间必须小于RedisLockTimeout，否则看门狗不会执行

	redisClient    *redis.Client    // redis实例
	ioWriter       io.Writer        // 日志输出实例
	watchdogQuitCh chan interface{} // 看门狗退出通道
	redisLockKey   string           // redis锁key的完整字符
	idfa           string           // 客户端标示
	debug          bool             // 是否开启调试模式
}

const redisLockKeyFormat = "lock@distributed:%s"

var (
	nodeLockInt            int64 // 节点锁数字
	ErrRedisLockKeyEmpty   = errors.New("redis lock key cannot be an empty string")
	ErrRedisClientNil      = errors.New("redis client cannot be nil")
	ErrGetNodeLockTimeout  = errors.New("timeout get node lock")
	ErrGetRedisLockTimeout = errors.New("timeout get redis lock")
)

func NewDistributedLock(opts ...DistributedLockOption) (*DistributedLock, error) {
	hostname, _ := os.Hostname()
	dl := &DistributedLock{
		ioWriter:          os.Stdout,
		Expiration:        10 * time.Second,
		NodeLockTimeout:   10 * time.Second,
		RedisLockTimeout:  10 * time.Second,
		NodeLockInterval:  1 * time.Millisecond,
		RedisLockInterval: 1 * time.Millisecond,
		WatchdogRate:      1 * time.Second,
		idfa:              hostname + "." + randIntString(),
	}
	for _, opt := range opts {
		opt(dl)
	}
	if dl.redisLockKey == "" {
		return nil, ErrRedisLockKeyEmpty
	}
	if dl.redisClient == nil {
		return nil, ErrRedisClientNil
	}
	return dl, nil
}

// 执行任务
func (dl *DistributedLock) Exec(fn func(*DistributedLock) (interface{}, error)) (interface{}, error) {
	nodeLockTimeoutCh := time.After(dl.NodeLockTimeout)
	for {
		select {
		case <-nodeLockTimeoutCh:
			dl.println("get node lock timeout")
			return nil, ErrGetNodeLockTimeout
		default:
			if !dl.nodeLockLock() {
				time.Sleep(dl.NodeLockInterval)
				//dl.println("waiting get node lock...")
				continue
			} else { // 获取到节点锁
				dl.println("got node lock")
				redisLockTimeoutCh := time.After(dl.RedisLockTimeout)
				for {
					select {
					case <-redisLockTimeoutCh:
						dl.nodeLockRelease()
						dl.println("get redis lock timeout")
						return nil, ErrGetRedisLockTimeout
					default:
						if randStr, ok := dl.redisLockLock(); !ok { // 获取redis锁失败
							time.Sleep(dl.RedisLockInterval)
							//dl.println("waiting get redis lock...")
							continue
						} else { // 获取到redis锁
							dl.println("got redis lock")
							dl.watchdogQuitCh = make(chan interface{}, 0)
							go dl.watchdog()
							dl.println("exec func starting")
							res, err := fn(dl)
							dl.println("exec func finished")
							dl.execAfter(randStr)
							return res, err
						}
					}
				}
			}
		}
	}
}

// 执行任务后(释放redis锁、看门狗、节点锁)
func (dl *DistributedLock) execAfter(value string) {
	if dl.redisClient.Get(dl.redisLockKey).Val() == value {
		dl.redisLockRelease()
		dl.watchdogClose()
		dl.nodeLockRelease()
	}
}

// 获取redis实例
func (dl *DistributedLock) RedisClient() *redis.Client {
	return dl.redisClient
}

// 获取redis锁key
func (dl *DistributedLock) RedisLockKey() string {
	return dl.redisLockKey
}

// 获取节点锁
func (dl *DistributedLock) nodeLockLock() bool {
	return atomic.CompareAndSwapInt64(&nodeLockInt, 0, 1)
}

// 释放节点锁
func (dl *DistributedLock) nodeLockRelease() {
	atomic.StoreInt64(&nodeLockInt, 0)
	dl.println("release node lock")
}

// 获取redis锁
func (dl *DistributedLock) redisLockLock() (string, bool) {
	s := randIntString()
	return s, dl.redisClient.SetNX(dl.redisLockKey, s, dl.Expiration).Val()
}

// 释放redis锁
func (dl *DistributedLock) redisLockRelease() {
	dl.redisClient.Del(dl.redisLockKey)
	dl.println("release redis lock")
}

// 看门狗
func (dl *DistributedLock) watchdog() {
	defer func() { recover() }()
	redisLockTimeout := time.After(dl.RedisLockTimeout)
	for {
		// 每隔 WatchdogRate 检查一下当前客户端是否持有redis锁，如果依然持有，那么就延长锁的过期时间
		time.Sleep(dl.WatchdogRate)
		select {
		case <-redisLockTimeout:
			dl.println("timeout watch dog")
			return
		case <-dl.watchdogQuitCh:
			dl.println("close watch dog")
			return
		default:
			if d := dl.redisClient.TTL(dl.redisLockKey).Val(); d > 0 {
				d2 := d + dl.WatchdogRate
				dl.redisClient.Expire(dl.redisLockKey, d2)
				dl.println(fmt.Sprintf("watch client holds redis lock, extend expiration %s to %s", d, d2))
			} else {
				return
			}
		}
	}
}

// 释放看门狗
func (dl *DistributedLock) watchdogClose() {
	defer func() { recover() }()
	close(dl.watchdogQuitCh)
}

// 打印日志
func (dl *DistributedLock) println(s string) {
	if dl.debug {
		ts := time.Now().Format("2006/01/02 15:04:05.00000")
		_, _ = fmt.Fprintf(dl.ioWriter, "[%s] <%s> %s\n", ts, dl.idfa, s)
	}
}

func randIntString() string {
	return strconv.Itoa(rand.New(rand.NewSource(time.Now().UnixNano())).Intn(99999999))
}
