package dm

import (
    "errors"
    "fmt"
    "reflect"
    "time"

    "github.com/bluele/gcache"
)

// LRUStore LRU模式的淘汰存储
type LRUStore struct {
    baseEvictStore
}

func NewLRUStore(size int, expire time.Duration) *LRUStore {
    e := gcache.New(size).LRU().Clock(clocker).Build()
    p := &LRUStore{
        baseEvictStore: baseEvictStore{
            cacheEngine:  e,
            checkExpired: false,
        },
    }
    return p
}

// LFUStore LFU模式的淘汰存储
type LFUStore struct {
    baseEvictStore
}

func NewLFUStore(size int) *LFUStore {
    e := gcache.New(size).LFU().Clock(clocker).Build()
    p := &LFUStore{
        baseEvictStore: baseEvictStore{
            cacheEngine:  e,
            checkExpired: false,
        },
    }
    return p
}

// ARCStore ARC模式的淘汰存储
type ARCStore struct {
    baseEvictStore
}

func NewARCStore(size int) *ARCStore {
    e := gcache.New(size).ARC().Clock(clocker).Build()
    p := &ARCStore{
        baseEvictStore: baseEvictStore{
            cacheEngine:  e,
            checkExpired: false,
        },
    }
    return p
}

// 基础的淘汰存储
type baseEvictStore struct {
    BaseStore

    cacheEngine  gcache.Cache
    checkExpired bool
}

func (b *baseEvictStore) BackFill(key KeyType, value interface{}) error {
    // 回填操作只需要Set
    return b.Set(key, value)
}

func (b *baseEvictStore) Size() int {
    return b.cacheEngine.Len(b.checkExpired)
}

func (b *baseEvictStore) HasKey(key KeyType) bool {
    return b.cacheEngine.Has(key)
}

func (b *baseEvictStore) Keys() (keys []KeyType) {
    ikeys := b.cacheEngine.Keys(b.checkExpired)
    if size := len(ikeys); size > 0 {
        keys = make([]KeyType, 0, size)
        for i := 0; i < len(ikeys); i++ {
            if k, ok := ikeys[i].(KeyType); ok {
                keys = append(keys, k)
            }
        }
    }
    return
}

func (b *baseEvictStore) Get(key KeyType) (value interface{}, err error) {
    value, err = b.cacheEngine.GetIFPresent(key)
    if err == gcache.KeyNotFoundError {
        return nil, KeyNotExists
    }
    return
}

func (b *baseEvictStore) Set(key KeyType, value interface{}) error {
    return b.cacheEngine.Set(key, value)
}

func (b *baseEvictStore) Del(key KeyType) (has bool) {
    return b.cacheEngine.Remove(key)
}

func (b *baseEvictStore) Fetch(key KeyType, valuePtr interface{}) error {
    if typ := reflect.TypeOf(valuePtr); typ.Kind() != reflect.Ptr {
        return fmt.Errorf("input type:%s not ptr", typ.Name())
    }

    data, err := b.Get(key)
    if err != nil {
        return err
    }

    vv := reflect.ValueOf(valuePtr)
    if !vv.IsValid() {
        return fmt.Errorf("input value unvalid")
    }

    v := reflect.ValueOf(data)
    if v.Kind() == reflect.Ptr {
        vv.Elem().Set(v.Elem())
    } else {
        vv.Elem().Set(v)
    }
    return nil
}

func (b *baseEvictStore) Iterator(iter func(keyType KeyType, value interface{}) bool) error {
    if iter == nil {
        return errors.New("input iter is nil")
    }

    kvs := b.cacheEngine.GetALL(b.checkExpired)
    for key, value := range kvs {
        if k, ok := key.(KeyType); ok {
            c := iter(k, value)
            if !c {
                break
            }
        }
    }
    return nil
}

func (b *baseEvictStore) Capacity() int {
    return b.cacheEngine.Len(b.checkExpired) * b.SeedSize()
}

func (b *baseEvictStore) Purge() {
    b.cacheEngine.Purge()
}

var clocker = &clock{}

type clock struct {}
func (*clock) Now() time.Time {
    return time.Now()
}