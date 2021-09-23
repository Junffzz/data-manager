package dm

import (
    "fmt"
    "math"
)

type EngineType int

const (
    ENGINE_NORMAL_MAP EngineType = 0
    ENGINE_NOGC_MAP   EngineType = 1
)

// KeyLatestStore 基于Key最新模式的淘汰存储
type KeyLatestStore struct {
    IStore

    minKey  KeyType    // 存储的最小Key
    maxKey  KeyType    // 存储的最大Key
    limit   int64      // 最大存储数量
    curSize int64      // 当前数量
}

func NewKeyLatestStore(et EngineType, seed interface{}, limit ...int) *KeyLatestStore {
    p := &KeyLatestStore{
        minKey: math.MaxInt64,
    }

    if len(limit) > 0 {
        p.SetLimit(int64(limit[0]))
    } else {
        p.SetLimit(LIMIT_INFINITE)
    }

    switch et {
    case ENGINE_NORMAL_MAP:
        p.IStore = NewNormalMap(seed)
    case ENGINE_NOGC_MAP:
        p.IStore = NewNogcMap(seed)
    default:
        panic(fmt.Errorf("EngineType:%v non support", et))
    }
    return p
}

// SetLimit 设置最大存储数量
func (k *KeyLatestStore) SetLimit(limit int64) {
    k.limit = limit
}

// Set TODO 尽量保障大批量Set过程中, key是连续且递减的, 从而防止频繁的淘汰数据
func (k *KeyLatestStore) Set(key KeyType, value interface{}) error {
    // 判断是否需要Set
    if k.curSize >= k.limit {
        if key < k.minKey {
            return nil
        }
    }

    if err := k.IStore.Set(key, value); err != nil {
        return err
    }

    // 设置成功 -> 数量+1
    k.curSize++

    if key > k.maxKey {
        k.maxKey = key
    }

    if k.minKey > key {
        k.minKey = key
    }

    // 当前数量超过limit -> 删除最小的Key
    for k.curSize > k.limit {
        k.Del(k.minKey)
        k.minKey++
    }
    return nil
}

func (k *KeyLatestStore) Del(key KeyType) (has bool) {
    if has = k.IStore.Del(key); has {
        k.curSize--
    }
    return
}