package dm

import (
    "container/heap"
    "fmt"
)

// todo 这个新的基于heap的store待优化

// KeyLatestStoreHeap 利用小顶堆实现存储最大
type KeyLatestStoreHeap struct {
    IStore

    ids   KeyHeap
    limit int // 最大存储数量
}

func NewKeyLatestStoreHeap(et EngineType, seed interface{}, limit ...int) *KeyLatestStoreHeap {
    p := &KeyLatestStoreHeap{}

    if len(limit) > 0 {
        p.SetLimit(int64(limit[0]))
        p.ids = make(KeyHeap, 0, p.limit)
    } else {
        p.SetLimit(LIMIT_INFINITE)
        p.ids = make(KeyHeap, 0, 1024)
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
func (k *KeyLatestStoreHeap) SetLimit(limit int64) {
    k.limit = int(limit)
}

func (k *KeyLatestStoreHeap) Set(key KeyType, value interface{}) error {
    // 判断是否需要Set
    if len(k.ids) >= k.limit {
        if key < k.ids[0] {
            return nil
        }
    }

    if err := k.IStore.Set(key, value); err != nil {
        return err
    }

    // 设置成功 -> 放到heap当中
    heap.Push(&k.ids, key)

    // 删除最小的Key
    for len(k.ids) > k.limit {
        k.IStore.Del(heap.Pop(&k.ids).(KeyType))
    }
    return nil
}

func (k *KeyLatestStoreHeap) Del(key KeyType) (has bool) {
    if has = k.IStore.Del(key); has {
        for i, v := range k.ids {
            if v == key {
                heap.Remove(&k.ids, i)
                break
            }
        }
    }
    return
}

type KeyHeap []KeyType

func (h KeyHeap) Len() int           { return len(h) }
func (h KeyHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h KeyHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *KeyHeap) Push(x interface{}) {
    *h = append(*h, x.(int))
}

func (h *KeyHeap) Pop() interface{} {
    old := *h
    n := len(old)
    x := old[n-1]
    *h = old[0 : n-1]
    return x
}
