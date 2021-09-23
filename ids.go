package dm

import (
    "sync"

    "github.com/RoaringBitmap/roaring"

    "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/convert"
)

// IdsManager 维护roaring.Bitmap的并发安全
type IdsManager struct {
    lock sync.RWMutex
    ids  *roaring.Bitmap
}

func NewIdsManager() *IdsManager {
    m := &IdsManager {
        ids: roaring.NewBitmap(),
    }
    return m
}

func (receiver *IdsManager) IsEmpty() bool {
    if receiver.ids == nil {
        return false
    }

    receiver.lock.RLock()
    defer receiver.lock.RUnlock()
    return receiver.ids.IsEmpty()
}

func (receiver *IdsManager) Clear() {
    if receiver.ids == nil {
        return
    }

    receiver.lock.Lock()
    defer receiver.lock.Unlock()
    receiver.ids.Clear()
}

func (receiver *IdsManager) Add(x uint32) {
    if receiver.ids == nil {
        return
    }

    receiver.lock.Lock()
    defer receiver.lock.Unlock()
    receiver.ids.Add(x)
}

func (receiver *IdsManager) AddMany(dat []uint32) {
    if receiver.ids == nil {
        return
    }

    receiver.lock.Lock()
    defer receiver.lock.Unlock()
    receiver.ids.AddMany(dat)
}

func (receiver *IdsManager) AddX(x interface{}) {
    receiver.Add(convert.Uint32(x))
}

func (receiver *IdsManager) AddInt(x int) {
    receiver.Add(uint32(x))
}

func (receiver *IdsManager) AddManyInt(dat []int) {
    if len(dat) == 0 {
        return
    }

    dat32 := make([]uint32, 0, len(dat))
    for _, d := range dat {
        dat32 = append(dat32, uint32(d))
    }
    receiver.AddMany(dat32)
}

func (receiver *IdsManager) Remove(x uint32) {
    if receiver.ids == nil {
        return
    }

    receiver.lock.Lock()
    defer receiver.lock.Unlock()
    receiver.ids.Remove(x)
}

func (receiver *IdsManager) Contains(x uint32) bool {
    if receiver.ids == nil {
        return false
    }

    receiver.lock.RLock()
    defer receiver.lock.RUnlock()
    return receiver.ids.Contains(x)
}

func (receiver *IdsManager) ContainsInt(x int) bool {
    return receiver.Contains(uint32(x))
}

func (receiver *IdsManager) ToArray() []uint32 {
    if receiver.ids == nil {
        return nil
    }

    receiver.lock.RLock()
    defer receiver.lock.RUnlock()
    return receiver.ids.ToArray()
}

func (receiver *IdsManager) ToArrayInt() []int {
    if receiver.ids == nil {
        return nil
    }

    receiver.lock.RLock()
    defer receiver.lock.RUnlock()

    ids := receiver.ids.ToArray()
    nids := make([]int, 0, len(ids))
    for _, id := range ids {
        nids = append(nids, int(id))
    }
    return nids
}