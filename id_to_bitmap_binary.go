package dm

import (
    "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/container/dm/croaring"
    "time"
)

const (
    flagArray = 0x0001
)

type nodes struct {
    flag    uint16
    ids     []uint16
    bitmaps []croaring.Bitmap
}

type IToBMapBinary map[uint16]nodes

type IdToBitmapBinary struct {
    fields []IToBMapBinary
    stats  IToBStats
}

func NewIdToBitmapBinary(invertNum ...int) *IdToBitmapBinary {
    num := 0
    if len(invertNum) > 0 {
        num = invertNum[0]
    }
    itb := &IdToBitmapBinary{
        fields: make([]IToBMapBinary, num, num),
    }
    for index := range itb.fields {
        itb.fields[index] = make(IToBMapBinary)
    }
    return itb
}

func (itb *IdToBitmapBinary) AddToBitmaps(id uint32, bitmaps ...croaring.Bitmap) {
    itb.add(id, bitmaps, func(bitmap croaring.Bitmap) {
        bitmap.Add(id)
    })
}

func (itb *IdToBitmapBinary) Add(id uint32, bitmaps ...croaring.Bitmap) {
    itb.add(id, bitmaps, nil)
}

func (itb *IdToBitmapBinary) add(id uint32, bitmaps []croaring.Bitmap, cb func(bitmap croaring.Bitmap)) {
    h, l := itb.splitId(id)
    if diff := len(bitmaps) - len(itb.fields); diff > 0 {
        for i := 0; i < diff; i++ {
            itb.fields = append(itb.fields, make(IToBMapBinary))
        }
    }
    for index, bitmap := range bitmaps {
        if cb != nil {
            cb(bitmap)
        }
        m, has := itb.fields[index][h]
        if !has {
            m.ids = make([]uint16, 0, 512)
            m.bitmaps = make([]croaring.Bitmap, 0, 512)
            itb.fields[index][h] = m
        }

        if m.flag & flagArray > 0 {
            m.bitmaps[l] = bitmap
            itb.stats.Count++
            continue
        }

        subIndex, has := binarySearch(m.ids, l)
        if has {
            m.bitmaps[subIndex] = bitmap
        } else {
            if subIndex >= len(m.ids) {
                m.ids = append(m.ids, l)
                m.bitmaps = append(m.bitmaps, bitmap)
            } else {
                m.ids = append(m.ids[0:subIndex+1], m.ids[subIndex:]...)
                m.bitmaps = append(m.bitmaps[0:subIndex+1], m.bitmaps[subIndex:]...)
                m.ids[subIndex] = l
                m.bitmaps[subIndex] = bitmap
                itb.stats.Binary.Move++
                itb.stats.Binary.LastMove = time.Now()
            }
            if cap(m.ids) > 65536 {
                ids := make([]uint16, len(m.ids), 65536)
                bitmapSlice := make([]croaring.Bitmap, len(m.bitmaps), 65536)

                copy(ids, m.ids)
                copy(bitmapSlice, m.bitmaps)
                m.ids = ids
                m.bitmaps = bitmapSlice
            }
            if len(m.ids) == 65536 {
                m.flag |= flagArray
                m.ids = []uint16{}
                itb.stats.Binary.Array++
            }

            itb.fields[index][h] = m
        }
        itb.stats.Count++
    }
}

func (itb *IdToBitmapBinary) DelFromBitmaps(id uint32) {
    itb.del(id, func(bitmap croaring.Bitmap) {
        bitmap.Remove(id)
        if bitmap.IsEmpty() {
            itb.stats.Empty++
        }
    })
}

func (itb *IdToBitmapBinary) Del(id uint32) {
    itb.del(id, nil)
}

func (itb *IdToBitmapBinary) del(id uint32, cb func(bitmap croaring.Bitmap)) {
    h, l := itb.splitId(id)
    for _, field := range itb.fields {
        m, hasH := field[h]
        if !hasH {
            break
        }

        if m.flag & flagArray > 0 {
            if m.bitmaps[l] == 0 {
                break
            }
            if cb != nil {
                cb(m.bitmaps[l])
            }
            m.bitmaps[l] = 0
            itb.stats.Count--
            continue
        }

        subIndex, has := binarySearch(m.ids, l)
        if !has {
            break
        }

        if m.bitmaps[subIndex] == 0 {
            break
        }

        if cb != nil {
            cb(m.bitmaps[subIndex])
        }

        m.bitmaps[subIndex] = 0

        itb.stats.Count--
    }
}

func (itb *IdToBitmapBinary) Replace(id uint32, bitmaps ...croaring.Bitmap) {
    itb.Del(id)
    itb.Add(id, bitmaps...)
}

func (itb *IdToBitmapBinary) ToArray(id uint32) (bitmaps []croaring.Bitmap) {
    itb.Iterate(id, func(bitmap croaring.Bitmap) bool {
        bitmaps = append(bitmaps, bitmap)
        return true
    })
    return
}

func (itb *IdToBitmapBinary) Iterate(id uint32, cb func(bitmap croaring.Bitmap) bool) {
    h, l := itb.splitId(id)
    for _, field := range itb.fields {
        m, hasH := field[h]
        if !hasH {
            break
        }

        if m.flag & flagArray > 0 {
            if m.bitmaps[l] == 0 {
                break
            }

            if !cb(m.bitmaps[l]) {
                break
            }
        }

        subIndex, has := binarySearch(m.ids, l)
        if !has {
            break
        }

        if m.bitmaps[subIndex] == 0 {
            break
        }

        if !cb(m.bitmaps[subIndex]) {
            break
        }
    }
}

func (itb *IdToBitmapBinary) splitId(id uint32) (h uint16, l uint16) {
    h = uint16((id >> 16) & 0xFFFF)
    l = uint16(id & 0xFFFF)
    return
}

func (itb *IdToBitmapBinary) GetEmptyNum() uint64 {
    return itb.stats.Empty
}

func (itb *IdToBitmapBinary) GetBitmapNum() uint64 {
    return itb.stats.Count
}

func (itb *IdToBitmapBinary) GetStats() IToBStats {
    return itb.stats
}

func (itb *IdToBitmapBinary) ResetEmptyNum() {
    itb.stats.Empty = 0
}

func binarySearch(nums []uint16, target uint16) (int, bool) {
    if len(nums) == 0 {
        return 0, false
    }
    if target > nums[len(nums)-1] {
        return len(nums), false
    }
    left := 0
    right := len(nums) - 1
    for left <= right {
        mid := left + (right-left)/2
        if nums[mid] == target {
            return mid, true
        } else if nums[mid] > target {
            if mid == 0 || nums[mid-1] < target {
                return mid, false
            }
            right = mid - 1
        } else if nums[mid] < target {
            left = mid + 1
        }
    }
    return -1, false
}
