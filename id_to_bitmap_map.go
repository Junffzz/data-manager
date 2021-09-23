package dm

import (
    "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/container/dm/croaring"
)

type IToBMap map[uint16]map[uint16]croaring.Bitmap



type IdToBitmap struct {
    lenFields int
    fields    []IToBMap
    stats     IToBStats
}

func NewIdToBitmap(invertNum ...int) *IdToBitmap {
    num := 0
    if len(invertNum) > 0 {
        num = invertNum[0]
    }
    itb := &IdToBitmap{
        lenFields: num,
        fields:    make([]IToBMap, num, num),
    }
    for index := range itb.fields {
        itb.fields[index] = make(IToBMap)
    }
    return itb
}

// AddToBitmaps 添加bitmap列表是直接将id添加到对应bitmap当中
func (itb *IdToBitmap) AddToBitmaps(id uint32, bitmaps ...croaring.Bitmap) {
    itb.add(id, bitmaps, func(bitmap croaring.Bitmap) {
        bitmap.Add(id)
    })
}

func (itb *IdToBitmap) Add(id uint32, bitmaps ...croaring.Bitmap) {
    itb.add(id, bitmaps, nil)
}

func (itb *IdToBitmap) add(id uint32, bitmaps []croaring.Bitmap, cb func(bitmap croaring.Bitmap)) {
    h, l := itb.splitId(id)
    if diff := len(bitmaps) - itb.lenFields; diff > 0 {
        for i := 0; i < diff; i++ {
            itb.lenFields++
            itb.fields = append(itb.fields, make(IToBMap))
        }
    }

    for index, bitmap := range bitmaps {
        if cb != nil {
            cb(bitmap)
        }
        m, has := itb.fields[index][h]
        if !has {
            m = make(map[uint16]croaring.Bitmap)
            itb.fields[index][h] = m
        }
        m[l] = bitmap
        itb.stats.Count++
    }
}

// DelFromBitmaps 删除id时，直接从bitmap中将该id删除
func (itb *IdToBitmap) DelFromBitmaps(id uint32) {
    itb.del(id, func(bitmap croaring.Bitmap) {
        bitmap.Remove(id)
        if bitmap.IsEmpty() {
            itb.stats.Empty++
        }
    })
}

func (itb *IdToBitmap) Del(id uint32) {
    itb.del(id, nil)
}

func (itb *IdToBitmap) del(id uint32, cb func(bitmap croaring.Bitmap)) {
    h, l := itb.splitId(id)
    for _, field := range itb.fields {
        m, hasH := field[h]
        if !hasH {
            break
        }

        bitmap, hasL := m[l]
        if !hasL {
            break
        }

        if cb != nil {
            cb(bitmap)
        }

        delete(m, l)

        if len(m) <= 0 {
            delete(field, h)
        }

        itb.stats.Count--
    }
}

func (itb *IdToBitmap) Replace(id uint32, bitmaps ...croaring.Bitmap) {
    itb.Del(id)
    itb.Add(id, bitmaps...)
}

func (itb *IdToBitmap) ToArray(id uint32) (bitmaps []croaring.Bitmap) {
    itb.Iterate(id, func(bitmap croaring.Bitmap) bool {
        bitmaps = append(bitmaps, bitmap)
        return true
    })
    return
}

func (itb *IdToBitmap) Iterate(id uint32, cb func(bitmap croaring.Bitmap) bool) {
    h, l := itb.splitId(id)
    for _, field := range itb.fields {
        m, hasH := field[h]
        if !hasH {
            break
        }

        bitmap, hasL := m[l]
        if !hasL {
            break
        }

        if !cb(bitmap) {
            break
        }
    }
}

func (itb *IdToBitmap) splitId(id uint32) (h uint16, l uint16) {
    h = uint16((id >> 16) & 0xFFFF)
    l = uint16(id & 0xFFFF)
    return
}

func (itb *IdToBitmap) GetEmptyNum() uint64 {
    return itb.stats.Empty
}

func (itb *IdToBitmap) GetBitmapNum() uint64 {
    return itb.stats.Count
}

func (itb *IdToBitmap) GetStats() IToBStats {
    return itb.stats
}

func (itb *IdToBitmap) ResetEmptyNum() {
    itb.stats.Empty = 0
}
