package dm

import (
    "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/container/dm/croaring"
    "time"
)

type IToBBinaryStats struct {
    Move     uint64    // Binary版本数据搬迁次数
    LastMove time.Time // Binary版本最后一次数据搬迁时间点
    Array    int
}

type IToBStats struct {
    Count  uint64 // 当前存储的bitmap总数
    Empty  uint64 // 发现为空的bitmap数量
    Binary IToBBinaryStats
}

type IIdToBitmap interface {
    AddToBitmaps(id uint32, bitmaps ...croaring.Bitmap)
    Add(id uint32, bitmaps ...croaring.Bitmap)
    DelFromBitmaps(id uint32)
    Del(id uint32)
    Replace(id uint32, bitmaps ...croaring.Bitmap)
    ToArray(id uint32) (bitmaps []croaring.Bitmap)
    Iterate(id uint32, cb func(bitmap croaring.Bitmap) bool)

    GetEmptyNum() uint64
    GetBitmapNum() uint64
    GetStats() IToBStats
    ResetEmptyNum()
}
