package dm

import (
    "fmt"
    "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/container/dm/croaring"
    "runtime"
    "testing"
    "time"
)

const MaxId = 600 * 10000

func TestIdToBitmapMem(t *testing.T) {
    itb := NewIdToBitmap(5)
    for i := 1; i <= MaxId; i++ {
        bitmaps := make([]croaring.Bitmap, 0, 10)
        for j := 1; j <= 10; j++ {
            bitmaps = append(bitmaps, croaring.Bitmap(i*10000+j))
        }
        itb.Add(uint32(i), bitmaps...)
    }
    for {
        fmt.Println(len(itb.ToArray(1)))
        fmt.Println("array", itb.stats)
        runtime.GC()
        time.Sleep(time.Minute)
    }
    fmt.Println("over")
}

func TestIdToBitMemRandom(t *testing.T) {
    itb := NewIdToBitmap(5)
    var k = 30
    for i := 1; i <= MaxBinaryId; i++ {
        bitmaps := make([]croaring.Bitmap, 0, 10)
        for j := 1; j <= k; j++ {
            bitmaps = append(bitmaps, croaring.Bitmap(i*10000+j))
        }
        k--
        if k <= 0 {
            k = 30
        }
        itb.Add(uint32(i), bitmaps...)
    }

    for {
        fmt.Println(len(itb.ToArray(1)))
        runtime.GC()
        time.Sleep(time.Minute)
    }

    fmt.Println("over")
}

func TestIdToBitmap(t *testing.T) {
    check := make(map[uint32]map[croaring.Bitmap]bool)

    itb := NewIdToBitmap(5)
    for i := 1; i <= MaxId; i++ {
        bitmaps := make([]croaring.Bitmap, 0, 10)
        for j := 1; j <= 10; j++ {
            bitmaps = append(bitmaps, croaring.Bitmap(i*10000+j))
            if _, has := check[uint32(i)]; !has {
                check[uint32(i)] = make(map[croaring.Bitmap]bool)
            }
            check[uint32(i)][croaring.Bitmap(i*10000+j)] = true
        }
        itb.Add(uint32(i), bitmaps...)
    }

    if stats := itb.GetStats(); stats.Count != MaxId*10 {
        t.Error("GetStats count err", stats)
    }

    for i := 1; i <= MaxId; i++ {
        itb.Iterate(uint32(i), func(bitmap croaring.Bitmap) bool {
            if _, has := check[uint32(i)][bitmap]; !has {
                t.Error("id", i, "check Iterate failed. because", bitmap)
                return false
            }
            return true
        })
    }

    for i := 1; i <= MaxId; i++ {
        array := itb.ToArray(uint32(i))
        for _, bitmap := range array {
            if _, has := check[uint32(i)][bitmap]; !has && len(array) != len(check[uint32(i)]) {
                t.Error("id", i, "check ToArray failed. because", bitmap)
            }
        }
    }

    for i := 1; i <= MaxId; i++ {
        itb.Del(uint32(i))
    }

    if stats := itb.GetStats(); stats.Count != 0 {
        t.Error("GetStats count err", stats)
    }

    for i := 1; i <= MaxId; i++ {
        array := itb.ToArray(50)
        if len(array) > 0 {
            t.Error("Del 50 failed. because", array)
        }
    }
    t.Log("end")
}
