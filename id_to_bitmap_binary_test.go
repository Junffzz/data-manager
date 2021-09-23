package dm

import (
    "fmt"
    "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/container/dm/croaring"
    "runtime"
    "testing"
    "time"
)

const MaxBinaryId = 600 * 10000

func Test_binarySearch(t *testing.T) {
    //fmt.Println(binarySearch([]uint16{}, 3))
    //fmt.Println(binarySearch([]uint16{1}, 3))
    //fmt.Println(binarySearch([]uint16{1, 2}, 3))
    //fmt.Println(binarySearch([]uint16{1, 2, 3}, 3))
    //fmt.Println(binarySearch([]uint16{2, 3, 6, 7, 8, 9}, 3))
    //fmt.Println(binarySearch([]uint16{2, 3, 6, 7, 8, 9}, 4))
    //fmt.Println(binarySearch([]uint16{2, 3, 6, 7, 8, 9}, 1))
    //fmt.Println(binarySearch([]uint16{2, 3, 6, 7, 8, 9}, 10))
}

func TestIdToBitBinaryMem(t *testing.T) {
    itb := NewIdToBitmapBinary(5)
    for i := 1; i <= MaxBinaryId; i++ {
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

func TestIdToBitBinaryMemRandom(t *testing.T) {
    itb := NewIdToBitmapBinary(5)
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
        fmt.Println("array", itb.stats.Binary.Array)
        runtime.GC()
        time.Sleep(time.Minute)
    }

    fmt.Println("over")
}

func TestIdToBitBinaryRandom(t *testing.T) {
    maxId := 20
    idMap := make(map[int]struct{}, maxId)
    itb := NewIdToBitmapBinary(5)
    for i := 0; i < maxId; i++ {
        idMap[i] = struct{}{}
    }

    for id, _ := range idMap {
        bitmaps := make([]croaring.Bitmap, 0, 10)
        for j := 1; j <= 10; j++ {
            bitmaps = append(bitmaps, croaring.Bitmap(id*10000+j))
        }
        itb.Add(uint32(id), bitmaps...)
    }
    fmt.Println("over")
}

func TestIdToBitmapBinary(t *testing.T) {
    check := make(map[uint32]map[croaring.Bitmap]bool)

    itb := NewIdToBitmapBinary(5)
    for i := 1; i <= MaxBinaryId; i++ {
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

    if stats := itb.GetStats(); stats.Count != MaxBinaryId*10 {
        t.Error("GetStats count err", stats)
    }

    for i := 1; i <= MaxBinaryId; i++ {
        itb.Iterate(uint32(i), func(bitmap croaring.Bitmap) bool {
            if _, has := check[uint32(i)][bitmap]; !has {
                t.Error("id", i, "check Iterate failed. because", bitmap)
                return false
            }
            return true
        })
    }

    for i := 1; i <= MaxBinaryId; i++ {
        array := itb.ToArray(uint32(i))
        for _, bitmap := range array {
            if _, has := check[uint32(i)][bitmap]; !has && len(array) != len(check[uint32(i)]) {
                t.Error("id", i, "check ToArray failed. because", bitmap)
            }
        }
    }

    for i := 1; i <= MaxBinaryId; i++ {
        itb.Del(uint32(i))
    }

    if stats := itb.GetStats(); stats.Count != 0 {
        t.Error("GetStats count err", stats)
    }

    for i := 1; i <= MaxBinaryId; i++ {
        array := itb.ToArray(50)
        if len(array) > 0 {
            t.Error("Del 50 failed. because", array)
        }
    }
    t.Log("end")
}
