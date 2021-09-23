package main

import (
    "fmt"
    "runtime"
    "time"

    "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/container/dm/croaring"
    // "github.com/RoaringBitmap/gocroaring"
    "github.com/RoaringBitmap/roaring"
)

// gc 1 @0.005s 1%: 0.084+0.20+0.010 ms clock, 0.67+0/0.070/0.21+0.085 ms cpu, 5->5->3 MB, 6 MB goal, 8 P
// gc 2 @0.011s 1%: 0.002+0.11+0.002 ms clock, 0.018+0.070/0.032/0.11+0.020 ms cpu, 9->9->7 MB, 10 MB goal, 8 P
// gc 3 @0.022s 0%: 0.002+0.20+0.012 ms clock, 0.019+0/0.15/0.14+0.10 ms cpu, 18->18->15 MB, 19 MB goal, 8 P
// gc 4 @0.048s 0%: 0.002+0.32+0.012 ms clock, 0.023+0/0.26/0.15+0.097 ms cpu, 36->36->30 MB, 37 MB goal, 8 P
// gc 5 @0.130s 0%: 0.003+0.70+0.013 ms clock, 0.024+0/0.61/0.20+0.10 ms cpu, 72->72->60 MB, 73 MB goal, 8 P
// gc 6 @0.324s 0%: 0.003+1.3+0.010 ms clock, 0.031+0/1.2/0.26+0.082 ms cpu, 144->144->120 MB, 145 MB goal, 8 P
// gc 7 @0.720s 0%: 0.003+2.1+0.019 ms clock, 0.028+0.29/0.91/1.9+0.15 ms cpu, 288->288->241 MB, 289 MB goal, 8 P
// gc 8 @1.461s 0%: 0.003+2.4+0.012 ms clock, 0.030+0.29/3.5/2.1+0.10 ms cpu, 577->577->482 MB, 578 MB goal, 8 P
// gc 9 @2.634s 0%: 0.003+0.12+0.004 ms clock, 0.031+0/0.12/0.10+0.033 ms cpu, 486->486->0 MB, 964 MB goal, 8 P (forced)
func testMap_int(n int) {
    m := make(map[int]int)
    for i := 0; i < n; i++ {
        m[i] = i
    }
}

// gc 1 @0.006s 2%: 0.005+1.2+0.009 ms clock, 0.046+0.11/1.4/0.38+0.077 ms cpu, 5->5->3 MB, 6 MB goal, 8 P
// gc 2 @0.012s 4%: 0.002+2.5+0.008 ms clock, 0.018+0.11/3.3/0.083+0.070 ms cpu, 8->8->7 MB, 9 MB goal, 8 P
// gc 3 @0.025s 5%: 0.003+4.7+0.008 ms clock, 0.024+0.038/7.0/1.1+0.064 ms cpu, 16->16->14 MB, 17 MB goal, 8 P
// gc 4 @0.054s 3%: 0.002+13+0.034 ms clock, 0.020+1.2/7.4/11+0.27 ms cpu, 32->33->28 MB, 33 MB goal, 8 P
// gc 5 @0.131s 4%: 0.003+17+0.018 ms clock, 0.026+0.27/33/5.9+0.15 ms cpu, 65->66->56 MB, 66 MB goal, 8 P
// gc 6 @0.289s 4%: 0.003+33+0.009 ms clock, 0.028+0.35/53/13+0.076 ms cpu, 131->132->113 MB, 132 MB goal, 8 P
// gc 7 @0.572s 4%: 0.003+46+0.011 ms clock, 0.025+0.39/91/108+0.091 ms cpu, 263->263->225 MB, 264 MB goal, 8 P
// gc 8 @1.226s 3%: 0.003+75+0.011 ms clock, 0.024+0.57/149/349+0.093 ms cpu, 526->527->451 MB, 527 MB goal, 8 P
// gc 9 @2.607s 3%: 0.003+177+0.010 ms clock, 0.031+2.4/352/842+0.086 ms cpu, 1053->1054->902 MB, 1054 MB goal, 8 P
// gc 10 @4.177s 2%: 0.003+0.46+0.003 ms clock, 0.028+0/0.39/1.7+0.030 ms cpu, 1022->1022->0 MB, 1805 MB goal, 8 P (forced)
func testMap_string(n int) {
    m := make(map[int]string)
    for i := 0; i < n; i++ {
        m[i] = fmt.Sprintf("Hello World %d", i)
    }
}

// gc 1 @0.076s 0%: 0.005+2.2+0.007 ms clock, 0.045+0.23/3.3/4.3+0.063 ms cpu, 6->6->4 MB, 7 MB goal, 8 P
// gc 2 @0.161s 0%: 0.003+5.5+0.015 ms clock, 0.027+0.19/9.0/2.9+0.12 ms cpu, 10->10->9 MB, 11 MB goal, 8 P
// gc 3 @0.343s 0%: 0.004+7.4+0.020 ms clock, 0.033+0.22/14/16+0.16 ms cpu, 20->20->18 MB, 21 MB goal, 8 P
// gc 4 @0.672s 1%: 0.002+18+0.010 ms clock, 0.023+0.23/32/20+0.083 ms cpu, 41->41->36 MB, 42 MB goal, 8 P
// gc 5 @1.373s 1%: 0.002+28+0.011 ms clock, 0.023+0.36/54/104+0.091 ms cpu, 82->82->73 MB, 83 MB goal, 8 P
// gc 6 @2.702s 1%: 0.003+64+0.009 ms clock, 0.029+0.66/124/231+0.073 ms cpu, 164->164->146 MB, 165 MB goal, 8 P
// gc 7 @5.393s 1%: 0.004+118+0.009 ms clock, 0.034+3.5/221/596+0.075 ms cpu, 329->329->292 MB, 330 MB goal, 8 P
// gc 8 @10.711s 1%: 0.003+313+0.005 ms clock, 0.029+6.0/617/1566+0.047 ms cpu, 658->659->584 MB, 659 MB goal, 8 P
// gc 9 @16.149s 0%: 0.003+62+0.003 ms clock, 0.027+0/100/370+0.028 ms cpu, 620->620->76 MB, 1169 MB goal, 8 P (forced)
// func testMap_pgocroaring(n int) {
//     m := make(map[int]*gocroaring.Bitmap)
//     for i := 0; i < n; i++ {
//         m[i] = gocroaring.New(uint32(i))
//     }
// }

// gc 1 @0.078s 0%: 0.005+1.9+0.012 ms clock, 0.047+0.14/2.0/2.2+0.10 ms cpu, 6->6->4 MB, 7 MB goal, 8 P
// gc 2 @0.179s 0%: 0.004+3.6+0.007 ms clock, 0.038+0.15/4.6/4.3+0.062 ms cpu, 10->10->8 MB, 11 MB goal, 8 P
// gc 3 @0.384s 0%: 0.003+5.5+0.010 ms clock, 0.025+0.084/8.6/4.5+0.082 ms cpu, 19->19->15 MB, 20 MB goal, 8 P
// gc 4 @0.762s 0%: 0.008+13+0.005 ms clock, 0.065+0.11/25/17+0.043 ms cpu, 38->38->31 MB, 39 MB goal, 8 P
// gc 5 @1.498s 0%: 0.003+14+0.012 ms clock, 0.024+0.098/28/31+0.098 ms cpu, 77->77->63 MB, 78 MB goal, 8 P
// gc 6 @2.870s 0%: 0.003+29+0.012 ms clock, 0.026+0.11/51/80+0.098 ms cpu, 155->155->126 MB, 156 MB goal, 8 P
// gc 7 @5.612s 0%: 0.003+49+0.004 ms clock, 0.031+2.3/89/254+0.035 ms cpu, 309->310->253 MB, 310 MB goal, 8 P
// gc 8 @11.371s 0%: 0.005+140+0.008 ms clock, 0.044+5.3/255/712+0.070 ms cpu, 619->619->506 MB, 620 MB goal, 8 P
// gc 9 @17.259s 0%: 0.004+30+0.004 ms clock, 0.032+0/24/156+0.038 ms cpu, 542->542->24 MB, 1012 MB goal, 8 P (forced)
// func testMap_gocroaring(n int) {
//     m := make(map[int]gocroaring.Bitmap)
//     for i := 0; i < n; i++ {
//         m[i] = *gocroaring.New(uint32(i))
//     }
// }

// gc 1 @0.048s 0%: 0.007+1.3+0.002 ms clock, 0.058+1.3/1.5/1.0+0.023 ms cpu, 4->4->3 MB, 5 MB goal, 8 P
// gc 2 @0.085s 0%: 0.003+3.1+0.018 ms clock, 0.024+0.12/3.0/2.5+0.14 ms cpu, 5->5->5 MB, 6 MB goal, 8 P
// gc 3 @0.117s 1%: 0.003+5.3+0.015 ms clock, 0.027+0.28/8.6/7.8+0.12 ms cpu, 10->10->10 MB, 11 MB goal, 8 P
// gc 4 @0.267s 1%: 0.004+9.4+0.005 ms clock, 0.035+0.42/16/17+0.045 ms cpu, 22->22->21 MB, 23 MB goal, 8 P
// gc 5 @0.545s 1%: 0.003+15+0.004 ms clock, 0.029+0.36/28/29+0.036 ms cpu, 45->45->43 MB, 46 MB goal, 8 P
// gc 6 @1.002s 1%: 0.003+22+0.017 ms clock, 0.031+0.34/39/75+0.13 ms cpu, 90->90->86 MB, 91 MB goal, 8 P
// gc 7 @1.898s 1%: 0.004+39+0.016 ms clock, 0.036+3.6/65/180+0.12 ms cpu, 181->181->172 MB, 182 MB goal, 8 P
// gc 8 @3.669s 1%: 0.003+116+0.013 ms clock, 0.029+8.2/207/586+0.10 ms cpu, 361->362->344 MB, 362 MB goal, 8 P
// gc 9 @7.262s 1%: 0.004+318+0.015 ms clock, 0.039+31/630/1544+0.12 ms cpu, 723->724->688 MB, 724 MB goal, 8 P
// gc 10 @11.078s 1%: 0.004+48+0.003 ms clock, 0.038+0/70/292+0.028 ms cpu, 737->737->152 MB, 1377 MB goal, 8 P (forced)
// 889.121898ms
// func testMap_pcroaring(n int) {
//     m := make(map[int]*croaring.Bitmap)
//     for i := 0; i < n; i++ {
//         m[i] = croaring.New(uint32(i))
//     }
// }

// gc 1 @0.025s 0%: 0.005+0.23+0.017 ms clock, 0.040+0.098/0.040/0.27+0.13 ms cpu, 6->6->3 MB, 7 MB goal, 8 P
// gc 2 @0.047s 0%: 0.002+0.13+0.002 ms clock, 0.022+0.10/0.045/0.16+0.022 ms cpu, 9->9->7 MB, 10 MB goal, 8 P
// gc 3 @0.095s 0%: 0.003+0.21+0.003 ms clock, 0.025+0.18/0.069/0.25+0.025 ms cpu, 18->18->15 MB, 19 MB goal, 8 P
// gc 4 @0.202s 0%: 0.003+0.25+0.003 ms clock, 0.024+0.23/0.018/0.14+0.025 ms cpu, 37->37->30 MB, 38 MB goal, 8 P
// gc 5 @0.458s 0%: 0.003+0.60+0.014 ms clock, 0.026+0.32/0.33/0.22+0.11 ms cpu, 74->74->60 MB, 75 MB goal, 8 P
// gc 6 @0.984s 0%: 0.003+1.2+0.022 ms clock, 0.031+0.26/0.89/0.20+0.17 ms cpu, 147->147->120 MB, 148 MB goal, 8 P
// gc 7 @2.005s 0%: 0.004+1.6+0.015 ms clock, 0.035+0.29/0.66/1.5+0.12 ms cpu, 295->295->241 MB, 296 MB goal, 8 P
// gc 8 @3.940s 0%: 0.004+3.9+0.016 ms clock, 0.032+0.22/3.4/0.32+0.13 ms cpu, 590->590->482 MB, 591 MB goal, 8 P
// gc 9 @6.007s 0%: 0.003+0.13+0.003 ms clock, 0.029+0/0.14/0.082+0.028 ms cpu, 498->498->0 MB, 964 MB goal, 8 P (forced)
// 2.286319ms
func testMap_croaring(n int) {
    m := make(map[int]croaring.Bitmap)
    for i := 0; i < n; i++ {
        m[i] = croaring.New(uint32(i))
    }
}

// gc 1 @0.007s 3%: 0.005+1.4+0.019 ms clock, 0.046+0.30/2.2/4.4+0.15 ms cpu, 4->4->3 MB, 5 MB goal, 8 P
// gc 2 @0.013s 5%: 0.002+2.5+0.014 ms clock, 0.019+0.031/4.2/10+0.11 ms cpu, 7->7->6 MB, 8 MB goal, 8 P
// gc 3 @0.026s 5%: 0.002+12+0.012 ms clock, 0.022+4.6/5.4/13+0.099 ms cpu, 13->14->14 MB, 14 MB goal, 8 P
// gc 4 @0.056s 7%: 0.003+11+0.038 ms clock, 0.025+0.48/21/52+0.30 ms cpu, 27->27->26 MB, 28 MB goal, 8 P
// gc 5 @0.123s 7%: 0.003+22+0.014 ms clock, 0.026+0.10/42/95+0.11 ms cpu, 55->56->53 MB, 56 MB goal, 8 P
// gc 6 @0.239s 8%: 0.005+64+0.019 ms clock, 0.044+1.0/127/286+0.15 ms cpu, 110->112->107 MB, 111 MB goal, 8 P
// gc 7 @0.547s 10%: 0.003+157+0.031 ms clock, 0.027+90/313/732+0.25 ms cpu, 220->222->211 MB, 221 MB goal, 8 P
// gc 8 @1.205s 11%: 0.021+288+0.005 ms clock, 0.17+197/540/1341+0.045 ms cpu, 441->442->420 MB, 442 MB goal, 8 P
// gc 9 @2.466s 13%: 0.003+936+0.019 ms clock, 0.028+550/1827/4501+0.15 ms cpu, 882->889->845 MB, 883 MB goal, 8 P
// gc 10 @5.225s 13%: 0.003+1711+0.023 ms clock, 0.026+459/3420/8546+0.18 ms cpu, 1764->1791->1703 MB, 1765 MB goal, 8 P
// gc 11 @8.854s 10%: 0.004+1.8+0.003 ms clock, 0.034+0/1.8/11+0.028 ms cpu, 2235->2235->0 MB, 3406 MB goal, 8 P (forced)
// 137.495416ms
func testMap_roaring(n int) {
    m := make(map[int]*roaring.Bitmap)
    for i := 0; i < n; i++ {
        m[i] = roaring.NewBitmap()
        m[i].Add(uint32(i))
    }
}

// gc 1 @0.005s 0%: 0.005+0.21+0.010 ms clock, 0.041+0/0.085/0.16+0.081 ms cpu, 5->5->3 MB, 6 MB goal, 8 P
// gc 2 @0.012s 0%: 0.002+0.14+0.007 ms clock, 0.018+0/0.093/0.16+0.062 ms cpu, 9->9->7 MB, 10 MB goal, 8 P
// gc 3 @0.026s 0%: 0.002+0.21+0.009 ms clock, 0.020+0/0.15/0.11+0.074 ms cpu, 18->18->15 MB, 19 MB goal, 8 P
// gc 4 @0.060s 0%: 0.002+0.40+0.023 ms clock, 0.022+0/0.038/0.49+0.19 ms cpu, 36->36->30 MB, 37 MB goal, 8 P
// gc 5 @0.140s 0%: 0.005+0.66+0.012 ms clock, 0.044+0/0.57/0.20+0.096 ms cpu, 72->72->60 MB, 73 MB goal, 8 P
// gc 6 @0.306s 0%: 0.003+1.2+0.024 ms clock, 0.028+0/1.2/0.21+0.19 ms cpu, 144->144->120 MB, 145 MB goal, 8 P
// gc 7 @0.637s 0%: 0.003+1.6+0.015 ms clock, 0.031+0/2.6/0.17+0.12 ms cpu, 288->289->241 MB, 289 MB goal, 8 P
// gc 8 @1.395s 0%: 0.004+2.3+0.016 ms clock, 0.032+0.30/3.2/2.0+0.13 ms cpu, 577->577->482 MB, 578 MB goal, 8 P
// gc 9 @2.360s 0%: 0.003+0.12+0.004 ms clock, 0.031+0/0.13/0.068+0.034 ms cpu, 486->486->0 MB, 964 MB goal, 8 P (forced)
func testMap_struct_with_int(n int) {
    type S struct {
        X int
    }
    m := make(map[int]S)
    for i := 0; i < n; i++ {
        m[i] = S{X : i}
    }
}

// gc 1 @0.008s 4%: 0.006+2.6+0.008 ms clock, 0.052+0.22/3.5/0.24+0.068 ms cpu, 6->6->4 MB, 7 MB goal, 8 P
// gc 2 @0.016s 5%: 0.002+3.6+0.010 ms clock, 0.021+0.17/4.9/0.12+0.087 ms cpu, 9->9->8 MB, 10 MB goal, 8 P
// gc 3 @0.030s 6%: 0.002+6.8+0.008 ms clock, 0.023+0.16/9.1/1.2+0.064 ms cpu, 19->19->16 MB, 20 MB goal, 8 P
// gc 4 @0.063s 5%: 0.002+14+0.051 ms clock, 0.022+0.24/17/17+0.40 ms cpu, 38->38->33 MB, 39 MB goal, 8 P
// gc 5 @0.158s 5%: 0.007+21+0.012 ms clock, 0.063+0.076/40/58+0.098 ms cpu, 77->77->66 MB, 78 MB goal, 8 P
// gc 6 @0.364s 4%: 0.004+31+0.011 ms clock, 0.035+0.42/62/105+0.091 ms cpu, 155->155->133 MB, 156 MB goal, 8 P
// gc 7 @0.774s 4%: 0.003+77+0.009 ms clock, 0.026+0.47/153/335+0.073 ms cpu, 309->309->266 MB, 310 MB goal, 8 P
// gc 8 @1.675s 3%: 0.002+108+0.009 ms clock, 0.023+0.59/215/521+0.074 ms cpu, 619->619->532 MB, 620 MB goal, 8 P
// gc 9 @2.821s 2%: 0.004+0.18+0.004 ms clock, 0.035+0/0.19/0.26+0.034 ms cpu, 556->556->0 MB, 1064 MB goal, 8 P (forced)
func testMap_pstruct_with_int(n int) {
    type S struct {
        X int
    }
    m := make(map[int]*S)
    for i := 0; i < n; i++ {
        m[i] = &S{X : i}
    }
}

func test_id_to_bitmap(n int) {
    type idx_id struct {
        idx uint32
        id  uint32
    }

    idset := make(map[idx_id]uintptr)

    for i := 0; i < 2000000; i++ {
        for j := 0; j < 30; j++ {
            idset[idx_id{idx: uint32(j), id: uint32(i)}] = uintptr(i * 100)
        }
    }

    m := runtime.MemStats{}
    runtime.ReadMemStats(&m)
    fmt.Printf("%+v\n", m)
}

func test_id_to_bitmap_ex(n int) {
    idsets := make([]map[uint32]uintptr, 0, 30)
    for i := 0; i < 30; i++ {
        idset := make(map[uint32]uintptr)
        for j := 0; j < 2000000; j++ {
            idset[uint32(j)] = uintptr(j * 100)
        }
        idsets = append(idsets, idset)
    }

    m := runtime.MemStats{}
    runtime.ReadMemStats(&m)
    fmt.Printf("%+v\n", m)
}

func test_id_to_bitmap_ex1(n int) {
    cnt := 30
    idsets := make([]map[uint16]map[uint16]uintptr, 0, cnt)
    for i := 0; i < cnt; i++ {
        idset := make(map[uint16]map[uint16]uintptr)
        for j := 0; j < 2000000; j++ {
            x := uint32(j)
            h := uint16(x >> 16) & 0xFFFF
            l := uint16(x & 0xFFFF)

            m1, has1 := idset[h]
            if !has1 {
                m1  = make(map[uint16]uintptr)
                idset[h] = m1
            }
            m1[l] = uintptr(j * 100)
        }
        idsets = append(idsets, idset)
    }

    m := runtime.MemStats{}
    runtime.ReadMemStats(&m)
    fmt.Printf("%+v\n", m)
}

func test_id_to_bitmap_ex2(n int) {
    idsets := make([]map[uint16]map[uint8]map[uint8]uintptr, 0, 30)
    for i := 0; i < 30; i++ {
        idset := make(map[uint16]map[uint8]map[uint8]uintptr)
        for j := 0; j < 2000000; j++ {
            x := uint32(j)
            h := uint16(x >> 16) & 0xFFFF
            m := uint8(x >> 8) & 0xFF
            l := uint8(x & 0xFF)

            m1, has1 := idset[h]
            if !has1 {
                m1  = make(map[uint8]map[uint8]uintptr)
                idset[h] = m1
            }

            m2, has2 := m1[m]
            if !has2 {
                m2  = make(map[uint8]uintptr)
                m1[m] = m2
            }

            m2[l] = uintptr(j * 100)
        }
        idsets = append(idsets, idset)
    }

    m := runtime.MemStats{}
    runtime.ReadMemStats(&m)
    fmt.Printf("%+v\n", m)
}

func main() {
    n := 10000000

    // testMap_int(n)
    // testMap_string(n)
    // testMap_pgocroaring(n)
    // testMap_gocroaring(n)
    // testMap_pcroaring(n)
    // testMap_croaring_uintptr(n)
    // testMap_croaring(n)
    // testMap_roaring(n)
    // testMap_pstruct_with_int(n)
    // testMap_struct_with_int(n)
    // test_id_to_bitmap(n)
    // test_id_to_bitmap_ex(n)
    //test_id_to_bitmap_ex1(n)
    test_id_to_bitmap_ex2(n)

    s := time.Now()
    runtime.GC()

    d := time.Since(s)
    fmt.Printf("%+v\n", d)
}