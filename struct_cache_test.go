package dm

import (
    "fmt"
    "reflect"
    "testing"
)

func Test_structCache_getIndexInfo(t *testing.T) {
    type A struct {
        Key1 int    `dm:"index"`
        Key2 string `dm:"invert"`
        Key3 int
        Key4 bool   `dm:"invert"`
        Key5 uint8  `dm:"invert"`
        Key6 uint8  `dm:"invert"`
        Key7 uint8
        Key8 uint8
        Key9 uint8
    }

    info, err := structs.getIndexInfo(reflect.TypeOf(A{}))
    if err != nil {
        panic(err)
    }

    fmt.Printf("indexField:%+v\n", info.indexField)

    for idx, field := range info.invertFields {
        fmt.Printf("invertFields idx:%d filed:%+v\n", idx, field)
    }

    fmt.Printf("fieldName2Idx:%+v\n", info.fieldName2Idx)
}
