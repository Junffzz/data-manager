package dm

import (
    "fmt"
    "testing"
)

func TestNewARCStore(t *testing.T) {
    s := NewARCStore(10)
    fmt.Printf("%+v\n", s)
}
