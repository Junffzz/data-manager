package dm

import (
    "fmt"
    "reflect"
    "sync"
)

var structs = newStructCache(true)

type structIndexInfo struct {
    indexField          *reflect.StructField  // 主键索引对应的结构体字段
    invertFields        []reflect.StructField // 倒排索引对应的结构体字段
    fieldName2Idx       map[string]int        // 字段名 -> 字段编号的映射
    fieldName2InvertIdx map[string]int        // 字段名 -> invertFields数组索引
}

// 获取主键索引的字段名
func (s *structIndexInfo) getIndexFieldName() string {
    if s == nil {
        return ""
    }
    if s.indexField == nil {
        return ""
    }
    return s.indexField.Name
}

// 根据倒排字段名获取倒排字段的序号
func (s *structIndexInfo) getInvertIdxByName(fieldName string) int {
    fieldIdx, has := s.fieldName2InvertIdx[fieldName]
    if !has {
        return -1
    }

    if fieldIdx >= len(s.invertFields) {
        return -1
    }

    return fieldIdx
}

// 根据倒排字段名获取倒排字段结构
func (s *structIndexInfo) getInvertByName(fieldName string) (structField *reflect.StructField) {
    fieldIdx := s.getInvertIdxByName(fieldName)
    if fieldIdx == -1 {
        return
    }

    return &(s.invertFields[fieldIdx])
}

// 根据倒排字段名获取倒排字段类型
func (s *structIndexInfo) getInvertTypeByName(fieldName string) (fieldType reflect.Type) {
    fieldIdx := s.getInvertIdxByName(fieldName)
    if fieldIdx == -1 {
        return
    }

    return s.invertFields[fieldIdx].Type
}

// structCache 对结构体进行分析 -> 缓存结构体类型对应的字段分析结果
type structCache struct {
    use   bool
    cache sync.Map
}

func newStructCache(use bool) *structCache {
    return &structCache{
        use: use,
    }
}

func (m *structCache) getIndexInfo(typ reflect.Type) (*structIndexInfo, error) {
    if typ.Kind() == reflect.Ptr {
        typ = typ.Elem()
    }

    // 类型不是结构体
    if typ.Kind() != reflect.Struct {
        return nil, fmt.Errorf("input type:%s is non struct", typ.Name())
    }

    // 使用结构体解析缓存
    if m.use {
        if v, ok := m.cache.Load(typ); ok {
            return v.(*structIndexInfo), nil
        }

        sii, err := m.analyseIndexInfo(typ)
        if err != nil {
            return nil, err
        }

        m.cache.Store(typ, sii)
        return sii, nil
    }

    return m.analyseIndexInfo(typ)
}

// 分析结构体 -> 解析其中的主键索引 + 倒排索引
func (m *structCache) analyseIndexInfo(typ reflect.Type) (*structIndexInfo, error) {
    count := typ.NumField()

    info := &structIndexInfo{
        invertFields:        make([]reflect.StructField, 0, count),
        fieldName2Idx:       make(map[string]int, count),
        fieldName2InvertIdx: make(map[string]int, count),
    }

    // invertFields的索引
    invertIdx := 0

    for i := 0; i < count; i++ {
        field := typ.Field(i)

        // 存储字段名 -> 字段编号的映射
        if len(field.Index) > 0 {
            info.fieldName2Idx[field.Name] = field.Index[0]
        }

        // 导出字段的PkgPath为空
        // 过滤非导出字段
        if field.PkgPath != "" {
            continue
        }

        switch field.Tag.Get(TAG_NAME) {
        case TAG_SKIP:
            continue
        case IDX_NAME:
            // 主键索引字段重复
            if info.indexField != nil {
                return nil, fmt.Errorf("[analyseIndexInfo] input typ:[%s] index field duplication", typ.Name())
            }
            info.indexField = &field
        case IVT_NAME:
            info.invertFields = append(info.invertFields, field)
            info.fieldName2InvertIdx[field.Name] = invertIdx
            invertIdx++
        }
    }

    // 类型没有主键索引字段
    if info.indexField == nil {
        return nil, fmt.Errorf("[analyseIndexInfo] input typ:[%s] has no index field", typ.Name())
    }

    return info, nil
}
