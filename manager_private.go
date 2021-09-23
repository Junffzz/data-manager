package dm

import (
    "reflect"

    "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/container/dm/croaring"
    "github.com/google/go-cmp/cmp"

    "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/exception"
    reflectEx "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/reflect"
)

type bitmapInfo struct {
    needFree bool               // 是否需要手动释放的
    bitmap   croaring.Bitmap    // bitmap
}

type bitmapInfos []bitmapInfo
func (p bitmapInfos) Len() int           { return len(p) }
func (p bitmapInfos) Less(i, j int) bool { return p[i].bitmap.Cardinality() < p[j].bitmap.Cardinality() }
func (p bitmapInfos) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type dataValueInfo struct {
    dataIndex   DataIndex                   // 主键索引值
    dataValue   reflect.Value               // 数据对应的反射值
    indexValue  reflect.Value               // 主键索引对应的反射值
    invertValue map[string]reflect.Value    // 倒排索引对应的反射值数组
}

// 获取主键索引值
func (d *dataValueInfo) getIndex() DataIndex {
    return d.dataIndex
}

/*************************************************************/

func (receiver *DataManager) init(store IStore, i2b IIdToBitmap) {
    receiver.dataStore = store
    receiver.fieldMap = make(InvertFieldMap)
    receiver.indexToBitmap = i2b
}

func (receiver *DataManager) RLock() {
    receiver.operateLock.RLock()
}

func (receiver *DataManager) RUnlock() {
    receiver.operateLock.RUnlock()
}

func (receiver *DataManager) Lock() {
    receiver.operateLock.Lock()
}

func (receiver *DataManager) Unlock() {
    receiver.operateLock.Unlock()
}

// 字段直接比较
func (receiver *DataManager) compareFields(data AnyData, fields FieldMap) bool {
    if len(fields) == 0 {
        return false
    }

    for name, value := range fields {
        // 获取索引字段的值
        dataValue, err := reflectEx.GetField(data, name)
        if err != nil {
            return false
        }

        // 使用cmp比较字段是否相同, 类型必须相同
        if cmp.Equal(dataValue, value) == false {
            return false
        }
    }

    return true
}

// 获取结构体的主键 + 倒排索引的Value
func (receiver *DataManager) getIndexValue(data AnyData) (dvi dataValueInfo) {
    v := reflect.ValueOf(data)
    if v.Kind() == reflect.Ptr {
        v = v.Elem()
    }

    dvi.dataValue  = v
    dvi.indexValue = dvi.dataValue.FieldByIndex(receiver.indexInfo.indexField.Index)
    dvi.dataIndex  = dvi.indexValue.Interface().(DataIndex)

    if size := len(receiver.indexInfo.invertFields); size > 0 {
        dvi.invertValue = make(map[string]reflect.Value, size)
        for i := 0; i < size; i++ {
            name := receiver.indexInfo.invertFields[i].Name
            dvi.invertValue[name] = dvi.dataValue.FieldByIndex(receiver.indexInfo.invertFields[i].Index)
        }
    }
    return
}

// wrap -> FieldInvertFilter
func (receiver *DataManager) wrapFieldInvertFilter(data AnyData, name string, value AnyData) (build bool) {
    exception.TryCatch(func() {
        build = true
        if receiver.invertFilter != nil {
            build = receiver.invertFilter.FieldInvertFilter(data, name, value)
        }
    })
    return
}

// wrap -> FieldInvertFilterMap
func (receiver *DataManager) wrapFieldInvertFilterMap(data AnyData) (filter FieldMap) {
    exception.TryCatch(func() {
        if receiver.invertFilter != nil {
            filter = receiver.invertFilter.FieldInvertFilterMap(data)
        }
    })
    return
}

// 倒排索引名 -> (倒排索引值 -> 记录ID的RBM)
func (receiver *DataManager) initFieldMap(name string) InvertValueMap {
    value, ok := receiver.fieldMap[name]
    if ok {
        return value
    }

    value = make(InvertValueMap)
    receiver.fieldMap[name] = value
    return value
}

func (receiver *DataManager) initAllFieldIndex(data AnyData, pdvi *dataValueInfo) {
    filter := receiver.wrapFieldInvertFilterMap(data)

    if pdvi == nil {
        dvi := receiver.getIndexValue(data)
        pdvi = &dvi
    }

    bitmaps := make([]croaring.Bitmap, 0, len(pdvi.invertValue))

    // 循环初始化倒排索引
    for name, value := range pdvi.invertValue {
        // 初始化出倒排字段的结构, 此处不应被过滤, 倒排字段必须先构建出结构
        // 否则, 有倒排的字段也会因为没有数据或被过滤, 在查询过程中触发慢查询(全表扫描)
        receiver.initFieldMap(name)

        // 过滤器判断
        // filter == nil -> 不进行过滤
        // filter != nil -> 未在filter中出现的fieldName -> 进行过滤
        if filter != nil {
            if _, ok := filter[name]; ok == false {
                continue
            }
        }

        if value.Kind() == reflect.Slice {
            for i := 0; i < value.Len(); i++ {
                elem := value.Index(i)
                // receiver.initFieldIndex(data, name, elem.Interface(), pdvi.dataIndex)
                bitmap := receiver.initFieldBitmap(data, name, elem.Interface())
                if bitmap.IsValid() {
                    bitmaps = append(bitmaps, bitmap)
                }
            }
        } else {
            // receiver.initFieldIndex(data, name, value.Interface(), pdvi.dataIndex)
            bitmap := receiver.initFieldBitmap(data, name, value.Interface())
            if bitmap.IsValid() {
                bitmaps = append(bitmaps, bitmap)
            }
        }
    }

    if len(bitmaps) > 0 {
        receiver.indexToBitmap.AddToBitmaps(uint32(pdvi.dataIndex), bitmaps...)
    }
}

func (receiver *DataManager) deleteAllFieldIndex(dataIndex DataIndex) {
    // TODO indexToBitmap中需要维护Bitmap的
    // 直接删除数据和Bitmap之间的关系
    receiver.indexToBitmap.DelFromBitmaps(uint32(dataIndex))
}

// 删除全部倒排字段的倒排索引
// 内部私有函数, indexInfo的空值不用检测
func (receiver *DataManager) deleteDataAllFieldIndex(data AnyData, pdvi *dataValueInfo) {
    if pdvi == nil {
        dvi := receiver.getIndexValue(data)
        pdvi = &dvi
    }

    // 循环删除倒排索引
    for name, value := range pdvi.invertValue {
        if value.Kind() == reflect.Slice {
            for i := 0; i < value.Len(); i++ {
                elem := value.Index(i)
                receiver.deleteFieldIndex(data, name, elem.Interface(), pdvi.dataIndex)
            }
        } else {
            receiver.deleteFieldIndex(data, name, value.Interface(), pdvi.dataIndex)
        }
    }
}

// 构造倒排字段的倒排索引
func (receiver *DataManager) initFieldBitmap(data AnyData, name string, value AnyData) (bitmap croaring.Bitmap) {
    // 当有定制的构建规则 -> 判断是否需要构建倒排索引
    if build := receiver.wrapFieldInvertFilter(data, name, value); build == false {
        return
    }

    // 保护性的初始化FieldMap结构
    // 倒排索引名 -> (倒排索引值 -> 记录ID的RBM)
    fieldInvert := receiver.initFieldMap(name)

    // 倒排索引值 -> 记录ID的RBM -> 如果不存在构建新的Bitmap
    has := false
    bitmap, has = fieldInvert[value]
    if !has {
        bitmap = croaring.New()
        fieldInvert[value] = bitmap
    }
    return
}

// 构造倒排字段的倒排索引
func (receiver *DataManager) initFieldIndex(data AnyData, name string, value AnyData, index DataIndex) {
    // 当有定制的构建规则 -> 判断是否需要构建倒排索引
    if build := receiver.wrapFieldInvertFilter(data, name, value); build == false {
        return
    }

    // 保护性的初始化FieldMap结构
    // 倒排索引名 -> (倒排索引值 -> 记录ID的RBM)
    fieldInvert := receiver.initFieldMap(name)

    // 倒排索引值 -> 记录ID的RBM -> RBM中添加记录的Index值
    if bm, ok := fieldInvert[value]; !ok {
        fieldInvert[value] = croaring.New(uint32(index))
    } else {
        bm.Add(uint32(index))
    }
}

// 删除倒排字段的倒排索引
func (receiver *DataManager) deleteFieldIndex(data AnyData, name string, value AnyData, index DataIndex) {
    // 倒排索引名 -> (倒排索引值 -> 记录ID的RBM)
    fieldInvert, hasInvert := receiver.fieldMap[name]
    if !hasInvert {
        return
    }

    // 倒排索引值 -> 记录ID的RBM
    bitmap, hasValue := fieldInvert[value]
    if !hasValue {
        return
    }

    // 记录ID的RMP中删除记录的Index值
    bitmap.Remove(uint32(index))

    // 当bitmap中没有元素时候, 删除map中的倒排索引值
    if bitmap.IsEmpty() {
        // 释放C侧的Bitmap资源
        croaring.Free(bitmap)

        delete(fieldInvert, value)
    }
}