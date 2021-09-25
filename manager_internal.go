package dm

import (
    "fmt"
    "reflect"
    "unsafe"

    "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/container/dm/croaring"
    "github.com/pkg/errors"

    "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/convert"
    reflectEx "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/reflect"
    "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/runtime"
    "git.100tal.com/wangxiao_xesbiz_operation/gently-utils/utils"
)

// AddDataIndex 添加主键索引过滤条件
func (fm FieldMap) AddDataIndex(index ...DataIndex) FieldMap {
    if size := len(index); size > 0 {
        v, has := fm[INDEX_FLAG]
        if !has {
            ids := make([]DataIndex, 0, len(index))
            ids = append(ids, index...)
            fm[INDEX_FLAG] = ids
        } else {
            if ids, ok := v.([]DataIndex); ok {
                ids = append(ids, index...)
                fm[INDEX_FLAG] = ids
            }
        }
    }
    return fm
}

// AddTypeConvertFlag 添加数值比对时类型转换的标记
func (fm FieldMap) AddTypeConvertFlag() FieldMap {
    fm[TYP_CONV_FLAG] = struct{}{}
    return fm
}

// HasTypeConvertFlag 判断是否有类型转换的标记
func (fm FieldMap) HasTypeConvertFlag() (has bool) {
    _, has = fm[TYP_CONV_FLAG]
    return
}

// CheckTypeConvertFlagOnce 判断是否有类型转换的标记 -> 如果存在标记 -> 删除标记
func (fm FieldMap) CheckTypeConvertFlagOnce() (has bool) {
    if _, has = fm[TYP_CONV_FLAG]; has {
        delete(fm, TYP_CONV_FLAG)
    }
    return
}

func (receiver *DataManager) initIndexInfo(data AnyData) error {
    if receiver.indexInfo == nil {
        // 获取结构体的索引字段信息
        if info, err := structs.getIndexInfo(reflect.TypeOf(data)); err != nil {
            return err
        } else {
            receiver.indexInfo = info
        }
    }
    return nil
}

func (receiver *DataManager) insertAndReturn(data AnyData, pdvis ...*dataValueInfo) (index DataIndex, ret error) {
    defer func() {
        if err := recover(); err != nil {
            ret = convert.Error(err)
        }
    }()

    // 插入数据必须为指针类型
    if typ := reflect.TypeOf(data); typ.Kind() != reflect.Ptr {
        return INVALID_IDX, fmt.Errorf("insert type:[%s] must be ptr", typ.Name())
    }

    // 初始化结构体的索引字段信息
    if err := receiver.initIndexInfo(data); err != nil {
        return INVALID_IDX, err
    }

    // 获取data的值信息 (主键 + 倒排等值)
    var pdvi *dataValueInfo
    if len(pdvis) > 0 && pdvis[0] != nil {
        pdvi = pdvis[0]
    } else {
        dvi := receiver.getIndexValue(data)
        pdvi = &dvi
    }

    // 检测主键索引是否冲突
    if ok := receiver.dataStore.HasKey(pdvi.dataIndex); ok {
        return pdvi.dataIndex, KeyConflict
    }

    // 获取数据的指针 -> 存入主键索引
    if err := receiver.dataStore.Set(pdvi.dataIndex, data); err != nil {
        return pdvi.dataIndex, err
    }

    // 构造全部倒排字段的倒排索引
    receiver.initAllFieldIndex(data, pdvi)

    return pdvi.dataIndex, nil
}

func (receiver *DataManager) updateAndReturn(index DataIndex, fieldMap FieldMap) (modified AnyData, ret error) {
    if len(fieldMap) == 0 {
        return nil, fmt.Errorf("%s index:[%+v] input fieldMap is empty, no field to update", runtime.CallerFunction(), index)
    }

    prepare := func(dataPtr AnyData) FieldNames {
        return FieldMapToFieldNames(fieldMap)
    }

    modify := func(dataPtr AnyData) error {
        modified = dataPtr
        if receiver.indexInfo != nil {
            return reflectEx.SetFields(dataPtr, fieldMap, receiver.indexInfo.fieldName2Idx)
        }
        return reflectEx.SetFields(dataPtr, fieldMap)
    }

    ret = receiver.modifyDataByIndex(prepare, modify, index)
    return
}

func (receiver *DataManager) replace(data AnyData) (err error) {
    // 初始化结构体的索引字段信息
    if err := receiver.initIndexInfo(data); err != nil {
        return err
    }

    dvi := receiver.getIndexValue(data)

    // 删除旧数据 -> 失败也不做特殊处理
    _ = receiver.delete(dvi.dataIndex)

    // 插入替换的数据, 使用已经获取的dvi, 防止重复反射
    _, err = receiver.insertAndReturn(data, &dvi)
    return

    // // 删除旧数据 -> 失败也不做特殊处理
    // _, _ = receiver.deleteAndReturn(dvi.dataIndex)
    //
    // // 插入替换数据
    // // insertAndReturn使用已经获取的dvi, 防止重复反射
    // if _, err := receiver.insertAndReturn(data, &dvi); err != nil {
    //     return err
    // }
    // return nil
}

func (receiver *DataManager) delete(index DataIndex) error {
    // 尝试删除index对应的全部倒排信息
    receiver.deleteAllFieldIndex(index)

    // 删除数据
    if has := receiver.dataStore.Del(index); !has {
        return KeyNotExists
    }
    return nil
}

func (receiver *DataManager) rebuildInvert(index DataIndex) error {
    data, ok := receiver.getDataByIndex(index)
    if !ok {
        return KeyNotExists
    }

    // 删除数据索引对应的全部倒排信息
    receiver.deleteAllFieldIndex(index)

    // 构建所有倒排字段对应的倒排索引
    receiver.initAllFieldIndex(data, nil)

    return nil
}

func (receiver *DataManager) getInvertBitmap(invert InvertValueMap, fieldName string, fieldValue interface{}, typeConvert bool) (result croaring.Bitmap) {
    // 需要进行类型转换
    if typeConvert {
        // 获取倒排字段的类型
        invertFieldType := receiver.indexInfo.getInvertTypeByName(fieldName)
        if invertFieldType == nil {
            return
        }

        // 倒排字段为Slice -> 倒排字段类型为元素类型
        if invertFieldType.Kind() == reflect.Slice {
            invertFieldType = invertFieldType.Elem()
        }

        // 判断入参类型是否可以转换到倒排字段类型
        if !reflect.TypeOf(fieldValue).ConvertibleTo(invertFieldType) {
            return
        }

        // 进行类型转换
        value := reflect.ValueOf(fieldValue)

        // 赋值类型转换后的值
        fieldValue = value.Convert(invertFieldType).Interface()
    }

    bitmap, hasValue := invert[fieldValue]

    // 查询条件未命中
    if !hasValue {
        return
    }

    // 查询结构及为空
    if bitmap.IsEmpty() {
        return
    }

    result = bitmap
    return
}

func (receiver *DataManager) getIndexsByFields(fieldMap FieldMap) (result croaring.Bitmap, missIndex FieldMap, err error) {
    if len(fieldMap) == 0 {
        // err = fmt.Errorf("%s need input FieldInfo", runtime.CallerFunction())
        // TODO 数据数量超过limit, 这里不允许查询全部数据

        result = croaring.New()
        _ = receiver.dataStore.Iterator(func(keyType KeyType, value interface{}) bool {
            result.Add(uint32(keyType))
            return true
        })
        return
    }

    defer func() {
        if e := recover(); e != nil {
            err = convert.Error(e)
        }
    }()

    // 默认比对不进行类型转换
    // 判断是否有 TYP_CONV_FLAG 标记
    // 有 TYP_CONV_FLAG 标记 -> 删除 TYP_CONV_FLAG 标记 -> 数值比对时进行类型转换
    typeConvert := fieldMap.CheckTypeConvertFlagOnce()

    // 每个字段对应的结果集列表
    bis := make(bitmapInfos, 0, len(fieldMap))

    // 对bitmapInfos中需要手动释放的bitmap进行延迟释放
    defer func() {
        for i := 0; i < len(bis); i++ {
            // 临时生成的结果集 -> 需要手动释放
            if bis[i].needFree {
                croaring.Free(bis[i].bitmap)
            }
        }
    }()

    // 获取bitmap列表
    for name, value := range fieldMap {
        // field是否有倒排索引
        invert, hasInvert := receiver.fieldMap[name]

        // 字段不存在倒排
        if !hasInvert {
            // 字段名可能是主键索引(主键索引过滤模式)
            if name == INDEX_FLAG || name == receiver.indexInfo.getIndexFieldName() {
                var ids []uint32
                switch v := value.(type) {
                case []DataIndex:
                    ids = make([]uint32, 0, len(v))
                    for _, i := range v {
                        ids = append(ids, uint32(i))
                    }
                case DataIndex:
                    ids = append(ids, uint32(v))
                case []interface{}:
                    ids = make([]uint32, 0, len(v))
                    for _, i := range v {
                        ids = append(ids, convert.Uint32(i))
                    }
                case interface{}:
                    ids = append(ids, convert.Uint32(v))
                }

                // 指定主键过滤 -> 主键过滤列表为空 -> 无数据直接返回
                if len(ids) <= 0 {
                    return
                }

                bitmap := croaring.New(ids...)
                bis = append(bis, bitmapInfo{true, bitmap})
                continue
            }

            err = fmt.Errorf("field:[%s] has no invert", name)
            return
        }

        switch reflect.TypeOf(value).Kind() {
        // 多值查询
        case reflect.Slice:
            vv := reflect.ValueOf(value)
            if size := vv.Len(); size > 0 {
                // 查询结果子集
                var subResult croaring.Bitmap
                for i := 0; i < size; i++ {
                    // 获取vi对应的bitmap
                    bitmap := receiver.getInvertBitmap(invert, name, vv.Index(i).Interface(), typeConvert)

                    // bitmap无效 -> 跳过
                    // 一组查询中可能存在无数据 -> 最终有数据即可
                    if !bitmap.IsValid() {
                        continue
                    }

                    // slice中的元素对应的倒排结果集 -> 取并集
                    if !subResult.IsValid() {
                        subResult = bitmap.Clone()
                    } else {
                        subResult.Or(bitmap)
                    }
                }

                // 结果集无数据 -> 释放结果集 -> 直接返回无数据
                if subResult.IsEmpty() {
                    croaring.Free(subResult)
                    return
                }

                // 临时生成的结果集 -> 需要手动free
                bis = append(bis, bitmapInfo{true, subResult})
            }
        // 默认单值查询
        default:
            // 获取value对应的bitmap
            bitmap := receiver.getInvertBitmap(invert, name, value, typeConvert)

            // bitmap无效 -> 直接返回无数据
            if !bitmap.IsValid() {
                return
            }

            // 非临时生成的结果集 -> 无需手动free
            bis = append(bis, bitmapInfo{false, bitmap})
        }
    }

    // 结果集按照bitmap中数据数量升序排序
    // todo 由于roaring底层需要遍历container才能获取数据数量，所以需要等优化底层结构后再开启排序
    // sort.Sort(bis)

    // 合并结果集
    for i := 0; i < len(bis); i++ {
        if !result.IsValid() {
            result = bis[i].bitmap.Clone()
        } else {
            result.And(bis[i].bitmap)
        }
    }
    return
}

func (receiver *DataManager) getDataByIndex(dataIndex DataIndex) (data AnyData, ok bool) {
    var err error
    data, err = receiver.dataStore.Get(dataIndex)
    if err != nil {
        return nil, false
    }
    return data, true
}

func (receiver *DataManager) getDataByIndexUnsafe(dataIndex DataIndex) (data AnyData, has bool) {
    var err error

    if dataStore, ok := receiver.dataStore.(IStoreExpand); ok {
        // unsafe方式获取数据
        data, err = dataStore.GetUnsafe(dataIndex)
        if err != nil {
            return nil, false
        }
    }

    if data == nil {
        // 默认方式获取数据
        data, err = receiver.dataStore.Get(dataIndex)
        if err != nil {
            return nil, false
        }
        data = unsafe.Pointer(reflect.ValueOf(data).Pointer())
    }

    return data, true
}

func (receiver *DataManager) getDatasByFields(fieldMap FieldMap, result *BaseQueryResult) {
    // 空数据 -> 直接返回 todo 20210804 由于支持被动缓存，本地没有数据，所以注释掉size判断
    // if receiver.dataStore.Size() == 0 {
    //     result.lastErr = ErrNoData
    //     return
    // }

    // 使用fileMap获取匹配的数据索引
    resultBitmap, queryMissIndex, getErr := receiver.getIndexsByFields(fieldMap)

    // 结果集使用结束 -> 释放bitmap的内存(内部有IsValid的判断)
    defer croaring.Free(resultBitmap)

    if getErr != nil {
        result.lastErr = errors.Wrap(getErr, runtime.CallerFunction())
    }

    // 查询过程中未命中的索引
    result.missIndex = queryMissIndex

    // 使用索引获取数据指针
    if resultBitmap.IsValid() && !resultBitmap.IsEmpty() {
        switch ITERTOR_MODE {
        case 0:
            dataIndexes := resultBitmap.ToArray()
            result.makeDataResult(len(dataIndexes))
            for i := 0; i < len(dataIndexes); i++ {
                dataIndex := DataIndex(dataIndexes[i])
                if data, ok := receiver.getDataByIndex(dataIndex); ok {
                    result.pushData(dataIndex, data)
                }
            }
        case 1:
            result.makeDataResult(int(resultBitmap.GetCardinality()))
            resultBitmap.Iterate(func(x uint32) bool {
                dataIndex := DataIndex(x)
                if data, ok := receiver.getDataByIndex(dataIndex); ok {
                    result.pushData(dataIndex, data)
                }
                return true
            })
        default:
            panic(fmt.Errorf("unknow ITERTOR_MODE %d", ITERTOR_MODE))
        }
    }
    return
}

func (receiver *DataManager) getIdsByFields(fieldMap FieldMap, result *BaseQueryIdsResult) {
    // 空数据 -> 直接返回 todo 20210804 由于支持被动缓存，本地没有数据，所以注释掉size判断
    // if receiver.dataStore.Size() == 0 {
    //     result.lastErr = ErrNoData
    //     return
    // }

    // 使用fileMap获取匹配的数据索引
    resultBitmap, queryMissIndex, getErr := receiver.getIndexsByFields(fieldMap)

    // 结果集使用结束 -> 释放bitmap的内存(内部有IsValid的判断)
    defer croaring.Free(resultBitmap)

    if getErr != nil {
        result.lastErr = errors.Wrap(getErr, runtime.CallerFunction())
    }

    // 查询过程中未命中的索引
    result.missIndex = queryMissIndex

    if resultBitmap.IsValid() && !resultBitmap.IsEmpty() {
        dataIndexes := resultBitmap.ToArray()
        result.makeDataResult(len(dataIndexes))
        for i := 0; i < len(dataIndexes); i++ {
            result.pushId(DataIndex(dataIndexes[i]))
        }
    }
}

func (receiver *DataManager) queryByIndex(indexs ...DataIndex) IQueryResult {
    result := &BaseQueryResult{
        dataManager: receiver,
    }

    result.queryCost = utils.CostRun(func() {
        if size := len(indexs); size > 0 {
            result.makeDataResult(size)
            for i := 0; i < size; i++ {
                if data, ok := receiver.getDataByIndex(indexs[i]); ok {
                    result.pushData(indexs[i], data)
                } else {
                    result.lastErr = errors.Wrapf(ErrNoData, "%s index:[%+v]", runtime.CallerFunction(), indexs[i])
                    break
                }
            }
        }
    })
    return result
}

func (receiver *DataManager) queryByFields(fieldMap FieldMap) IQueryResult {
    result := &BaseQueryResult{
        dataManager: receiver,
    }

    result.queryCost = utils.CostRun(func() {
        receiver.getDatasByFields(fieldMap, result)
    })
    return result
}

func (receiver *DataManager) queryIdsByFields(fieldMap FieldMap) IQueryResult {
    result := &BaseQueryIdsResult{
        dataManager: receiver,
    }

    result.queryCost = utils.CostRun(func() {
        receiver.getIdsByFields(fieldMap, result)
    })
    return result
}

// TODO 有待完善
// TODO PrepareModify 优化暂且关闭
// 1.删除并获取旧数据, 如果不存在 -> 直接跳出
// 2.判断旧数据是否为指针, 使用指针传递到modifier -> 进行修改
// 3.将修改后的数据重新插入
func (receiver *DataManager) modifyDataByIndex(prepare PrepareModify, modifier DataModifier, index DataIndex) (ret error) {
    data, has := receiver.getDataByIndex(index)
    if !has {
        return KeyNotExists
    }

    ret = modifier(data)

    // 替换数据
    if err := receiver.replace(data); err != nil {
        if ret != nil {
            return fmt.Errorf("modify err:%+v replace err:%+v", ret, err)
        }
        return err
    }
    return
}

// bitmapCapacity 统计bitmap占用内存容量
// total bitmap总数量
// fields 每个倒排字段申请的bitmap数量
func (receiver *DataManager) bitmapCount() (total uint64, fields map[string]uint64) {
    var temp uint64
    fields = make(map[string]uint64, len(receiver.fieldMap))
    for name, invertValueMap := range receiver.fieldMap {
        temp = uint64(len(invertValueMap))
        fields[name] = temp
        total += temp
    }
    return
}

type Stats struct {
    Capacity uint64 `json:"capacity"`
    IdCount  uint64 `json:"id_count"`
}

// bitmapCapacity 统计bitmap占用内存容量
// total 占用总内存大小和总id数量
// fields 每个倒排字段占用的内存大小和对应存储的id数量
func (receiver *DataManager) bitmapCapacity() (total Stats, fields map[string]*Stats) {
    var temp uint64
    fields = make(map[string]*Stats, len(receiver.fieldMap))
    for name, invertValueMap := range receiver.fieldMap {
        fields[name] = &Stats{0, 0}
        for _, bitmap := range invertValueMap {
            stats := bitmap.StatsStruct()

            // 计算容量占用
            temp = stats.BitmapContainerBytes + stats.ArrayContainerBytes + stats.RunContainerBytes
            fields[name].Capacity += temp
            total.Capacity += temp

            // 计算id数量
            fields[name].IdCount += stats.Cardinality
            total.IdCount += stats.Cardinality
        }
    }
    return
}

// idToBtmStats 获取数据管理器内部的IdToBitmap统计信息
func (receiver *DataManager) idToBtmStats() IToBStats {
    return receiver.indexToBitmap.GetStats()
}
