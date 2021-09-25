package dm

// BaseInvertBuildFilter 基础的倒排索引构建过滤器 -> 默认过滤器
type BaseInvertBuildFilter struct {
    IInvertBuildFilter
}

func (receiver *BaseInvertBuildFilter) FieldInvertFilter(data AnyData, fieldName string, fieldValue AnyData) (build bool) {
    return true
}

func (receiver *BaseInvertBuildFilter) FieldInvertFilterMap(data AnyData) (filter FieldMap) {
    return nil
}