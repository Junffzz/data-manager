package dm

// FieldInfosToFieldMap FieldInfo(s) -> FieldMap
func FieldInfosToFieldMap(fieldInfos ...FieldInfo) (fieldMap FieldMap) {
    fieldMap = make(FieldMap, len(fieldInfos))
    for _, info := range fieldInfos {
        fieldMap[info.Name] = info.Value
    }
    return
}

// FieldNamesToFieldMap FieldNames -> FieldMap
func FieldNamesToFieldMap(names FieldNames) (fieldMap FieldMap) {
    fieldMap = make(FieldMap, len(names))
    for _, name := range names {
        fieldMap[name] = 1
    }
    return
}

// FieldMapToFieldNames FieldMap -> FieldNames
func FieldMapToFieldNames(fieldMap FieldMap) (names FieldNames) {
    names = make(FieldNames, 0, len(fieldMap))
    for name, _ := range fieldMap {
        names = append(names, name)
    }
    return
}
