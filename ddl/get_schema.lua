local function _get_index_field_path(space, index_part)
    local space_field = space:format()[index_part.fieldno]

    -- this maybe unnecessary, if local spaces has no format
    -- its erroneous for ddl
    local field_name =  space_field and space_field.name
    local index_field_name = field_name or index_part.fieldno
    if not index_part.path then
        return index_field_name
    end

    local path = index_part.path
    if string.sub(index_part.path, 1, 1) ~= '.' then
        path = '.' .. path
    end

    local str_format = '%s%s'
    if type(index_field_name) == 'number' then
        str_format = '%d%s'
    end

    return string.format(str_format, index_field_name, path)
end

local function _get_index_parts(space, index_part)
    local ddl_index_part = {}
    ddl_index_part.path = _get_index_field_path(space, index_part)
    ddl_index_part.type = index_part.type
    ddl_index_part.collation = index_part.collation
    ddl_index_part.is_nullable = index_part.is_nullable
    return ddl_index_part
end

local function _get_index_function(index_func)
    local ddl_func_info = {}
    local box_func_info = box.func[index_func.name]

    ddl_func_info.name = index_func.name
    ddl_func_info.body = box_func_info.body
    ddl_func_info.is_deterministic = box_func_info.is_deterministic
    ddl_func_info.is_sandboxed = box_func_info.is_sandboxed
    ddl_func_info.opts = {is_multikey = box_func_info.is_multikey}
    return ddl_func_info
end

local function _get_index(box_space, box_index)
    local ddl_index = {}
    ddl_index.name = box_index.name
    ddl_index.type = box_index.type
    ddl_index.unique = box_index.unique

    ddl_index.parts = {}
    for _, box_index_part in pairs(box_index.parts) do
        table.insert(ddl_index.parts, _get_index_parts(box_space, box_index_part))
    end

    ddl_index.dimension = box_index.dimension
    ddl_index.distance = box_index.distance

    if box_index.sequence_id ~= nil then
        ddl_index.sequence = box.sequence[box_index.sequence_id]
        ddl_index.sequence.uid = nil
        ddl_index.sequence.id = nil
    end

    ddl_index.func = nil
    if box_index.func ~= nil then
        ddl_index.func = _get_index_function(box_index.func)
    end
    return ddl_index
end

local function _get_space(box_space)
    local ddl_space = {}
    ddl_space.is_local = box_space.is_local
    ddl_space.temporary = box_space.temporary
    ddl_space.engine = box_space.engine
    ddl_space.format = box_space:format()
    for _, v in ipairs(ddl_space.format) do
        if v.is_nullable == nil then
            v.is_nullable = false
        end
    end

    local indexes = {}
    for key, index in pairs(box_space.index) do
        if type(key) == 'number' then
            table.insert(indexes, _get_index(box_space, index))
        end
    end
    ddl_space.indexes = indexes
    return ddl_space
end

local function get_schema()
    local schema = {}
    for _, space in pairs(box.space) do
        if space.id >= box.schema.SYSTEM_ID_MAX then
            schema[space.name] = _get_space(space)
        end
    end

    return schema
end

return {
    get_schema = get_schema,
}
