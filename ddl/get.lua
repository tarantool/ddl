local utils = require('ddl.utils')
local cache = require('ddl.cache')
local ddl_check = require('ddl.check')

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
        if string.sub(index_part.path, 1, 3) ~= '[*]' then
            path = '.' .. path
        end
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

local function _get_index(box_space, box_index)
    local ddl_index = {}
    ddl_index.name = box_index.name
    ddl_index.type = box_index.type
    ddl_index.unique = box.space._index:get({box_space.id, box_index.id}).opts.unique

    ddl_index.parts = {}
    for _, box_index_part in pairs(box_index.parts) do
        table.insert(ddl_index.parts, _get_index_parts(box_space, box_index_part))
    end

    if box_index.type == 'RTREE' then
        ddl_index.dimension = box_index.dimension
        ddl_index.distance = box.space._index:get({box_space.id, box_index.id}).opts.distance or 'euclid'
    end

    return ddl_index
end

local function get_metadata(space_name, metadata_name)
    local metadata_space_name = string.format("_ddl_%s", metadata_name)

    if box.space[metadata_space_name] == nil then
        return nil
    end

    return box.space[metadata_space_name]:get{space_name}
end

local function get_sharding_func(space_name)
    return cache.internal.get(space_name)
end

local function get_sharding_func_raw(space_name)
    local record = cache.internal.get(space_name)

    if not record or not record.raw_tuple then
        return nil
    end

    record = record.raw_tuple

    if record.sharding_func_body ~= nil then
        return {body = record.sharding_func_body}
    end

    if record.sharding_func_name ~= nil then
        return record.sharding_func_name
    end

    return nil
end

local function get_sharding_key(space_name)
    local record = get_metadata(space_name, "sharding_key")
    return record and record.sharding_key
end

local function get_space_schema(space_name)
    local box_space = box.space[space_name]
    assert(box_space ~= nil)

    local space_ddl = {}
    space_ddl.is_local = box_space.is_local
    space_ddl.temporary = box_space.temporary
    space_ddl.engine = box_space.engine
    space_ddl.format = box_space:format()
    space_ddl.sharding_key = get_sharding_key(space_name)
    space_ddl.sharding_func = get_sharding_func_raw(space_name)
    for _, field in ipairs(space_ddl.format) do
        if field.is_nullable == nil then
            field.is_nullable = false
        end
    end

    local indexes = {}
    for key, index in pairs(box_space.index) do
        if type(key) == 'number' then
            table.insert(indexes, _get_index(box_space, index))
        end
    end
    space_ddl.indexes = indexes
    return space_ddl
end

local function prepare_sharding_func_for_call(space_name, sharding_func_def)
    if sharding_func_def.error ~= nil then
        return nil, sharding_func_def.error
    end

    if sharding_func_def.parsed_func_name ~= nil then
        local sharding_func = utils.get_G_function(sharding_func_def.parsed_func_name)
        if sharding_func ~= nil and
           ddl_check.internal.is_callable(sharding_func) == true then
            return sharding_func
        end
    end

    if sharding_func_def.callable ~= nil then
        return sharding_func_def.callable
    end

    return nil, string.format(
        "Wrong sharding function specified in DDL schema of space (%s)", space_name
    )
end

local function bucket_id(space_name, sharding_key)
    local sharding_func_def = get_sharding_func(space_name)
    if sharding_func_def == nil then
        return nil, string.format(
            "No sharding function specified in DDL schema of space (%s)", space_name
        )
    end
    local sharding_func, err =
        prepare_sharding_func_for_call(space_name, sharding_func_def)
    if err ~= nil then
        return nil, err
    end

    local ok, id = pcall(sharding_func, sharding_key)
    if not ok then
        return nil, string.format(
            "Failed to execute sharding function for space name (%s): %s",
            space_name, id
        )
    end

    return id
end

return {
    get_space_schema = get_space_schema,
    internal = {
        bucket_id = bucket_id,
    }
}
