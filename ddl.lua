#!/usr/bin/env tarantool

local ddl_get = require('ddl.get')
local ddl_set = require('ddl.set')
local ddl_check = require('ddl.check')
local utils = require('ddl.utils')
local ldigest = require('digest')

local function check_schema_format(schema)
    local err_msg = 'Bad argument #1 to ddl.%s'

    if type(schema) ~= 'table' then
        return nil, err_msg .. ' (schema expected, got ' .. type(schema) .. ')'
    end

    if type(schema.spaces) ~= 'table' then
        return nil, err_msg ..' invalid schema.spaces (table expected, got ' ..
            type(schema.spaces) .. ')'
    end

    return true
end

local function check_schema(schema)
    local ok, err = check_schema_format(schema)
    if not ok then
        return nil, string.format(err, 'check_schema')
    end

    if type(box.cfg) == 'function' then
        return nil, "Box isn't configured yet"
    end

    if box.cfg.read_only then
        return nil, "Box is read only"
    end

    for space_name, space_schema in pairs(schema.spaces) do
        local ok, err = ddl_check.check_space(space_name, space_schema)
        if not ok then
            return nil, err
        end
        if box.space[space_name] ~= nil then
            local diff = {}
            local current_schema = ddl_get.get_space_schema(space_name)
            local equal = utils.deepcmp(space_schema, current_schema, diff)
            if not equal then
                return nil, string.format(
                    "Incompatible schema: space[%q]" ..
                    " %s (expected %s, got %s)",
                    space_name, diff.path, diff.expected, diff.got
                )
            end
        else
            local ok, err = pcall(
                ddl_set.create_space,
                space_name, space_schema, {dummy = true}
            )

            local dummy = box.space['_ddl_dummy']
            if dummy then
                pcall(box.schema.space.drop, dummy.id)
            end

            if not ok then
                return nil, tostring(err):gsub('_ddl_dummy', space_name)
            end
        end
    end

    return true
end

local function set_schema(schema)
    local ok, err = check_schema_format(schema)
    if not ok then
        return nil, string.format(err, 'set_schema')
    end

    local ok, err = check_schema(schema)
    if not ok then
        return nil, err
    end

    local sharding_space = box.schema.space.create('_sharding_key', {
        format = {
            {name = 'space_name', type = 'string', is_nullable = false},
            {name = 'sharding_key', type = 'array', is_nullable = false}
        },
        if_not_exists = true
    })
    sharding_space:create_index(
        'space_name', {
            type = 'TREE',
            unique = true,
            parts = {{'space_name', 'string', is_nullable = false}},
            if_not_exists = true
        }
    )

    for space_name, space_schema in pairs(schema.spaces) do
        if box.space[space_name] == nil then
            ddl_set.create_space(space_name, space_schema)
        end
    end

    return true
end


local function get_schema()
    local schema = {}
    local spaces = {}
    for _, space in box.space._space:pairs({box.schema.SYSTEM_ID_MAX}, {iterator = "GT"}) do
        if space.name ~= '_sharding_key' then
            spaces[space.name] = ddl_get.get_space_schema(space.name)
        end
    end

    schema.spaces = spaces
    return schema
end


local function get_value(space, record, key)
    local field_position = {}
    for i, field in ipairs(space.format) do
        field_position[field.name] = i
    end

    local val = record[key]
    if not val then
        val = record[field_position[key]]
    end
    return val
end

local function bucket_id(schema, record, space_name, bucket_count)
    local ok, err = check_schema_format(schema)
    if not ok then
        error(string.format(err, 'bucket_id'))
    end

    if type(record) ~= 'table' then
        error(string.format(
            'Bad argument #2 to ddl.bucket_id (table expected, got %s)',
            type(record)
        ))
    end

    if type(space_name) ~= 'string' then
        error(string.format(
            'Bad argument #3 to ddl.bucket_id (string expected, got %s)',
            type(space_name)
        ))
    end

    if type(bucket_count) ~= 'number' then
        error(string.format(
            'Bad argument #4 to ddl.bucket_id (number expected, got %s)',
            type(bucket_count)
        ))
    end

    if bucket_count < 1 then
        error(string.format(
            'Bad argument #4 to ddl.bucket_id (positive expected, got %s)',
            bucket_count
        ))
    end

    local space = schema.spaces[space_name]
    if not space then
        return nil, string.format("Space %q isn't defined in schema", space_name)
    end

    local sharding_key = space.sharding_key
    if not space.sharding_key then
        return nil, string.format(
            "Space %q isn't sharded in schema", space_name
        )
    end

    local crc32 = ldigest.crc32.new()
    for _, key in ipairs(sharding_key) do
        local value = get_value(space, record, key)

        if type(value) == 'table' then
            return nil, string.format(
                'Unsupported value for sharding key %q (scalar expected, got table)',
                key
            )
        end

        crc32:update(tostring(value))
    end
    return crc32:result() % bucket_count + 1
end

return {
    check_schema = check_schema,
    set_schema = set_schema,
    get_schema = get_schema,
    bucket_id = bucket_id
}
