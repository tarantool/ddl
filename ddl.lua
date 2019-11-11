#!/usr/bin/env tarantool

local ddl_get = require('ddl.get')
local ddl_set = require('ddl.set')
local ddl_check = require('ddl.check')
local utils = require('ddl.utils')
local ldigest = require('digest')

local function check_schema_format(schema)
    local caller_name = debug.getinfo(3, "n").name
    local err_msg = 'Bad argument #1 to ddl.' .. caller_name

    if type(schema) ~= 'table' then
        error(err_msg ..
            ' (table expected, got ' .. type(schema) .. ')',
        3)
    end

    if type(schema.spaces) ~= 'table' then
        error(err_msg ..
        ' schema.spaces (table expected, got ' .. type(schema.spaces) .. ')',
        3)
    end

    -- for k, v in pairs(schema.spaces) do
    --     if type(k) ~= 'string' then
    --         error(err_msg ..
    --             ' shema.spaces (expected key value table, where key (space name) with type string, actual ' ..
    --             type(k) .. ')',
    --         2)
    --     end

    --     if type(v) ~= 'table' then
    --         error(err_msg ..
    --             ' shema.spaces expected (key value table, where value (space info) type table, actual ' ..
    --             type(v) .. ')',
    --         2)
    --     end
    -- end
end

local function check_schema(schema)
    local ok, err = pcall(check_schema_format, schema)
    if not ok then
        return nil, err
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
                '_ddl_dummy', space_schema
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
    local ok, err = pcall(check_schema_format, schema)
    if not ok then
        return nil, err
    end

    local ok, err = check_schema(schema)
    if not ok then
        return nil, err
    end

    for space_name, space_schema in pairs(schema.spaces) do
        if box.space[space_name] == nil then
            ddl_set.create_space(space_name, space_schema, true)
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

local function bucket_id(schema, record, space_name, bucket_count)
    local ok, err = pcall(check_schema_format, schema)
    if not ok then
        return nil, err
    end

    if type(record) ~= 'table' then
        return nil, string.format(
            'Bad argument #2 (expected table, got %s)',
            type(record)
        )
    end

    if type(space_name) ~= 'string' then
        return nil, string.format(
            'Bad argument #3 (expected string, got %s)',
            type(space_name)
        )
    end

    if type(bucket_count) ~= 'number' then
        return nil, string.format(
            'Bad argument #4 (expected number, got %s)',
            type(bucket_count)
        )
    end

    if bucket_count < 1 then
        return nil, string.format('Invalid bucket_count, it must be greater than 0')
    end

    local space = schema.spaces[space_name]
    if not space then
        return nil, 'No such space with name ' .. space_name
    end

    local sharding_key = space.sharding_key
    local crc32 = ldigest.crc32.new()

    for _, key in ipairs(sharding_key) do
        local value = record[key]

        if type(value) == 'table' then
            return nil, 'Not supported key type (expected scalar, got table)'
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
