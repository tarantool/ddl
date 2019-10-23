#!/usr/bin/env tarantool

local ddl_get = require('ddl.get')
local ddl_set = require('ddl.set')
local ddl_check = require('ddl.check')
local utils = require('ddl.utils')

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


    for k, v in pairs(schema.spaces) do
        if type(k) ~= 'string' then
            error(err_msg ..
                ' shema.spaces (expected key value table, where key (space name) with type string, actual ' ..
                type(k) .. ')',
            2)
        end

        if type(v) ~= 'table' then
            error(err_msg ..
                ' shema.spaces expected (key value table, where value (space info) type table, actual ' ..
                type(v) .. ')',
            2)
        end
    end
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
            ddl_set.create_space(space_name, space_schema)
        end
    end

    return true
end


local function get_schema()
    local schema = {}
    local spaces = {}
    for _, space in box.space._space:pairs({box.schema.SYSTEM_ID_MAX}, {iterator = "GT"}) do
        spaces[space.name] = ddl_get.get_space_schema(space.name)
    end

    schema.spaces = spaces
    return schema
end


return {
    check_schema = check_schema,
    set_schema = set_schema,
    get_schema = get_schema,
}
