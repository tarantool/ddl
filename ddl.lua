local ddl_get = require('ddl.get')
local ddl_set = require('ddl.set')
local ddl_check = require('ddl.check')
local ddl_db = require('ddl.db')
local utils = require('ddl.utils')

local function check_schema_format(schema)

    if type(schema) ~= 'table' then
        return nil, string.format(
            "Invalid schema (table expected, got %s)", type(schema)
        )
    end

    if type(schema.spaces) ~= 'table' then
        return nil, string.format(
            "spaces: must be a table, got %s", type(schema.spaces)
        )
    end

    if type(schema.functions) ~= 'nil' then
        return nil, string.format("functions: not supported")
    end

    if type(schema.sequences) ~= 'nil' then
        return nil, string.format("sequences: not supported")
    end

    do -- check redundant keys
        local k = utils.redundant_key(schema, {'spaces'})
        if k ~= nil then
            return nil, string.format(
                "Invalid schema: redundant key %q", k
            )
        end
    end

    return true
end

local function _check_schema(schema)
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
                    "Incompatible schema: spaces[%q]" ..
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

local function check_schema(schema)
    local ok, err = check_schema_format(schema)
    if not ok then
        return nil, err
    end

    if type(box.cfg) == 'function' then
        return nil, "'box' module isn't configured yet"
    end

    if box.cfg.read_only then
        return nil, "Instance is read-only (check box.cfg.read_only and box.info.status)"
    end

    return ddl_db.call_dry_run(_check_schema, schema)
end

local function _set_schema(schema)
    local sharding_space = box.schema.space.create('_ddl_sharding_key', {
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

local function set_schema(schema)
    local ok, err = check_schema_format(schema)
    if not ok then
        return nil, err
    end

    local ok, err = check_schema(schema)
    if not ok then
        return nil, err
    end

    return ddl_db.call_atomic(_set_schema, schema)
end

local function on_schema_change(func)
    if type(func) ~= 'function' then
        return nil, string.format("Passed value is not a function - %s", type(func))
    end
    ddl_set.internal.trigger_on_schema_change = func

    return true
end

local function get_schema()
    local spaces = {}
    for _, space in box.space._space:pairs({box.schema.SYSTEM_ID_MAX}, {iterator = "GT"}) do
        if space.name ~= '_ddl_sharding_key' then
            spaces[space.name] = ddl_get.get_space_schema(space.name)
        end
    end

    return {
        spaces = spaces,
    }
end

return {
    check_schema = check_schema,
    set_schema = set_schema,
    get_schema = get_schema,
    on_schema_change = on_schema_change,
}
