local log = require('log')
local ddl = require('ddl')
local yaml = require('yaml').new()
local errors = require('errors')

local cartridge = require('cartridge')
local twophase = require('cartridge.twophase')
local failover = require('cartridge.failover')
local vars = require('cartridge.vars').new('cartridge.roles.ddl-manager')

vars:new('on_patch_trigger', nil)
vars:new('_section_name', nil)
vars:new('_example_schema', nil)

local CheckSchemaError = errors.new_class('CheckSchemaError')

yaml.cfg({
    encode_use_tostring = true,
    encode_load_metatables = false,
    decode_save_metatables = false,
})

vars._section_name = 'schema.yml'
vars._example_schema = [[## Example:
#
# spaces:
#   customer:
#     engine: memtx
#     is_local: false
#     temporary: false
#     sharding_key: [customer_id]
#     format:
#       - {name: customer_id, type: unsigned, is_nullable: false}
#       - {name: bucket_id, type: unsigned, is_nullable: false}
#       - {name: fullname, type: string, is_nullable: false}
#     indexes:
#     - name: customer_id
#       unique: true
#       type: TREE
#       parts:
#         - {path: customer_id, type: unsigned, is_nullable: false}
#
#     - name: bucket_id
#       unique: false
#       type: TREE
#       parts:
#         - {path: bucket_id, type: unsigned, is_nullable: false}
#
#     - name: fullname
#       unique: true
#       type: TREE
#       parts:
#         - {path: fullname, type: string, is_nullable: false}
]]

-- Be gentle with cartridge.reload_roles
twophase.on_patch(nil, vars.on_patch_trigger)
function vars.on_patch_trigger(conf_new)
    local schema_yml = conf_new:get_readonly(vars._section_name)

    if schema_yml == nil or schema_yml == '' then
        conf_new:set_plaintext(vars._section_name, vars._example_schema)
    end
end
twophase.on_patch(vars.on_patch_trigger, nil)

local function apply_config(conf, opts)
    if not opts.is_master then
        return true
    end

    local schema_yml = conf[vars._section_name]
    if schema_yml == nil then
        return true
    end

    assert(type(schema_yml) == 'string')

    local schema = yaml.decode(schema_yml)
    if schema == nil then
        return true
    end

    assert(ddl.set_schema(schema))
    return true
end

local function validate_config(conf_new, _)
    local schema_yml = conf_new[vars._section_name]
    if schema_yml == nil then
        return true
    end

    local schema = yaml.decode(schema_yml)
    if schema == nil then
        return true
    end

    if type(box.cfg) == 'function' then
        log.info(
            "Schema validation skipped because" ..
            " the instance isn't bootstrapped yet"
        )
        return true
    elseif not failover.is_leader() then
        log.info(
            "Schema validation skipped because" ..
            " the instance isn't a leader"
        )
        return true
    end

    local ok, err = ddl.check_schema(schema)
    if not ok then
        return nil, CheckSchemaError:new(err)
    end

    return true
end

local function init()
    rawset(_G, 'ddl', ddl)
end

local function stop()
    rawset(_G, 'ddl', nil)
end

--- Get clusterwide schema as a YAML string.
--
-- @function get_clusterwide_schema_yaml
-- @treturn string yaml-encoded schema
local function get_clusterwide_schema_yaml()
    return cartridge.config_get_readonly(vars._section_name)
end

--- Get clusterwide schema as a Lua table.
--
-- In case there's no schema set, return empty schema `{spaces = {}}`.
--
-- @function get_clusterwide_schema_lua
-- @treturn table schema
local function get_clusterwide_schema_lua()
    local schema_yml = cartridge.config_get_readonly(vars._section_name)
    local schema_lua = schema_yml and yaml.decode(schema_yml)
    if schema_lua == nil then
        return {spaces = {}}
    else
        return schema_lua
    end
end

--- Apply schema (as a YAML string) on a whole cluster.
--
-- @function set_clusterwide_schema_yaml
-- @tparam string schema_yml
-- @treturn[1] boolean `true`
-- @treturn[2] nil
-- @treturn[2] table Error object
local function set_clusterwide_schema_yaml(schema_yml)
    local patch
    if schema_yml == nil then
        patch = {[vars._section_name] = box.NULL}
    elseif type(schema_yml) ~= 'string' then
        local err = string.format(
            'Bad argument #1 to set_clusterwide_schema_yaml' ..
            ' (?string expected, got %s)', type(schema_yml)
        )
        error(err, 2)
    else
        patch = {[vars._section_name] = schema_yml}
    end

    return cartridge.config_patch_clusterwide(patch)
end

--- Apply schema (as a Lua table) on a whole cluster.
--
-- @function set_clusterwide_schema_lua
-- @tparam string schema_lua
-- @treturn[1] boolean `true`
-- @treturn[2] nil
-- @treturn[2] table Error object
local function set_clusterwide_schema_lua(schema_lua)
    local patch
    if schema_lua == nil then
        patch = {[vars._section_name] = box.NULL}
    elseif type(schema_lua) ~= 'table' then
        local err = string.format(
            'Bad argument #1 to set_clusterwide_schema_lua' ..
            ' (?table expected, got %s)', type(schema_lua)
        )
        error(err, 2)
    else
        patch = {[vars._section_name] = yaml.encode(schema_lua)}
    end

    return cartridge.config_patch_clusterwide(patch)
end

--- Validate schema passed as a YAML string.
--
-- @function check_schema_yaml
-- @tparam string schema_yml
-- @treturn[1] boolean `true`
-- @treturn[2] nil
-- @treturn[2] table Error object
local function check_schema_yaml(schema_yml)
    if schema_yml == nil then
        return true
    elseif type(schema_yml) ~= 'string' then
        return nil, CheckSchemaError:new(
            'Bad argument #1 to check_schema_yaml' ..
            ' (?string expected, got %s)', type(schema_yml)
        )
    end

    local ok, schema_lua = pcall(yaml.decode, schema_yml)
    if not ok then
        return nil, CheckSchemaError:new(
            'Invalid YAML: %s', schema_lua
        )
    end

    if schema_lua == nil then
        return true
    end

    if not failover.is_leader() then
        return cartridge.rpc_call(
            'ddl-manager', 'check_schema_yaml', {schema_yml},
            {leader_only = true}
        )
    end

    local ok, err = ddl.check_schema(schema_lua)
    if not ok then
        return nil, CheckSchemaError:new(err)
    end

    return true
end

--- Validate schema passed as a Lua table.
--
-- @function check_schema_lua
-- @tparam string schema_lua
-- @treturn[1] boolean `true`
-- @treturn[2] nil
-- @treturn[2] table Error object
local function check_schema_lua(schema_lua)
    if schema_lua == nil then
        return true
    elseif type(schema_lua) ~= 'table' then
        return nil, CheckSchemaError:new(
            'Bad argument #1 to check_schema_lua' ..
            ' (?table expected, got %s)', type(schema_lua)
        )
    end

    -- Always perform encode + decode because
    -- that's how `set_schema_lua` works
    local ok, schema_yml = pcall(yaml.encode, schema_lua)
    if not ok then
        return nil, CheckSchemaError:new(
            'Encoding YAML failed: %s', schema_yml
        )
    end

    return cartridge.rpc_call(
        'ddl-manager', 'check_schema_yaml', {schema_yml},
        {prefer_local = true, leader_only = true}
    )
end

return {
    role_name = 'ddl-manager',
    permanent = true,
    _section_name = vars._section_name,
    _example_schema = vars._example_schema,

    init = init,
    stop = stop,
    validate_config = validate_config,
    apply_config = apply_config,

    get_clusterwide_schema_yaml = get_clusterwide_schema_yaml,
    get_clusterwide_schema_lua = get_clusterwide_schema_lua,

    set_clusterwide_schema_yaml = set_clusterwide_schema_yaml,
    set_clusterwide_schema_lua = set_clusterwide_schema_lua,

    check_schema_yaml = check_schema_yaml,
    check_schema_lua = check_schema_lua,

    CheckSchemaError = CheckSchemaError,
}
