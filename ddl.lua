local ddl_get = require('ddl.get')
local ddl_set = require('ddl.set')
local ddl_check = require('ddl.check')
local ddl_db = require('ddl.db')
local ddl_compare = require('ddl.compare')
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

    if type(schema.sequences) ~= 'nil' and type(schema.sequences) ~= 'table' then
        return nil, string.format(
            "sequences: must be a table or nil, got %s", type(schema.sequences)
        )
    end

    do -- check redundant keys
        local k = utils.redundant_key(schema, {'spaces', 'sequences'})
        if k ~= nil then
            return nil, string.format(
                "Invalid schema: redundant key %q", k
            )
        end
    end

    return true
end

local function _check_and_dry_run_sequences(sequences)
    for sequence_name, sequence_schema in pairs(sequences) do
        local ok, err = ddl_check.check_sequence(sequence_name, sequence_schema)
        if not ok then
            return nil, err
        end

        if box.sequence[sequence_name] ~= nil then
            local current_schema = ddl_get.get_sequence_schema(sequence_name)
            local _, err = ddl_compare.assert_equiv_sequence_schema(sequence_schema, current_schema)
            if err ~= nil then
                return nil, string.format(
                    "Incompatible schema: sequences[%q] %s", sequence_name, err)
            end
        else
            local ok, err = pcall(
                ddl_set.create_sequence,
                sequence_name, sequence_schema, {dummy = true}
            )

            local dummy = box.sequence['_ddl_dummy']
            if dummy then
                pcall(box.schema.sequence.drop, dummy.id)
            end

            if not ok then
                return nil, tostring(err):gsub('_ddl_dummy', sequence_name)
            end
        end
    end

    return true
end

local function _check_and_dry_run_spaces(spaces, sequences)
    for space_name, space_schema in pairs(spaces) do
        local ok, err = ddl_check.check_space(space_name, space_schema, sequences)
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

local function _check_and_dry_run_schema(schema)
    -- Create dry-run sequences before spaces since space indexes use sequences.
    local sequences = schema.sequences or {}
    local ok, err = _check_and_dry_run_sequences(sequences)
    if not ok then
        return nil, err
    end

    local spaces = schema.spaces
    local ok, err = _check_and_dry_run_spaces(spaces, sequences)
    if not ok then
        return nil, err
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

    return ddl_db.call_dry_run(_check_and_dry_run_schema, schema)
end

local function set_metadata_space(metadata_name, space_format)
    local metadata_space_name = string.format("_ddl_%s", metadata_name)

    local metadata_space = box.schema.space.create(metadata_space_name, {
        format = space_format,
        if_not_exists = true
    })

    metadata_space:create_index(
        'space_name', {
            type = 'TREE',
            unique = true,
            parts = {{'space_name', 'string', is_nullable = false}},
            if_not_exists = true
        }
    )
end

local function _set_schema(schema)
    set_metadata_space(
        'sharding_key',
        {
            {name = 'space_name', type = 'string', is_nullable = false},
            {name = 'sharding_key', type = 'array', is_nullable = false}
        }
    )
    set_metadata_space(
        'sharding_func',
        {
            {name = 'space_name', type = 'string', is_nullable = false},
            {name = 'sharding_func_name', type = 'string', is_nullable = true},
            {name = 'sharding_func_body', type = 'string', is_nullable = true},
        }
    )

    for sequence_name, sequence_schema in pairs(schema.sequences or {}) do
        if box.sequence[sequence_name] == nil then
            ddl_set.create_sequence(sequence_name, sequence_schema)
        end
    end

    for space_name, space_schema in pairs(schema.spaces) do
        if box.space[space_name] == nil then
            ddl_set.create_space(space_name, space_schema)
        end
    end

    return true
end

local function set_schema(schema)
    local ok, err = check_schema(schema)
    if not ok then
        return nil, err
    end

    return ddl_db.call_atomic(_set_schema, schema)
end

local function get_schema()
    local spaces = {}
    for _, space in box.space._space:pairs({box.schema.SYSTEM_ID_MAX}, {iterator = "GT"}) do
        if space.name ~= '_ddl_sharding_key' and space.name ~= '_ddl_sharding_func' then
            spaces[space.name] = ddl_get.get_space_schema(space.name)
        end
    end

    local sequences = {}
    for _, sequence in box.space._sequence:pairs() do
        sequences[sequence.name] = ddl_get.get_sequence_schema(sequence.name)
    end

    local next_k = next(sequences)
    local no_sequences = next_k == nil

    if no_sequences then
        -- For backward compatibility.
        sequences = nil
    end

    return {
        spaces = spaces,
        sequences = sequences,
    }
end

local function bucket_id(space_name, sharding_key)
    if type(space_name) ~= 'string' then
        return nil, string.format(
            "Invalid space name (string expected, got %s)", type(space_name)
        )
    end
    if sharding_key == nil then
        return nil, string.format(
            "Sharding key specified for space (%s) is nil", space_name)
    end

    local bucket_id, err = ddl_get.internal.bucket_id(
        space_name, sharding_key)
    if err ~= nil then
        return nil, err
    end

    return bucket_id
end

return {
    check_schema = check_schema,
    set_schema = set_schema,
    get_schema = get_schema,
    bucket_id = bucket_id,
    _VERSION = require('ddl.version'),
}
