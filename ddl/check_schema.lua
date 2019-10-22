#!/usr/bin/env tarantool

local utils = require('ddl.utils')

local hash_tree_index_field_types = {
    unsigned  = true,
    integer   = true,
    number    = true,
    string    = true,
    scalar    = true,
    boolean   = true,
    varbinary = true,
}



local index_info_map = {
    ['RTREE'] = {
        ['field_types'] = {
            ['array'] = true
        },
        ['is_multikey'] = false,
        ['is_multipart'] = false,
        ['engine'] = {
            ['memtx'] = true
        }
    },
    ['HASH'] = {
        ['field_types'] = hash_tree_index_field_types,
        ['is_multikey'] = false,
        ['is_multipart'] = true,
        ['engine'] = {
            ['memtx'] = true
        }
    },
    ['BITSET'] = {
        ['field_types'] = {
            ['unsigned'] = true,
            ['string'] = true,
        },
        ['is_multikey'] = false,
        ['is_multipart'] = false,
        ['engine'] = {
            ['memtx'] = true
        }
    },
    ['TREE'] = {
        ['field_types'] = hash_tree_index_field_types,
        ['is_multikey'] = true,
        ['is_multipart'] = true,
        ['engine'] = {
            ['memtx'] = true,
            ['vinyl'] = true,
        }
    }
}

local valid_field = {name = true, type = true, is_nullable = true}
local valid_space = {engine = true, is_local = true, temporary = true, format = true, indexes = true}
local valid_index = {type = true, name = true, unique = true, parts = true, field = true}
local valid_index_part = {path = true, type = true, is_nullable = true, collation = true}

local function validate_index_uniqueness(index_type, unique)
    if index_type == 'TREE' then
        return true
    end

    if index_type == 'HASH' then
        return unique == true
    end

    if index_type == 'BITSET' or index_type == 'RTREE' then
        return unique == false
    end
end


local function check_field(i, field)

    if type(field) ~= 'table' then
        return nil, string.format("field in space.format with idx '%d' is not a table", i)
    end

    if type(field.name) ~= 'string' then
        return nil, string.format(
            "field in space.format with idx '%d' has incorrect 'name' type '%s', expected 'string'",
            i, type(field.name)
        )
    end

    local redutant_fields = utils.find_redutant_fields(valid_field, field)
    if redutant_fields ~= nil then
        return nil, string.format(
            "field in space.format '%s' has redutant fields %s",
            field.name, table.concat(redutant_fields, ", ")
        )
    end

    local known_field_types = {
        unsigned  = true,
        integer   = true,
        number    = true,
        string    = true,
        scalar    = true,
        boolean   = true,
        varbinary = true,
        array     = true,
        map       = true,
        any       = true,
    }

    -- here check string regexp name
    if known_field_types[field.type] == nil then
        return nil, string.format(
            "field in space.format with name '%s' has incorrect type '%s'",
            field.name, field.type
        )
    end

    if type(field.is_nullable) ~= 'boolean' then
        return nil, string.format(
            "'field in space.format with name '%s' has incorrect is_nullable, it must be boolean, actual '%s'",
            field.name, field.is_nullable
        )
    end

    return true
end

local function check_path(path, index, space)
    local type_of_path = type(path)
    if type_of_path ~= 'string' then
        return nil, string.format(
            '"path" bad argument(string expecetd got %s)',
            type_of_path
        )
    end

    local field_name, json_path = unpack(string.split(path, '.', 1))
    local index_info = index_info_map[index.type]
    local format_field = space.fields[field_name]
    if not format_field then
        return nil, string.format(
            'path "%s" references to missing fieild in format "%s"',
            path, field_name
        )
    end

    if json_path then
        if format_field.type ~= 'map' then
            return nil, string.format(
                'path "%s" is json_path, but format field with name "%s" has type "%s", expected "map"',
                path, field_name, format_field.type
            )
        end

        if string.find(json_path, '[*]', 1, true) and (not index_info.is_multikey) then
            return nil, string.format(
                'path "%s" has multikey path, but index type "%s" doesnt support multikeys',
                path, index.type
            )
        end
    end
    return true
end

local function check_index_part(i, index, space)
    local part = index.parts[i]
    if type(part) ~= 'table' then
        return nil, string.format(
            'space[%q].indexes[%q].parts[%d].path (%s): %s' ..
            ' (table expected, got %s)',
            space.name, index.name, i, index.type, type(part)
        )
    end

    local redutant_fields = utils.find_redutant_fields(valid_index_part, part)
    if redutant_fields ~= nil then
        return nil, string.format(
            "space[%q].indexes[%q].parts[%d]: part contains redutant fields %s",
            space.name, index.name, i, table.concat(redutant_fields, ", ")
        )
    end

    local ok, err = check_path(part.path, index, space)
    if not ok then
        return nil, string.format(
            'space[%q].indexes[%q].parts[%d].path (%s): %s',
            space.name, index.name, i, index.type, err
        )
    end

    local field_name, _ = unpack(string.split(part.path, '.', 1))
    local space_format_field = space[field_name]

    if space_format_field.type ~= 'map' and space_format_field.type ~= 'any' then
        if space_format_field.type ~= part.type then
            return nil, string.format(
                'space[%q].indexes[%q].parts[%d].type (%s): type differs from space.format[%q] type "%s"',
                space.name, index.name, i, index.type, field_name, space_format_field.type
            )
        end
    end

    local index_info = index_info_map[index.type]
    if not index_info.field_types[part.type] then
        return nil, string.format(
            "part with idx '%d' has type '%s', but '%s' indexes doesnt support this",
            i, part.type or 'nil', index.type
        )
    end

    if part.type == 'string' then
        local collation_info = box.space._collation.index.name:select{part.collation or 'nil'}
        if #collation_info == 0 then
            return nil, string.format(
                "part with idx '%d' has type 'string' and collation must be set, but there is unknown collation '%s'",
                i, part.collation
            )
        end
    end

    if space_format_field.is_nullable ~= part.is_nullable then
        return nil, string.format(
            "part with idx '%d' is_nullable='%s', but format field '%s' has is_nullable='%s'",
            i, part.is_nullable, field_name, space_format_field.is_nullable
        )
    end

    return true
end


local function validate_primary_index(index)
    if not index.unique then
        return nil, string.format(
            "inedx '%s' is pramary and it must be unique, primary indexes can be 'TREE' or 'HASH'",
            index.name
        )
    end

    for i, part in ipairs(index.parts) do
        if string.find(part.path, '[*]', 1, true) then
            return nil , string.format(
                "index.parts with idx '%d' of index '%s' contains multikey ('%s'), " ..
                "but primary indexes can't be multikey",
                i, index.name, part.path
            )
        end
    end
    return true
end

local function check_index_parts(index, space)
    if not utils.is_array(index.parts) then
        return nil, string.format(
            'space[%q].indexes[%q] (%s): bad argument "parts"' ..
            ' (contiguous array of tables expected, got %s)',
            space.name, index.name, index.type, type(index.parts)
        )
    end

    for i, _ in ipairs(index.parts) do
        local res, err = check_index_part(i, index, space)
        if not res then
            return nil, string.format("in index '%s' %s", index.name, err)
        end
    end

    return true
end

local function check_index_field(index, space)
    local ok, err = check_path(index.field, index, space)
    if not ok then
        return nil, err
    end
    return true
end

local function check_index(i, index, space)
    if type(index) ~= 'table' then
        return nil, string.format(
            'space[%q]: bad argument indexes[%d] ' ..
            '(table expected, got %s)',
            space.name, i, type(index)
        )
    end

    if type(index.name) ~= 'string' then
        return nil, string.format(
            'space[%q].indexes[%d]: bad argument "name"' ..
            ' (string expected, got %s)',
            space.name, i, type(index.name)
        )
    end

    if type(index.type) ~= 'string' then
        return nil, string.format(
            'space[%q].indexes[%q]: bad argument "type"' ..
            ' (string expected, got %s)',
            space.name, index.name, type(index.type)
        )
    end

    local known_index_types = {
        TREE = true,
        HASH = true,
        RTREE = true,
        BITSET = true,
    }
    if not known_index_types[index.type] then
        return nil, string.format(
            "space[%q].indexes[%q]: unknown type %q",
            space.name, index.name, index.type
        )
    end

    if type(index.unique) ~= 'boolean' then
        return nil, string.format(
            'space[%q].indexes[%q]: bad argument "unique"' ..
            ' (boolean expected, got %s)',
            space.name, index.name, type(index.unique)
        )
    end

    if space.engine == 'vinyl' and index.type ~= 'TREE' then
        return nil, string.format(
            "space[%q].indexes[%q]: vinyl engine doesn't support %s indexes",
            space.name, index.name, index.type
        )
    end

    if index.type == 'TREE' then
        if i == 1 and index.unique ~= true then
            return nil, string.format(
                'space[%q].indexes[%q] (TREE): primary index must be unique',
                space.name, index.name
            )
        end

        local ok, err = check_index_parts(index, space)
        if not ok then
            return nil, err
        end

        -- if not utils.is_array(index.parts) then
        --     return nil, string.format(
        --         'space[%q].indexes[%q] (TREE): bad argument "parts"' ..
        --         ' (contiguous array of tables expected, got %s)',
        --         space.name, index.name, type(index.parts)
        --     )
        -- end
        -- for i, _ in ipairs(index.parts) do
        --     local res, err = check_index_part(i, index, space)
        --     if not res then
        --         return nil, string.format("in index '%s' %s", index.name, err)
        --     end
        -- end

    elseif index.type == 'HASH' then
        if index.unique ~= true then
            return nil, string.format(
                'space[%q].indexes[%q]: HASH index must be unique',
                space.name, index.name
            )
        end

        local ok, err = check_index_parts(index, space)
        if not ok then
            return nil, err
        end

    elseif index.type == 'RTREE' then
        if i == 1 then
            return nil, string.format(
                "space[%q].indexes[%q]: RTREE index can't be primary",
                space.name, index.name
            )
        end

        if index.unique ~= false then
            return nil, string.format(
                "space[%q].indexes[%q]: RTREE index can't be unique",
                space.name, index.name
            )
        end

        local ok, err = check_index_field(index, space)
        if not ok then
            return nil, err
        end

    elseif index.type == 'BITSET' then
        if i == 1 then
            return nil, string.format(
                "space[%q].indexes[%q]: BITSET index can't be primary",
                space.name, index.name
            )
        end

        if index.unique ~= false then
            return nil, string.format(
                "space[%q].indexes[%q]: BITSET index can't be unique",
                space.name, index.name
            )
        end

        local ok, err = check_index_field(index, space)
        if not ok then
            return nil, err
        end
    end

    local redutant_fields = utils.find_redutant_fields(valid_index, index)
    if redutant_fields ~= nil then
        return nil, string.format(
            "space.index '%s' has redutant fields %s",
            index.name, table.concat(redutant_fields, ", ")
        )
    end

    -- local index_info = index_info_map[index.type]
    -- if (not index_info) or (not index_info.engine[space_info.engine]) then
    --     return nil, string.format("space.index '%s' has incorect type '%s'", index.name, index.type or 'nil')
    -- end

    -- if type(index.unique) ~= 'boolean' then
    --     return nil, string.format(
    --         "space.index '%s' has incorect unique field, it must be set and its boolean type"
    --     )
    -- end

    -- if not validate_index_uniqueness(index.type, index.unique) then
    --     return nil, string.format(
    --         "space.index '%s' with type = '%s' doesn't alows 'unique=%s'",
    --         index.name, index.type, index.unique
    --     )
    -- end

    -- if not utils.is_array(index.parts) then
    --     return nil, string.format("space.index '%s' has incorect parts, it must be an array", index.name)
    -- end

    -- if not index_info.is_multipart and #index.parts > 1 then
    --     return nil, string.format(
    --         "space.index '%s' with type '%s' can't be multipart (parts must contain only one value)",
    --         index.name, index.type
    --     )
    -- end

    -- for i, _ in ipairs(index.parts) do
    --     local res, err = check_index_part(index, i, space_info)
    --     if not res then
    --         return nil, string.format("in index '%s' %s", index.name, err)
    --     end
    -- end

    -- if idx == 1 then
        -- return validate_primary_index(index)
    -- end

    return true
end


local function check_space(space)
    if type(space) ~= 'table' then
        return nil, 'space is not a table'
    end

    local redutant_fields = utils.find_redutant_fields(valid_space, space)
    if redutant_fields ~= nil then
        return nil, string.format(
            "space has redutant fields %s",
            table.concat(redutant_fields, ", ")
        )
    end

    local known_engines = {
        ['memtx'] = true,
        ['vinyl'] = true,
    }
    if not known_engines[space.engine] then
        return nil, string.format("space.engine '%s' is unknown", space.engine or 'nil')
    end

    if type(space.is_local) ~= 'boolean' then
        return nil, "space.is_local must be set and it's type boolean"
    end

    if type(space.temporary) ~= 'boolean' then
        return nil, "space.temporary must be set and it's type boolean"
    else
        if space.temporary and space.engine == 'vinyl' then
            return nil, "space.temporary must be set to 'false' with engine 'vinyl'"
        end
    end

    -- auxiliary kv-map to help indexes validation
    local space_fields = {}

    -- Check space.format fields
    if not utils.is_array(space.format) then
        return nil, "???"
    end
    for i, field_params in ipairs(space.format) do
        local ok, err = check_field(i, field_params)
        if not ok then
            return nil, err
        end

        space_fields[field_params.name] = {
            type = field_params.type,
            is_nullable = field_params.is_nullable,
        }
    end

    -- Check indexes
    if not utils.is_array(space.indexes) then
        return nil, 'space.indexes is not an array'
    end
    for i, index in ipairs(space.indexes) do
        local ok, err = check_index(i, index, {
            name = space.name,
            engine = space.engine,
            fields = space_fields,
        })

        if not ok then
            return nil, err
        end
    end
    return true
end


local function clear_apply(space_names)
    for _, space_name in ipairs(space_names) do
        if box.space[space_name] then
            box.space[space_name]:drop()
        end
    end
end


local function validate_with_apply(schema)
    local apply_ddl = require('ddl.set_schema')
    local applied_spaces = {}

    for space_name, space in pairs(schema) do
        local res, err = pcall(apply_ddl.set_space, space_name, space)
        table.insert(applied_spaces, space_name) -- it's hack, because space can be created, but indexes not
        if not res then
            clear_apply(applied_spaces)
            return nil, err
        end
    end

    clear_apply(applied_spaces)
    return true
end


local function check_schema(schema)
    local error_msg = 'Validation error: '
    if type(schema) ~= 'table' then
        return nil, error_msg .. 'scheme is not a table'
    end

    for space_name, space in pairs(schema) do
        local res, err = check_space(space)
        if not res then
            return nil, string.format("Error in space '%s': %s", space_name, err)
        end
    end

    return validate_with_apply(schema)
end

return {
    check_schema = check_schema,
}
