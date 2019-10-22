local log = require('log')
local utils = require('ddl.utils')

local valid_field_types = {
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

local hash_tree_index_field_types = {
    unsigned  = true,
    integer   = true,
    number    = true,
    string    = true,
    scalar    = true,
    boolean   = true,
    varbinary = true,
}

local known_engines = {
    ['memtx'] = true,
    ['vinyl'] = true,
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
local valid_index = {type = true, name = true, unique = true, parts = true}
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


local function validate_format_field(field, idx)
    if type(field) ~= 'table' then
        return nil, string.format("field in space.format with idx '%d' is not a table", idx)
    end

    if type(field.name) ~= 'string' then
        return nil, string.format(
            "field in space.format with idx '%d' has incorrect 'name' type '%s', expected 'string'",
            idx, type(field.name)
        )
    end

    local redutant_fields = utils.find_redutant_fields(valid_field, field)
    if redutant_fields ~= nil then
        return nil, string.format(
            "field in space.format '%s' has redutant fields %s",
            field.name, table.concat(redutant_fields, ", ")
        )
    end

    -- here check string regexp name
    if valid_field_types[field.type] == nil then
        return nil, string.format(
            "field in space.format with name '%s' has incorrect type '%s'",
            field.name, field.type or 'nil'
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


local function validate_index_part(index, idx, space_info)
    local part = index.parts[idx]
    if type(part) ~= 'table' then
        return nil, string.format('part with idx %d is not a table', idx)
    end

    local redutant_fields = utils.find_redutant_fields(valid_index_part, part)
    if redutant_fields ~= nil then
        return nil, string.format(
            "part with idx '%d' has redutant fields %s",
            idx, table.concat(redutant_fields, ", ")
        )
    end

    local type_of_path = type(part.path)
    if type_of_path ~= 'string' then
        return nil, string.format(
            "part with idx '%d' has not valid type '%s', expected 'string'",
            type_of_path or 'nil'
        )
    end

    -- todo after check that there is no symbols like '?', ':', etc
    local field_name, json_path = unpack(string.split(part.path, '.', 1))
    local index_info = index_info_map[index.type]
    local format_field = space_info.fields[field_name]
    if not format_field then 
        return nil, string.format(
            "part with idx '%d' references to missing fieild in format '%s'",
            idx, field_name
        )
    end

    if  json_path then
        if format_field.type ~= 'map' then
            return nil, string.format(
                "part with idx '%d' has json_path, but format field with name '%s' has type '%s', expected 'map'",
                idx, field_name, format_field.type
            )
        end

        if string.find(json_path, '[*]', 1, true) and (not index_info.is_multikey) then
            return nil, string.format(
                "part with idx '%d' has multikey path ('%s'), but index type '%s' doesnt support multikeys",
                idx, part.path, index.type
            )
        end
    end

    if format_field.type ~= 'map' and format_field.type ~= 'any' then
        if format_field.type ~= part.type then
            return nil, string.format(
                "part with idx '%d' has type '%s' and it differs from format field '%s' type '%s'",
                idx, part.type or 'nil', field_name, format_field.type
            )
        end
    end

    if not index_info.field_types[part.type] then
        return nil, string.format(
            "part with idx '%d' has type '%s', but '%s' indexes doesnt support this",
            idx, part.type or 'nil', index.type
        )
    end

    if part.type == 'string' then
        local collation_info = box.space._collation.index.name:select{part.collation or 'nil'}
        if #collation_info == 0 then
            return nil, string.format(
                "part with idx '%d' has type 'string' and collation must be set, but there is unknown collation '%s'",
                idx, part.collation
            )
        end
    end

    if format_field.is_nullable ~= part.is_nullable then
        return nil, string.format(
            "part with idx '%d' is_nullable='%s', but format field '%s' has is_nullable='%s'",
            idx, part.is_nullable, field_name, format_field.is_nullable
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

local function validate_index(index, idx, space_info)
    if type(index) ~= 'table' then
        return nil, string.format("space.index with index '%d' is not a table", idx)
    end

    if type(index.name) ~= 'string' then
        return nil, string.format(
            "space.index with index '%d' has incorrect name, it must be a string type", idx
        )
    end

    local redutant_fields = utils.find_redutant_fields(valid_index, index)
    if redutant_fields ~= nil then
        return nil, string.format(
            "space.index '%s' has redutant fields %s",
            index.name, table.concat(redutant_fields, ", ")
        )
    end

    local index_info = index_info_map[index.type]
    if (not index_info) or (not index_info.engine[space_info.engine]) then
        return nil, string.format("space.index '%s' has incorect type '%s'", index.name, index.type or 'nil')
    end

    if type(index.unique) ~= 'boolean' then
        return nil, string.format(
            "space.index '%s' has incorect unique field, it must be set and its boolean type"
        )
    end

    if not validate_index_uniqueness(index.type, index.unique) then
        return nil, string.format(
            "space.index '%s' with type = '%s' doesn't alows 'unique=%s'",
            index.name, index.type, index.unique
        )
    end

    if not utils.is_array(index.parts) then
        return nil, string.format("space.index '%s' has incorect parts, it must be an array", index.name)
    end

    if not index_info.is_multipart and #index.parts > 1 then
        return nil, string.format(
            "space.index '%s' with type '%s' can't be multipart (parts must contain only one value)",
            index.name, index.type
        )
    end

    for i, _ in ipairs(index.parts) do
        local res, err = validate_index_part(index, i, space_info)
        if not res then
            return nil, string.format("in index '%s' %s", index.name, err)
        end
    end

    if idx == 1 then
        return validate_primary_index(index)
    end

    return true
end


local function validate_space(space)
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

    if not utils.is_array(space.format) then
        return nil, 'space.format is not an array'
    end

    for _, format_field in ipairs(space.format) do
        local res, err = validate_format_field(format_field)
        if not res then
            return nil, err
        end
    end

    if not utils.is_array(space.indexes) then
        return nil, 'space.indexes is not an array'
    end

    local space_info = {
        engine = space.engine,
        fields = {}
    }

    for _, field in ipairs(space.format) do
        space_info.fields[field.name] = {type = field.type, is_nullable = field.is_nullable}
    end

    for i, index in ipairs(space.indexes) do
        local res, err = validate_index(index, i, space_info)
        if not res then
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


local function validate(schema)
    local error_msg = 'Validation error: '
    if type(schema) ~= 'table' then
        return nil, error_msg .. 'scheme is not a table'
    end

    for space_name, space in pairs(schema) do
        local res, err = validate_space(space)
        if not res then
            return nil, string.format("Error in space '%s': %s", space_name, err)
        end
    end

    return validate_with_apply(schema)
end

return {
    validate = validate,
}
