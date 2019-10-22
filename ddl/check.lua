#!/usr/bin/env tarantool

local utils = require('ddl.utils')

-- local hash_tree_index_field_types = {
--     unsigned  = true,
--     integer   = true,
--     number    = true,
--     string    = true,
--     scalar    = true,
--     boolean   = true,
--     varbinary = true,
-- }


-- local index_info_map = {
--     ['TREE'] = {
--         known_keys = {'name', 'type', 'unique', 'parts'},
--         can_be_unique = true,
--         ['field_types'] = hash_tree_index_field_types,
--         ['is_multikey'] = true,
--         ['is_multipart'] = true,
--         ['engine'] = {
--             ['memtx'] = true,
--             ['vinyl'] = true,
--         }
--     },
--     ['HASH'] = {
--         known_keys = {'name', 'type', 'unique', 'parts'},
--         ['field_types'] = hash_tree_index_field_types,
--         ['is_multikey'] = false,
--         ['is_multipart'] = true,
--         ['engine'] = {
--             ['memtx'] = true
--         }
--     },
--     ['RTREE'] = {
--         known_keys = {'name', 'type', 'unique', 'field', 'dimension', 'distance'},
--         can_be_unique = false,

--         ['field_types'] = {
--             ['array'] = true
--         },
--         ['is_multikey'] = false,
--         ['is_multipart'] = false,
--         ['engine'] = {
--             ['memtx'] = true
--         }
--     },
--     ['BITSET'] = {
--         ['field_types'] = {
--             ['unsigned'] = true,
--             ['string'] = true,
--         },
--         ['is_multikey'] = false,
--         ['is_multipart'] = false,
--         ['engine'] = {
--             ['memtx'] = true
--         }
--     },
-- }

local function check_field(i, field, space)
    if type(field) ~= 'table' then
        return nil, string.format(
            'space[%q]: bad argument fields[%d] ' ..
            '(table expected, got %s)',
            space.name, i, type(field)
        )
    end

    do -- check field.name
        if type(field.name) ~= 'string' then
            return nil, string.format(
                'space[%q].fields[%d]: bad argument "name"' ..
                ' (string expected, got %s)',
                space.name, i, type(field.name)
            )
        end
    end

    do -- check field.is_nullable
        if type(field.is_nullable) ~= 'boolean' then
            return nil, string.format(
                'space[%q].fields[%q]: bad argument "is_nullable"' ..
                ' (boolean expected, got %s)',
                space.name, field.name, type(field.is_nullable)
            )
        end
    end

    do -- check field.type
        if type(field.type) ~= 'string' then
            return nil, string.format(
                'space[%q].fields[%q]: bad argument "type"' ..
                ' (string expected, got %s)',
                space.name, field.name, type(field.type)
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

        if known_field_types[field.type] == nil then
            return nil, string.format(
                'space[%q].fields[%q]: unknown type %q',
                space.name, field.name, field.type
            )
        end
    end


    -- check redundant keys
    local k = utils.redundant_key(field,
        {'name', 'type', 'is_nullable'}
    )
    if k ~= nil then
        return nil, string.format(
            "space[%q].fields[%q]: redundant argument %q",
            space.name, field.name, k
        )
    end

    return true
end

local function check_path(path, index, space)
    if type(path) ~= 'string' then
        return nil, string.format(
            ' bad argument "path"' ..
            ' (string expecetd got %s)',
            type(path)
        )
    end

    local field_name, _ = unpack(string.split(path, '.', 1))
    -- local index_info = index_info_map[index.type]
    if not space.fields[field_name] then
        return nil, string.format(
            ' bad path %q referencing unknown field',
            path
        )
    end


    -- if json_path then
    --     if space.fields[field_name].type ~= 'map' then
    --         return nil, string.format(
    --             ' bad json path %q is json_path, but format field with name "%s" has type "%s", expected "map"',
    --             path, field_name, space.fields[field_name].type
    --         )
    --     end

    --     if string.find(json_path, '[*]', 1, true) and (not index_info.is_multikey) then
    --         return nil, string.format(
    --             'path "%s" has multikey path, but index type "%s" doesnt support multikeys',
    --             path, index.type
    --         )
    --     end
    -- end
    return true
end

local function check_index_part(i, index, space)
    local part = index.parts[i]

    if type(part) ~= 'table' then
        return nil, string.format(
            'space[%q].indexes[%q]: bad argument parts[%d]' ..
            ' (table expected, got %s)',
            space.name, index.name, i, type(part)
        )
    end

    local ok, err = check_path(part.path, index, space)
    if not ok then
        return nil, string.format(
            'space[%q].indexes[%q].parts[%d]: %s',
            space.name, index.name, i, index.type, err
        )
    end

    local field_name, _ = unpack(string.split(part.path, '.', 1))
    local space_format_field = space.fields[field_name]

    if space_format_field.type ~= 'map' and space_format_field.type ~= 'any' then
        if space_format_field.type ~= part.type then
            return nil, string.format(
                'space[%q].indexes[%q].parts[%d].type (%s): type differs from space.format[%q] type "%s"',
                space.name, index.name, i, index.type, field_name, space_format_field.type
            )
        end
    end

    -- local index_info = index_info_map[index.type]
    -- if not index_info.field_types[part.type] then
    --     return nil, string.format(
    --         "part with idx '%d' has type '%s', but '%s' indexes doesnt support this",
    --         i, part.type or 'nil', index.type
    --     )
    -- end

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

    -- check redundant keys
    local k = utils.redundant_key(space,
        {'path', 'type', 'collation', 'is_nullable'}
    )
    if k ~= nil then
        return nil, string.format(
            "space[%q].indexes[%q].parts[%d]: redundant argument %q",
            space.name, index.name, i, k
        )
    end

    return true
end


-- local function validate_primary_index(index)
--     if not index.unique then
--         return nil, string.format(
--             "inedx '%s' is pramary and it must be unique, primary indexes can be 'TREE' or 'HASH'",
--             index.name
--         )
--     end

--     for i, part in ipairs(index.parts) do
--         if string.find(part.path, '[*]', 1, true) then
--             return nil , string.format(
--                 "index.parts with idx '%d' of index '%s' contains multikey ('%s'), " ..
--                 "but primary indexes can't be multikey",
--                 i, index.name, part.path
--             )
--         end
--     end
--     return true
-- end

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
        return nil, string.format(
            'space[%q].indexes[%q].parts[%d]: %s',
            space.name, index.name, i, err
        )
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

    do -- check index.name
        if type(index.name) ~= 'string' then
            return nil, string.format(
                'space[%q].indexes[%d]: bad argument "name"' ..
                ' (string expected, got %s)',
                space.name, i, type(index.name)
            )
        end
    end

    do -- check index.unique
        if type(index.unique) ~= 'boolean' then
            return nil, string.format(
                'space[%q].indexes[%q]: bad argument "unique"' ..
                ' (boolean expected, got %s)',
                space.name, index.name, type(index.unique)
            )
        end
    end

    do -- check index.type
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

        local engines_support = {
            ['vinyl'] = {
                TREE = true,
            },
            ['memtx'] = {
                TREE = true,
                HASH = true,
                RTREE = true,
                BITSET = true,
            }
        }
        if not engines_support[space.engine][index.type] then
            return nil, string.format(
                "space[%q].indexes[%q]: %s engine doesn't support index.type %s",
                space.name, index.name, space.engine, index.type
            )
        end
    end

    if index.type == 'TREE' then
        if i == 1 and index.unique ~= true then
            return nil, string.format(
                'space[%q].indexes[%q]: primary TREE index must be unique',
                space.name, index.name
            )
        end

        -- local ok, err = check_index_parts(index, space)
        -- if not ok then
        --     return nil, err
        -- end

    elseif index.type == 'HASH' then
        if index.unique ~= true then
            return nil, string.format(
                'space[%q].indexes[%q]: HASH index must be unique',
                space.name, index.name
            )
        end

        -- local ok, err = check_index_parts(index, space)
        -- if not ok then
        --     return nil, err
        -- end

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

        -- local ok, err = check_index_field(index, space)
        -- if not ok then
        --     return nil, err
        -- end

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

        -- local ok, err = check_index_field(index, space)
        -- if not ok then
        --     return nil, err
        -- end
    end

    local k = utils.redundant_key(index,
        {'type', 'name', 'unique', 'parts', 'field'}
    )
    if k ~= nil then
        return nil, string.format(
            "space[%q].indexes[%q]: redutant argument %q",
            space.name, index.name, k
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


local function check_space(space_name, space)
    assert(type(space_name) == 'string')

    do -- check space.engine
        if type(space.engine) ~= 'string' then
            return nil, string.format(
                'space[%q]: bad argument "engine"' ..
                ' (string expected, got %s)',
                space_name, type(space.engine)
            )
        end

        local known_engines = {
            ['memtx'] = true,
            ['vinyl'] = true,
        }
        if not known_engines[space.engine] then
            return nil, string.format(
                "space[%q]: unknown engine %q",
                space_name, space.engine
            )
        end
    end

    do -- check space.is_local
        if type(space.is_local) ~= 'boolean' then
            return nil, string.format(
                'space[%q]: bad argument "is_local"' ..
                ' (boolean expected, got %s)',
                space_name, type(space.is_local)
            )
        end
    end

    do -- check space.temporary
        if type(space.temporary) ~= 'boolean' then
            return nil, string.format(
                'space[%q]: bad argument "temporary"' ..
                ' (string expected, got %s)',
                space_name, type(space.temporary)
            )
        end
        if space.engine == 'vinyl' and space.temporary then
            return nil, string.format(
                "space[%q]: vinyl engine doesn't support temporary spaces",
                space_name
            )
        end
    end

    -- auxiliary kv-map to help indexes validation
    local space_fields = {}

    do -- check space.format
        if not utils.is_array(space.format) then
            return nil, string.format(
                'space[%q]: bad argument "format"' ..
                ' (contiguous array expected, got %s)',
                space_name, type(space.format)
            )
        end
        for i, field_params in ipairs(space.format) do
            local ok, err = check_field(i, field_params, {
                name = space_name
            })
            if not ok then
                return nil, err
            end

            space_fields[field_params.name] = {
                type = field_params.type,
                is_nullable = field_params.is_nullable,
            }
        end
    end

    do -- check indexes
        if not utils.is_array(space.indexes) then
            return nil, string.format(
                'space[%q]: bad argument "indexes"' ..
                ' (contiguous array expected, got %s)',
                space_name, type(space.indexes)
            )
        end
        for i, index in ipairs(space.indexes) do
            local ok, err = check_index(i, index, {
                name = space_name,
                engine = space.engine,
                fields = space_fields,
            })

            if not ok then
                return nil, err
            end
        end
    end

    -- check redundant keys
    local k = utils.redundant_key(space,
        {'engine', 'is_local', 'temporary', 'format', 'indexes'}
    )
    if k ~= nil then
        return nil, string.format(
            "space[%q]: redundant argument %q",
            space_name, k
        )
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
    local ddl_set_schema = require('ddl.set_schema')
    local applied_spaces = {}

    for space_name, space in pairs(schema) do
        local res, err = xpcall(ddl_set_schema.set_schema, debug.traceback, space_name, space)
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
    if type(schema) ~= 'table' then
        return nil, string.format(
            'Invalid schema: table expected, got %s',
            type(schema)
        )
    end

    for space_name, space in pairs(schema) do
        if type(space_name) ~= 'string' then
            return nil, string.format(
                'Invalid schema: bad key %q' ..
                ' (string expected, got %s)',
                space_name, type(space_name)
            )
        end

        if type(space) ~= 'table' then
            return nil, string.format(
                "Invalid schema: bad argument space[%q]" ..
                " (table expected, got %s)",
                space_name, type(space)
            )
        end

        -- local res, err = check_space(space_name, space)
        -- if not res then
        --     return nil, err
        -- end
    end

    return validate_with_apply(schema)
end

return {
    check_schema = check_schema,
}
