#!/usr/bin/env tarantool

local utils = require('ddl.utils')
local db = require('ddl.db')

local function check_field(i, field, space)
    if type(field) ~= 'table' then
        return nil, string.format(
            "space[%q]: bad argument fields[%d] " ..
            "(table expected, got %s)",
            space.name, i, type(field)
        )
    end

    do -- check field.name
        if type(field.name) ~= 'string' then
            return nil, string.format(
                "space[%q].fields[%d]: bad argument 'name'" ..
                " (string expected, got %s)",
                space.name, i, type(field.name)
            )
        end
    end

    do -- check field.is_nullable
        if type(field.is_nullable) ~= 'boolean' then
            return nil, string.format(
                "space[%q].fields[%q]: bad argument 'is_nullable'" ..
                " (boolean expected, got %s)",
                space.name, field.name, type(field.is_nullable)
            )
        end
    end

    do -- check field.type
        if type(field.type) ~= 'string' then
            return nil, string.format(
                "space[%q].fields[%q]: bad argument 'type'" ..
                " (string expected, got %s)",
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
                "space[%q].fields[%q]: unknown type %q",
                space.name, field.name, field.type
            )
        end

        if not db.varbinary_allowed() and field.type == 'varbinary' then
            return nil, string.format(
                "space[%q].fields[%q]: varbinary type isn't allowed in your Tarantool version (%q)",
                space.name, field.name, db.version()
            )
        end
    end


    do -- check redundant keys
        local k = utils.redundant_key(field,
            {'name', 'type', 'is_nullable'}
        )
        if k ~= nil then
            return nil, string.format(
                "space[%q].fields[%q]: redundant argument %q",
                space.name, field.name, k
            )
        end
    end

    return true
end

local function is_path_multikey(path)
    return string.find(path, '[*]', 1, true) ~= nil
end

local function check_index_part_path(path, index, space)
    if type(path) ~= 'string' then
        return nil, string.format(
            "bad argument 'path'" ..
            " (string expecetd got %s)",
            type(path)
        )
    end

    local field_name, json_path = unpack(string.split(path, '.', 1))
    do -- check index.part.path references to existing field
        if not space.fields[field_name] then
            return nil, string.format(
                "path (%q) referencing to unknown field",
                path
            )
        end
    end

    do -- check that json_path can be used
        if not db.json_path_allowed() and json_path ~= nil then
            return nil, string.format(
                "path (%q) is json_path, but your Tarantool version (%q) doesn't support this",
                path, db.version()
            )
        end
    end

    do -- check, that json_path references to map field
        if json_path ~= nil and space.fields[field_name].type ~= 'map' then
            return nil, string.format(
                "path (%q) is json_path. It references to field[%q] with type %s, but expected map",
                path, field_name, space.fields[field_name].type
            )
        end
    end

    local allow_multikey = {
        HASH = false,
        TREE = true,
        BITSET = false,
        RTREE = false
    }

    do -- check multikey references
        if is_path_multikey(path) and not allow_multikey[index.type] then
            return nil, string.format(
                "path (%q) is multikey, but index type %s doesn't allow multikeys",
                path, index.type
            )
        end
    end

    return true
end


local function check_index_part_type(part_type, index_type)
    if type(part_type) ~= 'string' then
        return nil, string.format(
            "bad argument 'type' (expected string, got %s)",
            type(part_type)
        )
    end
    -- also we have an aliases like str or num, but its ignored
    local known_part_types = {
        unsigned = true,
        integer = true,
        scalar = true,
        string = true,
        number = true,
        array = true,
        boolean = true,
        varbinary = true,
    }

    if not known_part_types[part_type] then
        return nil, string.format(
            "unknown type %q",
            part_type
        )
    end

    if not db.varbinary_allowed() and part_type == 'varbinary' then
       return nil, string.format(
           "varbinary type isn't allowed in your Tarantool version (%q)",
           db.version()
        )
    end

    local err_template = "%s field type is unsupported in %s index type"

    if index_type == 'TREE' or index_type == 'HASH' then
        known_part_types.array = nil
        if not known_part_types[part_type] then
            return nil, string.format(err_template, part_type, index_type)
        end
    elseif index_type == 'BITSET' then
        if part_type ~= 'unsigned' and part_type ~= 'string' then
            return nil, string.format(err_template, part_type, index_type)
        end
    elseif index_type == 'RTREE' and part_type ~= 'array' then
        return nil, string.format(err_template, part_type, index_type)
    end
    return true
end

local function check_index_part_collation(collation)
    if collation == nil then
        return true
    end

    if type(collation) ~= 'string' then
        return nil, string.format(
            "bad argument 'collation', (expected string (or no collation), got %s)",
            type(collation)
        )
    end

    if box.space._collation.index.name:count{collation} == 0 then
        return nil, string.format("unknown collation %q", collation)
    end
    return true
end

local function check_index_part(i, index, space)
    local part = index.parts[i]

    if type(part) ~= 'table' then
        return nil, string.format(
            "space[%q].indexes[%q]: bad argument parts[%d]" ..
            " (table expected, got %s)",
            space.name, index.name, i, type(part)
        )
    end

    do -- check path for valid
        local ok, err = check_index_part_path(part.path, index, space)
        if not ok then
            return nil, string.format(
                "space[%q].indexes[%q].parts[%d].path: %s",
                space.name, index.name, i, err
            )
        end
    end

    local field_name, json_path = unpack(string.split(part.path, '.', 1))
    local space_format_field = space.fields[field_name]


    do -- check part.type is valid and it is valid for index type
        local ok, err = check_index_part_type(part.type, index.type)
        if not ok then
            return nil, string.format(
                "space[%q].indexes[%q].parts[%d].type: %s",
                space.name, index.name, i, err
            )
        end
    end

    do -- check index.part.type equals format.field.type
        if space_format_field.type ~= 'any' then
            if not (space_format_field.type == 'map' and json_path ~= nil) then
                if space_format_field.type ~= part.type then
                    return nil, string.format(
                        "space[%q].indexes[%q].parts[%d].type: type differs" ..
                        " from space.format.field[%q] (expected %s, got %s)",
                        space.name, index.name, i, field_name, space_format_field.type, part.type
                    )
                end
            end
        end
    end

    do -- check collation exist, collation can be ommited
        if part.type == 'string' then
            local ok, err = check_index_part_collation(part.collation)
            if not ok then
                return nil, string.format(
                    "space[%q].indexes[%q].parts[%d].collation: %s",
                    space.name, index.name, i, err
                )
            end
        elseif part.collation ~= nil then
            return nil, string.format(
                "space[%q].indexes[%q].parts[%d].collation: type %q doesn't allows collation (only string type)",
                space.name, index.name, i, part.type
            )
        end
    end

    do -- check is_nullable has correct format
        if type(part.is_nullable) ~= 'boolean' then
            return nil, string.format(
                "space[%q].indexes[%q].parts[%d].is_nullable: bad " ..
                "argument 'is_nullable' (expected boolean, got %s)",
                space.name, index.name, i, type(part.is_nullable)
            )
        end
    end

    do -- check the same nullability
        if space_format_field.is_nullable ~= part.is_nullable then
            return nil, string.format(
                "space[%q].indexes[%q].parts[%d].is_nullable: has different nullability with " ..
                "space.foramat.field[%q] (expected %s, got %s)",
                space.name, index.name, i, field_name, space_format_field.is_nullable, part.is_nullable
            )
        end
    end

    do -- check redundant keys
        local k = utils.redundant_key(part,
            {'path', 'type', 'collation', 'is_nullable'}
        )
        if k ~= nil then
            return nil, string.format(
                "space[%q].indexes[%q].parts[%d]: redundant argument %q",
                space.name, index.name, i, k
            )
        end
    end

    return true
end

local function check_index_parts(index, space)
    if not utils.is_array(index.parts) then
        return nil, string.format(
            "space[%q].indexes[%q]: bad argument 'parts'" ..
            " (contiguous array of tables expected, got %s)",
            space.name, index.name, type(index.parts)
        )
    end

    do -- check index.part is valid and it doesn't used twice in index
        local used_fields = {}
        for i, part in ipairs(index.parts) do
            local res, err = check_index_part(i, index, space)
            if not res then
                return nil, err
            end
            if used_fields[part.path] ~= nil then
                return nil, string.format(
                    "space[%q].indexes[%q].part[%d]: " ..
                    "field %q was indexed already by part[%d]",
                    space.name, index.name, i, index.parts[i].path,
                    used_fields[part.path].id
                )
            end
            used_fields[part.path] = {id = i}
        end
    end

    return true
end

local function check_index(i, index, space)
    if type(index) ~= 'table' then
        return nil, string.format(
            "space[%q]: bad argument indexes[%d] " ..
            "(table expected, got %s)",
            space.name, i, type(index)
        )
    end

    do -- check index.name
        if type(index.name) ~= 'string' then
            return nil, string.format(
                "space[%q].indexes[%d]: bad argument 'name'" ..
                " (string expected, got %s)",
                space.name, i, type(index.name)
            )
        end
    end

    do -- check index.unique
        if type(index.unique) ~= 'boolean' then
            return nil, string.format(
                "space[%q].indexes[%q]: bad argument 'unique'" ..
                " (boolean expected, got %s)",
                space.name, index.name, type(index.unique)
            )
        end
    end

    do -- check index.type
        if type(index.type) ~= 'string' then
            return nil, string.format(
                "space[%q].indexes[%q]: bad argument 'type'" ..
                " (string expected, got %s)",
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
                "space[%q].indexes[%q]: %s engine does not support index.type %s",
                space.name, index.name, space.engine, index.type
            )
        end
    end

    if index.type == 'TREE' then
        if i == 1 and index.unique ~= true then
            return nil, string.format(
                "space[%q].indexes[%q]: primary TREE index must be unique",
                space.name, index.name
            )
        end

    elseif index.type == 'HASH' then
        if index.unique ~= true then
            return nil, string.format(
                "space[%q].indexes[%q]: HASH index must be unique",
                space.name, index.name
            )
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

        if type(index.dimension) ~= 'number' then
            return nil, string.format(
                "space[%q].indexes[%q].dimension: bad argument 'dimension'" ..
                " (number expected in range [1, 20], got %s)",
                space.name, index.name, type(index.dimension)
            )
        end

        if index.dimension < 1 or index.dimension > 20 then
            return nil, string.format(
                "space[%q].indexes[%q].dimension: bad argument 'dimension'" ..
                " it must belong to range [1, 20], got %d",
                space.name, index.name, index.dimension
            )
        end

        if type(index.distance) ~= 'string' then
            return nil, string.format(
                "space[%q].indexes[%q].distance: bad argument 'distance'" ..
                " (string expected, got %s)",
                space.name, index.name, type(index.distance)
            )
        end

        local known_distances = {
            manhattan = true,
            euclid = true
        }

        if not known_distances[index.distance] then
            return nil, string.format(
                "space[%q].indexes[%q].distance: distance %q is unknown",
                space.name, index.name, index.distance
            )
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

    end

    do
        local ok, err = check_index_parts(index, space)
        if not ok then
            return nil, err
        end
    end

    do -- check bitset/rtree indexes is not multipart and doesn't contains m=nullable
        if (index.type == 'RTREE' or index.type == 'BITSET') then
            if #index.parts > 1 then
                return nil, string.format(
                    "space[%q].indexes[%q].parts: " ..
                    "%q index type doesn't support multipart keys, " ..
                    "actually it contains %d parts",
                    space.name, index.name, index.type, #index.parts
                )
            end
        end
    end

    local find_nullable_part = function(parts)
        for i, part in ipairs(parts) do
            if part.is_nullable == true then
                return i
            end
        end
        return nil
    end


    do -- check inedxes hash/bitset/rtree doesnt contains nullable parts
        if index.type ~= 'TREE' then
            local idx = find_nullable_part(index.parts)
            if idx ~= nil then
                return nil, string.format(
                    "space[%q].indexes[%q].part[%d]: " ..
                    "%q index type doesn't support nullable field",
                    space.name, index.name, idx, index.type
                )
            end
        end
    end

    do -- check primary index doesn't contains multikey and nullable parts
        if i == 1 then
            for i, part in ipairs(index.parts) do
                if is_path_multikey(part.path) then
                    return nil, string.format(
                        "space[%q].indexes[%q].part[%d].path: primary indexes" ..
                        " doesn't allows multikey, actually path %q is multikey",
                        space.name, index.name, i, part.path
                    )
                end
            end

            local idx = find_nullable_part(index.parts)
            if idx ~= nil then
                return nil, string.format(
                    "space[%q].indexes[%q].part[%d].path: primary indexes" ..
                    " can't contains nullable parts",
                    space.name, index.name, idx
                )
            end
        end
    end

    local keys = {'type', 'name', 'unique', 'parts', 'field'}
    if index.type == 'RTREE' then
        keys = {'type', 'name', 'unique', 'parts', 'field', 'dimension', 'distance'}
    end

    local k = utils.redundant_key(index, keys)
    if k ~= nil then
        return nil, string.format(
            "space[%q].indexes[%q]: redutant argument %q",
            space.name, index.name, k
        )
    end

    return true
end


local function check_space(space_name, space)
    if type(space_name) ~= 'string' then
        return nil, string.format(
            'space[%q]: invaliad space_name type (expected string, got %s)',
            space_name, type(space_name)
        )
    end

    if type(space) ~= 'table' then
        return nil, string.format(
            'space[%s]: invaliad space type (expected table, got %s)',
            space_name, type(space)
        )
    end

    do -- check space.engine
        if type(space.engine) ~= 'string' then
            return nil, string.format(
                "space[%q]: bad argument 'engine'" ..
                " (string expected, got %s)",
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
                "space[%q]: bad argument 'is_local'" ..
                " (boolean expected, got %s)",
                space_name, type(space.is_local)
            )
        end
    end

    do -- check space.temporary
        if type(space.temporary) ~= 'boolean' then
            return nil, string.format(
                "space[%q]: bad argument 'temporary'" ..
                " (string expected, got %s)",
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
                "space[%q]: bad argument 'format'" ..
                " (contiguous array expected, got %s)",
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

            if space_fields[field_params.name] ~= nil then
                return nil, string.format(
                    "space[%q].format[%d]:  this name %q already" ..
                    " used in this space.format",
                    space_name, i, field_params.name
                )
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
                "space[%q]: bad argument 'indexes'" ..
                " (contiguous array expected, got %s)",
                space_name, type(space.indexes)
            )
        end
        local used_names = {}
        for i, index in ipairs(space.indexes) do
            local ok, err = check_index(i, index, {
                name = space_name,
                engine = space.engine,
                fields = space_fields,
            })

            if not ok then
                return nil, err
            end

            if used_names[index.name] then
                return nil, string.format(
                    "space[%q].index[%d].name: this name %q already used in this space.indexes",
                    space_name, i, index.name, space_name, used_names[index.name].id
                )
            end

            used_names[index.name] = {id = i}
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


return {
    check_space = check_space,
    check_index_part_collation = check_index_part_collation,
    check_index_part_type = check_index_part_type,
    check_index_part_path = check_index_part_path,
    check_index_part = check_index_part,
    check_index_parts = check_index_parts,
    check_index = check_index,
    check_field = check_field,
}
