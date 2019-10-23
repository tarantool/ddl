#!/usr/bin/env tarantool

local t = require('luatest')
local db = require('test.db')
local ddl = require('ddl')

local g = t.group('check_schema')
g.before_all = db.init
g.setup = db.drop_all

local test_schema = {
    ['test'] = {
        engine = 'memtx',
        is_local = true,
        temporary = false,
        format = {
            {name = 'unsigned_nonnull', type = 'unsigned', is_nullable = false},
            {name = 'unsigned_nullable', type = 'unsigned', is_nullable = true},
            {name = 'integer_nonnull', type = 'integer', is_nullable = false},
            {name = 'integer_nullable', type = 'integer', is_nullable = true},
            {name = 'number_nonnull', type = 'number', is_nullable = false},
            {name = 'number_nullable', type = 'number', is_nullable = true},
            {name = 'boolean_nonnull', type = 'boolean', is_nullable = false},
            {name = 'boolean_nullable', type = 'boolean', is_nullable = true},
            {name = 'string_nonnull', type = 'string', is_nullable = false},
            {name = 'string_nullable', type = 'string', is_nullable = true},
            {name = 'scalar_nonnull', type = 'scalar', is_nullable = false},
            {name = 'scalar_nullable', type = 'scalar', is_nullable = true},
            {name = 'array_nonnull', type = 'array', is_nullable = false},
            {name = 'array_nullable', type = 'array', is_nullable = true},
            {name = 'varbinary_nonnull', type = 'varbinary', is_nullable = false},
            {name = 'varbinary_nullable', type = 'varbinary', is_nullable = true},
            {name = 'map_nonnull', type = 'map', is_nullable = false},
            {name = 'map_nullable', type = 'map', is_nullable = true},
            {name = 'any_nonnull', type = 'any', is_nullable = false},
            {name = 'any_nullable', type = 'any', is_nullable = true},
        },
    }
}

-- local function __test_invalid_ddl(schema, expected_message)
--     -- local res, err = ddl.validate(schema)
--     -- t.assert_nil(res)
--     -- t.assert
-- end


function g.test_invalid_format()
    local schema = {spaces = table.deepcopy(test_schema)}
    schema.spaces.test.indexes = {
        {
            type = 'HASH',
            name = 't',
            unique = true,
            parts = {{
                path = 'unsigned_nonnull', type = 'unsigned', is_nullable = false,
            }}
        },
        {
            name = 'r',
            type = 'RTREE',
            field = 'map_nonnull.data[*].name',
            unique = false,
        }
    }

    local log = require('log')
    local res, err = ddl.check_schema(schema)
    log.info(res)
    log.info(err)
end

function g.test_invalid_index_reference()

end

function g.test_invalid_index()

end
