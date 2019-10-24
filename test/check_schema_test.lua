#!/usr/bin/env tarantool

local t = require('luatest')
local db = require('test.db')
local ddl = require('ddl.check')
local log = require('log')

local g = t.group('check')
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


-- function g.test_invalid_format()
--     local schema = {spaces = table.deepcopy(test_schema)}
--     schema.spaces.test.indexes = {
--         {
--             type = 'HASH',
--             name = 't',
--             unique = true,
--             parts = {{
--                 path = 'unsigned_nonnull', type = 'unsigned', is_nullable = false,
--             }}
--         },
--         {
--             name = 'r',
--             type = 'RTREE',
--             field = 'map_nonnull.data[*].name',
--             unique = false,
--         }
--     }

--     local log = require('log')
--     local res, err = ddl.check_schema(schema)
--     log.info(res)
--     log.info(err)
-- end

function g.test_invalid_index_reference()

end

function g.test_invalid_index()

end


function g.test_part_collation()
    log.info(ddl)
    local res, err = ddl.check_part_collation('binary')
    t.assert(res)
    t.assert_not(err)

    local res, err = ddl.check_part_collation(nil)
    t.assert(res)
    t.assert_not(err)

    local res, err = ddl.check_part_collation('undefined')
    t.assert_not(res)
    t.assert_str_icontains(err, 'unknown collation "undefined"')
end

function g.test_part_type()
    local res, err = ddl.check_part_type('string', 'HASH')
    t.assert(res)
    t.assert_not(err)

    local res, err = ddl.check_part_type('unsigned', 'TREE')
    t.assert(res)
    t.assert_not(err)

    local res, err = ddl.check_part_type('string', 'BITSET')
    t.assert(res)
    t.assert_not(err)

    local res, err = ddl.check_part_type('unsigned', 'BITSET')
    t.assert(res)
    t.assert_not(err)

    local res, err = ddl.check_part_type('array', 'RTREE')
    t.assert(res)
    t.assert_not(err)

    local res, err = ddl.check_part_type('undefined', 'TREE')
    t.assert_not(res)
    t.assert_str_icontains(err, 'unknown type undefined')

    local res, err = ddl.check_part_type('unsigned', 'RTREE')
    t.assert_not(res)
    t.assert_str_icontains(err, 'unsigned field type is unsupported in RTREE index type')

    local res, err = ddl.check_part_type('array', 'TREE')
    t.assert_not(res)
    t.assert_str_icontains(err,  'array field type is unsupported in TREE index type')

    local res, err = ddl.check_part_type('array', 'HASH')
    t.assert_not(res)
    t.assert_str_icontains(err,  'array field type is unsupported in HASH index type')

    local res, err = ddl.check_part_type('integer', 'BITSET')
    t.assert_not(res)
    t.assert_str_icontains(err,  'integer field type is unsupported in BITSET index type')
end
