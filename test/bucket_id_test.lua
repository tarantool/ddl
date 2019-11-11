#!/usr/bin/env tarantool

local t = require('luatest')
local ddl = require('ddl')

local g = t.group('bucket_id')
local test_space = {
    engine = 'memtx',
    is_local = true,
    temporary = false,
    format = {
        {name = 'unsigned_nonnull', type = 'unsigned', is_nullable = false},
        {name = 'integer_nonnull', type = 'integer', is_nullable = false},
        {name = 'map_nonnull', type = 'map', is_nullable = false},
        {name = 'string_nonnull', type = 'string', is_nullable = false}
    },
}

g.setup = function()
    g.space = table.deepcopy(test_space)
    table.insert(g.space.format, 1, {
        name = 'bucket_id', type = 'unsigned', is_nullable = false
    })

    g.space.sharding_key = {'unsigned_nonnull', 'integer_nonnull', 'string_nonnull'}
    g.schema = {spaces = {
        space = g.space,
    }}
end


function g.test_invalid_input()
    local ok, err = ddl.bucket_id()
    t.assert_equals(ok, nil)
    t.assert_str_icontains(err,
        'Bad argument #1 to ddl.bucket_id (table expected, got nil)'
    )

    local ok, err = ddl.bucket_id({})
    t.assert_equals(ok, nil)
    t.assert_str_icontains(err,
        'Bad argument #1 to ddl.bucket_id schema.spaces (table expected, got nil)'
    )

    local ok, err = ddl.bucket_id({spaces = {}})
    t.assert_equals(ok, nil)
    t.assert_equals(err,
        'Bad argument #2 (expected table, got nil)'
    )

    local ok, err = ddl.bucket_id({spaces = {}}, {key = 'val'})
    t.assert_equals(ok, nil)
    t.assert_equals(err,
        'Bad argument #3 (expected string, got nil)'
    )

    local ok, err = ddl.bucket_id({spaces = {}}, {key = 'val'}, 'space')
    t.assert_equals(ok, nil)
    t.assert_equals(err,
        'Bad argument #4 (expected number, got nil)'
    )

    local ok, err = ddl.bucket_id({spaces = {}}, {key = 'val'}, 'space', 0)
    t.assert_equals(ok, nil)
    t.assert_equals(err,
        'Invalid bucket_count, it must be greater than 0'
    )

    local ok, err = ddl.bucket_id({spaces = {}}, {key = 'val'}, 'space', 4)
    t.assert_equals(ok, nil)
    t.assert_equals(err,
        'No such space with name space'
    )


    local invalid_schema = table.deepcopy(g.schema)
    invalid_schema.spaces.space.sharding_key = {'map_nonnull'}

    local record = {
        integer_nonnull = 5,
        unsigned_nonnull = 6,
        map_nonnull = {here = true}
    }

    local ok, err = ddl.bucket_id(invalid_schema, record, 'space', 4)
    t.assert_equals(ok, nil)
    t.assert_equals(err,
        'Not supported key type (expected scalar, got table)'
    )
end

function g.test_ok()
    local bucket_count = 8
    local record = {
        integer_nonnull = 5,
        unsigned_nonnull = 6,
        map_nonnull = {here = true},
        string_nonnull = 'string'
    }

    local crc32 = require('digest').crc32.new()
    crc32:update(tostring(5))
    crc32:update(tostring(6))
    crc32:update('string')
    local crc_res = crc32:result() % bucket_count

    local old_res, err = ddl.bucket_id(g.schema, record, 'space', bucket_count)
    t.assert_equals(err, nil)
    t.assert_equals(old_res, crc_res)

    local new_res, err = ddl.bucket_id(g.schema, record, 'space', bucket_count)
    t.assert_equals(err, nil)
    t.assert_equals(old_res, new_res)

    record.string_nonnull = 'stringstringstring'
    local new_res, err = ddl.bucket_id(g.schema, record, 'space', bucket_count)

    t.assert_equals(err, nil)
    t.assert_not_equals(old_res, new_res)
end