#!/usr/bin/env tarantool

local t = require('luatest')
local ddl = require('ddl')

local g = t.group('bucket_id')

local function test_schema(sharding_key)
    local test_space = {
        engine = 'memtx',
        is_local = true,
        temporary = false,
        format = {
            {name = 'string_nonnull', type = 'string', is_nullable = false},
            {name = 'integer_nonnull', type = 'integer', is_nullable = false},
            {name = 'unsigned_nonnull', type = 'unsigned', is_nullable = false},
            {name = 'anything', type = 'any', is_nullable = true},
        },
        indexes = {{
            name = 'pk',
            type = 'TREE',
            unique = true,
            parts = {
                {path = 'string_nonnull', type = 'string', is_nullable = false},
            },
        }},
    }
    if sharding_key ~= nil then
        table.insert(test_space.format,
            {name = 'bucket_id', type = 'unsigned', is_nullable = false}
        )
        table.insert(test_space.indexes,
            {
                name = 'bucket_id',
                type = 'HASH',
                unique = false,
                parts = {
                    {path = 'bucket_id', type = 'unsigned', is_nullable = false},
                }
            }
        )
        test_space.sharding_key = sharding_key
    end
    return {spaces = {test = test_space}}
end


function g.test_invalid_input()
    t.assert_error_msg_contains(
        'Bad argument #1 to ddl.bucket_id (schema expected, got nil)',
        ddl.bucket_id
    )

    t.assert_error_msg_contains(
        'Bad argument #1 to ddl.bucket_id invalid schema.spaces (table expected, got nil)',
        ddl.bucket_id, {}
    )

    t.assert_error_msg_contains(
        'Bad argument #2 to ddl.bucket_id (table expected, got nil)',
        ddl.bucket_id, {spaces = {}}
    )

    t.assert_error_msg_contains(
        'Bad argument #3 to ddl.bucket_id (string expected, got nil)',
        ddl.bucket_id, {spaces = {}}, {key = 'val'}
    )

    t.assert_error_msg_contains(
        'Bad argument #4 to ddl.bucket_id (number expected, got nil)',
        ddl.bucket_id, {spaces = {}}, {key = 'val'}, 'space'
    )

    t.assert_error_msg_contains(
        'Bad argument #4 to ddl.bucket_id (positive expected, got 0)',
        ddl.bucket_id, {spaces = {}}, {key = 'val'}, 'space', 0
    )

    local bucket_id, err = ddl.bucket_id(
        test_schema(), {key = 'val'}, 'X', 4
    )
    t.assert_equals(bucket_id, nil)
    t.assert_equals(err,
        [[Space "X" isn't defined in schema]]
    )

    local bucket_id, err = ddl.bucket_id(
        test_schema(), {key = 'val'}, 'test', 4
    )
    t.assert_equals(bucket_id, nil)
    t.assert_equals(err,
        [[Space "test" isn't sharded in schema]]
    )

    local bucket_id, err = ddl.bucket_id(
        test_schema({'anything'}), {anything = {0}}, 'test', 4
    )
    t.assert_equals(bucket_id, nil)
    t.assert_equals(err,
        'Unsupported value for sharding key "anything" (scalar expected, got table)'
    )

    local bucket_id, err = ddl.bucket_id(
        test_schema({'anything'}), {'a', 1, 2, {0}}, 'test', 4
    )
    t.assert_equals(bucket_id, nil)
    t.assert_equals(err,
        'Unsupported value for sharding key "anything" (scalar expected, got table)'
    )
end

local function crc32(s)
    local ret = require('digest').crc32.new()
    ret:update(s)
    return ret:result()
end

function g.test_ok()
    local function check(shk, record, expected)
        local uint32_max = tonumber(0xFFFFFFFFULL)
        local bucket_id, err = ddl.bucket_id(
            test_schema(shk), record, 'test', uint32_max
        )
        t.assert_equals(err, nil)
        t.assert_equals(bucket_id, crc32(expected) + 1)
    end

    local record = {
        string_nonnull = '1',
        integer_nonnull = 2,
        unsigned_nonnull = 3,
        anything = box.NULL,
    }

    local record = {'1', 2, 3, box.NULL}

    check({'string_nonnull'}, record, '1')
    check({'string_nonnull', 'integer_nonnull'}, record, '12')
    check({'string_nonnull', 'integer_nonnull', 'unsigned_nonnull'}, record, '123')
    check({'string_nonnull', 'unsigned_nonnull'}, record, '13')
    check({'unsigned_nonnull', 'string_nonnull'}, record, '31')
    check({'anything'}, {}, 'nil')
    check({'anything'}, {anything = 'nil'}, 'nil')
    check({'anything'}, {anything = box.NULL}, 'nil')
end