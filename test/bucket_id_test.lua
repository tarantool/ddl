#!/usr/bin/env tarantool

local ffi = require('ffi')
local t = require('luatest')
local db = require('test.db')
local ddl = require('ddl')
local helper = require('test.helper')

local REFERENCE_KEY = {1, 2, 3}
local REFERENCE_BUCKET_ID = 4018458701

local test_space = {
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
        {name = 'map_nonnull', type = 'map', is_nullable = false},
        {name = 'map_nullable', type = 'map', is_nullable = true},
        {name = 'any_nonnull', type = 'any', is_nullable = false},
        {name = 'any_nullable', type = 'any', is_nullable = true},
    },
}

local primary_index = {
    type = 'HASH',
    unique = true,
    parts = {
        {path = 'string_nonnull', is_nullable = false, type = 'string'},
        {path = 'unsigned_nonnull', is_nullable = false, type = 'unsigned'},
    },
    name = 'primary'
}

local bucket_id_idx = {
    type = 'TREE',
    unique = false,
    parts = {{path = 'bucket_id', type = 'unsigned', is_nullable = false}},
    name = 'bucket_id'
}

local g = t.group()
g.before_all(db.init)
g.before_each(function()
    db.drop_all()

    g.space = table.deepcopy(test_space)
    table.insert(g.space.format, 1, {
        name = 'bucket_id', type = 'unsigned', is_nullable = false
    })

    g.space.indexes = {
        table.deepcopy(primary_index),
        table.deepcopy(bucket_id_idx)
    }
    g.space.sharding_key = {'unsigned_nonnull', 'integer_nonnull'}
    g.schema = {
        spaces = {
            space = g.space,
        }
    }
end)

function g.test_bucket_id_invalid_space_name()
    local id, err = ddl.bucket_id(1, {})
    t.assert_str_contains(err,
        'Invalid space name (string expected, got number)')
    t.assert_equals(id, nil)
end

function g.test_bucket_id_no_such_space()
    local id, err = ddl.bucket_id('non-existent-space', {})
    t.assert_str_contains(err,
        'No sharding function specified in DDL schema of space (non-existent-space)')
    t.assert_equals(id, nil)
end

function g.test_bucket_id_sharding_key_is_nil()
    local id, err = ddl.bucket_id('space', nil)
    t.assert_str_contains(err, 'Sharding key specified for space (space) is nil')
    t.assert_equals(id, nil)
end

function g.test_bucket_id_sharding_func_empty_body()
    local ok, err = ddl.set_schema(g.schema)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)
    local sharding_func = ''
    box.space._ddl_sharding_func:insert({
        'space',
        box.NULL,
        sharding_func
    })

    local id, err = ddl.bucket_id('space', REFERENCE_KEY)
    t.assert_equals(err,
        'Failed to execute sharding function for space name (space): attempt to call a nil value')
    t.assert_equals(id, nil)
end

function g.test_bucket_id_sharding_func_body()
    g.schema.spaces.space.sharding_func = {
        body = helper.sharding_func_body
    }
    local ok, err = ddl.set_schema(g.schema)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)

    local id, err = ddl.bucket_id('space', REFERENCE_KEY)
    t.assert_equals(err, nil)
    t.assert_equals(type(id), 'number')
    t.assert_equals(id, REFERENCE_BUCKET_ID)
end

function g.test_bucket_id_sharding_func_user()
    local sharding_func_name = 'sharding_func'
    rawset(_G, sharding_func_name, helper.sharding_func)
    g.schema.spaces.space.sharding_func = sharding_func_name
    local ok, err = ddl.set_schema(g.schema)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)

    local id, err = ddl.bucket_id('space', REFERENCE_KEY)
    t.assert_equals(err, nil)
    t.assert_equals(type(id), 'number')
    t.assert_equals(id, REFERENCE_BUCKET_ID)
end

function g.test_bucket_id_sharding_func_table_call()
    local sharding_func_name = 'sharding_func_table_call'
    local tbl = {}
    setmetatable(tbl, {
        __call = helper.sharding_func
    })
    rawset(_G, sharding_func_name, tbl)
    g.schema.spaces.space.sharding_func = sharding_func_name

    local ok, err = ddl.set_schema(g.schema)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)

    local id, err = ddl.bucket_id('space', REFERENCE_KEY)
    t.assert_equals(err, nil)
    t.assert_equals(type(id), 'number')
    t.assert_equals(id, REFERENCE_BUCKET_ID)
end

function g.test_bucket_id_sharding_func_userdata_call()
    local sharding_func_name = 'sharding_func_userdata_call'

    -- Create a userdata object.
    local userdata_obj = newproxy(true)
    local mt = getmetatable(userdata_obj)
    mt.__call = helper.sharding_func
    rawset(_G, sharding_func_name, userdata_obj)
    t.assert_equals(type(userdata_obj), 'userdata')

    -- Set sharding function as a __call method of userdata object.
    rawset(_G, sharding_func_name, userdata_obj)
    g.schema.spaces.space.sharding_func = sharding_func_name

    -- Set DDL schema.
    local ok, err = ddl.set_schema(g.schema)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)

    -- Make sure __call method of userdata object can be used by bucket_id().
    local id, err = ddl.bucket_id('space', REFERENCE_KEY)
    t.assert_equals(err, nil)
    t.assert_equals(type(id), 'number')
    t.assert_equals(id, REFERENCE_BUCKET_ID)
end

function g.test_bucket_id_sharding_func_cdata_call()
    local sharding_func_name = 'sharding_func_cdata_call'

    -- Create a cdata object.
    ffi.cdef[[
       typedef struct {
	    double x;
	    double y;
       } point_t;
    ]]
    local point = ffi.metatype('point_t', {
        __call = helper.sharding_func
    })
    t.assert_equals(type(point), 'cdata')

    -- Set sharding function as a __call method of cdata object.
    rawset(_G, sharding_func_name, point())
    g.schema.spaces.space.sharding_func = sharding_func_name

    -- Set DDL schema.
    local ok, err = ddl.set_schema(g.schema)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)

    -- Make sure __call method of cdata object can be used by bucket_id().
    local id, err = ddl.bucket_id('space', REFERENCE_KEY)
    t.assert_equals(err, nil)
    t.assert_equals(type(id), 'number')
    t.assert_equals(id, REFERENCE_BUCKET_ID)
end

function g.test_bucket_id_without_sharding_func()
    local ok, err = ddl.set_schema(g.schema)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)

    local id, err = ddl.bucket_id('space', REFERENCE_KEY)
    t.assert_str_contains(err,
        'No sharding function specified in DDL schema of space')
    t.assert_equals(id, nil)
end
