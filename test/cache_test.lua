#!/usr/bin/env tarantool

local t = require('luatest')
local db = require('test.db')
local ddl = require('ddl')
local cache = require('ddl.cache')
local helper = require('test.helper')

local SPACE_NAME_IDX      = 1
local SHARD_FUNC_NAME_IDX = 2
local SHARD_FUNC_BODY_IDX = 3

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

local func_body_first  = 'function(key) return key + 1 end'
local func_body_second = 'function(key) return key + 2 end'

local function space_init(g)
    db.drop_all()

    g.space = {
        engine = 'memtx',
        is_local = true,
        temporary = false,
        format = table.deepcopy(helper.test_space_format())
    }
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
end

local g = t.group()
g.before_all(db.init)
g.before_each(space_init)

function g.test_cache_processed_func_body()
    g.schema.spaces.space.sharding_func = {
        body = func_body_first
    }
    local ok, err = ddl.set_schema(g.schema)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)

    local res = cache.internal.get('space')
    t.assert(res)
    t.assert(res.callable)
    t.assert_equals(res.error, nil)
    t.assert_type(res.callable, 'function')
    t.assert_equals(res.callable(42), 43)
    local id, err = ddl.bucket_id('space', 42)
    t.assert_equals(err, nil)
    t.assert_type(id, 'number')
    t.assert_equals(id, 43)
end

function g.test_cache_processed_func_name()
    local sharding_func_name = 'sharding_func'
    rawset(_G, sharding_func_name, function(key) return key + 1 end)
    g.schema.spaces.space.sharding_func = sharding_func_name

    local ok, err = ddl.set_schema(g.schema)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)

    local res = cache.internal.get('space')
    t.assert(res)
    t.assert(res.parsed_func_name)
    t.assert_type(res.parsed_func_name, 'table')
    t.assert_equals(res.parsed_func_name[1], 'sharding_func')
    local id, err = ddl.bucket_id('space', 42)
    t.assert_equals(err, nil)
    t.assert_type(id, 'number')
    t.assert_equals(id, 43)

    rawset(_G, sharding_func_name, nil)
end

function g.test_cache_nil_space()
    local sharding_func_name = 'sharding_func'
    rawset(_G, sharding_func_name, function(key) return key + 1 end)
    g.schema.spaces.space.sharding_func = sharding_func_name

    local ok, err = ddl.set_schema(g.schema)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)

    local res = cache.internal.get()
    t.assert_equals(res, nil)

    rawset(_G, sharding_func_name, nil)
end

-- when caching function name, we must store in cache
-- only name, because body can be changed manually
function g.test_body_changed_manually()
    local sharding_func_name = 'sharding_func'
    rawset(_G, sharding_func_name, function(key) return key + 10 end)
    g.schema.spaces.space.sharding_func = sharding_func_name

    local ok, err = ddl.set_schema(g.schema)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)

    local id, err = ddl.bucket_id('space', 42)
    t.assert_equals(err, nil)
    t.assert_type(id, 'number')
    t.assert_equals(id, 52)

    rawset(_G, sharding_func_name, function(key) return key + 11 end)
    local id, err = ddl.bucket_id('space', 42)
    t.assert_equals(err, nil)
    t.assert_type(id, 'number')
    t.assert_equals(id, 53)

    rawset(_G, sharding_func_name, nil)
end

function g.test_cache_schema_changed()
    g.schema.spaces.space.sharding_func = {
        body = func_body_first
    }
    local ok, err = ddl.set_schema(g.schema)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)

    local res = cache.internal.get('space')
    t.assert(res)
    t.assert(res.raw_tuple)
    t.assert_equals(res.raw_tuple[SPACE_NAME_IDX], 'space')
    t.assert_equals(res.raw_tuple[SHARD_FUNC_NAME_IDX], nil)
    t.assert_equals(res.raw_tuple[SHARD_FUNC_BODY_IDX], func_body_first)

    space_init(g)

    local res = cache.internal.get('space')
    t.assert_equals(res, nil)
    local id, err = ddl.bucket_id('space', 42)
    t.assert_equals(err, 'No sharding function specified in DDL schema of space (space)')
    t.assert_equals(id, nil)
end

-- test for _ddl_sharding_func:on_replace() trigger
-- cache must be rebuilded on space changed/updated
function g.test_cache_space_updated_body()
    g.schema.spaces.space.sharding_func = {
        body = func_body_first
    }
    local ok, err = ddl.set_schema(g.schema)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)

    local res = cache.internal.get('space')
    t.assert(res)
    t.assert(res.callable)
    t.assert_type(res.callable, 'function')
    t.assert_equals(res.callable(42), 43)
    t.assert(res.raw_tuple)
    t.assert_equals(res.error, nil)
    t.assert_equals(res.raw_tuple[SPACE_NAME_IDX], 'space')
    t.assert_equals(res.raw_tuple[SHARD_FUNC_NAME_IDX], nil)
    t.assert_equals(res.raw_tuple[SHARD_FUNC_BODY_IDX], func_body_first)
    local id, err = ddl.bucket_id('space', 42)
    t.assert_equals(err, nil)
    t.assert_type(id, 'number')
    t.assert_equals(id, 43)

    box.space._ddl_sharding_func
       :update({'space'}, {{'=', SHARD_FUNC_BODY_IDX, func_body_second}})

    local res = cache.internal.get('space')
    t.assert(res)
    t.assert(res.callable)
    t.assert_type(res.callable, 'function')
    t.assert_equals(res.callable(42), 44)
    t.assert(res.raw_tuple)
    t.assert_equals(res.error, nil)
    t.assert_equals(res.raw_tuple[SPACE_NAME_IDX], 'space')
    t.assert_equals(res.raw_tuple[SHARD_FUNC_NAME_IDX], nil)
    t.assert_equals(res.raw_tuple[SHARD_FUNC_BODY_IDX], func_body_second)
    local id, err = ddl.bucket_id('space', 42)
    t.assert_equals(err, nil)
    t.assert_type(id, 'number')
    t.assert_equals(id, 44)
end

function g.test_cache_space_updated_name()
    local sharding_func_first = 'sharding_func_first'
    rawset(_G, sharding_func_first, function(key) return key + 1 end)
    rawset(_G, 'sharding_func_second', function(key) return key + 2 end)
    g.schema.spaces.space.sharding_func = sharding_func_first
    local ok, err = ddl.set_schema(g.schema)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)

    local res = cache.internal.get('space')
    t.assert(res)
    t.assert(res.parsed_func_name)
    t.assert_type(res.parsed_func_name, 'table')
    t.assert_equals(res.parsed_func_name[1], 'sharding_func_first')
    t.assert_equals(res.raw_tuple[SPACE_NAME_IDX], 'space')
    t.assert_equals(res.raw_tuple[SHARD_FUNC_NAME_IDX], 'sharding_func_first')
    local id, err = ddl.bucket_id('space', 42)
    t.assert_equals(err, nil)
    t.assert_type(id, 'number')
    t.assert_equals(id, 43)

    box.space._ddl_sharding_func
       :update({'space'}, {{'=', SHARD_FUNC_NAME_IDX, 'sharding_func_second'}})

    local res = cache.internal.get('space')
    t.assert(res)
    t.assert(res.parsed_func_name)
    t.assert_type(res.parsed_func_name, 'table')
    t.assert_equals(res.parsed_func_name[1], 'sharding_func_second')
    t.assert_equals(res.raw_tuple[SPACE_NAME_IDX], 'space')
    t.assert_equals(res.raw_tuple[SHARD_FUNC_NAME_IDX], 'sharding_func_second')
    local id, err = ddl.bucket_id('space', 42)
    t.assert_equals(err, nil)
    t.assert_type(id, 'number')
    t.assert_equals(id, 44)
end
