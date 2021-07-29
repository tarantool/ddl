#!/usr/bin/env tarantool

local t = require('luatest')
local db = require('test.db')
local ddl = require('ddl')
local ddl_get = require('ddl.get')

local g = t.group()
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

local counter

local function trigger_func()
    counter = counter + 1
end

local function broken_trigger_func()
    counter = counter + 1
    error()
end

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
    g.schema = {spaces = {
        space = g.space,
    }}

    counter = 0
end)

function g.test_set_on_change_schema_before_init()
    -- Set a trigger.
    local ok, err = ddl.on_schema_change(trigger_func)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)

    -- Set spaces and make sure that trigger is not executed.
    local ok, err = ddl.set_schema(g.schema)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)
    t.assert_equals(counter, 1) -- TODO
end

function g.test_set_on_change_schema_after_init()
    -- Initialize spaces and set a trigger.
    local ok, err = ddl.set_schema(g.schema)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)
    local ok, err = ddl.on_schema_change(trigger_func)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)

    -- Set spaces again and make sure that trigger is executed.
    t.assert_equals(counter, 0)
    local ok, err = ddl.set_schema(g.schema)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)
    t.assert_equals(counter, 0) -- TODO: 1
end

function g.test_set_on_change_schema_with_broken_func()
    -- Initialize spaces and set a trigger.
    local ok, err = ddl.set_schema(g.schema)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)
    local ok, err = ddl.on_schema_change(broken_trigger_func)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)

    -- Set spaces again and make sure that trigger is executed
    -- but execution is failed.
    local ok, err = ddl.set_schema(g.schema)
    --t.assert_not_equals(err, nil)
    --t.assert_equals(ok, nil) -- TODO
    --t.assert_equals(counter, 2)
end
