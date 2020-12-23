#!/usr/bin/env tarantool

local t = require('luatest')
local db = require('test.db')
local ddl = require('ddl')

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

local sharding_key_format = {
    {name = 'space_name', type = 'string', is_nullable = false},
    {name = 'sharding_key', type = 'array', is_nullable = false}
}

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
end)

local function normalize_rows(rows)
    local normalized = {}
    for _, row in ipairs(rows) do
        table.insert(normalized, row:totable())
    end
    return normalized
end

function g.test_no_sharding_spaces()
    local space = table.deepcopy(test_space)
    space.indexes = {primary_index}
    local schema = {
        spaces = {
            space = space
        }
    }

    local ok, err = ddl.set_schema(schema)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)

    local _ddl_sharding_key = box.space['_ddl_sharding_key']
    t.assert_not_equals(_ddl_sharding_key, nil)
    t.assert_equals(_ddl_sharding_key:select(), {})

    local ddl_schema = ddl.get_schema()
    t.assert_equals(schema, ddl_schema)
end

function g.test_one_sharding_space_ok()
    local ok, err = ddl.set_schema(g.schema)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)

    local _ddl_sharding_key = box.space['_ddl_sharding_key']
    t.assert_not_equals(_ddl_sharding_key, nil)
    t.assert_equals(_ddl_sharding_key:format(), sharding_key_format)

    t.assert_equals(
        normalize_rows(_ddl_sharding_key:select()),
        {
            {'space', g.space.sharding_key}
        }
    )

    local ddl_schema = ddl.get_schema()
    t.assert_equals(ddl_schema, ddl_schema)
end


function g.test_invalid_format()
    table.remove(g.space.indexes, 2)
    local ok, err = ddl.set_schema(g.schema)

    t.assert_equals(ok, nil)
    t.assert_equals(err,
        [[spaces["space"].indexes: sharding_key exists in the space, but there's ]] ..
        [[no bucket_id defined in 'indexes' section]]
    )

    t.assert_equals(box.space['_ddl_sharding_key'], nil)

    local ddl_schema = ddl.get_schema()
    t.assert_equals(ddl_schema, {spaces = {}})
end

function g.test_two_sharding_spaces()
    local space_without_key = table.deepcopy(test_space)
    space_without_key.indexes = {primary_index}

    local space_one = g.space
    local space_two = table.deepcopy(g.space)
    space_two.sharding_key = {
        'unsigned_nonnull', 'integer_nonnull', 'string_nonnull'
    }

    local schema = {
        spaces = {
            space_without_key = space_without_key,
            space_one = space_one,
            space_two = space_two
        }
    }

    local ok, err = ddl.set_schema(schema)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)

    local _ddl_sharding_key = box.space['_ddl_sharding_key']
    t.assert_not_equals(_ddl_sharding_key, nil)

    t.assert_items_equals(
        normalize_rows(_ddl_sharding_key:select()),
        {
            {'space_one', g.space.sharding_key},
            {'space_two', space_two.sharding_key}
        }
    )

    local ddl_schema = ddl.get_schema()
    t.assert_equals(ddl_schema, schema)
end

function g.test_apply_sequently()
    local ok, err = ddl.set_schema(g.schema)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)

    local ddl_schema = ddl.get_schema()
    t.assert_equals(ddl_schema, g.schema)

    local _ddl_sharding_key = box.space['_ddl_sharding_key']
    t.assert_not_equals(_ddl_sharding_key, nil)

    local new_schema = table.deepcopy(g.schema)
    new_schema.spaces.new_space = g.space

    local ok, err = ddl.set_schema(new_schema)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)

    local _ddl_sharding_key = box.space['_ddl_sharding_key']
    t.assert_not_equals(_ddl_sharding_key, nil)

    t.assert_items_equals(
        normalize_rows(_ddl_sharding_key:select()),
        {
            {'space', g.space.sharding_key},
            {'new_space', g.space.sharding_key}
        }
    )

    local ddl_schema = ddl.get_schema()
    t.assert_equals(ddl_schema, new_schema)
end
