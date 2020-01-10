#!/usr/bin/env tarantool

local t = require('luatest')
local db = require('test.db')
local ddl = require('ddl')

local g = t.group('check_sharding_key')
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

g.before_all = db.init
g.setup = function()
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
end


function g.test_sharding_key_ok()
    local space = table.deepcopy(test_space)
    space.indexes = {primary_index}
    local schema = {
        spaces = {
            space = space
        }
    }

    local res, err = ddl.check_schema(schema)
    t.assert_equals(err, nil)
    t.assert_equals(res, true)


    local res, err = ddl.check_schema(g.schema)
    t.assert_equals(err, nil)
    t.assert_equals(res, true)
end


function  g.test_invalid_format_bucket_id()
    local old_sharding_key = g.space.sharding_key
    g.space.sharding_key = nil

    local ok, err = ddl.check_schema(g.schema)
    t.assert_str_icontains(err,
        [[spaces["space"].format["bucket_id"]: bucket_id used for ]] ..
        [[sharding, but no sharding_key was supplied]]
    )
    t.assert_equals(ok, nil)

    g.space.sharding_key = old_sharding_key
    g.space.format[1].type = 'integer'
    g.space.indexes[2].parts[1].type = 'integer'

    local ok, err = ddl.check_schema(g.schema)
    t.assert_str_icontains(err,
        [[spaces["space"].format["bucket_id"]: invalid field type]] ..
        [[ (unsigned expected, got integer)]]
    )
    t.assert_equals(ok, nil)

    table.remove(g.space.format, 1)
    table.remove(g.space.indexes, 2)
    local ok, err = ddl.check_schema(g.schema)
    t.assert_str_icontains(err,
        [[spaces["space"].format: sharding_key exists in space,]] ..
        [[ but there is no bucket_id in format]]
    )
    t.assert_equals(ok, nil)
end

function g.test_invalid_index_bucket_id()
    local old_sharding_key = g.space.sharding_key
    g.space.sharding_key = nil

    local ok, err = ddl.check_schema(g.schema)
    t.assert_str_icontains(err,
        [[spaces["space"].format["bucket_id"]: bucket_id used for]] ..
        [[ sharding, but no sharding_key was supplied]]
    )
    t.assert_equals(ok, nil)

    g.space.sharding_key = old_sharding_key
    g.space.indexes[2].unique = true
    local ok, err = ddl.check_schema(g.schema)
    t.assert_str_icontains(err,
        [[spaces["space"].indexes["bucket_id"]: bucket_id index can't be unique]]
    )
    t.assert_equals(ok, nil)

    g.space.indexes[2] = table.deepcopy(bucket_id_idx)
    table.insert(g.space.indexes[2].parts, {
        path = 'integer_nonnull', type = 'integer', is_nullable = false
    })
    local ok, err = ddl.check_schema(g.schema)
    t.assert_str_icontains(err,
        [[spaces["space"].indexes["bucket_id"]: incorrect parts size (1 expected, got 2)]]
    )
    t.assert_equals(ok, nil)

    table.remove(g.space.indexes[2].parts, 1)
    local ok, err = ddl.check_schema(g.schema)
    t.assert_str_icontains(err,
        [[spaces["space"].indexes["bucket_id"].parts[1].path: invalid field ]] ..
        [[reference (reference to bucket_id expected, got integer_nonnull)]]
    )
    t.assert_equals(ok, nil)

    table.remove(g.space.indexes, 2)
    local ok, err = ddl.check_schema(g.schema)
    t.assert_str_icontains(err,
        [[spaces["space"].indexes: sharding_key exists in space,]] ..
        [[ but there is no bucket_id in indexes]]
    )
    t.assert_equals(ok, nil)
end

function g.test_invalid_sharding_key()
    table.insert(g.space.sharding_key, g.space.sharding_key[2])
    local ok, err = ddl.check_schema(g.schema)
    t.assert_str_icontains(err,
        [[spaces["space"].sharding_key: sharding_key contains duplicate "integer_nonnull"]]
    )
    t.assert_equals(ok, nil)

    g.space.sharding_key = {key = 5}
    local ok, err = ddl.check_schema(g.schema)
    t.assert_str_icontains(err,
        [[spaces["space"]: bad argument sharding_key (contiguous array expected, got table]]
    )
    t.assert_equals(ok, nil)

end

function g.test_invalid_key_reference()
    g.space.sharding_key = {'undefined_field'}
    local ok, err = ddl.check_schema(g.schema)
    t.assert_str_icontains(err,
        [[spaces["space"].sharding_key["undefined_field"]: invalid reference]] ..
        [[ to format["undefined_field"], no such field]]
    )
    t.assert_equals(ok, nil)
end

function g.test_invalid_key_reference_type()
    g.space.sharding_key = {'map_nonnull'}
    local ok, err = ddl.check_schema(g.schema)
    t.assert_str_icontains(err,
        [[spaces["space"].sharding_key["map_nonnull"]: key refereces to field ]] ..
        [[with type map, but it's unsupported yet]]
    )
    t.assert_equals(ok, nil)

    g.space.sharding_key = {'array_nonnull'}
    local ok, err = ddl.check_schema(g.schema)
    t.assert_str_icontains(err,
        [[spaces["space"].sharding_key["array_nonnull"]: key refereces to field ]] ..
        [[with type array, but it's unsupported yet]]
    )
    t.assert_equals(ok, nil)
end

function g.test_invalid_key_reference_path()
    g.space.sharding_key = {'map_nonnull.data'}
    local ok, err = ddl.check_schema(g.schema)
    t.assert_str_icontains(err,
        [[spaces["space"].sharding_key["map_nonnull.data"]: key with json]] ..
        [[ part is unsupported yet]]
    )
    t.assert_equals(ok, nil)
end
