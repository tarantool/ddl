#!/usr/bin/env tarantool

local t = require('luatest')
local db = require('test.db')
local ddl = require('ddl')
local ffi = require('ffi')

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

function g.test_sharding_func_dot_notation()
    local some_module = {
        sharding_func = function(key) return key end
    }
    local user_sharding_func_name = 'some_module.sharding_func'
    rawset(_G, 'some_module', some_module)
    g.space.sharding_func = user_sharding_func_name
    local res, err = ddl.check_schema(g.schema)
    t.assert_equals(err, nil)
    t.assert_equals(res, true)
    -- remove test data
    rawset(_G, user_sharding_func_name, nil)
end

function g.test_user_sharding_func_with_body()
    g.space.sharding_func = {body = 'function(key) return <...> end'}

    local res, err = ddl.check_schema(g.schema)
    t.assert_equals(err, nil)
    t.assert_equals(res, true)
end

function g.test_user_sharding_func_function_call()
    local user_sharding_func_name = 'user_sharding_func'
    rawset(_G, user_sharding_func_name, function(key) return key end)
    g.space.sharding_func = user_sharding_func_name

    local res, err = ddl.check_schema(g.schema)
    t.assert_equals(err, nil)
    t.assert_equals(res, true)
    -- remove test data
    rawset(_G, user_sharding_func_name, nil)
end

function g.test_user_sharding_func_table_call()
    local user_sharding_func_name = 'user_sharding_func'
    local user_sharding_func_table = setmetatable({}, {
        __call = function(_, key) return key end
    })
    rawset(_G, user_sharding_func_name, user_sharding_func_table)
    g.space.sharding_func = user_sharding_func_name

    local res, err = ddl.check_schema(g.schema)
    t.assert_equals(err, nil)
    t.assert_equals(res, true)
    -- remove test data
    rawset(_G, user_sharding_func_name, nil)
end

function g.test_user_sharding_func_cdata_call()
    local user_sharding_func_name = 'user_sharding_func'
    ffi.cdef[[
        typedef struct
        {
            int data;
        } test_check_struct_t;
    ]]

    local test_check_struct = ffi.metatype('test_check_struct_t', {
        __call = function(_, key) return key end
    })
    rawset(_G, user_sharding_func_name, test_check_struct())
    g.space.sharding_func = user_sharding_func_name

    local res, err = ddl.check_schema(g.schema)
    t.assert_equals(err, nil)
    t.assert_equals(res, true)
    -- remove test data
    rawset(_G, user_sharding_func_name, nil)
end

function g.test_user_sharding_func_protected_metatable()
    -- protected metatable has non-table type -> cannot detect
    -- object is callable-> ok
    local user_sharding_func_name = 'user_sharding_func'
    local user_sharding_func_table = setmetatable({}, {
        __call = function(_, key) return key end, __metatable = 'protected'
    })
    rawset(_G, user_sharding_func_name, user_sharding_func_table)
    g.space.sharding_func = user_sharding_func_name

    local ok, err = ddl.check_schema(g.schema)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)
    -- remove test data
    rawset(_G, user_sharding_func_name, nil)

    -- protected metatable has table type with __call metamethod -> cannot detect
    -- object is callable -> ok
    local user_sharding_func_name = 'user_sharding_func'
    local user_sharding_func_table = setmetatable({}, {
        __metatable = {__call = function(_, key) return key end}
    })
    rawset(_G, user_sharding_func_name, user_sharding_func_table)
    g.space.sharding_func = user_sharding_func_name

    local ok, err = ddl.check_schema(g.schema)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)
    -- remove test data
    rawset(_G, user_sharding_func_name, nil)
end


function g.test_user_sharding_func_userdata_call()
    local user_sharding_func_name = 'user_sharding_func'

    local user_sharding_func_userdata = newproxy(true)
    local mt = getmetatable(user_sharding_func_userdata)
    mt.__call = function(_, key) return key end
    rawset(_G, user_sharding_func_name, user_sharding_func_userdata)
    g.space.sharding_func = user_sharding_func_name

    local res, err = ddl.check_schema(g.schema)
    t.assert_equals(err, nil)
    t.assert_equals(res, true)
    -- remove test data
    rawset(_G, user_sharding_func_name, nil)
end


function  g.test_invalid_format_bucket_id()
    local old_sharding_key = g.space.sharding_key
    g.space.sharding_key = nil

    local ok, err = ddl.check_schema(g.schema)
    t.assert_equals(err,
        [[spaces["space"].format["bucket_id"]: bucket_id is used for]] ..
        [[ sharding, but there's no spaces["space"].sharding_key defined]]
    )
    t.assert_equals(ok, nil)

    g.space.sharding_key = old_sharding_key
    g.space.format[1].type = 'integer'
    g.space.indexes[2].parts[1].type = 'integer'

    local ok, err = ddl.check_schema(g.schema)
    t.assert_equals(err,
        [[spaces["space"].format["bucket_id"].type: bad value]] ..
        [[ (unsigned expected, got integer)]]
    )
    t.assert_equals(ok, nil)

    table.remove(g.space.format, 1)
    table.remove(g.space.indexes, 2)
    local ok, err = ddl.check_schema(g.schema)
    t.assert_equals(err,
        [[spaces["space"].format: sharding_key exists in the space,]] ..
        [[ but there's no bucket_id defined in 'format' section]]
    )
    t.assert_equals(ok, nil)
end

function g.test_invalid_index_bucket_id()
    local old_sharding_key = g.space.sharding_key
    g.space.sharding_key = nil

    local ok, err = ddl.check_schema(g.schema)
    t.assert_equals(err,
        [[spaces["space"].format["bucket_id"]: bucket_id is used for]] ..
        [[ sharding, but there's no spaces["space"].sharding_key defined]]
    )
    t.assert_equals(ok, nil)

    -- remove `bucket_id` field
    local old_format_field = g.space.format[1]
    table.remove(g.space.format, 1)

    -- replace reference field to non-bucket_id field in the `bucket_id` index
    local old_path = g.space.indexes[2].parts[1]
    table.remove(g.space.indexes[2].parts, 1)
    table.insert(g.space.indexes[2].parts, 1, {
        path = 'unsigned_nonnull', type = 'unsigned', is_nullable = false
    })

    local ok, err = ddl.check_schema(g.schema)
    t.assert_equals(err,
        [[spaces["space"].indexes["bucket_id"]: bucket_id is used for]] ..
        [[ sharding, but there's no spaces["space"].sharding_key defined]]
    )
    t.assert_equals(ok, nil)

    -- restore `bucket_id` field in space format
    table.insert(g.space.format, 1, old_format_field)
    -- restore reference field in `bucket_id` index
    table.insert(g.space.indexes[2].parts, 1, old_path)

    g.space.sharding_key = old_sharding_key
    g.space.indexes[2].unique = true
    local ok, err = ddl.check_schema(g.schema)
    t.assert_equals(err,
        [[spaces["space"].indexes["bucket_id"].unique: bucket_id index can't be unique]]
    )
    t.assert_equals(ok, nil)

    g.space.indexes[2] = table.deepcopy(bucket_id_idx)
    table.insert(g.space.indexes[2].parts, {
        path = 'integer_nonnull', type = 'integer', is_nullable = false
    })
    local ok, err = ddl.check_schema(g.schema)
    t.assert_equals(err,
        [[spaces["space"].indexes["bucket_id"].parts:]] ..
        [[ bucket_id index can't be composite (1 part expected, got 2 parts)]]
    )
    t.assert_equals(ok, nil)

    table.remove(g.space.indexes[2].parts, 1)
    local ok, err = ddl.check_schema(g.schema)
    t.assert_equals(err,
        [[spaces["space"].indexes["bucket_id"].parts[1].path: invalid field ]] ..
        [[reference (reference to bucket_id expected, got integer_nonnull)]]
    )
    t.assert_equals(ok, nil)

    table.remove(g.space.indexes, 2)
    local ok, err = ddl.check_schema(g.schema)
    t.assert_equals(err,
        [[spaces["space"].indexes: sharding_key exists in the space,]] ..
        [[ but there's no bucket_id defined in 'indexes' section]]
    )
    t.assert_equals(ok, nil)
end

function g.test_invalid_sharding_key()
    table.insert(g.space.sharding_key, g.space.sharding_key[2])
    local ok, err = ddl.check_schema(g.schema)
    t.assert_equals(err,
        [[spaces["space"].sharding_key: sharding key contains duplicate "integer_nonnull"]]
    )
    t.assert_equals(ok, nil)

    g.space.sharding_key = {key = 5}
    local ok, err = ddl.check_schema(g.schema)
    t.assert_equals(err,
        [[spaces["space"].sharding_key: bad value (contiguous array expected, got table)]]
    )
    t.assert_equals(ok, nil)
end

function g.test_invalid_key_reference()
    g.space.sharding_key = {'undefined_field'}
    local ok, err = ddl.check_schema(g.schema)
    t.assert_equals(err,
        [[spaces["space"].sharding_key["undefined_field"]: invalid reference]] ..
        [[ to format["undefined_field"], no such field]]
    )
    t.assert_equals(ok, nil)
end

function g.test_invalid_key_reference_type()
    g.space.sharding_key = {'map_nonnull'}
    local ok, err = ddl.check_schema(g.schema)
    t.assert_equals(err,
        [[spaces["space"].sharding_key["map_nonnull"]: key references to field]] ..
        [[ with map type, but it's not supported yet]]
    )
    t.assert_equals(ok, nil)

    g.space.sharding_key = {'array_nonnull'}
    local ok, err = ddl.check_schema(g.schema)
    t.assert_equals(err,
        [[spaces["space"].sharding_key["array_nonnull"]: key references to field]] ..
        [[ with array type, but it's not supported yet]]
    )
    t.assert_equals(ok, nil)
end

function g.test_invalid_key_reference_path()
    g.space.sharding_key = {'map_nonnull.data'}
    local ok, err = ddl.check_schema(g.schema)
    t.assert_equals(err,
        [[spaces["space"].sharding_key["map_nonnull.data"]: key containing JSONPath isn't supported yet]]
    )
    t.assert_equals(ok, nil)
end

function g.test_sharding_func_not_exists()
    g.space.sharding_func = 'bad_sharding_func'

    local ok, err = ddl.check_schema(g.schema)
    t.assert_equals(err,
        [[spaces["space"].sharding_func: unknown sharding_func "bad_sharding_func"]]
    )
    t.assert_equals(ok, nil)
end

function g.test_sharding_func_invalid_type()
    g.space.sharding_func = 5

    local ok, err = ddl.check_schema(g.schema)
    t.assert_equals(err,
        [[spaces["space"].sharding_func: bad value (string or table expected, got number)]]
    )
    t.assert_equals(ok, nil)
end

function g.test_sharding_func_without_body()
    g.space.sharding_func = {}

    local ok, err = ddl.check_schema(g.schema)
    t.assert_equals(err,
        [[spaces["space"].sharding_func: sharding_func exists in the space as table,]] ..
        [[but there's no 'body' field in sharding_func section]]
    )
    t.assert_equals(ok, nil)
end

function g.test_sharding_func_invalid_body()
    g.space.sharding_func = {body = 4}

    local ok, err = ddl.check_schema(g.schema)
    t.assert_equals(err,
        [[spaces["space"].sharding_func.body: bad value ]] ..
        [[(string expected, got number)]]
    )
    t.assert_equals(ok, nil)
end

function g.test_user_sharding_func_table_without_metatable()
    local user_sharding_func_name = 'user_sharding_func'
    rawset(_G, user_sharding_func_name, {})
    g.space.sharding_func = user_sharding_func_name

    local ok, err = ddl.check_schema(g.schema)
    t.assert_equals(err,
        [[spaces["space"].sharding_func: unknown sharding_func "user_sharding_func"]]
    )
    t.assert_equals(ok, nil)
    -- remove test data
    rawset(_G, user_sharding_func_name, nil)
end

function g.test_user_sharding_func_userdata_without_metatable()
    local user_sharding_func_name = 'user_sharding_func'
    rawset(_G, user_sharding_func_name, newproxy(true))
    g.space.sharding_func = user_sharding_func_name

    local ok, err = ddl.check_schema(g.schema)
    t.assert_equals(err,
        [[spaces["space"].sharding_func: unknown sharding_func "user_sharding_func"]]
    )
    t.assert_equals(ok, nil)
    -- remove test data
    rawset(_G, user_sharding_func_name, nil)
end

function g.test_user_sharding_func_invalid_call_type()
    local user_sharding_func_name = 'user_sharding_func'
    local user_sharding_func_table = setmetatable({}, {
        __call = {}
    })
    rawset(_G, user_sharding_func_name, user_sharding_func_table)
    g.space.sharding_func = user_sharding_func_name

    local ok, err = ddl.check_schema(g.schema)
    t.assert_equals(err,
        [[spaces["space"].sharding_func: unknown sharding_func "user_sharding_func"]]
    )
    t.assert_equals(ok, nil)
    -- remove test data
    rawset(_G, user_sharding_func_name, nil)
end

function g.test_user_sharding_func_invalid_recursion_metatable()
    local user_sharding_func_name = 'user_sharding_func'
    local call_func = setmetatable({}, {
        __call = function(_, key) return key end
    })
    local user_sharding_func_table = setmetatable({}, {
        __call = call_func
    })
    rawset(_G, user_sharding_func_name, user_sharding_func_table)
    g.space.sharding_func = user_sharding_func_name

    local ok, err = ddl.check_schema(g.schema)
    t.assert_equals(err,
        [[spaces["space"].sharding_func: unknown sharding_func "user_sharding_func"]]
    )
    t.assert_equals(ok, nil)
    -- remove test data
    rawset(_G, user_sharding_func_name, nil)
end

function g.test_sharding_func_invalid_name_component()
    -- first symbol is a dot
    local user_sharding_func_name = '.vshard.router.bucket_id_strcrc32'
    rawset(_G, user_sharding_func_name, function(key) return key end)
    g.space.sharding_func = user_sharding_func_name
    local ok, err = ddl.check_schema(g.schema)
    t.assert_equals(err,
        [[spaces["space"].sharding_func: unknown sharding_func ".vshard.router.bucket_id_strcrc32"]]
    )
    t.assert_equals(ok, nil)
    -- remove test data
    rawset(_G, user_sharding_func_name, nil)

    -- last symbol is a dot
    local user_sharding_func_name = 'vshard.router.bucket_id_strcrc32.'
    rawset(_G, user_sharding_func_name, function(key) return key end)
    g.space.sharding_func = user_sharding_func_name
    local ok, err = ddl.check_schema(g.schema)
    t.assert_equals(err,
        [[spaces["space"].sharding_func: unknown sharding_func "vshard.router.bucket_id_strcrc32."]]
    )
    t.assert_equals(ok, nil)
    -- remove test data
    rawset(_G, user_sharding_func_name, nil)

    -- first symbol is a digit
    local user_sharding_func_name = '5vshard.router.bucket_id_strcrc32'
    rawset(_G, user_sharding_func_name, function(key) return key end)
    g.space.sharding_func = user_sharding_func_name
    local ok, err = ddl.check_schema(g.schema)
    t.assert_equals(err,
        [[spaces["space"].sharding_func: unknown sharding_func "5vshard.router.bucket_id_strcrc32"]]
    )
    t.assert_equals(ok, nil)
    -- remove test data
    rawset(_G, user_sharding_func_name, nil)

    -- chunk between two dots is empty
    local user_sharding_func_name = 'vshard.router..bucket_id_strcrc32'
    rawset(_G, user_sharding_func_name, function(key) return key end)
    g.space.sharding_func = user_sharding_func_name
    local ok, err = ddl.check_schema(g.schema)
    t.assert_equals(err,
        [[spaces["space"].sharding_func: unknown sharding_func "vshard.router..bucket_id_strcrc32"]]
    )
    t.assert_equals(ok, nil)
    -- remove test data
    rawset(_G, user_sharding_func_name, nil)

    -- chunk with incorrect character
    local user_sharding_func_name = 'vshard.router.b&cket_id_strcrc32'
    rawset(_G, user_sharding_func_name, function(key) return key end)
    g.space.sharding_func = user_sharding_func_name
    local ok, err = ddl.check_schema(g.schema)
    t.assert_equals(err,
        [[spaces["space"].sharding_func: unknown sharding_func "vshard.router.b&cket_id_strcrc32"]]
    )
    t.assert_equals(ok, nil)
    -- remove test data
    rawset(_G, user_sharding_func_name, nil)

    -- single component
    local user_sharding_func_name = 'user_sha(ding_func'
    rawset(_G, user_sharding_func_name, function(key) return key end)
    g.space.sharding_func = user_sharding_func_name
    local ok, err = ddl.check_schema(g.schema)
    t.assert_equals(err,
        [[spaces["space"].sharding_func: unknown sharding_func "user_sha(ding_func"]]
    )
    t.assert_equals(ok, nil)
    -- remove test data
    rawset(_G, user_sharding_func_name, nil)
end
