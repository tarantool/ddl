#!/usr/bin/env tarantool

local t = require('luatest')
local db = require('test.db')
local ddl = require('ddl')
local ffi = require('ffi')

local helper = require('test.helper')

local g = t.group()
local test_space = {
    engine = 'memtx',
    is_local = true,
    temporary = false,
    format = helper.test_space_format(),
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

local sharding_func_format = {
    {name = 'space_name', type = 'string', is_nullable = false},
    {name = 'sharding_func_name', type = 'string', is_nullable = true},
    {name = 'sharding_func_body', type = 'string', is_nullable = true},
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
    g.space.sharding_func = {body = 'function(key) return <...> end'}
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

    local _ddl_sharding_func = box.space['_ddl_sharding_func']
    t.assert_not_equals(_ddl_sharding_func, nil)
    t.assert_equals(_ddl_sharding_func:select(), {})

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

    local _ddl_sharding_func = box.space['_ddl_sharding_func']
    t.assert_not_equals(_ddl_sharding_func, nil)
    t.assert_equals(_ddl_sharding_func:format(), sharding_func_format)

    t.assert_equals(
        normalize_rows(_ddl_sharding_func:select()),
        {
            {'space', box.NULL, g.space.sharding_func.body}
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
    t.assert_equals(box.space['_ddl_sharding_func'], nil)

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
    local some_module = {
        sharding_func = function(key) return key end
    }
    local sharding_func_module = 'some_module'
    local sharding_func_name = sharding_func_module .. '.sharding_func'
    rawset(_G, sharding_func_module, some_module)
    space_two.sharding_func = sharding_func_name

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

    local _ddl_sharding_func = box.space['_ddl_sharding_func']
    t.assert_not_equals(_ddl_sharding_func, nil)

    t.assert_items_equals(
        normalize_rows(_ddl_sharding_func:select()),
        {
            {'space_one', box.NULL, g.space.sharding_func.body},
            {'space_two', space_two.sharding_func}
        }
    )

    local ddl_schema = ddl.get_schema()
    t.assert_equals(ddl_schema, schema)
    -- remove test data
    rawset(_G, sharding_func_module, nil)
end

function g.test_apply_sequently()
    local ok, err = ddl.set_schema(g.schema)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)

    local ddl_schema = ddl.get_schema()
    t.assert_equals(ddl_schema, g.schema)

    local _ddl_sharding_key = box.space['_ddl_sharding_key']
    t.assert_not_equals(_ddl_sharding_key, nil)

    local _ddl_sharding_func = box.space['_ddl_sharding_func']
    t.assert_not_equals(_ddl_sharding_func, nil)

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

    local _ddl_sharding_func = box.space['_ddl_sharding_func']
    t.assert_not_equals(_ddl_sharding_func, nil)

    t.assert_items_equals(
        normalize_rows(_ddl_sharding_func:select()),
        {
            {'space', box.NULL, g.space.sharding_func.body},
            {'new_space', box.NULL, g.space.sharding_func.body}
        }
    )

    local ddl_schema = ddl.get_schema()
    t.assert_equals(ddl_schema, new_schema)
end

function g.test_ddl_sharding_key_space()
    local space_one = table.deepcopy(g.space)
    space_one.sharding_key = {
        'unsigned_nonnull', 'integer_nonnull', 'string_nonnull'
    }

    local schema = {
        spaces = {
            space_one = space_one
        }
    }

    local ok, err = ddl.set_schema(schema)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)

    local _ddl_sharding_key = box.space['_ddl_sharding_key']
    t.assert_not_equals(_ddl_sharding_key, nil)

    local format = _ddl_sharding_key:format()
    t.assert_equals(#format, 2)
    t.assert_equals(format, {
	    {is_nullable = false, name = "space_name", type = "string"},
	    {is_nullable = false, name = "sharding_key", type = "array"},
        })

    t.assert_items_equals(
        normalize_rows(_ddl_sharding_key:select()),
        {
            {'space_one', space_one.sharding_key}
        }
    )

    local ddl_schema = ddl.get_schema()
    t.assert_equals(ddl_schema, schema)
end

function g.test_ddl_sharding_func_dot_notation()
    local some_module = {
        sharding_func = function(key) return key end
    }
    local sharding_func_module = 'some_module'
    local user_sharding_func_name = sharding_func_module .. '.sharding_func'
    rawset(_G, sharding_func_module, some_module)

    local space_one = table.deepcopy(g.space)
    space_one.sharding_func = user_sharding_func_name

    local schema = {
        spaces = {
            space_one = space_one
        }
    }

    local ok, err = ddl.set_schema(schema)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)

    local _ddl_sharding_func = box.space['_ddl_sharding_func']
    t.assert_not_equals(_ddl_sharding_func, nil)

    local format = _ddl_sharding_func:format()
    t.assert_equals(#format, 3)
    t.assert_equals(format, sharding_func_format)

    t.assert_items_equals(
        normalize_rows(_ddl_sharding_func:select()),
        {
            {'space_one', space_one.sharding_func}
        }
    )

    local ddl_schema = ddl.get_schema()
    t.assert_equals(ddl_schema, schema)
    -- remove test data
    rawset(_G, sharding_func_module, nil)
end

function g.test_ddl_user_sharding_function_call()
    local space_one = table.deepcopy(g.space)
    local user_sharding_func_name = 'user_sharding_func'
    rawset(_G, user_sharding_func_name, function(key) return key end)
    space_one.sharding_func = user_sharding_func_name

    local schema = {
        spaces = {
            space_one = space_one
        }
    }

    local ok, err = ddl.set_schema(schema)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)

    local _ddl_sharding_func = box.space['_ddl_sharding_func']
    t.assert_not_equals(_ddl_sharding_func, nil)

    local format = _ddl_sharding_func:format()
    t.assert_equals(#format, 3)
    t.assert_equals(format, sharding_func_format)

    t.assert_items_equals(
        normalize_rows(_ddl_sharding_func:select()),
        {
            {'space_one', space_one.sharding_func}
        }
    )

    local ddl_schema = ddl.get_schema()
    t.assert_equals(ddl_schema, schema)
    -- remove test data
    rawset(_G, user_sharding_func_name, nil)
end

function g.test_ddl_user_sharding_func_table_call()
    local space_one = table.deepcopy(g.space)
    local user_sharding_func_name = 'user_sharding_func'
    local user_sharding_func = setmetatable({}, {
        __call = function(_, key) return key end
    })
    rawset(_G, user_sharding_func_name, user_sharding_func)
    space_one.sharding_func = user_sharding_func_name

    local schema = {
        spaces = {
            space_one = space_one
        }
    }

    local ok, err = ddl.set_schema(schema)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)

    local _ddl_sharding_func = box.space['_ddl_sharding_func']
    t.assert_not_equals(_ddl_sharding_func, nil)

    local format = _ddl_sharding_func:format()
    t.assert_equals(#format, 3)
    t.assert_equals(format, sharding_func_format)

    t.assert_items_equals(
        normalize_rows(_ddl_sharding_func:select()),
        {
            {'space_one', space_one.sharding_func}
        }
    )

    local ddl_schema = ddl.get_schema()
    t.assert_equals(ddl_schema, schema)
    -- remove test data
    rawset(_G, user_sharding_func_name, nil)
end

function g.test_ddl_user_sharding_func_cdata_call()
    local space_one = table.deepcopy(g.space)
    local user_sharding_func_name = 'user_sharding_func'
    ffi.cdef[[
        typedef struct
        {
            int data;
        } test_set_struct_t;
    ]]

    ffi.metatype('test_set_struct_t', {
        __call = function(_, key) return key end
    })
    local test_set_struct = ffi.new('test_set_struct_t')
    rawset(_G, user_sharding_func_name, test_set_struct)
    space_one.sharding_func = user_sharding_func_name

    local schema = {
        spaces = {
            space_one = space_one
        }
    }

    local ok, err = ddl.set_schema(schema)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)

    local _ddl_sharding_func = box.space['_ddl_sharding_func']
    t.assert_not_equals(_ddl_sharding_func, nil)

    local format = _ddl_sharding_func:format()
    t.assert_equals(#format, 3)
    t.assert_equals(format, sharding_func_format)

    t.assert_items_equals(
        normalize_rows(_ddl_sharding_func:select()),
        {
            {'space_one', space_one.sharding_func}
        }
    )

    local ddl_schema = ddl.get_schema()
    t.assert_equals(ddl_schema, schema)
    -- remove test data
    rawset(_G, user_sharding_func_name, nil)
end

function g.test_user_sharding_func_userdata_call()
    local space_one = table.deepcopy(g.space)
    local user_sharding_func_name = 'user_sharding_func'

    local user_sharding_func_userdata = newproxy(true)
    local mt = getmetatable(user_sharding_func_userdata)
    mt.__call = function(_, key) return key end
    rawset(_G, user_sharding_func_name, user_sharding_func_userdata)
    space_one.sharding_func = user_sharding_func_name

    local schema = {
        spaces = {
            space_one = space_one
        }
    }

    local ok, err = ddl.set_schema(schema)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)

    local _ddl_sharding_func = box.space['_ddl_sharding_func']
    t.assert_not_equals(_ddl_sharding_func, nil)

    local format = _ddl_sharding_func:format()
    t.assert_equals(#format, 3)
    t.assert_equals(format, sharding_func_format)

    t.assert_items_equals(
        normalize_rows(_ddl_sharding_func:select()),
        {
            {'space_one', space_one.sharding_func}
        }
    )

    local ddl_schema = ddl.get_schema()
    t.assert_equals(ddl_schema, schema)
    -- remove test data
    rawset(_G, user_sharding_func_name, nil)
end

function g.test_ddl_user_sharding_func_with_body()
    local space_one = table.deepcopy(g.space)

    local schema = {
        spaces = {
            space_one = space_one
        }
    }

    local ok, err = ddl.set_schema(schema)
    t.assert_equals(err, nil)
    t.assert_equals(ok, true)

    local _ddl_sharding_func = box.space['_ddl_sharding_func']
    t.assert_not_equals(_ddl_sharding_func, nil)

    local format = _ddl_sharding_func:format()
    t.assert_equals(#format, 3)
    t.assert_equals(format, sharding_func_format)

    t.assert_items_equals(
        normalize_rows(_ddl_sharding_func:select()),
        {
            {'space_one', box.NULL, space_one.sharding_func.body}
        }
    )

    local ddl_schema = ddl.get_schema()
    t.assert_equals(ddl_schema, schema)
end
