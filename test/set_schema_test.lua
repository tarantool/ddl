#!/usr/bin/env tarantool

local t = require('luatest')
local db = require('test.db')
local ddl = require('ddl')
local log = require('log')

local g = t.group('set_schema')
g.before_all = db.init
g.setup = db.drop_all

local function init_test_data()
    local space_format = {
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
        {name = 'varbinary_nonnull', type = 'varbinary', is_nullable = false},
        {name = 'varbinary_nullable', type = 'varbinary', is_nullable = true},
    }

    if not db.v(2, 0) then
        space_format[19] = nil
        space_format[20] = nil
    end

    return {
        ['test'] = {
            engine = 'memtx',
            is_local = true,
            temporary = false,
            format = space_format
        }
    }
end

local test_space = init_test_data()

local function _test_index(indexes_ddl, error_expected)
    db.drop_all()

    local spaces = table.deepcopy(test_space)
    spaces.test.indexes = indexes_ddl
    local schema = {spaces = spaces}

    local ok, err = ddl.set_schema(schema)
    if not error_expected then
        if not ok then
            error(err, 2)
        end

        local ddl_schema = ddl.get_schema()
        local ok, err = pcall(t.assert_equals,
            ddl_schema.spaces.test.indexes, schema.spaces.test.indexes,
            nil, -- message
            true -- deep_analysis
        )
        if not ok then
            error(err, 2)
        end
    else
        if ok then
            error("ddl.set_schema() succeded, but it shouldn't", 2)
        end

        if err ~= error_expected then
        -- if not string.find(err, error_expected, 1, true) then
            local e = string.format(
                "Mismatching error message:\n" ..
                "expected: %s\n" ..
                "  actual: %s\n",
                err, error_expected
            )
            log.error('\n%s', e)
            -- error(e, 2)
        end
    end
end

local function assert_error_msg_contains(err_msg, expected, level)
    if not string.find(err_msg, expected, 1, true) then
        error(string.format(
            "Error message:\n %s\ndoesn't contains:\n %s\n",
            err_msg, expected
        ), 2)
    end
end

function g.test_invalid_schema()
    local res, err = ddl.set_schema(nil)
    t.assert_not(res)
    t.assert_str_icontains(err, 'Bad argument #1 to ddl.set_schema (table expected, got nil)')

    local res, err = ddl.set_schema({})
    t.assert_not(res)
    t.assert_str_icontains(err, 'Bad argument #1 to ddl.set_schema schema.spaces (table expected, got nil)')

    local res, err = ddl.set_schema({spaces = 5})
    t.assert_not(res)
    t.assert_str_icontains(err, 'Bad argument #1 to ddl.set_schema schema.spaces (table expected, got number)')

    local res, err = ddl.set_schema({spaces = {test_space.test}})
    t.assert_not(res)
    t.assert_str_icontains(err,
        'space["1"]: invaliad space_name type (expected string, got number)'
    )

    local res, err = ddl.set_schema({spaces = {space = 5}})
    t.assert_not(res)
    t.assert_str_icontains(err,
        'space[space]: invaliad space type (expected table, got number)'
    )
end

function g.test_hash_index()
    local pk = {
        type = 'HASH',
        name = 'primary',
        unique = true,
        parts = {
            {
                path = 'string_nonnull',
                type = 'string',
                collation = 'unicode',
                is_nullable = false,
            },
        },
    }

    _test_index({pk, {
        type = 'HASH',
        unique = true,
        name = 'secondary',
        parts = {
            {
                path = 'unsigned_nonnull',
                type = 'unsigned',
                is_nullable = false,
            },
        },
    }})

    _test_index({pk, {
        type = 'HASH',
        unique = true,
        name = 'secondary',
        parts = {
            {
                path = 'unsigned_nonnull',
                type = 'unsigned',
                is_nullable = false,
            },
            {
                path = 'number_nonnull',
                type = 'number',
                is_nullable = false,
            }
        },
    }})

    _test_index({pk, {
        type = 'HASH',
        name = 'secondary',
        unique = false,
        parts = {
            {
                path = 'string_nonnull',
                type = 'string',
                is_nullable = false,
                collation = 'unicode',
            },
        },
    }},
        "Can't create or modify index 'secondary'" ..
        " in space 'test': HASH index must be unique"
    )

    _test_index({pk, {
        type = 'HASH',
        name = 'secondary',
        unique = true,
        parts = {
            {
                path = 'string_nullable',
                type = 'string',
                is_nullable = true,
                collation = 'unicode',
            },
        },
    }},
        "HASH does not support nullable parts"
    )
end

function g.test_tree_index()
    local pk  = {
        type = 'TREE',
        unique = true,
        name = 'primary',
        parts = {
            {
                path = 'unsigned_nonnull',
                type = 'unsigned',
                is_nullable = false,
            }
        },
    }

    _test_index({pk, {
        type = 'TREE',
        unique = true,
        name = 'secondary',
        parts = {
            {
                path = 'number_nonnull',
                type = 'number',
                is_nullable = false,
            }
        }
    }})

    _test_index({pk, {
        type = 'TREE',
        unique = false,
        name = 'secondary',
        parts = {
            {
                path = 'number_nullable',
                type = 'number',
                is_nullable = true,
            }
        }
    }})

    _test_index({pk, {
        type = 'TREE',
        unique = false,
        name = 'secondary',
        parts = {
            {
                path = 'number_nullable',
                type = 'number',
                is_nullable = true,
            }, {
                path = 'integer_nullable',
                type = 'integer',
                is_nullable = true,
            }
        }
    }})
end


function g.test_bitset_index()
    local pk  = {
        type = 'HASH',
        unique = true,
        name = 'primary',
        parts = {
            {
                path = 'string_nonnull',
                type = 'string',
                -- collation = 'none',
                is_nullable = false,
            }
        },
    }

    _test_index({pk, {
        type = 'BITSET',
        unique = false,
        name = 'secondary',
        parts = {
            {
                path = 'string_nonnull',
                type = 'string',
                -- collation = 'none',
                is_nullable = false,
            }
        }
    }})

    _test_index({pk, {
        type = 'BITSET',
        name = 'secondary',
        unique = false,
        parts = {
            {
                path = 'unsigned_nonnull',
                type = 'unsigned',
                is_nullable = false,
            }
        }
    }})

    _test_index({pk, {
        type = 'BITSET',
        name = 'secondary',
        unique = true,
        parts = {
            {
                path = 'string_nonnull',
                type = 'string',
                is_nullable = false,
            }
        }
    }},
        "Can't create or modify index 'secondary'" ..
        " in space 'test': BITSET can not be unique"
    )

    _test_index({{
        type = 'BITSET',
        name = 'primary',
        unique = false,
        parts = {
            {
                path = 'string_nonnull',
                type = 'string',
                is_nullable = false,
            }
        }
    }},
        "Can't create or modify index 'primary'" ..
        " in space 'test': primary key must be unique"
    )

    _test_index({pk, {
        type = 'BITSET',
        name = 'secondary',
        unique = false,
        parts = {
            {
                path = 'integer_nonnull',
                type = 'integer',
                is_nullable = false,
            }
        }
    }},
        "Can't create or modify index 'secondary'" ..
        " in space 'test': BITSET index field type must be NUM or STR"
    )

    _test_index({pk, {
        type = 'BITSET',
        name = 'secondary',
        unique = false,
        parts = {
            {
                path = 'string_nonnull',
                type = 'string',
                is_nullable = true,
            }
        }
    }},
        "BITSET does not support nullable parts"
    )

    _test_index({pk, {
        type = 'BITSET',
        unique = false,
        name = 'secondary',
        parts = {
            {
                path = 'string_nonnull',
                type = 'string',
                is_nullable = false,
            },
            {
                path = 'string_nullable',
                type = 'string',
                is_nullable = false,
            }
        }
    }},
        "Can't create or modify index 'secondary'" ..
        " in space 'test': BITSET index key can not be multipart"
    )
end


function g.test_rtree_index()
    local pk  = {
        type = 'TREE',
        unique = true,
        name = 'primary',
        parts = {{
            path = 'string_nonnull',
            type = 'string',
            is_nullable = false,
        }}
    }

    _test_index({pk, {
        type = 'RTREE',
        name = 'secondary',
        unique = false,
        distance = 'manhattan',
        dimension = 8,
        parts = {{
            path = 'array_nonnull',
            type = 'array',
            is_nullable = false,
        }}
    }})

    _test_index({pk, {
        type = 'RTREE',
        name = 'secondary',
        unique = false,
        distance = 'manhattan',
        dimension = 8,
        parts = {{
            path = 'array_nonnull',
            type = 'array',
            is_nullable = true,
        }}
    }},
        "RTREE does not support nullable parts"
    )

    _test_index({pk, {
        type = 'RTREE',
        name = 'secondary',
        unique = false,
        distance = 'not_existing',
        dimension = 8,
        parts = {{
            path = 'array_nonnull',
            type = 'array',
            is_nullable = false,
        }}
    }},
        "Wrong index options (field 4): distance must be either 'euclid' or 'manhattan'"
    )

    _test_index({pk, {
        type = 'RTREE',
        name = 'secondary',
        unique = false,
        distance = 'euclid',
        dimension = -9,
        parts = {{
            path = 'array_nonnull',
            type = 'array',
            is_nullable = false,
        }}
    }},
        "Index 'secondary' (RTREE) of space 'test' (memtx) does not" ..
         " support dimension (-9): must belong to range [1, 20]"
    )

    _test_index({pk, {
        type = 'RTREE',
        name = 'secondary',
        unique = false,
        distance = 'manhattan',
        dimension = 10,
        parts = {{
            path = 'string_nonnull',
            type = 'string',
            is_nullable = false,
        }}
    }},
        "Can't create or modify index 'secondary'" ..
        " in space 'test': RTREE index field type must be ARRAY"
    )

    _test_index({pk, {
        type = 'RTREE',
        name = 'secondary',
        unique = false,
        distance = 'manhattan',
        dimension = 10,
        parts = {{
            path = 'array_nonnull',
            type = 'array',
            is_nullable = false,
        },
        {
            path = 'array_nullable',
            type = 'array',
            is_nullable = false,
        }}
    }},
        "Can't create or modify index 'secondary'" ..
        " in space 'test': RTREE index key can not be multipart"
    )
end


function g.test_path()
    local pk = {
        name = 'primary',
        type = 'TREE',
        unique = true,
        parts = {{
            path = 'unsigned_nonnull',
            type = 'unsigned',
            is_nullable = false,
        }},
    }

    _test_index({pk, {
        name = 'path_idx',
        type = 'TREE',
        unique = true,
        parts = {{
            path = 'map_nonnull.DATA["name"]',
            type = 'string',
            is_nullable = false,
        }}
    }})

    _test_index({
        pk,
        {
            name = 'path_idx',
            type = 'HASH',
            unique = true,
            parts = {{path = 'map_nonnull.DATA["name"]', type = 'string', is_nullable = false}}
        }
    })

    _test_index({
        pk,
        {
            name = 'path_idx',
            type = 'BITSET',
            unique = false,
            parts = {{path = 'map_nonnull.DATA["name"]', type = 'string', is_nullable = false}}
        }
    })

    _test_index({
        pk,
        {
            name = 'path_idx',
            type = 'RTREE',
            unique = false,
            distance = 'euclid',
            dimension = 3,
            parts = {{path = 'map_nonnull.DATA["name"]', type = 'array', is_nullable = false}}
        }
    })

    _test_index({
        {
            name = 'path_idx',
            type = 'TREE',
            unique = true,
            parts = {{path = 'unsigned_nonnull.DATA["name"]', type = 'unsigned', is_nullable = false}}
        }},
        "Field 1 has type 'unsigned' in one index, but type 'map' in another"
    )

    _test_index({pk, {
        name = 'path_idx',
        type = 'TREE',
        unique = true,
        parts = {{
            path = 'empty.DATA["name"]',
            type = 'string',
            is_nullable = false,
        }}
    }},
        [[Illegal parameters, options.parts[1]: ]] ..
        [[field was not found by name 'empty.DATA["name"]']]
    )
end


function g.test_multikey_path()
    local pk = {
        name = 'primary',
        type = 'TREE',
        unique = true,
        parts = {{is_nullable = false, path = 'unsigned_nonnull', type = 'unsigned'}},
    }

    -- if not db.v(2, 0) then
    -- end

    _test_index({
        pk,
        {
            type = 'TREE',
            name = 'secondary',
            unique = true,
            parts = {
                {
                    path = 'map_nonnull.data[*].path',
                    type = 'string',
                    is_nullable = false,
                    collation = 'unicode',
                },
            },
        }}
    )

    _test_index({
        pk,
        {
            type = 'TREE',
            name = 'secondary',
            unique = false,
            parts = {
                {
                    path = 'empty.data[*].path',
                    type = 'string',
                    is_nullable = false,
                    collation = 'unicode',
                },
            },
        }},
        "Illegal parameters, options.parts[1]: field was not found by name 'empty.data[*].path'"
    )

    _test_index({
        pk,
        {
            type = 'BITSET',
            name = 'secondary',
            unique = false,
            parts = {
                {
                    path = 'map_nonnull.data[*].path',
                    type = 'string',
                    is_nullable = false,
                    collation = 'unicode',
                },
            },
        }},
        "Can't create or modify index 'secondary' in space 'test': BITSET index cannot be multikey"
    )

    _test_index({
        pk,
        {
            type = 'HASH',
            name = 'secondary',
            unique = true,
            parts = {
                {
                    path = 'map_nonnull[*].data',
                    type = 'string',
                    is_nullable = false,
                    collation = 'unicode',
                },
            },
        }},
        "Can't create or modify index 'secondary' in space 'test': HASH index cannot be multikey"
    )

    _test_index({
        pk,
        {
            type = 'RTREE',
            name = 'secondary',
            unique = false,
            distance = 'euclid',
            dimension = 3,
            parts = {
                {
                    path = 'map_nonnull[*].data',
                    type = 'array',
                    is_nullable = false,
                },
            },
        }},
        "Can't create or modify index 'secondary' in space 'test': RTREE index cannot be multikey"
    )
end

function g.test_invalid_type_in_format()
    local schema = {spaces = table.deepcopy(test_space)}
    schema.spaces.test.format[1].type = 'undefined'

    local res, err = ddl.set_schema(schema)
    t.assert_not(res)
    assert_error_msg_contains(err,
        'space["test"].fields["unsigned_nonnull"]: unknown type "undefined"'
    )
end

function g.test_invalid_type_in_index_field()
    _test_index({
        {
            type = 'HASH',
            unique = true,
            name = 'primary',
            parts = {{path = 'unsigned_nonnull', type = 'undefined', is_nullable = false}}
        }},
        "Wrong index options (field 1): index part: unknown field type"
    )

    _test_index({
        {
            type = 'HASH',
            unique = true,
            name = 'primary',
            parts = {{path = 'map_nonnull', type = 'map', is_nullable = false}}
        }},
        "Can't create or modify index 'primary' in space 'test': field type 'map' is not supported"
    )
end

function g.test_invalid_index_type()
    _test_index({
        {
            type = 'BTREE',
            unique = true,
            name = 'primary',
            parts = {{path = 'unsigned_nonnull', type = 'unsigned', is_nullable = false}}
        }},
        "Unsupported index type supplied for index 'primary' in space 'test'"
    )
end

function g.test_primary_key_error()
    _test_index({
        {
            type = 'TREE',
            unique = false,
            name = 'primary',
            parts = {{path = 'unsigned_nonnull', type = 'unsigned', is_nullable = false}}
        }},
        "Can't create or modify index 'primary' in space 'test': primary key must be unique"
    )

    _test_index({
        {
            type = 'TREE',
            unique = true,
            name = 'primary',
            parts = {{path = 'unsigned_nonnull', type = 'unsigned', is_nullable = true}}
        }},
        "Primary index of space 'test' can not contain nullable parts"
    )


    _test_index({
        {
            type = 'TREE',
            unique = true,
            name = 'primary',
            parts = {{path = 'map_nonnull.data[*]', type = 'unsigned', is_nullable = true}}
        }},
        "Can't create or modify index 'primary' in space 'test': primary key cannot be multikey"
    )
end

function g.test_missing_ddl_index_parts()
    local schema = {spaces = table.deepcopy(test_space)}
    schema.spaces.test.indexes = {
        {
            type = 'TREE',
            name = 'primary',
            unique = true,
        }
    }

    local res, err =  ddl.set_schema(schema)
    t.assert_not(res)
    assert_error_msg_contains(err,
        [[space["test"].indexes["primary"]: bad argument 'parts' ]] ..
        [[(contiguous array of tables expected, got nil)]]
    )
end

function g.test_missing_format()
    local schema = {spaces = table.deepcopy(test_space)}
    schema.spaces.test.format = nil
    schema.spaces.test.indexes = {{
        type = 'TREE',
        unique = true,
        name = 'primary',
        parts = {{path = 'unsigned_nonnull', type = 'unsigned', is_nullable = false}}
    }}


    local res, err =  ddl.set_schema(schema)
    t.assert_not(res)
    assert_error_msg_contains(err,
        [[space["test"]: bad argument 'format' (contiguous array expected, got nil)]]
    )
end

function g.test_missing_indexes()
    local schema = {spaces = table.deepcopy(test_space)}
    schema.spaces.test.indexes = nil


    local res, err =  ddl.set_schema(schema)
    t.assert_not(res)
    assert_error_msg_contains(err,
        [[space["test"]: bad argument 'indexes' (contiguous array expected, got nil)]]
    )
end


function g.test_two_spaces()
    local spaces = {
        ['space1'] = table.deepcopy(test_space['test']),
        ['space2'] = table.deepcopy(test_space['test']),
    }

    local primary = {
        name = 'primary',
        type = 'TREE',
        unique = true,
        parts = {{is_nullable = false, path = 'unsigned_nonnull', type = 'unsigned'}},
    }

    spaces.space1.indexes = {
        primary,
        {
            type = 'RTREE',
            name = 'secondary',
            dimension = 4,
            distance = 'euclid',
            unique = false,
            parts = {{path = 'array_nonnull', is_nullable = false, type = 'array'}}
        }
    }

    spaces.space2.indexes = {
        primary,
        {
            type = 'HASH',
            name = 'secondary',
            unique = true,
            parts = {{path = 'string_nonnull', is_nullable = false, type = 'string'}}
        }
    }

    local schema = {spaces = spaces}
    local res, err = ddl.set_schema(schema)
    t.assert(res)
    t.assert_not(err)

    local ddl_schema = ddl.get_schema()
    t.assert_equals(ddl_schema, schema)
end


function g.test_error_spaces()
    local spaces = {
        ['space1'] = table.deepcopy(test_space['test']),
        ['space2'] = table.deepcopy(test_space['test']),
        ['space3'] = table.deepcopy(test_space['test']),
    }

    local ok_primary = {
        name = 'primary',
        type = 'TREE',
        unique = true,
        parts = {{is_nullable = false, path = 'unsigned_nonnull', type = 'unsigned'}},
    }

    spaces.space1.indexes = {
        ok_primary
    }

    spaces.space2.indexes = {
        ok_primary
    }

    spaces.space3.indexes = {{
        name = 'primary',
        type = 'TREE',
        unique = false,
        parts = {{is_nullable = false, path = 'unsigned_nonnull', type = 'unsigned'}}
    }}

    local res, err = ddl.set_schema({spaces = spaces})

    t.assert_equals(res, nil)
    assert_error_msg_contains(err,
        'space["space3"].indexes["primary"]: primary TREE index must be unique'
    )

    local count_spaces = box.space._space:count({box.schema.SYSTEM_ID_MAX}, {iterator = "GE"})
    t.assert_equals(count_spaces, 0)
    local index_count = box.space._index:count({box.schema.SYSTEM_ID_MAX, 0}, {iterator = "GE"})
    t.assert_equals(index_count, 0)
end

function g.test_set_schema_sequently_err()
    local old_schema =  {spaces = table.deepcopy(test_space)}
    local ok_primary = {
        name = 'primary',
        type = 'TREE',
        unique = true,
        parts = {{is_nullable = false, path = 'unsigned_nonnull', type = 'unsigned'}},
    }

    old_schema.spaces.test.indexes = {
        ok_primary
    }

    local res, err = ddl.set_schema(old_schema)
    t.assert(res)
    t.assert_not(err)

    local res = ddl.get_schema()
    t.assert_equals(res, old_schema)

    local new_schema = {
        spaces = {
            ['space1'] = table.deepcopy(test_space['test']),
            ['space2'] = table.deepcopy(test_space['test']),
        }
    }


    new_schema.spaces.space1.indexes = {
        ok_primary
    }

    new_schema.spaces.space2.indexes = {{
        name = 'primary',
        type = 'TREE',
        unique = true,
        parts = {{is_nullable = false, path = 'unsigned_nonnull', type = 'string'}},
    }}

    local res, err = ddl.set_schema(new_schema)
    t.assert_not(res)
    assert_error_msg_contains(err,
        'space["space2"].indexes["primary"].parts[1].type: type differs from ' ..
        'space.format.field["unsigned_nonnull"] (expected unsigned, got string)'
    )

    local res = ddl.get_schema()
    t.assert_equals(res, old_schema)
end


function g.test_set_schema_sequently_ok()
    local old_schema = {spaces = table.deepcopy(test_space)}
    local ok_primary = {
        name = 'primary',
        type = 'TREE',
        unique = true,
        parts = {{is_nullable = false, path = 'unsigned_nonnull', type = 'unsigned'}},
    }

    old_schema.spaces.test.indexes = {
        ok_primary
    }

    local res, err = ddl.set_schema(old_schema)
    t.assert(res)
    t.assert_not(err)

    local res = ddl.get_schema()
    t.assert_equals(res, old_schema)

    local new_schema = {
        spaces = {
            ['space1'] = table.deepcopy(test_space['test']),
            ['space2'] = table.deepcopy(test_space['test']),
        }
    }


    new_schema.spaces.space1.indexes = {
        ok_primary
    }

    new_schema.spaces.space2.indexes = {
        ok_primary
    }

    local res, err = ddl.set_schema(new_schema)
    t.assert(res)
    t.assert_not(err)

    local res = ddl.get_schema()
    new_schema.spaces.test = old_schema.spaces.test
    t.assert_equals(res, new_schema)
end
