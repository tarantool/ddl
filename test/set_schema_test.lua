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

    if not db.v(2, 2) then
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
                error_expected, err
            )
            log.error('\n%s', e)
            error(e, 2)
        end
    end
end

local function assert_error_msg_contains(err_msg, expected, level)
    if not level then
        level = 1
    end
    if not string.find(err_msg, expected, level, true) then
        error(string.format(
            "Error message:\n %s\ndoesn't contains:\n %s\n",
            err_msg, expected
        ), 2)
    end
end

function g.test_empty_spaces_schema()
    local res, err = ddl.set_schema({})
    t.assert_not(err)
    t.assert(res)
end

function g.test_invalid_schema()
    local res, err = ddl.set_schema(nil)
    t.assert_not(res)
    t.assert_equals(err, 'Invalid schema (table expected, got nil)')

    local res, err = ddl.set_schema(box.NULL)
    t.assert_not(res)
    t.assert_equals(err, 'Invalid schema (table expected, got cdata)')

    local res, err = ddl.set_schema(true)
    t.assert_not(res)
    t.assert_equals(err, 'Invalid schema (table expected, got boolean)')

    local res, err = ddl.set_schema({spaces = 5})
    t.assert_not(res)
    t.assert_equals(err,
        'spaces: must be a table, got number'
    )

    local res, err = ddl.set_schema({spaces = {[1] = test_space.test}})
    t.assert_not(res)
    t.assert_equals(err,
        'spaces[1]: invaliad space_name (string expected, got number)'
    )

    local res, err = ddl.set_schema({spaces = {space = 5}})
    t.assert_not(res)
    t.assert_equals(err,
        'spaces["space"]: must be a table, got number'
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
        [[spaces["test"].indexes["secondary"]: HASH index must be unique]]
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
        [[spaces["test"].indexes["secondary"].parts[1]: HASH index type]] ..
        [[ doesn't support nullable fields]]
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
        [[space["test"].indexes["secondary"]: BITSET index can't be unique]]
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
        [[space["test"].indexes["primary"]: BITSET index can't be primary]]
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
        [[space["test"].indexes["secondary"].parts[1].type: integer field ]] ..
        [[type is unsupported in BITSET index type]]
    )

    _test_index({pk, {
        type = 'BITSET',
        name = 'secondary',
        unique = false,
        parts = {
            {
                path = 'string_nullable',
                type = 'string',
                is_nullable = true,
            }
        }
    }},
        [[space["test"].indexes["secondary"].part[1]: BITSET index ]] ..
        [[type doesn't support nullable field]]
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
                is_nullable = true,
            }
        }
    }},
        [[space["test"].indexes["secondary"].parts: BITSET index type doesn't ]] ..
        [[support multipart keys, actually it contains 2 parts]]
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
            path = 'array_nullable',
            type = 'array',
            is_nullable = true,
        }}
    }},
        [[space["test"].indexes["secondary"].part[1]: "RTREE" ]] ..
        [[index type doesn't support nullable field]]
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
        [[space["test"].indexes["secondary"].distance: distance "not_existing" is unknown]]
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
        [[space["test"].indexes["secondary"].dimension: bad argument 'dimension']] ..
        [[ it must belong to range [1, 20], got -9]]
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
        [[space["test"].indexes["secondary"].parts[1].type: string field]] ..
        [[ type is unsupported in RTREE index type]]
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
            is_nullable = true,
        }}
    }},
        [[space["test"].indexes["secondary"].parts: "RTREE" index type doesn't]] ..
        [[ support multipart keys, actually it contains 2 parts]]
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

    if not db.v(2,0) then
        _test_index({
            pk,
            {
                type = 'TREE',
                name = 'secondary',
                unique = true,
                parts = {
                    {
                        path = 'map_nonnull.DATA["name"]',
                        type = 'string',
                        is_nullable = false,
                        collation = 'unicode',
                    },
                },
            }},
            string.format(
                [[space["test"].indexes["secondary"].parts[1].path: path ]] ..
                [[(map_nonnull.DATA["name"]) is json_path, but your Tarantool ]] ..
                [[version (%s) doesn't support this]],
                _TARANTOOL
            )
        )
        t.success()
    end

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
        [[space["test"].index["path_idx"]: Field 1 has type 'unsigned' in one index, but type 'map' in another]]
        -- [[space["test"].indexes["path_idx"].parts[1].path: path (unsigned_nonnull.DATA["name"])]] ..
        -- [[ is json_path. It references to field[unsigned_nonnull] with type unsigned, but expected map]]
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
        [[space["test"].indexes["path_idx"].parts[1].path: path (empty.DATA["name"])]] ..
        [[ referencing to unknown field]]
    )
end


function g.test_multikey_path()
    local pk = {
        name = 'primary',
        type = 'TREE',
        unique = true,
        parts = {{is_nullable = false, path = 'unsigned_nonnull', type = 'unsigned'}},
    }

    local multikey_index =  {
        type = 'TREE',
        name = 'secondary',
        unique = true,
        parts = {
            {
                path = 'array_nonnull[*].path',
                type = 'string',
                is_nullable = false,
                collation = 'unicode',
            },
        },
    }

    if not db.v(2, 2) then
        _test_index(
            {pk, multikey_index},
            string.format(
                [[space["test"].indexes["secondary"].parts[1].path:]] ..
                [[ path (array_nonnull[*].path) is multikey_path,]] ..
                [[ but your Tarantool version (%s) doesn't support this]],
                _TARANTOOL
            )
        )
        t.success()
    end

    _test_index({
        pk, multikey_index
    })

    _test_index({
        pk, {
            name = 'path_idx',
            type = 'TREE',
            unique = true,
            parts = {
                {
                    path = 'array_nonnull[*].data',
                    type = 'unsigned',
                    is_nullable = false
            }
        }
    }})

    _test_index({pk, {
            name = 'path_idx',
            type = 'TREE',
            unique = true,
            parts = {
                {
                    path = 'map_nonnull[*].data',
                    type = 'unsigned',is_nullable = false
                }
            }
        }},
        [[space["test"].index["path_idx"]: Field 15 has type 'map' in one index, but type 'array' in another]]
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
        [[space["test"].indexes["secondary"].parts[1].path: path (empty.data[*].path)]] ..
        [[ referencing to unknown field]]
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
        [[space["test"].indexes["secondary"].parts[1].path: path (map_nonnull.data[*].path) ]] ..
        [[is multikey, but index type BITSET doesn't allow multikeys]]
    )

    _test_index({
        pk,
        {
            type = 'HASH',
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
        }},
        [[space["test"].indexes["secondary"].parts[1].path: path (map_nonnull.data[*].path)]] ..
        [[ is multikey, but index type HASH doesn't allow multikeys]]
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
                    path = 'map_nonnull.data[*].path',
                    type = 'array',
                    is_nullable = false,
                },
            },
        }},
        [[space["test"].indexes["secondary"].parts[1].path: path (map_nonnull.data[*].path)]] ..
        [[ is multikey, but index type RTREE doesn't allow multikeys]]
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
        'space["test"].indexes["primary"].parts[1].type: unknown type "undefined"'
    )

    _test_index({
        {
            type = 'HASH',
            unique = true,
            name = 'primary',
            parts = {{path = 'map_nonnull', type = 'map', is_nullable = false}}
        }},
        'space["test"].indexes["primary"].parts[1].type: unknown type "map"'
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
        'space["test"].indexes["primary"]: unknown type "BTREE"'
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
        'space["test"].indexes["primary"]: primary TREE index must be unique'
    )

    _test_index({
        {
            type = 'TREE',
            unique = true,
            name = 'primary',
            parts = {{path = 'unsigned_nullable', type = 'unsigned', is_nullable = true}}
        }},
        [[space["test"].indexes["primary"].part[1].path: primary ]] ..
        [[indexes can't contains nullable parts]]
    )


    if db.v(2, 2) then
        _test_index({
            {
                type = 'TREE',
                unique = true,
                name = 'primary',
                parts = {{path = 'map_nonnull.data[*]', type = 'unsigned', is_nullable = false}}
            }},
            [[space["test"].indexes["primary"].part[1].path: primary indexes doesn't allows multikey,]] ..
            [[ actually path (map_nonnull.data[*]) is multikey]]
        )
    end
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
