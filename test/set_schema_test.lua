#!/usr/bin/env tarantool

local t = require('luatest')
local db = require('test.db')
local ddl = require('ddl')
local log = require('log')

local helper = require('test.helper')

local g = t.group()
g.before_all(db.init)
g.before_each(db.drop_all)

local function init_test_data()
    local space_format = helper.test_space_format()

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

    local res, err = ddl.set_schema({})
    t.assert_not(res)
    t.assert_equals(err,
        'spaces: must be a table, got nil'
    )

    local res, err = ddl.set_schema({spaces = 5})
    t.assert_not(res)
    t.assert_equals(err,
        'spaces: must be a table, got number'
    )

    local res, err = ddl.set_schema({spaces = box.NULL})
    t.assert_not(res)
    t.assert_equals(err,
        'spaces: must be a table, got cdata'
    )

    local res, err = ddl.set_schema({spaces = {}, functions = {}})
    t.assert_not(res)
    t.assert_equals(err,
        'functions: not supported'
    )

    local res, err = ddl.set_schema({spaces = {}, sequences = {}})
    t.assert_not(res)
    t.assert_equals(err,
        'sequences: not supported'
    )

    local res, err = ddl.set_schema({spaces = {}, meta = {}})
    t.assert_not(res)
    t.assert_equals(err,
        'Invalid schema: redundant key "meta"'
    )

    local res, err = ddl.set_schema({spaces = {[1] = test_space.test}})
    t.assert_not(res)
    t.assert_equals(err,
        'spaces[1]: invalid space name (string expected, got number)'
    )

    local res, err = ddl.set_schema({spaces = {space = 5}})
    t.assert_not(res)
    t.assert_equals(err,
        'spaces["space"]: bad value (table expected, got number)'
    )
end

-- Indexes -------------------------------------------------------------
------------------------------------------------------------------------

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
        [[spaces["test"].indexes["secondary"].parts[1]: index of HASH type]] ..
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
        [[spaces["test"].indexes["secondary"]: BITSET index can't be unique]]
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
        [[spaces["test"].indexes["primary"]: BITSET index can't be primary]]
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
        [[spaces["test"].indexes["secondary"].parts[1].type: integer field ]] ..
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
        [[spaces["test"].indexes["secondary"].parts[1]: index of ]] ..
        [[BITSET type doesn't support nullable fields]]
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
        [[spaces["test"].indexes["secondary"].parts: index of BITSET type can't ]] ..
        [[be composite (currently, index contains 2 parts)]]
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
        [[spaces["test"].indexes["secondary"].parts[1]: index ]] ..
        [[of RTREE type doesn't support nullable fields]]
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
        [[spaces["test"].indexes["secondary"].distance: unknown distance "not_existing"]]
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
        [[spaces["test"].indexes["secondary"].dimension: incorrect value]] ..
        [[ (must be in range [1, 20], got -9)]]
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
        [[spaces["test"].indexes["secondary"].parts[1].type: string field]] ..
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
        [[spaces["test"].indexes["secondary"].parts: index of RTREE type can't]] ..
        [[ be composite (currently, index contains 2 parts)]]
    )
end

function g.test_missing_indexes()
    local schema = {spaces = table.deepcopy(test_space)}
    schema.spaces.test.indexes = nil


    local res, err =  ddl.set_schema(schema)
    t.assert_not(res)
    assert_error_msg_contains(err,
        [[spaces["test"].indexes: bad value (contiguous array expected, got nil)]]
    )
end

-- Index parts ---------------------------------------------------------
------------------------------------------------------------------------

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
                [[spaces["test"].indexes["secondary"].parts[1].path: path ]] ..
                [[(map_nonnull.DATA["name"]) is JSONPath, but your Tarantool ]] ..
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

    local error_expected
    if db.v(2, 8) then
        -- See: https://github.com/tarantool/tarantool/issues/4707
        error_expected = 'spaces["test"].indexes["path_idx"]:' ..
            " Field 1 (unsigned_nonnull) has type 'unsigned' in one index," ..
            " but type 'map' in another"
    else
        error_expected = 'spaces["test"].indexes["path_idx"]:' ..
            " Field 1 has type 'unsigned' in one index," ..
            " but type 'map' in another"
    end
    _test_index({
        {
            name = 'path_idx',
            type = 'TREE',
            unique = true,
            parts = {{path = 'unsigned_nonnull.DATA["name"]', type = 'unsigned', is_nullable = false}}
        }},
        error_expected
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
        [[spaces["test"].indexes["path_idx"].parts[1].path: path (empty.DATA["name"])]] ..
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
                [[spaces["test"].indexes["secondary"].parts[1].path:]] ..
                [[ JSONPath (array_nonnull[*].path) has wildcard,]] ..
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



    local error_expected
    if db.v(2, 8) then
        -- See: https://github.com/tarantool/tarantool/issues/4707
        error_expected = 'spaces["test"].indexes["path_idx"]:' ..
            " Field 15 (map_nonnull) has type 'map' in one index," ..
            " but type 'array' in another"
    else
        error_expected = 'spaces["test"].indexes["path_idx"]:' ..
            " Field 15 has type 'map' in one index," ..
            " but type 'array' in another"
    end
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
        error_expected
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
        [[spaces["test"].indexes["secondary"].parts[1].path: path (empty.data[*].path)]] ..
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
        [[spaces["test"].indexes["secondary"].parts[1].path: JSONPath ]] ..
        [[(map_nonnull.data[*].path) has wildcard, but index type BITSET doesn't allow this]]
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
        [[spaces["test"].indexes["secondary"].parts[1].path: JSONPath ]] ..
        [[(map_nonnull.data[*].path) has wildcard, but index type HASH doesn't allow this]]
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
        [[spaces["test"].indexes["secondary"].parts[1].path: JSONPath ]] ..
        [[(map_nonnull.data[*].path) has wildcard, but index type RTREE doesn't allow this]]
    )
end

function g.test_invalid_type_in_format()
    local schema = {spaces = table.deepcopy(test_space)}
    schema.spaces.test.format[1].type = 'undefined'

    local res, err = ddl.set_schema(schema)
    t.assert_not(res)
    assert_error_msg_contains(err,
        'spaces["test"].format["unsigned_nonnull"].type: unknown type "undefined"'
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
        'spaces["test"].indexes["primary"].parts[1].type: unknown type "undefined"'
    )

    _test_index({
        {
            type = 'HASH',
            unique = true,
            name = 'primary',
            parts = {{path = 'map_nonnull', type = 'map', is_nullable = false}}
        }},
        'spaces["test"].indexes["primary"].parts[1].type: unknown type "map"'
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
        'spaces["test"].indexes["primary"].type: unknown type "BTREE"'
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
        'spaces["test"].indexes["primary"]: primary TREE index must be unique'
    )

    _test_index({
        {
            type = 'TREE',
            unique = true,
            name = 'primary',
            parts = {{path = 'unsigned_nullable', type = 'unsigned', is_nullable = true}}
        }},
        [[spaces["test"].indexes["primary"].parts[1].is_nullable: primary ]] ..
        [[index can't contain nullable parts]]
    )


    if db.v(2, 2) then
        _test_index({
            {
                type = 'TREE',
                unique = true,
                name = 'primary',
                parts = {{path = 'map_nonnull.data[*]', type = 'unsigned', is_nullable = false}}
            }},
            [[spaces["test"].indexes["primary"].parts[1].path: primary index doesn't allow]] ..
            [[ JSONPath wildcard (path map_nonnull.data[*] has wildcard)]]
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
        [[spaces["test"].indexes["primary"].parts: bad value ]] ..
        [[(contiguous array of tables expected, got nil)]]
    )
end

-- Space format --------------------------------------------------------
------------------------------------------------------------------------

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
        [[spaces["test"].format: bad value (contiguous array expected, got nil)]]
    )
end

function g.test_annotated_format()
    local schema = {spaces = table.deepcopy(test_space)}
    schema.spaces.test.indexes = {{
        type = 'TREE',
        unique = true,
        name = 'primary',
        parts = {{path = 'unsigned_nonnull', type = 'unsigned', is_nullable = false}}
    }}

    local k = 1ULL
    schema.spaces.test.format[2][k] = 'forbidden-cdata'
    local res, err = ddl.set_schema(schema)
    t.assert_equals(res, nil)
    t.assert_equals(err,
        'spaces["test"].format["unsigned_nullable"]:' ..
        ' bad key 1ULL (string expected, got cdata)'
    )
    schema.spaces.test.format[2][k] = nil

    schema.spaces.test.format[1].scale = 'bananas'
    local res, err = ddl.set_schema(schema)
    t.assert_equals({res, err}, {true, nil})
    t.assert_equals(ddl.get_schema(), schema)
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
        'spaces["space3"].indexes["primary"]: primary TREE index must be unique'
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
        'spaces["space2"].indexes["primary"].parts[1].type: type differs from ' ..
        'spaces["space2"].format["unsigned_nonnull"].type (unsigned expected, got string)'
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

function g.test_tarantool_2_3_types()
    local spaces = table.deepcopy(test_space)
    spaces.test.format = {
        {name = 'scalar_nonnull', type = 'scalar', is_nullable = false},
        {name = 'scalar_nullable', type = 'scalar', is_nullable = true},
        {name = 'decimal_nonnull', type = 'decimal', is_nullable = false},
        {name = 'decimal_null', type = 'decimal', is_nullable = true},
        {name = 'double_nonnull', type = 'double', is_nullable = false},
        {name = 'double_null', type = 'double', is_nullable = true},
    }

    local schema = {spaces = spaces}

    local ok_primary = {
        name = 'primary',
        type = 'TREE',
        unique = true,
        parts = {
            {is_nullable = false, path = 'decimal_nonnull', type = 'scalar'},
            {is_nullable = false, path = 'double_nonnull', type = 'scalar'},
            {is_nullable = false, path = 'scalar_nonnull', type = 'double'},
        }
    }

    schema.spaces.test.indexes = {ok_primary}
    local ok, err = ddl.set_schema(schema)

    if not db.v(2, 3) then
        t.assert_equals(ok, nil)
        t.assert_equals(err,
            [[spaces["test"]: Failed to create space 'test':]] ..
            [[ field 3 has unknown field type]]
        )
        t.success()
    end


    t.assert_equals(err, nil)
    t.assert_equals(ok, true)

    local err_primary = {
        name = 'primary',
        type = 'TREE',
        unique = true,
        parts = {
            {is_nullable = false, path = 'decimal_nonnull', type = 'double'},
            {is_nullable = false, path = 'double_nonnull', type = 'scalar'},
            {is_nullable = false, path = 'scalar_nonnull', type = 'double'},
        }
    }

    schema.spaces.test.indexes = {err_primary}
    local ok, err = ddl.set_schema(schema)
    t.assert_equals(ok, nil)
    t.assert_equals(err,
        'spaces["test"].indexes["primary"].parts[1].type: type differs from' ..
        ' spaces["test"].format["decimal_nonnull"].type' ..
        ' (decimal expected, got double)'
    )
end

function g.test_tarantool_2_4_types()
    local spaces = table.deepcopy(test_space)
    spaces.test.format = {
        {name = 'uuid_nonnull', type = 'uuid', is_nullable = false},
        {name = 'uuid_nullable', type = 'uuid', is_nullable = true},
    }

    local schema = {spaces = spaces}

    local ok_primary = {
        name = 'primary',
        type = 'TREE',
        unique = true,
        parts = {
            {is_nullable = false, path = 'uuid_nonnull', type = 'uuid'},
        }
    }

    schema.spaces.test.indexes = {ok_primary}
    local ok, err = ddl.set_schema(schema)

    if not db.v(2, 4) then
        t.assert_equals(ok, nil)
        t.assert_equals(err,
            [[spaces["test"]: Failed to create space 'test':]] ..
            [[ field 1 has unknown field type]]
        )
        t.success()
    end

    t.assert_equals(err, nil)
    t.assert_equals(ok, true)
end

function g.test_transactional_ddl()
    local spaces = {
        test = {
            engine = 'memtx',
            is_local = true,
            temporary = false,
            format = {
                {name = 'id', type = 'unsigned', is_nullable = false},
                {name = 'bucket_id', is_nullable = false, type = 'unsigned'},
            },
            indexes = {{
                name = 'doomed',
                type = 'TREE',
                parts = {{path = 'id', type = 'unsigned', is_nullable = false}},
                unique = true,
            }, {
                name = 'bucket_id',
                type = 'TREE',
                unique = false,
                parts = {{path = 'bucket_id', is_nullable = false, type = 'unsigned'}}
            }},
            sharding_key = {'id'},
        }
    }

    -- Cause ddl.check_schema failure
    local function dummy_failure(_, new)
        if box.space[new.id].name == '_ddl_dummy'
        and new.name == 'doomed'
        then
            error('Dummy index creation is doomed', 0)
        end
    end

    -- Cause ddl.set_schema failure
    local function actual_failure(_, new)
        if box.space[new.id].name ~= '_ddl_dummy'
        and new.name == 'doomed'
        then
            error('Actual index creation is doomed', 0)
        end
    end

    local lsn1 = box.info.lsn

    box.space._index:on_replace(dummy_failure)

    t.assert_equals(
        {ddl.set_schema({spaces = spaces})},
        {nil, 'spaces["test"].indexes["doomed"]: Dummy index creation is doomed'}
    )

    local lsn2 = box.info.lsn
    if db.v(2, 2) then
        t.assert_equals(lsn2, lsn1)
    else
        t.assert_not_equals(lsn2, lsn1)
    end
    t.assert_not(box.is_in_txn())

    box.space._index:on_replace(actual_failure, dummy_failure)

    t.assert_error_msg_equals(
        'spaces["test"].indexes["doomed"]: Actual index creation is doomed',
        function() ddl.set_schema({spaces = spaces}) end
    )

    local lsn3 = box.info.lsn
    if db.v(2, 2) then
        t.assert_equals(lsn3, lsn2)
    else
        t.assert_not_equals(lsn3, lsn2)
    end
    t.assert_not(box.is_in_txn())

    box.space._index:on_replace(nil, actual_failure)
end
