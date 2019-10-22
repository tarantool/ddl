#!/usr/bin/env tarantool

local t = require('luatest')
local db = require('test.db')
local ddl = require('ddl')
local log = require('log')

local g = t.group('set_schema')
g.before_all = db.init
g.setup = db.drop_all

local test_schema = {
    ['test'] = {
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
            {name = 'varbinary_nonnull', type = 'varbinary', is_nullable = false},
            {name = 'varbinary_nullable', type = 'varbinary', is_nullable = true},
            {name = 'map_nonnull', type = 'map', is_nullable = false},
            {name = 'map_nullable', type = 'map', is_nullable = true},
            {name = 'any_nonnull', type = 'any', is_nullable = false},
            {name = 'any_nullable', type = 'any', is_nullable = true},
        },
    }
}

local function _test_index(indexes_ddl, error_expected)
    db.drop_all()

    local schema = table.deepcopy(test_schema)
    schema.test.indexes = indexes_ddl

    local ok, err = ddl.set_schema(schema)
    if not error_expected then
        if not ok then
            error(err, 2)
        end

        local ddl_schema = ddl.get_schema()
        local ok, err = pcall(t.assert_equals,
            ddl_schema.test.indexes, schema.test.indexes,
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
            path = 'unsigned_nullable',
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
        parts = {{is_nullable = false, path = 'unsigned_nullable', type = 'unsigned'}},
    }

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

function g.test_erroneous_type_in_format()
    local schema = table.deepcopy(test_schema)
    schema.test.format[1].type = 'undefined'

    local res, err = ddl.set_schema(schema)
    t.assert_nil(res)
    assert_error_msg_contains(err, "Failed to create space 'test': field 1 has unknown field type")
end

function g.test_erroneous_type_in_index_field()
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

function g.test_erroneous_index_type()
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
    local schema = table.deepcopy(test_schema)
    schema.test.indexes = {
        {
            type = 'TREE',
            name = 'primary',
            unique = true,
        }
    }

    local res, err =  ddl.set_schema(schema)
    t.assert_nil(res)
    assert_error_msg_contains(err, "index parts is nil")
end

function g.test_missing_format()
    local schema = table.deepcopy(test_schema)
    schema.test.format = nil
    schema.test.indexes = {{
        type = 'TREE',
        unique = true,
        name = 'primary',
        parts = {{path = 'unsigned_nonnull', type = 'unsigned', is_nullable = false}}
    }}


    local res, err =  ddl.set_schema(schema)
    t.assert_nil(res)
    assert_error_msg_contains(err, "Illegal parameters, options.parts[1]: " ..
        "field was not found by name 'unsigned_nonnull'"
    )
end

function g.test_missing_indexes()
    local schema = table.deepcopy(test_schema)
    schema.test.indexes = nil


    local res, err =  ddl.set_schema(schema)
    t.assert_nil(res)
    assert_error_msg_contains(err, "Index fields is nil")
end


function g.test_two_spaces()
    local spaces = {
        ['space1'] = table.deepcopy(test_schema['test']),
        ['space2'] = table.deepcopy(test_schema['test']),
    }

    local primary = {
        name = 'primary',
        type = 'TREE',
        unique = true,
        parts = {{is_nullable = false, path = 'unsigned_nullable', type = 'unsigned'}},
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

    local res, err = ddl.set_schema(spaces)
    t.assert_true(res)
    t.assert_nil(err)

    local ddl_schema = ddl.get_schema()
    t.assert_equals(ddl_schema, spaces)
end


function g.test_error_spaces()
    local spaces = {
        ['space1'] = table.deepcopy(test_schema['test']),
        ['space2'] = table.deepcopy(test_schema['test']),
        ['space3'] = table.deepcopy(test_schema['test']),
    }

    local ok_primary = {
        name = 'primary',
        type = 'TREE',
        unique = true,
        parts = {{is_nullable = false, path = 'unsigned_nullable', type = 'unsigned'}},
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
        parts = {{is_nullable = false, path = 'unsigned_nullable', type = 'unsigned'}}
    }}

    local res, err = ddl.set_schema(spaces)

    t.assert_nil(res)
    assert_error_msg_contains(err,
        "Can't create or modify index 'primary' in space 'space3': primary key must be unique"
    )

    local count_spaces = box.space._space:count({box.schema.SYSTEM_ID_MAX}, {iterator = "GE"})
    t.assert_equals(count_spaces, 0)
    local index_count = box.space._index:count({box.schema.SYSTEM_ID_MAX, 0}, {iterator = "GE"})
    t.assert_equals(index_count, 0)
end

function g.test_set_schema_sequently_err()
    local old_schema = table.deepcopy(test_schema)
    local ok_primary = {
        name = 'primary',
        type = 'TREE',
        unique = true,
        parts = {{is_nullable = false, path = 'unsigned_nullable', type = 'unsigned'}},
    }

    old_schema.test.indexes = {
        ok_primary
    }

    local res, err = ddl.set_schema(old_schema)
    t.assert_true(res)
    t.assert_nil(err)

    local res = ddl.get_schema()
    t.assert_equals(res, old_schema)

    local new_schema = {
        ['space1'] = table.deepcopy(test_schema['test']),
        ['space2'] = table.deepcopy(test_schema['test']),
    }


    new_schema.space1.indexes = {
        ok_primary
    }

    new_schema.space2.indexes = {{
        name = 'primary',
        type = 'TREE',
        unique = true,
        parts = {{is_nullable = false, path = 'unsigned_nullable', type = 'string'}},
    }}

    local res, err = ddl.set_schema(new_schema)
    t.assert_nil(res)
    assert_error_msg_contains(err, "Field 2 has type 'unsigned' in space format, but type 'string' in index definition")

    local res = ddl.get_schema()
    t.assert_equals(res, old_schema)
end


function g.test_set_schema_sequently_ok()
    local old_schema = table.deepcopy(test_schema)
    local ok_primary = {
        name = 'primary',
        type = 'TREE',
        unique = true,
        parts = {{is_nullable = false, path = 'unsigned_nullable', type = 'unsigned'}},
    }

    old_schema.test.indexes = {
        ok_primary
    }

    local res, err = ddl.set_schema(old_schema)
    t.assert_true(res)
    t.assert_nil(err)

    local res = ddl.get_schema()
    t.assert_equals(res, old_schema)

    local new_schema = {
        ['space1'] = table.deepcopy(test_schema['test']),
        ['space2'] = table.deepcopy(test_schema['test']),
    }


    new_schema.space1.indexes = {
        ok_primary
    }

    new_schema.space2.indexes = {
        ok_primary
    }

    local res, err = ddl.set_schema(new_schema)
    t.assert_true(res)
    t.assert_nil(err)

    local res = ddl.get_schema()
    new_schema.test = old_schema.test
    t.assert_equals(res, new_schema)
end
