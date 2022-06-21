#!/usr/bin/env tarantool

local t = require('luatest')
local db = require('test.db')
local ddl_check = require('ddl.check')
local ddl = require('ddl')

local helper = require('test.helper')

local g = t.group()
g.before_all(db.init)
g.before_each(db.drop_all)

local space_format = helper.test_space_format()
table.insert(space_format, {name = 'decimal_nonnull', type = 'decimal', is_nullable = false})
table.insert(space_format, {name = 'decimal_null', type = 'decimal', is_nullable = true})
table.insert(space_format, {name = 'double_nonnull', type = 'double', is_nullable = false})
table.insert(space_format, {name = 'double_null', type = 'double', is_nullable = true})
table.insert(space_format, {name = 'uuid_nonnull', type = 'uuid', is_nullable = false})
table.insert(space_format, {name = 'uuid_null', type = 'uuid', is_nullable = true})
table.insert(space_format, {name = 'annotated', type = 'any', is_nullable = true, comment = 'x'})

local test_space = {
    engine = 'memtx',
    is_local = true,
    temporary = false,
    format = space_format
}

local test_indexes = {{
        type = 'HASH',
        unique = true,
        parts = {
            {path = 'string_nonnull', is_nullable = false, type = 'string'},
            {path = 'unsigned_nonnull', is_nullable = false, type = 'unsigned'},
        },
        name = 'primary'
    }, {
        type = 'TREE',
        unique = false,
        parts = {
            {path = 'integer_nonnull', is_nullable = false, type = 'integer'},
            {path = 'unsigned_nonnull', is_nullable = false, type = 'unsigned'},
        },
        name = 'secondary'
    }, {
        type = 'BITSET',
        unique = false,
        parts = {
            {path = 'string_nonnull', is_nullable = false, type = 'string'},
        },
        name = 'third'
    }, {
        type = 'RTREE',
        unique = false,
        dimension = 10,
        distance = 'manhattan',
        parts = {
            {path = 'array_nonnull', is_nullable = false, type = 'array'},
        },
        name = 'fourth'
    }
}

local function init_space_info(space)
    local space_info = {
        name = 'space',
        engine = 'memtx',
        fields = {},
    }

    for _, v in ipairs(space.format) do
        space_info.fields[v.name] = {
            type = v.type,
            is_nullable = v.is_nullable
        }
    end
    return space_info
end

local test_space_info = init_space_info(test_space)


function g.test_index_part_collation()
    local ok, err = ddl_check.check_index_part_collation('unicode')
    t.assert(ok)
    t.assert_not(err)

    local ok, err = ddl_check.check_index_part_collation(nil)
    t.assert(ok)
    t.assert_not(err)

    local ok, err = ddl_check.check_index_part_collation(5)

    t.assert_not(ok)
    t.assert_equals(err,
        "bad value (?string expected, got number)"
    )

    local ok, err = ddl_check.check_index_part_collation("undefined")
    t.assert_not(ok)
    t.assert_equals(err, 'unknown collation "undefined"')
end


function g.test_index_part_type()
    local ok, err = ddl_check.check_index_part_type(nil, 'TREE')
    t.assert_not(ok)
    t.assert_equals(err, "bad value (string expected, got nil)")

    local ok, err = ddl_check.check_index_part_type('undefined', 'TREE')
    t.assert_not(ok)
    t.assert_equals(err, 'unknown type "undefined"')

    local ok, err = ddl_check.check_index_part_type('string', 'TREE')
    t.assert(ok)
    t.assert_not(err)

    local ok, err = ddl_check.check_index_part_type('array', 'TREE')
    t.assert_not(ok)
    t.assert_equals(err, "array field type is unsupported in TREE index type")


    local ok, err = ddl_check.check_index_part_type('unsigned', 'HASH')
    t.assert(ok)
    t.assert_not(err)

    local ok, err = ddl_check.check_index_part_type('array', 'HASH')
    t.assert_not(ok)
    t.assert_equals(err, "array field type is unsupported in HASH index type")


    local ok, err = ddl_check.check_index_part_type('unsigned', 'BITSET')
    t.assert(ok)
    t.assert_not(err)

    local ok, err = ddl_check.check_index_part_type('string', 'BITSET')
    t.assert(ok)
    t.assert_not(err)

    local ok, err = ddl_check.check_index_part_type('integer', 'BITSET')
    t.assert_not(ok)
    t.assert_equals(err, "integer field type is unsupported in BITSET index type")


    local ok, err = ddl_check.check_index_part_type('array', 'RTREE')
    t.assert(ok)
    t.assert_not(err)

    local ok, err = ddl_check.check_index_part_type('string', 'RTREE')
    t.assert_not(ok)
    t.assert_equals(err, "string field type is unsupported in RTREE index type")

    local ok, err = ddl_check.check_index_part_type('uuid', 'HASH')
    t.assert(ok)
    t.assert_not(err)

    local ok, err = ddl_check.check_index_part_type('uuid', 'TREE')
    t.assert(ok)
    t.assert_not(err)

    local ok, err = ddl_check.check_index_part_type('uuid', 'BITSET')
    t.assert_not(ok)
    t.assert_equals(err, "uuid field type is unsupported in BITSET index type")

    local ok, err = ddl_check.check_index_part_type('uuid', 'RTREE')
    t.assert_not(ok)
    t.assert_equals(err, "uuid field type is unsupported in RTREE index type")
end

function g.test_varbinaty_index_part_type()
    if db.v(2, 2) then
        local ok, err = ddl_check.check_index_part_type('varbinary', 'TREE')
        t.assert(ok)
        t.assert_not(err)
    else
        local ok, err = ddl_check.check_index_part_type('varbinary', 'TREE')
        t.assert_not(ok)
        t.assert_equals(err, string.format(
            "varbinary type isn't allowed in your Tarantool version (%s)",
            _TARANTOOL
        ))
    end
end

function g.test_datetime_index_part_type()
    if db.v(2, 10) then
        local ok, err = ddl_check.check_index_part_type('datetime', 'TREE')
        t.assert(ok)
        t.assert_not(err)
    else
        local ok, err = ddl_check.check_index_part_type('datetime', 'TREE')
        t.assert_not(ok)
        t.assert_equals(err, string.format(
            "datetime type isn't allowed in your Tarantool version (%s)",
            _TARANTOOL
        ))
    end
end

function g.test_index_part_path()
    local index_info = {type = 'HASH'}

    local ok, err = ddl_check.check_index_part_path(nil, index_info, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err, "bad value (string expected, got nil)")

    local ok, err = ddl_check.check_index_part_path(5, index_info, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err, "bad value (string expected, got number)")


    local ok, err = ddl_check.check_index_part_path('unsigned_nonnull', index_info, test_space_info)
    t.assert(ok)
    t.assert_not(err)

    local ok, err = ddl_check.check_index_part_path('no_reference', index_info, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err, 'path (no_reference) referencing to unknown field')
end

function g.test_index_part_json_path()
    if not db.v(2, 0) then
        local ok, err = ddl_check.check_index_part_path(
            'map_nonnull.data.name', {type = 'HASH'}, test_space_info
        )
        t.assert_equals(err, string.format(
            "path (map_nonnull.data.name) is JSONPath," ..
            " but your Tarantool version (%s) doesn't support this",
            _TARANTOOL
        ))
        t.assert_not(ok)

        t.success()
    end

    local ok, err = ddl_check.check_index_part_path(
        'map_nonnull.data.name', {type = 'BITSET'}, test_space_info
    )
    t.assert(ok)
    t.assert_not(err)

    local ok, err = ddl_check.check_index_part_path(
        'map_nonnull.data[*].name', {type = 'TREE'}, test_space_info
    )

    if db.v(2, 2) then
        t.assert(ok)
        t.assert_not(err)
    else
        t.assert_not(ok)
        t.assert_equals(err, string.format(
            [[JSONPath (map_nonnull.data[*].name) has wildcard, but your Tarantool version]] ..
            [[ (%s) doesn't support this]], _TARANTOOL
        ))
        t.success()
    end

    local ok, err = ddl_check.check_index_part_path('array_nonnull[*]', {type = 'TREE'}, test_space_info)
    t.assert_not(err)
    t.assert(ok)

    local ok, err = ddl_check.check_index_part_path(
        'map_nonnull.data[*].name', {type = 'HASH'}, test_space_info
    )
    t.assert_not(ok)
    t.assert_equals(err,
        "JSONPath (map_nonnull.data[*].name) has wildcard," ..
        " but index type HASH doesn't allow this"
    )

    local ok, err = ddl_check.check_index_part_path(
        'map_nonnull.data[*].name', {type = 'BITSET'}, test_space_info
    )
    t.assert_not(ok)
    t.assert_equals(err,
        "JSONPath (map_nonnull.data[*].name) has wildcard," ..
        " but index type BITSET doesn't allow this"
    )

    local ok, err = ddl_check.check_index_part_path(
        'map_nonnull.data[*].name', {type = 'RTREE'}, test_space_info
    )
    t.assert_not(ok)
    t.assert_equals(err,
        "JSONPath (map_nonnull.data[*].name) has wildcard," ..
        " but index type RTREE doesn't allow this"
    )

    local ok, err = ddl_check.check_index_part_path(
        'map_nonnull.data[*].name', {type = 'HASH'}, test_space_info
    )
    t.assert_not(ok)
    t.assert_equals(err,
        "JSONPath (map_nonnull.data[*].name) has wildcard," ..
        " but index type HASH doesn't allow this"
    )
end

function g.test_index_part_types_equality()
    local index = {
        type = 'HASH',
        name = 'primary',
        parts = {
            {path = 'unsigned_nonnull', type = 'unsigned', is_nullable = false},
            {path = 'any_nonnull', type = 'string', is_nullable = false},
            {path = 'string_nonnull', type = 'unsigned', is_nullable = false},
            {path = 'string_nonnull', type = 'integer', is_nullable = false},
            {path = 'map_nonnull.data.name', type = 'string', is_nullable = false},
            {path = 'map_nonnull', type = 'string', is_nullable = false},
        }
    }

    local ok, err = ddl_check.check_index_part(1, index, test_space_info)
    t.assert(ok)
    t.assert_not(err)

    local ok, err = ddl_check.check_index_part(2, index, test_space_info)
    t.assert(ok)
    t.assert_not(err)

    local ok, err = ddl_check.check_index_part(3, index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        'spaces["space"].indexes["primary"].parts[3].type: type differs from ' ..
        'spaces["space"].format["string_nonnull"].type (string expected, got unsigned)'
    )

    local ok, err = ddl_check.check_index_part(4, index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        'spaces["space"].indexes["primary"].parts[4].type: type differs from ' ..
        'spaces["space"].format["string_nonnull"].type (string expected, got integer)'
    )

    if db.v(2, 0) then
        local ok, err = ddl_check.check_index_part(5, index, test_space_info)
        t.assert(ok)
        t.assert_not(err)

        local ok, err = ddl_check.check_index_part(6, index, test_space_info)
        t.assert_not(ok)
        t.assert_equals(err,
            'spaces["space"].indexes["primary"].parts[6].type: type differs ' ..
            'from spaces["space"].format["map_nonnull"].type (map expected, got string)'
        )
    end
end


function g.test_index_part_nullable_equality()
    local index = {
        type = 'HASH',
        name = 'primary',
        parts = {
            {path = 'unsigned_nonnull', type = 'unsigned', is_nullable = false},
            {path = 'string_nonnull', type = 'string', is_nullable = false},
            {path = 'string_nonnull', type = 'string', is_nullable = nil},
            {path = 'string_nonnull', type = 'string', is_nullable = 5},
            {path = 'string_nonnull', type = 'string', is_nullable = true},
            {path = 'string_nullable', type = 'string', is_nullable = false},
        }
    }

    local ok, err = ddl_check.check_index_part(1, index, test_space_info)
    t.assert(ok)
    t.assert_not(err)

    local ok, err = ddl_check.check_index_part(2, index, test_space_info)
    t.assert(ok)
    t.assert_not(err)

    local ok, err = ddl_check.check_index_part(3, index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        [[spaces["space"].indexes["primary"].parts[3].is_nullable:]] ..
        [[ bad value (boolean expected, got nil)]]
    )

    local ok, err = ddl_check.check_index_part(4, index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        [[spaces["space"].indexes["primary"].parts[4].is_nullable:]] ..
        [[ bad value (boolean expected, got number)]]
    )

    local ok, err = ddl_check.check_index_part(5, index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        'spaces["space"].indexes["primary"].parts[5].is_nullable:' ..
        ' has different nullability with' ..
        ' spaces["space"].format["string_nonnull"].is_nullable (false expected, got true)'
    )

    local ok, err = ddl_check.check_index_part(6, index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        'spaces["space"].indexes["primary"].parts[6].is_nullable:' ..
        ' has different nullability with' ..
        ' spaces["space"].format["string_nullable"].is_nullable (true expected, got false)'
    )
end

function g.test_invalid_index_part()
    local index = {
        type = 'HASH',
        name = 'primary',
        parts = {},
        unique = true
    }

    -- here check that part is not a table
    local ok, err = ddl_check.check_index_part(1, index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        'spaces["space"].indexes["primary"].parts[1]: bad value' ..
        ' (table expected, got nil)'
    )

    index.parts = {5}
    local ok, err = ddl_check.check_index_part(1, index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        'spaces["space"].indexes["primary"].parts[1]: bad value' ..
        ' (table expected, got number)'
    )
end


-- maybe refactor to some tests
function g.test_index_part()
    local index = {
        type = 'HASH',
        name = 'primary',
        parts = {},
        unique = true
    }

    -- check no reference to field
    index.parts = {
        {path = 'no_reference', type = 'unsigned', is_nullable = false}
    }
    local ok, err = ddl_check.check_index_part(1, index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        'spaces["space"].indexes["primary"].parts[1].path: path (no_reference)' ..
        ' referencing to unknown field'
    )

    -- check one ok collation
    index.parts = {
        {path = 'string_nonnull', type = 'string', is_nullable = false}
    }
    local ok, err = ddl_check.check_index_part(1, index, test_space_info)
    t.assert(ok)
    t.assert_not(err)

    index.parts = {
        {path = 'string_nonnull', type = 'string', is_nullable = false, collation = 'unicode'}
    }
    local ok, err = ddl_check.check_index_part(1, index, test_space_info)
    t.assert(ok)
    t.assert_not(err)

    -- check unknown collation
    index.parts = {
        {path = 'string_nonnull', type = 'string', is_nullable = false, collation = 'undefined'}
    }
    local ok, err = ddl_check.check_index_part(1, index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        'spaces["space"].indexes["primary"].parts[1].collation: unknown collation "undefined"'
    )

    -- check collation with not string type
    index.parts = {
        {path = 'unsigned_nonnull', type = 'unsigned', is_nullable = false, collation = 'undefined'}
    }
    local ok, err = ddl_check.check_index_part(1, index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        [[spaces["space"].indexes["primary"].parts[1].collation: type unsigned]] ..
        [[ doesn't allow collation (only string type does)]]
    )

    -- chceck index part types equality
    index.parts = {
        {path = 'string_nonnull', type = 'string', is_nullable = false, collation = 'unicode'}
    }
    local ok, err = ddl_check.check_index_part(1, index, test_space_info)
    t.assert(ok)
    t.assert_not(err)

    index.parts = {
        {path = 'string_nonnull', type = 'unsigned', is_nullable = false, collation = 'unicode'}
    }
    local ok, err = ddl_check.check_index_part(1, index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        'spaces["space"].indexes["primary"].parts[1].type: type differs from ' ..
        'spaces["space"].format["string_nonnull"].type (string expected, got unsigned)'
    )

    index.parts = {
        {path = 'string_nonnull', type = 'undefined', is_nullable = false, collation = 'binary'}
    }
    local ok, err = ddl_check.check_index_part(1, index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        'spaces["space"].indexes["primary"].parts[1].type: unknown type "undefined"'
    )
end


function g.test_index_parts()
    local index = {
        type = 'TREE',
        name = 'primary',
        parts = {
            {path = 'string_nonnull', type = 'string', is_nullable = false},
            {path = 'unsigned_nonnull', type = 'unsigned', is_nullable = false},
        }
    }

    local ok, err = ddl_check.check_index_parts(index, test_space_info)
    t.assert(ok)
    t.assert_not(err)


    index.parts[1] = {path = 'no_reference', type = 'string', is_nullable = false}
    local ok, err = ddl_check.check_index_parts(index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        'spaces["space"].indexes["primary"].parts[1].path: ' ..
        'path (no_reference) referencing to unknown field'
    )


    index.parts = nil
    local ok, err = ddl_check.check_index_parts(index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        [[spaces["space"].indexes["primary"].parts: bad value]] ..
        [[ (contiguous array of tables expected, got nil)]]
    )


    index.parts = 5
    local ok, err = ddl_check.check_index_parts(index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        [[spaces["space"].indexes["primary"].parts: bad value]] ..
        [[ (contiguous array of tables expected, got number)]]
    )


    index.parts = {
        key = 'value',
        {path = 'string_nonnull', type = 'string', is_nullable = false}
    }
    local ok, err = ddl_check.check_index_parts(index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        [[spaces["space"].indexes["primary"].parts: bad value]] ..
        [[ (contiguous array of tables expected, got table)]]
    )

    local index_with_same_parts = {
        type = 'HASH',
        name = 'hash',
        parts = {
            {path = 'string_nonnull', type = 'string', is_nullable = false},
            {path = 'string_nonnull', type = 'string', is_nullable = false},
        }
    }
    local ok, err = ddl_check.check_index_parts(index_with_same_parts, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        'spaces["space"].indexes["hash"].parts[2]: field ' ..
        '"string_nonnull" is already indexed in parts[1]'
    )
end


function g.test_check_index_hash()
    local hash_index = {
        type = 'HASH',
        name = 'hash',
        unique = true,
        parts = {
            {path = 'string_nonnull', type = 'string', is_nullable = false},
            {path = 'unsigned_nonnull', type = 'unsigned', is_nullable = false},
        }
    }

    local ok, err = ddl_check.check_index(2, hash_index, test_space_info)
    t.assert(ok)
    t.assert_not(err)


    local bad_index = table.deepcopy(hash_index)
    bad_index.unique = false

    local ok, err = ddl_check.check_index(2, bad_index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err, 'spaces["space"].indexes["hash"]: HASH index must be unique')


    local bad_index = table.deepcopy(hash_index)
    bad_index.parts = {{path = 'array_nonnull', type = 'array', is_nullable = false}}

    local ok, err = ddl_check.check_index(2, bad_index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        'spaces["space"].indexes["hash"].parts[1].type: array field' ..
        ' type is unsupported in HASH index type'
    )


    if db.v(2, 2) then
        local bad_index = table.deepcopy(hash_index)
        bad_index.parts[3] = {
            path = 'map_nonnull.data[*].name', type = 'string', is_nullable = false
        }

        local ok, err = ddl_check.check_index(2, bad_index, test_space_info)
        t.assert_not(ok)
        t.assert_equals(err,
            [[spaces["space"].indexes["hash"].parts[3].path: JSONPath (map_nonnull.data[*].name)]] ..
            [[ has wildcard, but index type HASH doesn't allow this]]
        )
    end
end

function g.test_tree_index()
    local tree_index = {
        type = 'TREE',
        name = 'tree',
        unique = true,
        parts = {
            {path = 'string_nonnull', type = 'string', is_nullable = false},
            {path = 'unsigned_nonnull', type = 'unsigned', is_nullable = false}
        }
    }

    local ok, err = ddl_check.check_index(2, tree_index, test_space_info)
    t.assert(ok)
    t.assert_not(err)


    tree_index.parts = {{path = 'array_nonnull', type = 'array', is_nullable = false}}

    local ok, err = ddl_check.check_index(2, tree_index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        'spaces["space"].indexes["tree"].parts[1].type: array' ..
        ' field type is unsupported in TREE index type'
    )
end


function g.test_bitset_index()
    local bitset_index = {
        type = 'BITSET',
        name = 'bitset',
        unique = false,
        parts = {
            {path = 'string_nonnull', type = 'string', is_nullable = false},
        }
    }

    local ok, err = ddl_check.check_index(2, bitset_index, test_space_info)
    t.assert(ok)
    t.assert_not(err)


    local bad_index = table.deepcopy(bitset_index)
    bad_index.unique = true
    local ok, err = ddl_check.check_index(2, bad_index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err, [[spaces["space"].indexes["bitset"]: BITSET index can't be unique]])


    local bad_index = table.deepcopy(bitset_index)
    bad_index.parts = {{path = 'string_nullable', type = 'string', is_nullable = true}}
    local ok, err = ddl_check.check_index(2, bad_index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        [[spaces["space"].indexes["bitset"].parts[1]: index of BITSET type doesn't]] ..
        [[ support nullable fields]]
    )


    local bad_index = table.deepcopy(bitset_index)
    bad_index.parts = {{path = 'integer_nonnull', type = 'integer', is_nullable = false}}
    local ok, err = ddl_check.check_index(2, bad_index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        [[spaces["space"].indexes["bitset"].parts[1].type: integer field]] ..
        [[ type is unsupported in BITSET index type]]
    )


    bitset_index.parts[2] = {path = 'unsigned_nonnull', type = 'unsigned', is_nullable = false}
    local ok, err = ddl_check.check_index(2, bitset_index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        [[spaces["space"].indexes["bitset"].parts: index of BITSET type can't]] ..
        [[ be composite (currently, index contains 2 parts)]]
    )
end


function g.test_rtree_index()
    local rtree_index = {
        type = 'RTREE',
        name = 'rtree',
        unique = false,
        dimension = 5,
        distance = 'manhattan',
        parts = {
            {path = 'array_nonnull', type = 'array', is_nullable = false},
        }
    }

    local ok, err = ddl_check.check_index(2, rtree_index, test_space_info)
    t.assert(ok)
    t.assert_not(err)


    local bad_index = table.deepcopy(rtree_index)
    bad_index.unique = true
    local ok, err = ddl_check.check_index(2, bad_index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err, [[spaces["space"].indexes["rtree"]: RTREE index can't be unique]])


    local bad_index = table.deepcopy(rtree_index)
    bad_index.parts = {{path = 'array_nullable', type = 'array', is_nullable = true}}
    local ok, err = ddl_check.check_index(2, bad_index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        [[spaces["space"].indexes["rtree"].parts[1]: index of RTREE type doesn't]] ..
        [[ support nullable fields]]
    )


    local bad_index = table.deepcopy(rtree_index)
    bad_index.parts = {{path = 'string_nonnull', type = 'string', is_nullable = false}}
    local ok, err = ddl_check.check_index(2, bad_index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        [[spaces["space"].indexes["rtree"].parts[1].type: string field]] ..
        [[ type is unsupported in RTREE index type]]
    )


    local bad_index = table.deepcopy(rtree_index)
    bad_index.dimension = nil
    local ok, err = ddl_check.check_index(2, bad_index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        [[spaces["space"].indexes["rtree"].dimension: bad value]] ..
        [[ (number in range [1, 20] expected, got nil)]]
    )


    local bad_index = table.deepcopy(rtree_index)
    bad_index.dimension = -1
    local ok, err = ddl_check.check_index(2, bad_index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        [[spaces["space"].indexes["rtree"].dimension: incorrect value]] ..
        [[ (must be in range [1, 20], got -1)]]
    )


    local bad_index = table.deepcopy(rtree_index)
    bad_index.distance = nil
    local ok, err = ddl_check.check_index(2, bad_index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        [[spaces["space"].indexes["rtree"].distance: bad value]] ..
        [[ (string expected, got nil)]]
    )

    local bad_index = table.deepcopy(rtree_index)
    bad_index.distance = 'unknown'
    local ok, err = ddl_check.check_index(2, bad_index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        'spaces["space"].indexes["rtree"].distance: unknown distance "unknown"'
    )


    rtree_index.parts[2] = {path = 'array_nullable', type = 'array', is_nullable = true}
    local ok, err = ddl_check.check_index(2, rtree_index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        [[spaces["space"].indexes["rtree"].parts: index of RTREE type can't]] ..
        [[ be composite (currently, index contains 2 parts)]]
    )
end


function g.test_invalid_index()

    local ok, err = ddl_check.check_index(1, nil, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err, 'spaces["space"].indexes[1]: bad value (table expected, got nil)')


    local ok, err = ddl_check.check_index(1, 5, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err, 'spaces["space"].indexes[1]: bad value (table expected, got number)')


    local bad_index = {}
    local ok, err = ddl_check.check_index(1, bad_index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err, [[spaces["space"].indexes[1].name: bad value (string expected, got nil)]])


    bad_index.name = 'primary'
    local ok, err = ddl_check.check_index(1, bad_index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        [[spaces["space"].indexes["primary"].unique: bad value (boolean expected, got nil)]]
    )


    bad_index.unique = false
    local ok, err = ddl_check.check_index(1, bad_index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        [[spaces["space"].indexes["primary"].type: bad value (string expected, got nil)]]
    )


    bad_index.type = 'BTREE'
    local ok, err = ddl_check.check_index(1, bad_index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err, 'spaces["space"].indexes["primary"].type: unknown type "BTREE"')


    bad_index.type = 'HASH'
    local space_info = table.deepcopy(test_space_info)
    space_info.engine = 'vinyl'

    local ok, err = ddl_check.check_index(1, bad_index, space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        [[spaces["space"].indexes["primary"]: vinyl engine doesn't support HASH index type]]
    )


    local good_index = {
        type = 'TREE',
        name = 'primary',
        unique = false,
        parts = {}
    }

    local bad_index = table.deepcopy(good_index)
    bad_index.parts = {{path = 'string_nullable', type = 'string', is_nullable = false}}

    local ok, err = ddl_check.check_index(2, bad_index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        [[spaces["space"].indexes["primary"].parts[1].is_nullable: has different nullability]] ..
        [[ with spaces["space"].format["string_nullable"].is_nullable (true expected, got false)]]
    )


    local bad_index = table.deepcopy(good_index)
    bad_index.parts = nil

    local ok, err = ddl_check.check_index(2, bad_index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        [[spaces["space"].indexes["primary"].parts: bad value]] ..
        [[ (contiguous array of tables expected, got nil)]]
    )
end


function g.test_primary_index()
    local good_index = {
        type = 'TREE',
        name = 'primary',
        unique = true,
        parts = {
            {path = 'string_nonnull', type = 'string', is_nullable = false},
            {path = 'unsigned_nonnull', type = 'unsigned', is_nullable = false}
        }
    }

    local ok, err = ddl_check.check_index(1, good_index, test_space_info)
    t.assert(ok)
    t.assert_not(err)

    local bad_index = table.deepcopy(good_index)
    bad_index.unique = false

    local ok, err = ddl_check.check_index(1, bad_index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err, 'spaces["space"].indexes["primary"]: primary TREE index must be unique')


    local bad_index = table.deepcopy(good_index)
    bad_index.parts[3] = {path = 'map_nonnull.data[*].name', type = 'string', is_nullable = false}
    if db.v(2, 2) then
        local ok, err = ddl_check.check_index(1, bad_index, test_space_info)
        t.assert_not(ok)
        t.assert_equals(err,
            [[spaces["space"].indexes["primary"].parts[3].path: primary index doesn't]] ..
            [[ allow JSONPath wildcard (path map_nonnull.data[*].name has wildcard)]]
        )
    end

    local bad_index = table.deepcopy(good_index)
    bad_index.type = 'BITSET'

    local ok, err = ddl_check.check_index(1, bad_index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err, [[spaces["space"].indexes["primary"]: BITSET index can't be primary]])


    local bad_index = table.deepcopy(good_index)
    bad_index.type = 'RTREE'

    local ok, err = ddl_check.check_index(1, bad_index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err, [[spaces["space"].indexes["primary"]: RTREE index can't be primary]])


    local bad_index = table.deepcopy(good_index)
    bad_index.parts[3] = {path = 'string_nullable', type = 'string', is_nullable = true}

    local ok, err = ddl_check.check_index(1, bad_index, test_space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        [[spaces["space"].indexes["primary"].parts[3].is_nullable: primary index can't contain nullable parts]]
    )
end

function g.test_invalid_space()
    local space = {}

    local ok, err = ddl_check.check_space(4, {})
    t.assert_not(ok)
    t.assert_equals(err, 'spaces[4]: invalid space name (string expected, got number)')

    local ok, err = ddl_check.check_space('space', 5)
    t.assert_not(ok)
    t.assert_equals(err, 'spaces["space"]: bad value (table expected, got number)')


    local ok, err = ddl_check.check_space('space', space)
    t.assert_not(ok)
    t.assert_equals(err, 'spaces["space"].engine: bad value (string expected, got nil)')


    space.engine = 'undefined'
    local ok, err = ddl_check.check_space('space', space)
    t.assert_not(ok)
    t.assert_equals(err, 'spaces["space"].engine: unknown engine "undefined"')


    space.engine = 'vinyl'
    local ok, err = ddl_check.check_space('space', space)
    t.assert_not(ok)
    t.assert_equals(err, 'spaces["space"].is_local: bad value (boolean expected, got nil)')


    space.is_local = true
    local ok, err = ddl_check.check_space('space', space)
    t.assert_not(ok)
    t.assert_equals(err, 'spaces["space"].temporary: bad value (boolean expected, got nil)')


    space.temporary = true
    local ok, err = ddl_check.check_space('space', space)
    t.assert_not(ok)
    t.assert_equals(err, [[spaces["space"]: vinyl engine doesn't support temporary spaces]])


    space.temporary = false
    local ok, err = ddl_check.check_space('space', space)
    t.assert_not(ok)
    t.assert_equals(err, 'spaces["space"].format: bad value (contiguous array expected, got nil)')


    space.format = {}
    local ok, err = ddl_check.check_space('space', space)
    t.assert_not(ok)
    t.assert_equals(err,
        'spaces["space"].indexes: bad value (contiguous array expected, got nil)'
    )
end

function g.test_invalid_space_field_format()
    local space = table.deepcopy(test_space)
    space.indexes = test_indexes

    space.format[10] = space.format[1]
    local ok, err = ddl_check.check_space('space', space)
    t.assert_not(ok)
    t.assert_equals(err,
        'spaces["space"].format[10].name: this space already' ..
        ' has field with name "unsigned_nonnull"'
    )

    space.format[10] = {name = 'field', type = 'not_found', is_nullable = false}
    local ok, err = ddl_check.check_space('space', space)
    t.assert_not(ok)
    t.assert_equals(err, 'spaces["space"].format["field"].type: unknown type "not_found"')


    space.format[5] = {name = 'field', type = 'unsigned'}
    local ok, err = ddl_check.check_space('space', space)
    t.assert_not(ok)
    t.assert_equals(err,
        'spaces["space"].format["field"].is_nullable: bad value' ..
        ' (boolean expected, got nil)'
    )


    space.format[5] = {type = 'unsigned'}
    local ok, err = ddl_check.check_space('space', space)
    t.assert_not(ok)
    t.assert_equals(err,
        'spaces["space"].format[5].name: bad value (string expected, got nil)'
    )


    space.format = nil
    local ok, err = ddl_check.check_space('space', space)
    t.assert_not(ok)
    t.assert_equals(err,
        'spaces["space"].format: bad value (contiguous array expected, got nil)'
    )


    space.format = {name = 'x'}
    local ok, err = ddl_check.check_space('space', space)
    t.assert_not(ok)
    t.assert_equals(err,
        'spaces["space"].format: bad value (contiguous array expected, got table)'
    )
end

function g.test_invalid_space_indexes()
    local space = table.deepcopy(test_space)

    local ok, err = ddl_check.check_space('space', space)
    t.assert_not(ok)
    t.assert_equals(err,
        'spaces["space"].indexes: bad value (contiguous array expected, got nil)'
    )


    space.indexes = table.deepcopy(test_indexes)
    space.indexes[2] = space.indexes[1]
    local ok, err = ddl_check.check_space('space', space)
    t.assert_not(ok)
    t.assert_equals(err,
        'spaces["space"].indexes[2].name: this space already has index with name "primary"'
    )


    space.indexes = table.deepcopy(test_indexes)
    space.indexes[1].type = 'BITSET'
    local ok, err = ddl_check.check_space('space', space)
    t.assert_not(ok)
    t.assert_equals(err,
        [[spaces["space"].indexes["primary"]: BITSET index can't be primary]]
    )
end

function g.test_space_ok()
    local space = table.deepcopy(test_space)
    space.indexes = test_indexes

    local ok, err = ddl_check.check_space('space', space)
    t.assert(ok)
    t.assert_not(err)
end


function g.test_field()
    local ok, err = ddl_check.check_field(
        1, test_space.format[1], test_space_info
    )
    t.assert(ok)
    t.assert_not(err)
    local space_info = {name = 'space'}


    local ok, err = ddl_check.check_field(1, nil, space_info)
    t.assert_not(ok)
    t.assert_equals(err, 'spaces["space"].format[1]: bad value (table expected, got nil)')


    local ok, err = ddl_check.check_field(1, {}, space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        'spaces["space"].format[1].name: bad value (string expected, got nil)'
    )


    local ok, err = ddl_check.check_field(1, {name = 'x'}, space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        [[spaces["space"].format["x"].is_nullable: bad value (boolean expected, got nil)]]
    )


    local ok, err = ddl_check.check_field(1, {name = 'x', is_nullable = false}, space_info)
    t.assert_not(ok)
    t.assert_equals(err,
        'spaces["space"].format["x"].type: bad value (string expected, got nil)'
    )


    local ok, err = ddl_check.check_field(
        1, {name = 'x', type = 'undefined', is_nullable = false}, space_info
    )
    t.assert_not(ok)
    t.assert_equals(err, 'spaces["space"].format["x"].type: unknown type "undefined"')


    local ok, err = ddl_check.check_field(
        1, {name = 'x', type = 'varbinary', is_nullable = false}, space_info
    )
    if db.v(2, 2) then
        t.assert(ok)
        t.assert_not(err)
    else
        t.assert_not(ok)
        t.assert_equals(err, string.format(
            [[spaces["space"].format["x"].type: varbinary type ]] ..
            [[isn't allowed in your Tarantool version (%s)]],
            _TARANTOOL
        ))
    end

    local ok, err = ddl_check.check_field(
        1, {name = 'x', type = 'datetime', is_nullable = false}, space_info
    )
    if db.v(2, 10) then
        t.assert(ok)
        t.assert_not(err)
    else
        t.assert_not(ok)
        t.assert_equals(err, string.format(
            [[spaces["space"].format["x"].type: datetime type ]] ..
            [[isn't allowed in your Tarantool version (%s)]],
            _TARANTOOL
        ))
    end
end

function g.test_scalar_types()
    local function get_test_space(index)
        local space = table.deepcopy(test_space)
        space.indexes = {index}
        return space
    end

    local index = {
        name = 'pk',
        type = 'TREE',
        parts = {
            -- shuffling interer <-> scalar in an index is valid
            {path = 'scalar_nonnull', type = 'integer', is_nullable = false},
            {path = 'integer_nonnull', type = 'scalar', is_nullable = false},
        },
        unique = true,
    }
    local res, err = ddl_check.check_space('space', get_test_space(index))
    t.assert_equals(err, nil)
    t.assert_equals(res, true)

    local index = {
        name = 'pk',
        type = 'TREE',
        parts = {
            -- shuffling decimal <-> scalar in an index is valid
            {path = 'scalar_nonnull', type = 'decimal', is_nullable = false},
            {path = 'decimal_nonnull', type = 'scalar', is_nullable = false},
            {path = 'double_nonnull', type = 'scalar', is_nullable = false},
        },
        unique = true,
    }
    local res, err = ddl_check.check_space('space', get_test_space(index))
    t.assert_equals(err, nil)
    t.assert_equals(res, true)

    local index = {
        name = 'pk',
        type = 'TREE',
        parts = {
            -- shuffling decimal <-> scalar in an index is valid
            {path = 'scalar_nonnull', type = 'double', is_nullable = false},
            {path = 'decimal_nonnull', type = 'scalar', is_nullable = false},
            {path = 'double_nonnull', type = 'scalar', is_nullable = false},
        },
        unique = true,
    }
    local res, err = ddl_check.check_space('space', get_test_space(index))
    t.assert_equals(err, nil)
    t.assert_equals(res, true)

    --------------------------------------------------------------------

    local index = {
        name = 'pk',
        type = 'TREE',
        parts = {
            {path = 'integer_nonnull', type = 'string', is_nullable = false},
        },
        unique = true,
    }
    local res, err = ddl_check.check_space('space', get_test_space(index))
    t.assert_equals(err,
        [[spaces["space"].indexes["pk"].parts[1].type: type differs from ]] ..
        [[spaces["space"].format["integer_nonnull"].type (integer expected, got string)]]
    )
    t.assert_equals(res, nil)


    local index = {
        name = 'pk',
        type = 'TREE',
        parts = {
            {path = 'decimal_nonnull', type = 'double', is_nullable = false},
        },
        unique = true,
    }
    local res, err = ddl_check.check_space('space', get_test_space(index))
    t.assert_equals(err,
        [[spaces["space"].indexes["pk"].parts[1].type: type differs from ]] ..
        [[spaces["space"].format["decimal_nonnull"].type ]] ..
        [[(decimal expected, got double)]]
    )
    t.assert_equals(res, nil)

    --------------------------------------------------------------------

    local index = {
        name = 'pk',
        type = 'TREE',
        parts = {
            {path = 'map_nonnull', type = 'scalar', is_nullable = false},
        },
        unique = true,
    }
    local res, err = ddl_check.check_space('space', get_test_space(index))
    t.assert_str_icontains(err,
        [[spaces["space"].indexes["pk"].parts[1].type: type differs from ]] ..
        [[spaces["space"].format["map_nonnull"].type (map expected, got scalar)]]
    )
    t.assert_equals(res, nil)

end

function g.test_ro_schema()
    local mt_readonly = {
        __newindex = function()
            error('table is read-only', 2)
        end
    }

    local function set_readonly(tbl, ro)
        for _, v in pairs(tbl) do
            if type(v) == 'table' then
                set_readonly(v, ro)
            end
        end

        if ro then
            setmetatable(tbl, mt_readonly)
        else
            setmetatable(tbl, nil)
        end

        return tbl
    end

    local space = table.deepcopy(test_space)
    space.indexes = table.deepcopy(test_indexes)

    set_readonly(space, true)

    t.assert_error_msg_contains(
        'table is read-only',
        function()
            local space = table.deepcopy(space)
            space.additional_info = 'hi'
        end
    )

    local res, err = ddl_check.check_space('space', space)
    t.assert_not(err)
    t.assert(res)
end

function g.test_memtx_and_vinyl()
    local function get_test_space(engine)
        local space = {
            engine = engine,
            is_local = true,
            temporary = false,
            format = {
                {name = 'unsigned_nonnull', type = 'unsigned', is_nullable = false},
            },
            indexes = {{
                name = 'pk',
                type = 'TREE',
                parts = {
                    {path = 'unsigned_nonnull', type = 'unsigned', is_nullable = false},
                },
                unique = true,
            }},
        }

        return space
    end

    local memtx_space = get_test_space('memtx')
    local vinyl_space = get_test_space('vinyl')

    local res, err = ddl.check_schema({spaces = {memtx_space = memtx_space, vinyl_space = vinyl_space}})
    t.assert_equals(err, nil)
    t.assert_equals(res, true)
end

function g.test_transactional_ddl()
    local function get_test_space()
        return {
            engine = 'memtx',
            is_local = false,
            temporary = false,
            format = {
                {name = 'id', type = 'unsigned', is_nullable = false},
            },
            indexes = {{
                name = 'pk',
                type = 'TREE',
                parts = {{path = 'id', type = 'unsigned', is_nullable = false}},
                unique = true,
            }, {
                name = 'sk',
                type = 'TREE',
                parts = {{path = 'id', type = 'unsigned', is_nullable = false}},
                unique = false,
            }},
        }
    end
    local spaces = {
        s1 = get_test_space(),
        s2 = get_test_space(),
    }

    local _check_space = ddl_check.check_space
    local cnt = 0
    ddl_check.check_space = function(...)
        -- It's important to make at least 1 transaction
        -- before simulating a failure
        cnt = cnt + 1
        if cnt < 2 then
            return _check_space(...)
        else
            error('Everybody lies', 0)
        end
    end

    local lsn1 = box.info.lsn

    -- The transaction must always be rolled back.
    -- Even in case of a bug in the code.
    t.assert_error_msg_equals('Everybody lies', function()
        ddl.check_schema({spaces = spaces})
    end)

    local lsn2 = box.info.lsn
    if db.v(2, 2) then
        t.assert_equals(lsn2, lsn1)
    else
        t.assert_not_equals(lsn2, lsn1)
    end
    t.assert_not(box.is_in_txn())

    -- Fix the bug and try again, transaction is still rolled back
    ddl_check.check_space = _check_space
    t.assert_equals({ddl.check_schema({spaces = spaces})}, {true, nil})

    local lsn3 = box.info.lsn
    if db.v(2, 2) then
        t.assert_equals(lsn3, lsn2)
    else
        t.assert_not_equals(lsn3, lsn2)
    end
    t.assert_not(box.is_in_txn())
end
