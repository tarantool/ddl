#!/usr/bin/env tarantool

local t = require('luatest')
local db = require('test.db')
local ddl = require('ddl')

local g = t.group()
g.before_all(db.init)
g.before_each(db.drop_all)

function g.test_valid_ddl_format()
    g.space = box.schema.space.create('test_schema', {
        engine = 'memtx',
        is_local = false,
        temporary = false,
        format = {
            {name = 'field1', type = 'string', is_nullable = false},
            {name = 'field2', type = 'unsigned', is_nullable = false},
            {name = 'field3', type = 'string', is_nullable = false},
        }
    })

    g.space:create_index('primary', {
        type = 'HASH',
        unique = true,
        parts = {
            {'field1', 'string', collation = 'unicode', is_nullable = false},
            {'field2', 'unsigned', is_nullable = false}
        }
    })

    g.space:create_index('secondary', {
        type = 'TREE',
        unique = false,
        parts = {{'field3', 'string', collation = 'unicode_ci', is_nullable = true}}
    })

    local res = ddl.get_schema()
    t.assert_equals(res.spaces, {
        ['test_schema'] = {
            engine = 'memtx',
            is_local = false,
            temporary = false,
            format = {
                {name = 'field1', type = 'string', is_nullable = false},
                {name = 'field2', type = 'unsigned', is_nullable = false},
                {name = 'field3', type = 'string', is_nullable = false},
            },
            indexes = {
                {
                    type = 'HASH',
                    unique = true,
                    name = 'primary',
                    parts = {
                        {
                            path = 'field1',
                            type = 'string',
                            is_nullable = false,
                            collation = 'unicode',
                        },
                        {
                            path = 'field2',
                            type = 'unsigned',
                            is_nullable = false,
                        },
                    },
                },
                {
                    type = 'TREE',
                    unique = false,
                    name = 'secondary',
                    parts = {
                        {
                            path = 'field3',
                            type = 'string',
                            is_nullable = true,
                            collation = 'unicode_ci',
                        },
                    },
                },
            }
        }
    })
end

function g.test_blank()
    g.space = box.schema.space.create('blank')

    local res = ddl.get_schema()
    t.assert_equals(res.spaces['blank'], {
        engine = 'memtx',
        is_local = false,
        temporary = false,
        format = {},
        indexes = {},
    })
end

function g.test_no_index()
    g.space = box.schema.space.create('no_index', {
        format = {
            {name = 'field1', type = 'string'},
            {name = 'field2'},
            {name = 'field3', is_nullable = true},
            {name = 'field4', is_nullable = false},
        }
    })

    local res = ddl.get_schema()
    t.assert_equals(res.spaces, {
        ['no_index'] = {
            engine = 'memtx',
            is_local = false,
            temporary = false,
            format = {
                {name = 'field1', type = 'string', is_nullable = false},
                {name = 'field2', type = 'any', is_nullable = false},
                {name = 'field3', type = 'any', is_nullable = true},
                {name = 'field4', type = 'any', is_nullable = false},
            },
            indexes = {},
        }
    })
end


function g.test_no_format()
    g.space = box.schema.space.create('no_format')
    g.space:create_index('pk')
    g.space:create_index('sk', {
        unique = false,
        parts = {{2, 'string'}}
    })

    local res = ddl.get_schema()
    t.assert_equals(res.spaces['no_format'], {
        engine = 'memtx',
        is_local = false,
        temporary = false,
        format = {},
        indexes = {
            {
                name = 'pk',
                type = 'TREE',
                unique = true,
                parts = {
                    {
                        path = 1,
                        is_nullable = false,
                        type = 'unsigned',
                    },
                }
            },
            {
                name = 'sk',
                type = 'TREE',
                unique = false,
                parts = {
                    {
                        path = 2,
                        type = 'string',
                        is_nullable = false,
                        collation = nil,
                    }
                }
            }
        }
    })
end


function g.test_hash_index()
    g.space = box.schema.space.create('hash_indexes', {
        engine = 'memtx',
        is_local = false,
        temporary = false,
        format = {
            {name = 'field1', type = 'string'},
            {name = 'field2', type = 'unsigned'},
            {name = 'field3', type = 'string'},
        }
    })

    g.space:create_index('primary', {
        type = 'HASH',
        unique = true,
        parts = {
            {'field1', 'string', collation = 'unicode', is_nullable = false}
        },
    })

    g.space:create_index('secondary', {
        type = 'HASH',
        unique = true,
        parts = {
            {'field2', 'unsigned', is_nullable = false},
            {'field3', 'string', collation = 'unicode', is_nullable = false}
        }
    })

    local res = ddl.get_schema()

    t.assert_equals(res.spaces, {
        ['hash_indexes'] = {
            engine = 'memtx',
            is_local = false,
            temporary = false,
            format = {
                {name = 'field1', type = 'string', is_nullable = false},
                {name = 'field2', type = 'unsigned', is_nullable = false},
                {name = 'field3', type = 'string', is_nullable = false},
            },
            indexes = {
                {
                    type = 'HASH',
                    unique = true,
                    name = 'primary',
                    parts = {
                        {
                            path = 'field1',
                            type = 'string',
                            is_nullable = false,
                            collation = 'unicode',
                        },
                    },
                },
                {
                    type = 'HASH',
                    unique = true,
                    name = 'secondary',
                    parts = {
                        {
                            path = 'field2',
                            type = 'unsigned',
                            is_nullable = false,
                        },
                        {
                            path = 'field3',
                            type = 'string',
                            is_nullable = false,
                            collation = 'unicode',
                        },
                    },
                },
            }
        }
    })
end


function g.test_with_tree_index()
    g.space = box.schema.space.create('tree_indexes', {
        engine = 'memtx',
        is_local = false,
        temporary = false,
        format = {
            {name = 'field1', type = 'string', is_nullable = false},
            {name = 'field2', type = 'unsigned', is_nullable = false},
            {name = 'field3', type = 'string', is_nullable = true},
        }
    })

    g.space:create_index('primary', {
        type = 'TREE',
        unique = true,
        parts = {
            {'field1', 'string', collation = 'unicode', is_nullable = false}
        },
    })

    g.space:create_index('secondary', {
        type = 'TREE',
        unique = false,
        parts = {
            {'field2', 'unsigned', is_nullable = false},
            {'field3', 'string', collation = 'unicode', is_nullable = true}
        }
    })
    local res = ddl.get_schema()

    t.assert_equals(res.spaces, {
        ['tree_indexes'] = {
            engine = 'memtx',
            is_local = false,
            temporary = false,
            format = {
                {name = 'field1', type = 'string', is_nullable = false},
                {name = 'field2', type = 'unsigned', is_nullable = false},
                {name = 'field3', type = 'string', is_nullable = true},
            },
            indexes = {
                {
                    type = 'TREE',
                    unique = true,
                    name = 'primary',
                    parts = {
                        {
                            path = 'field1',
                            type = 'string',
                            is_nullable = false,
                            collation = 'unicode'
                        },
                    },
                },
                {
                    type = 'TREE',
                    unique = false,
                    name = 'secondary',
                    parts = {
                        {
                            path = 'field2',
                            type = 'unsigned',
                            is_nullable = false,
                        },
                        {
                            path = 'field3',
                            type = 'string',
                            is_nullable = true,
                            collation = 'unicode'
                        },
                    },
                },
            }
        }
    })
end


function g.test_with_bitset_index()
    g.space = box.schema.space.create('bitset_indexes', {
        engine = 'memtx',
        is_local = false,
        temporary = false,
        format = {
            {name = 'field1', type = 'string', is_nullable = false},
            {name = 'field2', type = 'string', is_nullable = false},
        }
    })

    g.space:create_index('primary', {
        type = 'TREE',
        unique = true,
        parts = {
            {'field1', 'string', collation = 'unicode', is_nullable = false}
        },
    })

    --bitset doesnt support nullable is not multipart not unique
    g.space:create_index('secondary', {
        type = 'BITSET',
        unique = false,
        parts = {
            {'field2', 'string', collation = 'unicode_ci', is_nullable = false},
        }
    })

    local res = ddl.get_schema()
    t.assert_equals(res.spaces, {
        ['bitset_indexes'] = {
            engine = 'memtx',
            is_local = false,
            temporary = false,
            format = {
                {name = 'field1', type = 'string', is_nullable = false},
                {name = 'field2', type = 'string', is_nullable = false},
            },
            indexes = {
                {
                    type = 'TREE',
                    unique = true,
                    name = 'primary',
                    parts = {
                        {
                            path = 'field1',
                            type = 'string',
                            is_nullable = false,
                            collation = 'unicode'
                        },
                    },
                },
                {
                    name = 'secondary',
                    type = 'BITSET',
                    parts = {
                        {
                            path = 'field2',
                            type = 'string',
                            is_nullable = false,
                            collation = 'unicode_ci'
                        }
                    },
                    unique = false,
                }
            }
        }
    })
end


function g.test_rtree_index()
    g.space = box.schema.space.create('rtree_indexes')
    g.space:format({
        {name = 'field1', type = 'string', is_nullable = false},
        {name = 'field2', type = 'array', is_nullable = false},
    })

    g.space:create_index('primary', {
        type = 'HASH',
        unique = true,
        parts = {{'field1', 'string', collation = 'unicode', is_nullable = false}}
    })

    g.space:create_index('rtree', {
        type = 'RTREE',
        unique = false,
        dimension = 3,
        distance = 'manhattan',
        parts = {'field2'}
    })

    local res = ddl.get_schema()

    t.assert_equals(res.spaces, {
        ['rtree_indexes'] = {
            engine = 'memtx',
            is_local = false,
            temporary = false,
            format = {
                {name = 'field1', type = 'string', is_nullable = false},
                {name = 'field2', type = 'array', is_nullable = false},
            },
            indexes = {
                {
                    type = 'HASH',
                    unique = true,
                    name = 'primary',
                    parts = {
                        {
                            path = 'field1',
                            type = 'string',
                            is_nullable = false,
                            collation = 'unicode',
                        },
                    },
                },
                {
                    type = 'RTREE',
                    name = 'rtree',
                    parts = {
                        {
                            path = 'field2',
                            type = 'array',
                            is_nullable = false,
                        },
                    },
                    dimension = 3,
                    distance = 'manhattan',
                    unique = false,
                }
            }
        }
    })
end


function g.test_with_function_index()
    t.skip('Not implemented yet')
    g.space = box.schema.space.create('functional_index')
    g.space:format({
        {name = 'name', type = 'string', is_nullable = false},
        {name = 'addr', type = 'string', is_nullable = false}
    })
    g.space:create_index('name_idx', {
        type = 'TREE',
        unique = true,
        parts = {{'name', 'string'}}
    })

    local func_body = [[
        function(tuple)
            local address = string.split(tuple[2])
            local ret = {}
            for _, v in pairs(address) do
                table.insert(ret, {utf8.upper(v)})
            end
            return ret
        end
    ]]
    box.schema.func.create('address',{
        body = func_body,
        is_deterministic = true,
        is_sandboxed = true,
        opts = {is_multikey = true}
    })

    -- as said in doc: The function must access key-part values by index, not by field name.
    -- and Wrong functional index definition: key part numbers must be sequential and first part number must be 1
    -- maybe if we have functional index, there is no need in path
    g.space:create_index('addr_idx', {
        unique = false, func = 'address', parts = {{1, 'string', collation = 'unicode_ci'}}
    })

    local res = ddl.get_schema()
    t.assert_equals(res.spaces, {
        ['functional_index'] = {
            engine = 'memtx',
            is_local = false,
            temporary = false,
            format = {
                {name = 'name', type = 'string', is_nullable = false},
                {name = 'addr', type = 'string', is_nullable = false},
            },
            indexes = {
                {
                    name = 'name_idx',
                    type = 'TREE',
                    unique = true,
                    parts = {{
                        path = 'name',
                        type = 'string',
                        is_nullable = false
                    }}
                },
                {
                    name = 'addr_idx',
                    type = 'TREE',
                    unique = false,
                    parts = {
                        {
                            path = 'name',
                            type = 'string',
                            is_nullable = false,
                            collation = 'unicode_ci'
                        }
                    },
                    func = {
                        name = 'address',
                        body = func_body,
                        is_sandboxed = true,
                        is_deterministic = true,
                        opts = {is_multikey = true}
                    }
                },
            }
        }
    })
end

function g.test_sequence_index()
    g.space = box.schema.space.create('with_sequence')
    g.space:format({
        {name = 'seq_id', type = 'unsigned', is_nullable = false},
        {name = 'first', type = 'string', is_nullable = false},
        {name = 'second', type = 'string', is_nullable = false},
    })

    local seq_name = 'seq'
    local seq_opts = {
        start = 1,
        min = 0,
        max = 100,
        cycle = true,
        cache = 0,
        step = 5,
    }
    box.schema.sequence.create(seq_name, seq_opts)

    g.space:create_index('seq_index', {
        type = 'TREE',
        unique = true,
        sequence = seq_name,
    })

    local res = ddl.get_schema()

    t.assert_equals(res, {
        spaces = {
            ['with_sequence'] = {
                engine = 'memtx',
                is_local = false,
                temporary = false,
                format = {
                    {name = 'seq_id', type = 'unsigned', is_nullable = false},
                    {name = 'first', type = 'string', is_nullable = false},
                    {name = 'second', type = 'string', is_nullable = false},
                },
                indexes = {
                    {
                        name = 'seq_index',
                        type = 'TREE',
                        unique = true,
                        parts = {{is_nullable = false, path = 'seq_id', type = 'unsigned'}},
                        sequence = seq_name,
                    },
                },
            },
        },
        sequences = {
            [seq_name] = seq_opts,
        },
    })
end

function g.test_path()
    if not db.v(2, 0) then
        t.skip('No json path support in this tarantool')
    end

    g.space = box.schema.space.create('path_indexes')
    g.space:format({
        {name = 'id', type = 'unsigned', is_nullable = false},
        {name = 'json_data', type = 'map', is_nullable = false},
    })

    g.space:create_index('primary', {
        type = 'TREE',
        unique = true,
        parts = {{'id', 'unsigned', is_nullable = false}}
    })

    g.space:create_index('path_idx', {
        type = 'TREE',
        unique = true,
        parts = {{'json_data.DATA["name"]', 'string'}}
    })

    local res = ddl.get_schema()
    t.assert_equals(res.spaces, {
        ['path_indexes'] = {
            engine = 'memtx',
            is_local = false,
            temporary = false,
            format = {
                {name = 'id', type = 'unsigned', is_nullable = false},
                {name = 'json_data', type = 'map', is_nullable = false},
            },
            indexes = {
                {
                    name = 'primary',
                    type = 'TREE',
                    unique = true,
                    parts = {{is_nullable = false, path = 'id', type = 'unsigned'}},
                },
                {
                    name = 'path_idx',
                    type = 'TREE',
                    unique = true,
                    parts = {{path = 'json_data.DATA["name"]', type = 'string', is_nullable = false}}
                }
            }
        }
    })
end


function g.test_two_spaces()
    g.spaces = {}
    g.spaces['first'] = box.schema.space.create('first')
    g.spaces['first']:format({
        {name = 'field1', type = 'string', is_nullable = false},
        {name = 'field2', type = 'string', is_nullable = false},
    })

    g.spaces['first']:create_index('primary', {
        type = 'HASH',
        unique = true,
        parts = {{'field1', 'string', collation = 'unicode', is_nullable = false}}
    })

    g.spaces['second'] = box.schema.space.create('second')
    g.spaces['second']:format({
        {name = 'field1', type = 'unsigned', is_nullable = false},
        {name = 'field2', type = 'string', is_nullable = false},
    })


    g.spaces['second']:create_index('primary', {
        type = 'TREE',
        unique = true,
        parts = {{'field1', 'unsigned', is_nullable = false}}
    })

    local res = ddl.get_schema()

    t.assert_equals(res.spaces, {
        ['first'] = {
            engine = 'memtx',
            is_local = false,
            temporary = false,
            format = {
                {name = 'field1', type = 'string', is_nullable = false},
                {name = 'field2', type = 'string', is_nullable = false},
            },
            indexes = {
                {
                    type = 'HASH',
                    unique = true,
                    name = 'primary',
                    parts = {
                        {
                            path = 'field1',
                            type = 'string',
                            is_nullable = false,
                            collation = 'unicode',
                        },
                    },
                },
            }
        },
        ['second'] = {
            engine = 'memtx',
            is_local = false,
            temporary = false,
            format = {
                {name = 'field1', type = 'unsigned', is_nullable = false},
                {name = 'field2', type = 'string', is_nullable = false},
            },
            indexes = {
                {
                    type = 'TREE',
                    unique = true,
                    name = 'primary',
                    parts = {
                        {
                            path = 'field1',
                            type = 'unsigned',
                            is_nullable = false,
                        },
                    },
                },
            }
        }
    })
end


function g.test_get_schema_with_default_values()
    g.space = box.schema.space.create('test_schema')
    g.space:format({
        {name = 'field1', type = 'string'},
        {name = 'field2', type = 'unsigned'},
        {name = 'field3', type = 'array'},
        {name = 'field4', type = 'array'},
        {name = 'map_field', type = 'map'}
    })

    g.space:create_index('primary', {
        type = 'HASH',
        unique = true,
        parts = {{'field1', collation = 'unicode'}, {'field2'}}
    })

    g.space:create_index('rtree', {
        type = 'RTREE',
        unique = false,
        parts = {'field4'}
    })

    local res = ddl.get_schema()
    t.assert_equals(res.spaces, {
        ['test_schema'] = {
            engine = 'memtx',
            is_local = false,
            temporary = false,
            format = {
                {is_nullable = false, name = 'field1', type = 'string'},
                {is_nullable = false, name = 'field2', type = 'unsigned'},
                {is_nullable = false, name = 'field3', type = 'array'},
                {is_nullable = false, name = 'field4', type = 'array'},
                {is_nullable = false, name = 'map_field', type = 'map'},
            },
            indexes = {
                {
                    type = 'HASH',
                    unique = true,
                    name = 'primary',
                    parts = {
                        {
                            path = 'field1',
                            type = 'string',
                            is_nullable = false,
                            collation = 'unicode',
                        },
                        {
                            path = 'field2',
                            type = 'unsigned',
                            is_nullable = false,
                            collation = nil,
                        },
                    },
                },
                {
                    type = 'RTREE',
                    unique = false,
                    name = 'rtree',
                    parts = {
                        {
                            path = 'field4',
                            type = 'array',
                            is_nullable = false,
                        },
                    },
                    dimension = 2,
                    distance = 'euclid',
                }
            }
        }
    })
end

g.test_gh_108_fieldno_index_no_space_format = function()
    box.schema.space.create('weird_space')
    box.space['weird_space']:create_index('pk')

    local schema, err = ddl.get_schema()
    t.assert_equals(err, nil)
    t.assert_equals(schema, {spaces = {weird_space = {
        engine = "memtx",
        format = {},
        indexes = {
            {
                name = "pk",
                parts = {{is_nullable = false, path = 1, type = "unsigned"}},
                type = "TREE",
                unique = true,
            },
        },
        is_local = false,
        temporary = false,
    }}})

    local _, err = ddl.check_schema(schema)
    t.assert_equals(err, nil)
end

g.test_gh_108_fieldno_index_in_space_format = function()
    box.schema.space.create('weird_space', {
        format = {{name = 'id', type = 'unsigned', is_nullable = false}}
    })
    box.space['weird_space']:create_index('pk', {
        unique = true,
        parts = {{field = 1, type = 'unsigned', is_nullable = false}}
    })

    local schema, err = ddl.get_schema()
    t.assert_equals(err, nil)
    t.assert_equals(schema, {spaces = {weird_space = {
        engine = "memtx",
        format = {{is_nullable = false, name = "id", type = "unsigned"}},
        indexes = {
            {
                name = "pk",
                parts = {{is_nullable = false, path = "id", type = "unsigned"}},
                type = "TREE",
                unique = true,
            },
        },
        is_local = false,
        temporary = false,
    }}})

    local _, err = ddl.check_schema(schema)
    t.assert_equals(err, nil)
end

g.test_gh_108_fieldno_index_outside_space_format = function()
    box.schema.space.create('weird_space', {
        format = {{name = 'id', type = 'unsigned', is_nullable = false}}
    })
    box.space['weird_space']:create_index('pk', {
        unique = true,
        parts = {{field = 2, type = 'string', is_nullable = false}}
    })

    local schema, err = ddl.get_schema()
    t.assert_equals(err, nil)
    t.assert_equals(schema, {spaces = {weird_space = {
        engine = "memtx",
        format = {{is_nullable = false, name = "id", type = "unsigned"}},
        indexes = {
            {
                name = "pk",
                parts = {{is_nullable = false, path = 2, type = "string"}},
                type = "TREE",
                unique = true,
            },
        },
        is_local = false,
        temporary = false,
    }}})

    local _, err = ddl.check_schema(schema)
    t.assert_equals(err, nil)
end
