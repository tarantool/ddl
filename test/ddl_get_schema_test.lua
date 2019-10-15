local ddl = require('ddl.get_schema')
local fio = require('fio')
local t = require('luatest')

local g = t.group('ddl_get_schema')

g.before_all = function()
    g.workdir = fio.tempdir()
    box.cfg{
        wal_mode = 'none',
        work_dir = g.workdir,
    }
end

g.after_all = function()
    fio.rmtree(g.workdir)
end

g.teardown = function()
    if g.space ~= nil then
        g.space:drop()
    end
    g.space = nil

    if g.spaces ~= nil then
        for _, space in pairs(g.spaces) do
            space:drop()
        end
    end

    g.spaces = nil
end



function g.test_get_schema_valid_ddl_format()
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
    t.assert_equals(res, {
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


function g.test_no_index()
    g.space = box.schema.space.create('no_index', {
        engine = 'memtx',
        is_local = false,
        temporary = false,
        format = {
            {name = 'field1', type = 'string', is_nullable = false},
            {name = 'field2', type = 'unsigned', is_nullable = false},
            {name = 'field3', type = 'string', is_nullable = false},
        }
    })

    local res = ddl.get_schema()
    t.assert_equals(res, {
        ['no_index'] = {
            engine = 'memtx',
            is_local = false,
            temporary = false,
            format = {
                {name = 'field1', type = 'string', is_nullable = false},
                {name = 'field2', type = 'unsigned', is_nullable = false},
                {name = 'field3', type = 'string', is_nullable = false},
            },
            indexes = {},
        }
    })
end


function g.test_not_specified_format()
    g.space = box.schema.space.create('not_specified_format')
    g.space:create_index('primarykey')
    g.space:create_index('multikey', {unique = false, parts = {{2, 'string', path = 'data[*].name'}}})

    local res = ddl.get_schema()
    t.assert_equals(res, {
        ['not_specified_format'] = {
            engine = 'memtx',
            is_local = false,
            temporary = false,
            format = {},
            indexes = {
                {
                    name = 'primarykey',
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
                    name = 'multikey',
                    type = 'TREE',
                    unique = false,
                    parts = {
                        {
                            path = '2.data[*].name',
                            type = 'string',
                            is_nullable = false,
                            collation = nil,
                        }
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
            {name = 'field1', type = 'string', is_nullable = false},
            {name = 'field2', type = 'unsigned', is_nullable = false},
            {name = 'field3', type = 'string', is_nullable = false},
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

    t.assert_equals(res, {
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

    t.assert_equals(res, {
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
    t.assert_equals(res, {
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
                    }
                    -- unique is nil
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

    t.assert_equals(res, {
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
                    -- distance = 'euclid', -- ignored in db
                }
            }
        }
    })
end


function g.test_with_function_index()
    g.space = box.schema.space.create('functional_index')
    g.space:format({
        {name = 'name', type = 'string', is_nullable = false},
        {name = 'addr', type = 'string', is_nullable = false}
    })
    g.space:create_index('name_idx', {type = 'TREE', unique = true, parts = {{'name', 'string'}}})
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
    t.assert_equals(res, {
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

    local seq_opts = {
        start = 1,
        min = 0,
        max = 100,
        cycle = true,
        cache = 0,
        step = 5,
    }
    box.schema.sequence.create('seq', seq_opts)
    g.space:create_index('seq_index', {
        type = 'TREE',
        unique = true,
        sequence = 'seq'
    })

    local res = ddl.get_schema()
    local seq_info = seq_opts
    seq_info.name = 'seq'

    t.assert_equals(res,  {
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
                    sequence = seq_info,
                }
            }
        }
    })
end

function g.test_path()
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
    t.assert_equals(res, {
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

    t.assert_equals(res, {
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
    t.assert_equals(res, {
        ['test_schema'] = {
            engine = 'memtx',
            is_local = false,
            temporary = false,
            format = {
                {name = 'field1', type = 'string'},
                {name = 'field2', type = 'unsigned'},
                {name = 'field3', type = 'array'},
                {name = 'field4', type = 'array'},
                {name = 'map_field', type = 'map'},
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
                    unique = nil,
                    name = 'rtree',
                    parts = {
                        {
                            path = 'field4',
                            type = 'array',
                            is_nullable = false,
                        },
                    },
                    dimension = 2,
                }
            }
        }
    })
end
