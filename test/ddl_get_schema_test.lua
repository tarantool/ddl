local ddl = require('ddl.get_schema')
local fio = require('fio')
local log = require('log')
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
end

function g.test_get_schema_with_default_values()
    g.space = box.schema.space.create('test_schema')
    log.info(g.space)
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

function g.test_get_schema_not_specified_format()
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
                            path = '2data[*].name', -- there is no delimiter as . is it normal?
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

function g.test_get_schema_with_function_index()
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

function g.test_get_schema_valid_ddl_format()
end

function g.test_get_schema_two_spaces()
end

function g.test_hash_index()
end

function g.test_rtree_index()
end

function g.test_tree_index()
end

function g.test_bitset_index()
end
