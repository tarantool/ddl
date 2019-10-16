local ddl = require('ddl')
local fio = require('fio')
local log = require('log')
local t = require('luatest')
local utils = require('ddl.utils')

local g = t.group('ddl_set_schema')

local function create_test_format(opts)
    return {}
end

g.before_all = function()
    g.workdir = fio.tempdir()
    box.cfg{
        wal_mode = 'none',
        work_dir = g.workdir,
    }


    g.spaces = {}
    g.format = {
        ['hash_indexes'] = {
            engine = 'memtx',
            is_local = true,
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
    }
end

g.after_all = function()
    fio.rmtree(g.workdir)
end

g.teardown = function()
    local box_spaces = box.space._space:select({512}, {iterator = "GE"})
    for _, space in pairs(box_spaces) do
        box.space[space.name]:drop()
    end
end

function g.test_set_schema_with_hash_index()
    local res, err = ddl.set_schema(g.format)
    log.info(err)
    t.assert_true(res)
    t.assert_nil(err)
    local ddl_schema = ddl.get_schema()
    log.info(ddl_schema)
    t.assert_equals(ddl_schema, g.format)
    log.info(g.format)
end