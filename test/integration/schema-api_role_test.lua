local fio = require('fio')
local t = require('luatest')
local g = t.group('schema-api')

local helpers = require('test.helper')

math.randomseed(os.time())

g.before_all(function()
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('schema-api_role_init'),
        replicasets = {
            {
                uuid = helpers.uuid('a'),
                alias = 'schema-api',
                roles = { 'schema-api' },
                servers = {
                    { instance_uuid = helpers.uuid('a', 1), alias = 'schema-api' },
                },
            }
        }
    })
    g.cluster:start()
    g._section_name = 'schema.yml'
    g._example_schema = [[
     spaces:
       customer:
         engine: memtx
         is_local: false
         temporary: false
         sharding_key: [customer_id]
         format:
           - {name: customer_id, type: unsigned, is_nullable: false}
           - {name: bucket_id, type: unsigned, is_nullable: false}
           - {name: fullname, type: string, is_nullable: false}
         indexes:
         - name: customer_id
           unique: true
           type: TREE
           parts:
             - {path: customer_id, type: unsigned, is_nullable: false}

         - name: bucket_id
           unique: false
           type: TREE
           parts:
             - {path: bucket_id, type: unsigned, is_nullable: false}

         - name: fullname
           unique: true
           type: TREE
           parts:
             - {path: fullname, type: string, is_nullable: false}
    ]]
    g.tbl_with_test_schema = {}
    g.tbl_with_test_schema[g._section_name] = g._example_schema
end)

g.after_all(function()
    g.cluster:stop()
    fio.rmtree(g.cluster.datadir)
end)

g.test_ddl_get_schema = function()

    local tbl = g.cluster.main_server.net_box:call("ddl.get_schema")
    t.assert_equals(tbl['spaces'], {})

    g.cluster.main_server.net_box:eval([[
        box.schema.space.create("test1")
    ]])
    tbl = g.cluster.main_server.net_box:call("ddl.get_schema")
    t.assert_equals(tbl['spaces'], {
        test1 = {
            engine = "memtx",
            format = {},
            indexes = {},
            is_local = false,
            temporary = false,
        },
    })

    g.cluster.main_server.net_box:eval([[
        box.schema.space.create("test2")
    ]])
    tbl = g.cluster.main_server.net_box:call("ddl.get_schema")
    t.assert_equals(tbl['spaces'], {
        test1 = {
            engine = "memtx",
            format = {},
            indexes = {},
            is_local = false,
            temporary = false,
        },
        test2 = {
            engine = "memtx",
            format = {},
            indexes = {},
            is_local = false,
            temporary = false,
        },
    })

end

g.test_validate_config = function()
    local _, err = g.cluster.main_server.net_box:eval([[
        schema_api = require('cartridge.roles.schema-api')
        return schema_api.validate_config(...)
    ]], { g.tbl_with_test_schema, {} })
    t.assert_equals(err, nil)
end

g.test_apply_config = function()

    local _, err = g.cluster.main_server.net_box:eval([[
        schema_api = require('cartridge.roles.schema-api')
        return schema_api.apply_config(...)
    ]], { g.tbl_with_test_schema, { is_master = true } })

    t.assert_equals(err, nil)

    local tbl = g.cluster.main_server.net_box:call("ddl.get_schema")
    t.assert_equals(tbl['spaces'], {
        customer = {
            engine = "memtx",
            format = {
                { is_nullable = false, name = "customer_id", type = "unsigned" },
                { is_nullable = false, name = "bucket_id", type = "unsigned" },
                { is_nullable = false, name = "fullname", type = "string" },
            },
            indexes = {
                {
                    name = "customer_id",
                    parts = { { is_nullable = false, path = "customer_id", type = "unsigned" } },
                    type = "TREE",
                    unique = true,
                },
                {
                    name = "bucket_id",
                    parts = { { is_nullable = false, path = "bucket_id", type = "unsigned" } },
                    type = "TREE",
                    unique = false,
                },
                {
                    name = "fullname",
                    parts = { { is_nullable = false, path = "fullname", type = "string" } },
                    type = "TREE",
                    unique = true,
                },
            },
            is_local = false,
            sharding_key = { "customer_id" },
            temporary = false,
        },
        test1 = {
            engine = "memtx",
            format = {},
            indexes = {},
            is_local = false,
            temporary = false,
        },
        test2 = {
            engine = "memtx",
            format = {},
            indexes = {},
            is_local = false,
            temporary = false,
        },
    })
end
