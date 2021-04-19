local t = require('luatest')
local g = t.group()

local fio = require('fio')
local yaml = require('yaml')
local helpers = require('test.helper')

g.before_all(function()
    t.skip_if(
        not pcall(require, 'cartridge'),
        'cartridge not installed'
    )

    g.cluster = require('cartridge.test-helpers').Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('srv_basic'),
        replicasets = {{
            alias = 'main',
            roles = {},
            servers = 2,
        }},
    })

    g.space = {
        engine = "memtx",
        format = {},
        indexes = {},
        is_local = false,
        temporary = false,
    }

    g.cluster:start()
    g.s1 = g.cluster:server('main-1')
    g.s2 = g.cluster:server('main-2')
end)

g.after_all(function()
    g.cluster:stop()
    fio.rmtree(g.cluster.datadir)
end)

local function get_section()
    local ddl_manager = require('cartridge.roles.ddl-manager')
    return g.s1.net_box:call(
        'package.loaded.cartridge.config_get_readonly',
        {ddl_manager._section_name}
    )
end

local function srv_call(srv, fn_name, ...)
    return srv.net_box:eval([[
        local fn_name, args = ...
        local cartridge = require('cartridge')
        local ddl_manager = cartridge.service_get('ddl-manager')
        return ddl_manager[fn_name](unpack(args))
    ]], {fn_name, {...}})
end

local function call(fn_name, ...)
    return srv_call(g.s1, fn_name, ...)
end

function g.test_yaml()
    --------------------------------------------------------------------
    local schema = ''
    local ok, err = call('check_schema_yaml', schema)
    t.assert_equals({ok, err}, {true, nil})
    local ok, err = call('set_clusterwide_schema_yaml', schema)
    t.assert_equals({ok, err}, {true, nil})

    t.assert_str_matches(call('get_clusterwide_schema_yaml'), '## Example:\n.+')
    t.assert_equals(call('get_clusterwide_schema_lua'), {spaces = {}})
    t.assert_equals(get_section(), '')

    --------------------------------------------------------------------
    local schema = nil
    local ok, err = call('check_schema_yaml', schema)
    t.assert_equals({ok, err}, {true, nil})
    local ok, err = call('set_clusterwide_schema_yaml', schema)
    t.assert_equals({ok, err}, {true, nil})

    t.assert_str_matches(call('get_clusterwide_schema_yaml'), '## Example:\n.+')
    t.assert_equals(call('get_clusterwide_schema_lua'), {spaces = {}})
    t.assert_equals(get_section(), nil)

    --------------------------------------------------------------------
    local schema = ' '
    local ok, err = call('check_schema_yaml', schema)
    t.assert_equals({ok, err}, {true, nil})
    local ok, err = call('set_clusterwide_schema_yaml', schema)
    t.assert_equals({ok, err}, {true, nil})

    t.assert_equals(call('get_clusterwide_schema_yaml'), ' ')
    t.assert_equals(call('get_clusterwide_schema_lua'), {spaces = {}})
    t.assert_equals(get_section(), ' ')

    --------------------------------------------------------------------
    local schema = box.NULL
    local ok, err = call('check_schema_yaml', schema)
    t.assert_equals({ok, err}, {true, nil})
    local ok, err = call('set_clusterwide_schema_yaml', schema)
    t.assert_equals({ok, err}, {true, nil})

    t.assert_str_matches(call('get_clusterwide_schema_yaml'), '## Example:\n.+')
    t.assert_equals(call('get_clusterwide_schema_lua'), {spaces = {}})
    t.assert_equals(get_section(), nil)

    --------------------------------------------------------------------
    local schema = 'null'
    local ok, err = call('check_schema_yaml', schema)
    t.assert_equals({ok, err}, {true, nil})
    local ok, err = call('set_clusterwide_schema_yaml', schema)
    t.assert_equals({ok, err}, {true, nil})

    t.assert_equals(call('get_clusterwide_schema_yaml'), 'null')
    t.assert_equals(call('get_clusterwide_schema_lua'), {spaces = {}})
    t.assert_equals(get_section(), 'null')

    --------------------------------------------------------------------
    local schema = 'spaces: {}'
    local ok, err = call('check_schema_yaml', schema)
    t.assert_equals({ok, err}, {true, nil})
    local ok, err = call('set_clusterwide_schema_yaml', schema)
    t.assert_equals({ok, err}, {true, nil})

    t.assert_equals({ok, err}, {true, nil})
    t.assert_equals(call('get_clusterwide_schema_yaml'), 'spaces: {}')
    t.assert_equals(call('get_clusterwide_schema_lua'), {spaces = {}})
    t.assert_equals(get_section(), 'spaces: {}')

    --------------------------------------------------------------------
    local schema = 'not-a-table'
    local expected_error = 'Invalid schema (table expected, got string)'

    local ok, err = srv_call(g.s1, 'check_schema_yaml', schema)
    t.assert_equals(ok, nil)
    t.assert_covers(err, {
        class_name = 'CheckSchemaError',
        err = expected_error,
    })

    local ok, err = srv_call(g.s2, 'check_schema_yaml', schema)
    t.assert_equals(ok, nil)
    t.assert_covers(err, {
        class_name = 'CheckSchemaError',
        err = '"localhost:13301": ' .. expected_error,
    })

    local ok, err = call('set_clusterwide_schema_yaml', schema)
    t.assert_equals(ok, nil)
    t.assert_covers(err, {
        class_name = 'CheckSchemaError',
        err = '"localhost:13301": ' .. expected_error,
    })

    --------------------------------------------------------------------
    local schema = '{}'
    local expected_error = 'spaces: must be a table, got nil'

    local ok, err = srv_call(g.s1, 'check_schema_yaml', schema)
    t.assert_equals(ok, nil)
    t.assert_covers(err, {
        class_name = 'CheckSchemaError',
        err = expected_error,
    })

    local ok, err = srv_call(g.s2, 'check_schema_yaml', schema)
    t.assert_equals(ok, nil)
    t.assert_covers(err, {
        class_name = 'CheckSchemaError',
        err = '"localhost:13301": ' .. expected_error,
    })

    local ok, err = call('set_clusterwide_schema_yaml', schema)
    t.assert_equals(ok, nil)
    t.assert_covers(err, {
        class_name = 'CheckSchemaError',
        err = '"localhost:13301": ' .. expected_error,
    })

    --------------------------------------------------------------------
    local schema = ']['

    local ok, err = srv_call(g.s1, 'check_schema_yaml', schema)
    t.assert_equals(ok, nil)
    t.assert_covers(err, {
        class_name = 'CheckSchemaError',
        err = 'Invalid YAML: unexpected END event',
    })

    local ok, err = srv_call(g.s2, 'check_schema_yaml', schema)
    t.assert_equals(ok, nil)
    t.assert_covers(err, {
        class_name = 'CheckSchemaError',
        err = 'Invalid YAML: unexpected END event',
    })

    local ok, err = call('set_clusterwide_schema_yaml', schema)
    t.assert_equals(ok, nil)
    t.assert_covers(err, {
        class_name = 'LoadConfigError',
        err = 'Error parsing section "schema.yml": unexpected END event',
    })

    --------------------------------------------------------------------
    local schema = {}
    local expected_error = 'Bad argument #1 to check_schema_yaml' ..
        ' (?string expected, got table)'

    local ok, err = srv_call(g.s1, 'check_schema_yaml', schema)
    t.assert_equals(ok, nil)
    t.assert_covers(err, {
        class_name = 'CheckSchemaError',
        err = expected_error,
    })

    local ok, err = srv_call(g.s2, 'check_schema_yaml', schema)
    t.assert_equals(ok, nil)
    t.assert_covers(err, {
        class_name = 'CheckSchemaError',
        err = expected_error,
    })

    t.assert_error_msg_equals(
        'Bad argument #1 to set_clusterwide_schema_yaml' ..
        ' (?string expected, got table)',
        call, 'set_clusterwide_schema_yaml', schema
    )

    --------------------------------------------------------------------
    local schema = yaml.encode({spaces = {s_yaml = g.space}})

    local ok, err = call('check_schema_yaml', schema)
    t.assert_equals({ok, err}, {true, nil})

    local ok, err = call('set_clusterwide_schema_yaml', schema)
    t.assert_equals({ok, err}, {true, nil})

    t.assert_covers(
        g.s1.net_box:call('ddl.get_schema').spaces,
        {s_yaml = g.space}
    )

    t.assert_equals(
        call('get_clusterwide_schema_lua'),
        {spaces = {s_yaml = g.space}}
    )
end

function g.test_lua()
    --------------------------------------------------------------------
    local schema = {spaces = {}}
    local ok, err = call('check_schema_lua', schema)
    t.assert_equals({ok, err}, {true, nil})
    local ok, err = call('set_clusterwide_schema_lua', schema)
    t.assert_equals({ok, err}, {true, nil})

    local yml = '---\nspaces: []\n...\n'
    t.assert_equals(call('get_clusterwide_schema_lua'), {spaces = {}})
    t.assert_equals(call('get_clusterwide_schema_yaml'), yml)
    t.assert_equals(get_section(), yml)

    --------------------------------------------------------------------
    local schema = box.NULL
    local ok, err = call('check_schema_lua', schema)
    t.assert_equals({ok, err}, {true, nil})
    local ok, err = call('set_clusterwide_schema_lua', schema)
    t.assert_equals({ok, err}, {true, nil})

    t.assert_equals(call('get_clusterwide_schema_lua'), {spaces = {}})
    t.assert_str_matches(call('get_clusterwide_schema_yaml'), '## Example:\n.+')
    t.assert_equals(get_section(), nil)

    --------------------------------------------------------------------
    local schema = {}
    local expected_error = 'spaces: must be a table, got nil'

    local ok, err = srv_call(g.s1, 'check_schema_lua', schema)
    t.assert_equals(ok, nil)
    t.assert_covers(err, {
        class_name = 'CheckSchemaError',
        err = expected_error,
    })

    local ok, err = srv_call(g.s2, 'check_schema_lua', schema)
    t.assert_equals(ok, nil)
    t.assert_covers(err, {
        class_name = 'CheckSchemaError',
        err = '"localhost:13301": ' .. expected_error,
    })

    local ok, err = call('set_clusterwide_schema_lua', {})
    t.assert_equals(ok, nil)
    t.assert_covers(err, {
        class_name = 'CheckSchemaError',
        err = '"localhost:13301": ' .. expected_error,
    })

    --------------------------------------------------------------------
    local schema = '{}'
    local expected_error = 'Bad argument #1 to check_schema_lua' ..
        ' (?table expected, got string)'

    local ok, err = srv_call(g.s1, 'check_schema_lua', schema)
    t.assert_equals(ok, nil)
    t.assert_covers(err, {
        class_name = 'CheckSchemaError',
        err = expected_error,
    })

    local ok, err = srv_call(g.s2, 'check_schema_lua', schema)
    t.assert_equals(ok, nil)
    t.assert_covers(err, {
        class_name = 'CheckSchemaError',
        err = expected_error,
    })

    t.assert_error_msg_equals(
        'Bad argument #1 to set_clusterwide_schema_lua' ..
        ' (?table expected, got string)',
        call, 'set_clusterwide_schema_lua', '{}'
    )

    --------------------------------------------------------------------
    local schema = {spaces = {s_lua = g.space}}

    local ok, err = call('check_schema_lua', schema)
    t.assert_equals({ok, err}, {true, nil})

    local ok, err = call('set_clusterwide_schema_lua', schema)
    t.assert_equals({ok, err}, {true, nil})

    t.assert_covers(
        g.s1.net_box:call('ddl.get_schema').spaces,
        {s_lua = g.space}
    )
    t.assert_equals(
        yaml.decode(call('get_clusterwide_schema_yaml')),
        {spaces = {s_lua = g.space}}
    )
end

function g.test_example_schema()
    local fun = require('fun')
    local schema = fun.map(
        function(l) return l:gsub('^# ', '') end,
        call('get_clusterwide_schema_yaml'):split('\n')
    ):totable()
    schema = table.concat(schema, '\n')

    local ok, err = call('check_schema_yaml', schema)
    t.assert_equals({ok, err}, {true, nil})
    local ok, err = call('set_clusterwide_schema_yaml', schema)
    t.assert_equals({ok, err}, {true, nil})

    local space_name = next(yaml.decode(schema).spaces)

    for _, srv in pairs(g.cluster.servers) do
        helpers.retrying({}, function()
            srv.net_box:ping()
            t.assert(srv.net_box.space[space_name],
                string.format('Missing space %q on %s', space_name, srv.alias)
            )
        end)
    end
end
