local ddl = require('ddl.validator')
local fio = require('fio')
local log = require('log')

local t = require('luatest')
local g = t.group('ddl_validate')

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

local function clear_box()
    for _, space in box.space._space:pairs({box.schema.SYSTEM_ID_MAX}, {iterator = "GE"}) do
        box.space[space.name]:drop()
    end
end

g.teardown = function()
    clear_box()
end

local __test_invalid_ddl(schema, expected_message)

	local res, err = ddl.validate(schema)
	t.assert_nil(res)
	t.assert
end

function g.test_invalid_format()

end

function g.test_invalid_index_reference()

end

function g.test_invalid_index()

end
