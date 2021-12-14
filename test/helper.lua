require('strict').on()

local db = require('test.db')

local fio = require('fio')
local digest = require('digest')
local helpers = table.copy(require('luatest').helpers)

helpers.project_root = fio.dirname(debug.sourcedir())

local __fio_tempdir = fio.tempdir
fio.tempdir = function(base)
    base = base or os.getenv('TMPDIR')
    if base == nil or base == '/tmp' then
        return __fio_tempdir()
    else
        local random = digest.urandom(9)
        local suffix = digest.base64_encode(random, {urlsafe = true})
        local path = fio.pathjoin(base, 'tmp.cartridge.' .. suffix)
        fio.mktree(path)
        return path
    end
end

function helpers.test_space_format()
    local space_format = {
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
        {name = 'map_nonnull', type = 'map', is_nullable = false},
        {name = 'map_nullable', type = 'map', is_nullable = true},
        {name = 'any_nonnull', type = 'any', is_nullable = false},
        {name = 'any_nullable', type = 'any', is_nullable = true},
    }

    if db.v(2, 2) then
        table.insert(space_format, {name = 'varbinary_nonnull', type = 'varbinary', is_nullable = false})
        table.insert(space_format, {name = 'varbinary_nullable', type = 'varbinary', is_nullable = true})
    end

    return table.deepcopy(space_format)
end

function helpers.entrypoint(name)
    local path = fio.pathjoin(
            helpers.project_root,
            'test', 'entrypoint',
            string.format('%s.lua', name)
    )
    if not fio.path.exists(path) then
        error(path .. ': no such entrypoint', 2)
    end
    return path
end

helpers.sharding_func_body = [[
function(shard_key)
    local digest = require('digest')
    if type(shard_key) ~= 'table' then
        return digest.crc32(tostring(shard_key))
    else
        local crc32 = digest.crc32.new()
        for _, v in ipairs(shard_key) do
            crc32:update(tostring(v))
        end
        return crc32:result()
    end
end
]]

function helpers.sharding_func(shard_key_1, shard_key_2)
    -- Sharding key is passed as a second argument when
    -- sharding function is used as a metamethod __call for a table.
    local shard_key = shard_key_2 or shard_key_1
    local digest = require('digest')
    if type(shard_key) ~= 'table' then
        return digest.crc32(tostring(shard_key))
    else
        local crc32 = digest.crc32.new()
        for _, v in ipairs(shard_key) do
            crc32:update(tostring(v))
        end
        return crc32:result()
    end
end

return helpers
