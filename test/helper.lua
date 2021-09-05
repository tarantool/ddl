require('strict').on()

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
