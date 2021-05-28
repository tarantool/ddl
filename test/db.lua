#!/usr/bin/env tarantool

local fio = require('fio')
local t = require('luatest')

local tempdir = fio.tempdir()
t.after_suite(function()
    fio.rmtree(tempdir)
end)

local function init()
    box.cfg{
        memtx_dir = tempdir,
        vinyl_dir = tempdir,
        wal_dir = tempdir,
    }
end

local function drop_all()
	for _, space in box.space._space:pairs({box.schema.SYSTEM_ID_MAX}, {iterator = "GT"}) do
        box.space[space.name]:drop()
    end
end

-- Check if tarantool version >= required
local function v(req_major, req_minor)
    req_minor = req_minor or 0
    assert(type(req_major) == 'number')
    assert(type(req_minor) == 'number')

    local t_major, t_minor = string.match(_TARANTOOL, '^(%d+)%.(%d+)')
    t_major = tonumber(t_major)
    t_minor = tonumber(t_minor)

    if t_major < req_major then
        return false
    elseif t_major > req_major then
        return true
    end

    if t_minor < req_minor then
        return false
    elseif t_minor > req_minor then
        return true
    end

    return true
end

return {
	init = init,
	drop_all = drop_all,
    v = v,
}
