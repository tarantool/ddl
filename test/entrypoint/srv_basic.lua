#!/usr/bin/env tarantool

require('strict').on()

local cartridge = require('cartridge')
local ok, err = cartridge.cfg({
    workdir = 'tmp/db',
    roles = {
        'cartridge.roles.ddl-manager',
    }
})

assert(ok, tostring(err))
