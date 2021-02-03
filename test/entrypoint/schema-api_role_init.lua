#!/usr/bin/env tarantool

require('strict').on()

local cartridge = require('cartridge')
local ok, err = cartridge.cfg({
    workdir = 'tmp/db',
    roles = {
        'cartridge.roles.schema-api'
    },
    cluster_cookie = 'schema-api-cluster-cookie',
})

assert(ok, tostring(err))
