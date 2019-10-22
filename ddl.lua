#!/usr/bin/env tarantool

local ddl_get_schema = require('ddl.get_schema')
local ddl_set_schema = require('ddl.set_schema')
local ddl_check_schema = require('ddl.check_schema')

return {
    check_schema = ddl_check_schema.check_schema,
    set_schema = ddl_set_schema.set_schema,
    get_schema = ddl_get_schema.get_schema,
}
