#!/usr/bin/env tarantool

local ddl_get_schema = require('ddl.get_schema')
local ddl_set_schema = require('ddl.set_schema')
local ddl_validator = require('ddl.validator')

local function set_schema(schema)
    local res, err = ddl_validator.validate(schema)
    if not res then
        return nil, err
    end
    return ddl_set_schema.set_schema(schema)
end

return {
    get_schema = ddl_get_schema.get_schema,
    set_schema = set_schema,
}
