local ddl_get_schema = require('ddl.get_schema')
local ddl_set_schema = require('ddl.set_schema')

return {
    get_schema = ddl_get_schema.get_schema,
    set_schema = ddl_set_schema.set_schema,
}
