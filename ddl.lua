local ddl_get_schema = require('ddl.get_schema')
local ddl_set_schema = require('ddl.set_schema')

return {
    get_shcema = ddl_get_schema.get_shema,
    set_schema = ddl_set_schema.set_schema,
}
