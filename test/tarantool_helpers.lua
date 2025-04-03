local luatest = require('luatest')
local luatest_utils = require('luatest.utils')

local tarantool_helpers = {}

tarantool_helpers.is_tarantool3 = function()
    local tarantool_version = luatest_utils.get_tarantool_version()
    return luatest_utils.version_ge(tarantool_version, luatest_utils.version(3, 0, 0))
end

tarantool_helpers.skip_if_tarantool3 = function()
    luatest.skip_if(tarantool_helpers.is_tarantool3(), 'Tarantool 3 is not supported')
end

return tarantool_helpers
