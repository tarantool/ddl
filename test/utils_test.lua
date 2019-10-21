#!/usr/bin/env tarantool

local t = require('luatest')
local ddl_utils = require('ddl.utils')

local g = t.group('utils')

function g.test_is_array()
    t.assert_true(ddl_utils.is_array({}))
    t.assert_true(ddl_utils.is_array({1, 2, 3}))
    t.assert_true(ddl_utils.is_array({{'a'}, { b = 4 }}))

    t.assert_false(ddl_utils.is_array(nil))
    t.assert_false(ddl_utils.is_array('aaaa'))
    t.assert_false(ddl_utils.is_array(5))
    t.assert_false(ddl_utils.is_array({a = 4}))
    t.assert_false(ddl_utils.is_array({{'int'}, name = 'a'}))
    t.assert_false(ddl_utils.is_array({nil, 2, x = 5}))
end

function g.test_find_first_duplicate()
    local objects = {{key = 4, value = 3}, {key = 4, value = 5}}
    t.assert_equals(ddl_utils.find_first_duplicate(objects, 'key'), 4)
    t.assert_nil(ddl_utils.find_first_duplicate(objects, 'value'))
end
