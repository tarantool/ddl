#!/usr/bin/env tarantool

local t = require('luatest')
local ddl_utils = require('ddl.utils')

local g = t.group()

function g.test_is_array()
    t.assert(ddl_utils.is_array({}))
    t.assert(ddl_utils.is_array({1, 2, 3}))
    t.assert(ddl_utils.is_array({{'a'}, { b = 4 }}))

    t.assert_not(ddl_utils.is_array(nil))
    t.assert_not(ddl_utils.is_array('aaaa'))
    t.assert_not(ddl_utils.is_array(5))
    t.assert_not(ddl_utils.is_array({a = 4}))
    t.assert_not(ddl_utils.is_array({{'int'}, name = 'a'}))
    t.assert_not(ddl_utils.is_array({nil, 2, x = 5}))
end

function g.test_find_first_duplicate()
    local objects = {{key = 4, value = 3}, {key = 4, value = 5}}
    t.assert_equals(ddl_utils.find_first_duplicate(objects, 'key'), 4)
    t.assert_not(ddl_utils.find_first_duplicate(objects, 'value'))
end

function g.test_is_number()
    t.assert(ddl_utils.is_number(0))
    t.assert(ddl_utils.is_number(1))
    t.assert(ddl_utils.is_number(1.213))
    t.assert(ddl_utils.is_number(-1.213))
    t.assert(ddl_utils.is_number(math.huge))
    t.assert(ddl_utils.is_number(0 / 0))
    t.assert(ddl_utils.is_number(1LL))
    t.assert(ddl_utils.is_number(1ULL))

    t.assert_not(ddl_utils.is_number(nil))
    t.assert_not(ddl_utils.is_number(box.NULL))
    t.assert_not(ddl_utils.is_number(''))
    t.assert_not(ddl_utils.is_number('1.234'))
    t.assert_not(ddl_utils.is_number({1, 2,}))
end

function g.test_concat_arrays()
    t.assert(ddl_utils.concat_arrays({1}, {2, 3}), {1, 2, 3})
    t.assert(ddl_utils.concat_arrays({}, {2, 3}), {2, 3})
    t.assert(ddl_utils.concat_arrays({1}, {}), {1})
end
