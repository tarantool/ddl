#!/usr/bin/env tarantool

local ffi = require('ffi')

local function deepcmp(got, expected, extra)
    if extra == nil then
        extra = {}
    end

    if type(expected) == "number" or type(got) == "number" then
        extra.got = got
        extra.expected = expected
        if got ~= got and expected ~= expected then
            return true -- nan
        end
        return got == expected
    end

    if ffi.istype('bool', got) then got = (got == 1) end
    if ffi.istype('bool', expected) then expected = (expected == 1) end
    if got == nil and expected == nil then return true end

    if type(got) ~= type(expected) then
        extra.got = type(got)
        extra.expected = type(expected)
        return false
    end

    if type(got) ~= 'table' then
        extra.got = got
        extra.expected = expected
        return got == expected
    end

    local path = extra.path or '/'

    for i, v in pairs(got) do
        extra.path = path .. '/' .. i
        if not deepcmp(v, expected[i], extra) then
            return false
        end
    end

    for i, v in pairs(expected) do
        extra.path = path .. '/' .. i
        if not deepcmp(got[i], v, extra) then
            return false
        end
    end

    extra.path = path

    return true
end

local function find_first_duplicate(arr_objects, object_field)
    local key_map = {}
    for _, object in ipairs(arr_objects) do
        local key = object[object_field]
        if key_map[key] ~= nil then
            return key
        end
        key_map[key] = true
    end
    return nil
end

local function table_find(table, value)
    for k, v in pairs(table) do
        if v == value then
            return k
        end
    end

    return nil
end

local function is_array(data)
    if type(data) ~= 'table' then
        return false
    end

    local i = 0
    for _, _ in pairs(data) do
        i = i + 1
        if type(data[i]) == 'nil' then
            return false
        end
    end

    return true
end

local function redundant_key(tbl, known_keys)
    for k, _ in pairs(tbl) do
        if not table_find(known_keys, k) then
            return k
        end
    end

    return nil
end

return {
    deepcmp = deepcmp,
    is_array = is_array,
    redundant_key = redundant_key,
    find_first_duplicate = find_first_duplicate,
}
