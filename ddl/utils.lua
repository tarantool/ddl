local ffi = require('ffi')
local bit = require('bit')

-- copy from LuaJIT lj_char.c
local lj_char_bits = {
    0,
    1,  1,  1,  1,  1,  1,  1,  1,  1,  3,  3,  3,  3,  3,  1,  1,
    1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
    2,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,
    152,152,152,152,152,152,152,152,152,152,  4,  4,  4,  4,  4,  4,
    4,176,176,176,176,176,176,160,160,160,160,160,160,160,160,160,
    160,160,160,160,160,160,160,160,160,160,160,  4,  4,  4,  4,132,
    4,208,208,208,208,208,208,192,192,192,192,192,192,192,192,192,
    192,192,192,192,192,192,192,192,192,192,192,  4,  4,  4,  4,  1,
    128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,
    128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,
    128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,
    128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,
    128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,
    128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,
    128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,
    128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128
}

local LJ_CHAR_IDENT = 0x80
local LJ_CHAR_DIGIT = 0x08

local LUA_KEYWORDS = {
    ['and'] = true,
    ['end'] = true,
    ['in'] = true,
    ['repeat'] = true,
    ['break'] = true,
    ['false'] = true,
    ['local'] = true,
    ['return'] = true,
    ['do'] = true,
    ['for'] = true,
    ['nil'] = true,
    ['then'] = true,
    ['else'] = true,
    ['function'] = true,
    ['not'] = true,
    ['true'] = true,
    ['elseif'] = true,
    ['if'] = true,
    ['or'] = true,
    ['until'] = true,
    ['while'] = true,
}

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

local function lj_char_isident(n)
    return bit.band(lj_char_bits[n + 2], LJ_CHAR_IDENT) == LJ_CHAR_IDENT
end

local function lj_char_isdigit(n)
    return bit.band(lj_char_bits[n + 2], LJ_CHAR_DIGIT) == LJ_CHAR_DIGIT
end

local function check_name_isident(name)
    if name == nil or name == '' then
        return false
    end

    -- sharding function name cannot
    -- be equal to lua keyword
    if LUA_KEYWORDS[name] then
        return false
    end

    -- sharding function name cannot
    -- begin with a digit
    local char_number = string.byte(name:sub(1,1))
    if lj_char_isdigit(char_number) then
        return false
    end

    -- sharding func name must be sequence
    -- of letters, digits, or underscore symbols
    for i = 1, #name do
        local char_number = string.byte(name:sub(i,i))
        if not lj_char_isident(char_number) then
            return false
        end
    end

    return true
end

-- split sharding func name in dot notation by dot
-- foo.bar.baz -> chunks: foo bar baz
-- foo -> chunks: foo
--
-- func_name parameter may be a string in dot notation or table
-- if func_name type is of type table it is assumed that it is already split
local function get_G_function(func_name)
    local sharding_func = _G
    local chunks

    if type(func_name) == 'string' then
        chunks = string.split(func_name, '.')
    else
        chunks = func_name
    end

    -- check is the each chunk an identifier
    for _, chunk in pairs(chunks) do
        if not check_name_isident(chunk) or sharding_func == nil then
            return nil
        end
        sharding_func = rawget(sharding_func, chunk)
    end

    return sharding_func
end

return {
    deepcmp = deepcmp,
    is_array = is_array,
    redundant_key = redundant_key,
    find_first_duplicate = find_first_duplicate,
    lj_char_isident = lj_char_isident,
    lj_char_isdigit = lj_char_isdigit,
    LUA_KEYWORDS = LUA_KEYWORDS,
    get_G_function = get_G_function,
}
