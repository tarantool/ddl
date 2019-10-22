#!/usr/bin/env tarantool

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

local function find_redutant_fields(valid_fields, map)
    local redutant_fields = {}
    for k, _ in pairs(map) do
        if valid_fields[k] == nil then
            table.insert(redutant_fields, k)
        end
    end

    if #redutant_fields > 0 then
        return redutant_fields
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

return {
    is_array = is_array,
    find_redutant_fields = find_redutant_fields,
    find_first_duplicate = find_first_duplicate,
}
