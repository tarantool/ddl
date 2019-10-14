local function array_size(arr)
    local counter = 0
    for _, _ in pairs(arr) do
        counter = counter + 1
    end
    return counter
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

local function is_array(data)
    if type(data) ~= 'table' then
        return false
    end

    return array_size(data) == #data
end

return {
    array_size = array_size,
    find_first_duplicate = find_first_duplicate,
    is_array = is_array,
}
