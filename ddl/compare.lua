local INT64_MIN = tonumber64('-9223372036854775808')
local INT64_MAX = tonumber64('9223372036854775807')

local function get_sequence_defaults(opts)
    -- https://github.com/tarantool/tarantool/blob/05e69108076ae22f33f8fc55b463c6babb6478fe/src/box/lua/schema.lua#L2847-L2855
    local ascending = not opts.step or opts.step > 0
    return {
        step = 1,
        min = ascending and 1 or INT64_MIN,
        max = ascending and INT64_MAX or -1,
        start = ascending and (opts.min or 1) or (opts.max or -1),
        cache = 0,
        cycle = false,
    }
end

local function assert_equiv_sequence_schema(sequence_schema, current_schema)
    local defaults = get_sequence_defaults(sequence_schema)

    for key, current_value in pairs(current_schema) do
        local ddl_value = sequence_schema[key]

        if ddl_value ~= nil then
            if ddl_value ~= current_value then
                return nil, ('%s (expected %s, got %s)'):format(key, current_value, ddl_value)
            end
        else
            local default_value = defaults[key]
            if default_value ~= current_value then
                return nil, ('%s (expected %s, got nil and default is %s)'):format(key, current_value, default_value)
            end
        end
    end

    return true
end

return {
    assert_equiv_sequence_schema = assert_equiv_sequence_schema,
}
