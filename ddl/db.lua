-- Utilities borrowed from tarantool/crud
-- https://github.com/tarantool/crud/blob/2d3d47937fd02d938424659bc659fdc24a32dc8a/crud/common/utils.lua#L434-L555

local function get_version_suffix(suffix_candidate)
    if type(suffix_candidate) ~= 'string' then
        return nil
    end

    if suffix_candidate:find('^entrypoint$')
    or suffix_candidate:find('^alpha%d$')
    or suffix_candidate:find('^beta%d$')
    or suffix_candidate:find('^rc%d$') then
        return suffix_candidate
    end

    return nil
end

local suffix_with_digit_weight = {
    alpha = -3000,
    beta  = -2000,
    rc    = -1000,
}

local function get_version_suffix_weight(suffix)
    if suffix == nil then
        return 0
    end

    if suffix:find('^entrypoint$') then
        return -math.huge
    end

    for header, weight in pairs(suffix_with_digit_weight) do
        local pos, _, digits = suffix:find('^' .. header .. '(%d)$')
        if pos ~= nil then
            return weight + tonumber(digits)
        end
    end

    error(('Unexpected suffix %q, parse with "utils.get_version_suffix" first'):format(suffix))
end

local function is_version_ge(major, minor,
                             patch, suffix,
                             major_to_compare, minor_to_compare,
                             patch_to_compare, suffix_to_compare)
    major = major or 0
    minor = minor or 0
    patch = patch or 0
    local suffix_weight = get_version_suffix_weight(suffix)

    major_to_compare = major_to_compare or 0
    minor_to_compare = minor_to_compare or 0
    patch_to_compare = patch_to_compare or 0
    local suffix_weight_to_compare = get_version_suffix_weight(suffix_to_compare)

    if major > major_to_compare then return true end
    if major < major_to_compare then return false end

    if minor > minor_to_compare then return true end
    if minor < minor_to_compare then return false end

    if patch > patch_to_compare then return true end
    if patch < patch_to_compare then return false end

    if suffix_weight > suffix_weight_to_compare then return true end
    if suffix_weight < suffix_weight_to_compare then return false end

    return true
end

local function get_tarantool_version()
    local version_parts = rawget(_G, '_TARANTOOL'):split('-', 1)

    local major_minor_patch_parts = version_parts[1]:split('.', 2)
    local major = tonumber(major_minor_patch_parts[1])
    local minor = tonumber(major_minor_patch_parts[2])
    local patch = tonumber(major_minor_patch_parts[3])

    local suffix = get_version_suffix(version_parts[2])

    return major, minor, patch, suffix
end

local function tarantool_version_at_least(wanted_major, wanted_minor, wanted_patch)
    local major, minor, patch, suffix = get_tarantool_version()

    return is_version_ge(major, minor, patch, suffix,
                         wanted_major, wanted_minor, wanted_patch, nil)
end


local function json_path_allowed()
    return tarantool_version_at_least(2, 1)
end

local function multikey_path_allowed()
    return tarantool_version_at_least(2, 2)
end

local function varbinary_allowed()
    return tarantool_version_at_least(2, 2)
end

local function datetime_allowed()
    return tarantool_version_at_least(2, 10)
end

-- https://github.com/tarantool/tarantool/issues/4083
local function transactional_ddl_allowed()
    return tarantool_version_at_least(2, 2)
end


local function atomic_tail(status, ...)
    if not status then
        box.rollback()
        error((...), 0)
    end
    box.commit()
    return ...
end

local function call_atomic(fun, ...)
    if transactional_ddl_allowed() then
        box.begin()
    end
    return atomic_tail(pcall(fun, ...))
end

local function dry_run_tail(status, ...)
    if not status then
        box.rollback()
        error((...), 0)
    end
    box.rollback()
    return ...
end

local function call_dry_run(fun, ...)
    if transactional_ddl_allowed() then
        box.begin()
    end
    return dry_run_tail(pcall(fun, ...))
end

return {
    json_path_allowed = json_path_allowed,
    varbinary_allowed = varbinary_allowed,
    multikey_path_allowed = multikey_path_allowed,
    transactional_ddl_allowed = transactional_ddl_allowed,
    datetime_allowed = datetime_allowed,

    call_atomic = call_atomic,
    call_dry_run = call_dry_run,
}
