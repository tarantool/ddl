#!/usr/bin/env tarantool

local function version()
    return _TARANTOOL
end

local function check_version(expected_major, expected_minor)
    local db_ver = version()
    local db_major, db_minor = db_ver:match('^(%d+)%.(%d+)')
    local major, minor = tonumber(db_major), tonumber(db_minor)

    if major < expected_major then
        return false
    elseif major > expected_major then
        return true
    end

    if minor < expected_minor then
        return false
    end
    return true
end

local function json_path_allowed()
    return check_version(2, 1)
end

local function multikey_path_allowed()
    return check_version(2, 2) --major_version() > 1 and minor_version() > 1
end

local function varbinary_allowed()
    return check_version(2, 2) --major_version() > 1
end

return {
    json_path_allowed = json_path_allowed,
    version = version,
    varbinary_allowed = varbinary_allowed,
    multikey_path_allowed = multikey_path_allowed,
}