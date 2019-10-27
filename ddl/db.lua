#!/usr/bin/env tarantool

local function major_version()
    local db_ver = require('tarantool').version
    local major = db_ver:match('^(%d+)%.')

    return tonumber(major)
end

local function version()
    return require('tarantool').version
end

local function json_path_allowed()
    return major_version() > 1
end

local function varbinary_allowed()
    return major_version() > 1
end

return {
    json_path_allowed = json_path_allowed,
    version = version,
    varbinary_allowed = varbinary_allowed,
}