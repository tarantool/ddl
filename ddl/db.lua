local function check_version(expected_major, expected_minor)
    local db_major, db_minor = string.match(_TARANTOOL, '^(%d+)%.(%d+)')
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
    return check_version(2, 2)
end

local function varbinary_allowed()
    return check_version(2, 2)
end

local function datetime_allowed()
    return check_version(2, 10)
end


-- https://github.com/tarantool/tarantool/issues/4083
local function transactional_ddl_allowed()
    return check_version(2, 2)
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
