local cache = nil

local SPACE_NAME_IDX      = 1
local SHARD_FUNC_NAME_IDX = 2
local SHARD_FUNC_BODY_IDX = 3

-- Build cache.
--
-- We don't need to call this function with any type of locking:
-- _ddl_sharding_func is memtx space, so calling :pairs() on it
-- is atomic
--
-- Cache structure format:
--
-- cache = {
--     spaces = {
--         [space_name] = {
--             -- raw sharding metadata, used for ddl.get()
--             raw_tuple = <tuple object> (<nil> at error),
--             -- parsed dot notation (like {'foo', 'bar'})
--             parsed_func_name = <table> or <nil>
--             -- a function ready to call
--             callable = <function> or <nil>,
--             -- string with an error: not nil only if setting callable fails
--             error = <string> or <nil>,
--         }
--     },
--     -- current schema version
--     schema_version = <...>,
-- }

-- function returns nothing
local function cache_build()
    -- clear cache
    cache.spaces = {}

    if box.space._ddl_sharding_func == nil then
        return
    end

    for _, tuple in box.space._ddl_sharding_func:pairs() do
        local space_name = tuple[SPACE_NAME_IDX]
        local func_name = tuple[SHARD_FUNC_NAME_IDX]
        local func_body = tuple[SHARD_FUNC_BODY_IDX]

        cache.spaces[space_name] = {
            raw_tuple = tuple
        }

        if func_body ~= nil then
            local sharding_func, err = loadstring('return ' .. func_body)
            if sharding_func == nil then
                cache.spaces[space_name].error =
                    string.format("Body is incorrect in sharding_func for space (%s): %s",
                    space_name, err)
            else
                cache.spaces[space_name].callable = sharding_func()
            end
        elseif func_name ~= nil then
            -- we cannot save the function itself into the cache,
            -- because the function can be changed in runtime and
            -- there is no way to catch this change
            local chunks = string.split(func_name, '.')
            cache.spaces[space_name].parsed_func_name = chunks
        end
    end

    cache.schema_version = box.internal.schema_version()

end

-- Rebuild cache if _ddl_sharding_func space changed.
local function cache_set_trigger()
    if box.space._ddl_sharding_func == nil then
        return
    end

    local trigger_found = false

    for _, func in pairs(box.space._ddl_sharding_func:on_replace()) do
        if func == cache_build then
            trigger_found = true
            break
        end
    end

    if not trigger_found then
        box.space._ddl_sharding_func:on_replace(cache_build)
    end
end

-- Get data from cache.
-- Returns all cached data for "space_name" or nil.
local function cache_get(space_name)
    if space_name == nil then
        return nil
    end

    -- using tarantool internal API.
    -- this is not reliable, but it is the only way to track
    -- schema_version changes. Fix it if a public method appears:
    -- https://github.com/tarantool/tarantool/issues/6544
    local schema_version = box.internal.schema_version()

    if not cache then
        cache = {}
        cache_build()
        cache_set_trigger()
    end

    -- rebuild cache if database schema changed
    if schema_version ~= cache.schema_version then
        cache_build()
        cache_set_trigger()
    end

    return cache.spaces[space_name]
end

return {
    internal = {
        get = cache_get,
    }
}
