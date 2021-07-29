local ddl_get = require('ddl.get')

local M = {
    trigger_on_schema_change = nil,
}

local function create_index(box_space, ddl_index)
    if ddl_index.parts == nil then
        error("index parts is nil")
    end

    local index_parts = {}
    for _, ddl_index_part in ipairs(ddl_index.parts) do
        local index_part = {
            ddl_index_part.path, ddl_index_part.type,
            is_nullable = ddl_index_part.is_nullable,
            collation = ddl_index_part.collation,
        }
        table.insert(index_parts, index_part)
    end

    local sequence_name = nil
    if ddl_index.sequence ~= nil then
        local sequence = box.schema.sequence.create(ddl_index.sequence)
        sequence_name = sequence.name
    end

    box_space:create_index(ddl_index.name, {
        type = ddl_index.type,
        unique = ddl_index.unique,
        parts = index_parts,
        sequence = sequence_name,
        dimension = ddl_index.dimension,
        distance = ddl_index.distance,
        func = ddl_index.func and ddl_index.func.name,
    })
    return true
end

local function create_sharding_key(space_name, space)
    if not space.sharding_key then
        return
    end

    box.space._ddl_sharding_key:insert{space_name, space.sharding_key}
end

local function is_schema_set(space_name)
    local schema = ddl_get.get_space_schema(space_name)
    if schema.spaces ~= nil then
        return true
    end

    return false
end

local function create_space(space_name, space_schema, opts)
    local is_dummy = opts and opts.dummy
    if is_dummy then
        space_name = '_ddl_dummy'
    end


    local ok, data = pcall(box.schema.space.create, space_name, {
        engine = space_schema.engine,
        is_local = space_schema.is_local,
        temporary = space_schema.temporary,
        format = space_schema.format,
    })

    if not ok then
        error(
            string.format("spaces[%q]: %s", space_name, data), 0
        )
    end

    if space_schema.indexes == nil then
        error(
            string.format("spaces[%q]: 'indexes' section is missing (nil)", space_name), 0
        )
    end

    local box_space = data
    for i, index in ipairs(space_schema.indexes) do
        local ok, data = pcall(create_index, box_space, index)
        if not ok then
            error(string.format(
                "spaces[%q].indexes[%q]: %s",  space_name, index.name or i,
                data
            ), 0)
        end
    end

    if not is_dummy then
        local ok, err = pcall(create_sharding_key, space_name, space_schema)
        if not ok then
            return nil, string.format("spaces[%q].sharding_key: %s", space_name, err)
        end
    end

    if is_schema_set(space_name) then
        ddl_get.internal.space_ddl_cache = nil
    end

    if M.trigger_on_schema_change ~= nil then
        local ok = pcall(M.trigger_on_schema_change)
        if not ok then
            return nil, "Execution of trigger 'on_schema_change' is failed"
        end
    end

    return true
end

return {
    create_space = create_space,
    internal = M,
}
