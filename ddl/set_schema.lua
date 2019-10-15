local function _set_index(box_space, ddl_index)
    if ddl_index.func ~= nil then
        box.schema.func.create(ddl_index.func.name, {
            body = ddl_index.func.body,
            is_deterministic = ddl_index.func.is_deterministic,
            is_sandboxed = ddl_index.is_sandboxed,
            opts = ddl_index.func.opts,
        })
    end

    local index_parts = {}
    for _, ddl_index_part in ipairs(ddl_index.parts) do
        local index_part = {
            ddl_index_part.path, ddl_index_part.type,
            is_nullable = ddl_index_part.is_nullable, collation = ddl_index_part.collation,
        }
        table.insert(index_parts, index_part)
    end

    box_space:create_index(ddl_index.name, {
        type = ddl_index.type,
        unique = ddl_index.unique,
        parts = index_parts,
        seuence = ddl_index.sequence,
        dimension = ddl_index.dimension,
        distance = ddl_index.distance,
        func = ddl_index.func and ddl_index.func.name,
    })
    return true
end

local function _set_space(space_name, space)
    local box_space = box.schema.space.create(space_name, {
        engine = space.engine,
        is_local = space.is_local,
        temporary = space.temporary,
        format = space.format,
    })

    for _, index in ipairs(space.indexes) do
        _set_index(box_space, index)
    end
    return true
end

local function set_schema(spaces)
    box.begin()
    for space_name, space in pairs(spaces) do
        local status, data = pcall(_set_space, space_name, space)
        if not status then
            box.rollback()
            return nil, data
        end
    end

    if box.is_in_txn() then
        box.commit()
    end

    return true
end


return {
    set_schema = set_schema,
}
