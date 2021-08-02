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

local function create_metadata(space_name, metadata, metadata_name)
    if not next(metadata) then
        return
    end

    local metadata_space_name = string.format("_ddl_%s", metadata_name)

    box.space[metadata_space_name]:insert{space_name, unpack(metadata, 1, table.maxn(metadata))}
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
        local ok, err = pcall(create_metadata, space_name, {space_schema.sharding_key}, "sharding_key")
        if not ok then
            return nil, string.format("spaces[%q].sharding_key: %s", space_name, err)
        end

        local sharding_func_body = nil
        if type(space_schema.sharding_func) == 'table' then
            sharding_func_body = space_schema.sharding_func.body
        end

        local sharding_func_name = nil
        if type(space_schema.sharding_func) == 'string' then
            sharding_func_name = space_schema.sharding_func
        end

        local ok, err = pcall(create_metadata, space_name, {sharding_func_name, sharding_func_body}, "sharding_func")
        if not ok then
            return nil, string.format("spaces[%q].sharding_func: %s", space_name, err)
        end
    end
    return true
end

return {
    create_space = create_space,
}
