#!/usr/bin/env tarantool

local ddl = require('ddl')
local db = require('test.db')
local clock = require('clock')
local helper = require('test.helper')

local ITERS = 10000000

local primary_index = {
    type = 'HASH',
    unique = true,
    parts = {
        {path = 'string_nonnull', is_nullable = false, type = 'string'},
        {path = 'unsigned_nonnull', is_nullable = false, type = 'unsigned'},
    },
    name = 'primary'
}

local bucket_id_idx = {
    type = 'TREE',
    unique = false,
    parts = {{path = 'bucket_id', type = 'unsigned', is_nullable = false}},
    name = 'bucket_id'
}

local function space_init()
    db.drop_all()

    local space = {
        engine = 'memtx',
        is_local = true,
        temporary = false,
        format = table.deepcopy(helper.test_space_format())
    }
    table.insert(space.format, 1, {
        name = 'bucket_id', type = 'unsigned', is_nullable = false
    })

    space.indexes = {
        table.deepcopy(primary_index),
        table.deepcopy(bucket_id_idx)
    }
    space.sharding_key = {'unsigned_nonnull', 'integer_nonnull'}
    local schema = {
        spaces = {
            space = space,
        }
    }

    return schema
end

local function run_body()
    local schema = space_init()
    schema.spaces.space.sharding_func = {
        body = helper.sharding_func_body
    }

    local _, err = ddl.set_schema(schema)
    if err then
        print(err)
        os.exit()
    end

    for i=1,ITERS do
        local _, err = ddl.bucket_id('space', i)
        if err then
            print(err)
            os.exit()
        end
    end
end

local function run_baseline()
    local schema = space_init()
    schema.spaces.space.sharding_func = {
        body = helper.sharding_func_body
    }

    local _, err = ddl.set_schema(schema)
    if err then
        print(err)
        os.exit()
    end

    for i=1,ITERS do
        helper.sharding_func(i)
        if err then
            print(err)
            os.exit()
        end
    end
end

local function run_name()
    local schema = space_init()
    local sharding_func_name = 'sharding_func'
    rawset(_G, sharding_func_name, helper.sharding_func)
    schema.spaces.space.sharding_func = sharding_func_name

    local _, err = ddl.set_schema(schema)
    if err then
        print(err)
        os.exit()
    end

    for i=1,ITERS do
        local _, err = ddl.bucket_id('space', i)
        if err then
            print(err)
            os.exit()
        end
    end
end

local function main()
    local benchs = {
        {"baseline",      run_baseline},
        {"function body", run_body    },
        {"function name", run_name    }
    }

    -- suppress logs from tarantool
    require("log").level(1)
    db.init()

    for _,b in pairs(benchs) do
        io.write(string.format("* cache benchmark: %s... ", b[1]))
        io.flush()
        local start = clock.monotonic()
        b[2]()
        local finish = clock.monotonic()
        print(string.format("%0.3f seconds", finish - start))
    end

    os.exit()
end

main()
