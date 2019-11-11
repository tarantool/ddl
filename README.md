# DDL

DDL module for Tarantool 1.10+

## Contents

- [DDL](#ddl)
  - [Contents](#contents)
  - [API](#api)
  - [Input data format](#input-data-format)
  - [Bucket ID calculation example](#bucket-id-calculation-example)
  - [Building and testing](#building-and-testing)

## API

 - ### Set spaces format
    `ddl.set_schema(schema)`
    - If no spaces existed before, create them.
    - If a space exists, check the space's format and indexes.
    - If the format/indexes are different from those in the database,
      return an error.
    - The module doesn't drop or alter any indexes.
    - Spaces omitted in the DDL are ignored, the module doesn't check them.

    Return values: `true` if no error, otherwise return `nil, err`

  - ### Check compatibility
    `ddl.check_schema(schema)`
    - Check that a `set_schema()` call will raise no error.

    Return values: `true` if no error, otherwise return `nil, err`

  - ### Get spaces format
    `ddl.get_schema()`
    - Scan spaces and return the database schema.

  - ### Get bucket id
    `ddl.bucket_id(schema, record, space_name, bucket_count)`
    - Return `bucket_id` of the tuple according to `sharding_key` of a
      particular space schema.
    - Input param `record` is a key-value table

## Input data format

```lua
format = {
    spaces = {
        [space_name] = {
            engine = 'vinyl' | 'memtx',
            is_local = true | false,
            temporary = true | false,
            format = {
                {
                    name = '...',
                    is_nullable = true | false,
                    type = 'unsigned' | 'string' | 'varbinary' |
                            'integer' | 'number' | 'boolean' |
                            'array' | 'scalar' | 'any' | 'map'
                },
                ...
            },
            indexes = {
                -- array of index parameters
                -- integer keys are used as index.id
                -- index parameters depend on the index type
                {
                    type = 'TREE'|'HASH',
                    name = '...',
                    unique = true|false, -- hash index is always unique
                    parts = {
                        -- array of part parameters
                        {
                            path = field_name.jsonpath,
                            -- may be multipath if '[*]' is used,
                            type = 'unsigned' | 'string' | 'varbinary' |
                                'integer' | 'number' | 'boolean' | 'scalar',
                            is_nullable = true | false,
                            collation = nil | 'none' |
                                'unicode' | 'unicode_ci' | '...',
                            -- collation must be set, if and only if
                            -- type == 'string'.
                            -- to see full list of collations
                            -- just run box.space._collation:select()
                        }
                    },
                    sequence = '...', -- sequence_name
                    function = '...', -- function_name
                }, {
                    type = 'RTREE',
                    name = '...',
                    unique = false, -- rtree can't be unique
                    parts = {
                        -- array with only one part parameter
                        {
                            path = field_name.jsonpath,
                            type = 'array',
                            -- rtree index must use array field
                            is_nullable = true|false,
                        }
                    },
                    dimension = number,
                    distance = 'euclid'|'manhattan',
                }, {
                    type = BITSET,
                    name = '...',
                    unique = false, -- bitset index can't be unique
                    parts = {
                        -- array with only one part parameter
                        {
                            path = field_name.jsonpath,
                            type = 'unsigned' | 'string',
                            -- bitset index doesn't support any other
                            -- field types
                            is_nullable = true|false,
                        }
                    },
                },
                ...
            },
            sharding_key = nil | {
                -- array of strings (field_names)
            },
        },
        ...
    },
    functions = { -- Not implemented yet
        [function_name] = {
            body = '...',
            is_deterministic = true|false,
            is_sandboxed = true|false,
            is_multikey = true|false,
        },
        ...
    },
    sequences = { -- Not implemented yet
        [seqence_name] = {
            start
            min
            max
            cycle
            cache
            step

        }
    }
}
```

## Bucket ID calculation example

```lua
local data = {'Ivan', 'Ivanov', 26}
local bucket_id = ddl.bucket_id(schema, data, 'People')
data.bucket_id = bucket_id
vshard.router.callrw(bucket_id, 'box.space.People:insert', data)
```

## Building and testing

```bash
tarantoolctl rocks make
```

```bash
tarantoolctl rocks install luatest 0.3.0
tarantoolctl rocks install luacheck 0.25.0
make test -C build.luarocks ARGS="-V"
```
