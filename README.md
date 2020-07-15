# DDL

DDL module for Tarantool 1.10+

## Contents

- [API](#api)
  - [Set spaces format](#set-spaces-format)
  - [Check compatibility](#check-compatibility)
  - [Get spaces format](#get-spaces-format)
- [Input data format](#input-data-format)
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
                            'array' | 'scalar' | 'any' | 'map' |
                            'decimal' | 'double' | 'uuid'
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
                                'integer' | 'number' | 'boolean' | 'scalar' |
                                'decimal' | 'double' | 'uuid',
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
                --
                -- sharded space must have:
                -- field: {name = 'bucket_id', is_nullable = false, type = 'unsigned'}
                -- index: {
                --     name = 'bucket_id',
                --     type = 'TREE',
                --     unique = false,
                --     parts = {{path = 'bucket_id', is_nullable = false, type = 'unsigned'}}
                -- }
                --
                -- unsharded spaces must NOT have
                -- field and index named 'bucket_id'
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

## Schema example

```lua
local schema = {
    spaces = {
        customer = {
            engine = 'memtx',
            is_local = false,
            temporary = false,
            format = {
                {name = 'customer_id', is_nullable = false, type = 'unsigned'},
                {name = 'bucket_id', is_nullable = false, type = 'unsigned'},
                {name = 'fullname', is_nullable = false, type = 'string'},
            },
            indexes = {{
                name = 'customer_id',
                type = 'TREE',
                unique = true,
                parts = {
                    {path = 'customer_id', is_nullable = false, type = 'unsigned'}
                }
            }, {
                name = 'bucket_id',
                type = 'TREE',
                unique = false,
                parts = {
                    {path = 'bucket_id', is_nullable = false, type = 'unsigned'}
                }
            }, {
                name = 'fullname',
                type = 'TREE',
                unique = true,
                parts = {
                    {path = 'fullname', is_nullable = false, type = 'string'}
                }
            }},
            sharding_key = {'customer_id'},
        },
        account = {
            engine = 'memtx',
            is_local = false,
            temporary = false,
            format = {
                {name = 'account_id', is_nullable = false, type = 'unsigned'},
                {name = 'customer_id', is_nullable = false, type = 'unsigned'},
                {name = 'bucket_id', is_nullable = false, type = 'unsigned'},
                {name = 'balance', is_nullable = false, type = 'string'},
                {name = 'name', is_nullable = false, type = 'string'},
            },
            indexes = {{
                name = 'account_id',
                type = 'TREE',
                unique = true,
                parts = {
                    {path = 'account_id', is_nullable = false, type = 'unsigned'}
                }
            }, {
                name = 'customer_id',
                type = 'TREE',
                unique = false,
                parts = {
                    {path = 'customer_id', is_nullable = false, type = 'unsigned'}
                }
            }, {
                name = 'bucket_id',
                type = 'TREE',
                unique = false,
                parts = {
                    {path = 'bucket_id', is_nullable = false, type = 'unsigned'}
                }
            }},
            sharding_key = {'customer_id'},
        }
    }
}
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
