# DDL

Tarantool DDL (Data Definition Language) module

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
      - Check that a `set_schema` call will raise no error.

    Return values: `true` if no error, otherwise return `nil, err`

  - ### Get spaces format
    `ddl.get_schema()`
    - Scan spaces and return the database schema.

## Input data format
```
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
                            path = string (field_name[.path] within field incl. multipath with[*],
                            type = '...',
                            -- unsigned | string | varbinary | integer |
                            -- number | boolean | scalar
                            collation = nil|'none'|'unicode'|'unicode_ci'|...,
                            -- collation must be set, if and only if
                            -- type =
                            -- to see full list of collations
                            -- just run box.space._collation:select()
                            is_nullable = true|false,
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
                            path = string (field_name[.path] within field,
                            type = 'array'
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
                            path = string (field_name[.path] within field,
                            type = 'unsigned | string'
                            is_nullable = true|false,
                        }
                    },
                },
                ...
            },
        },
        ...
    },
    functions = {
        [function_name] = {
            body = '...',
            is_deterministic = true|false,
            is_sandboxed = true|false,
            is_multikey = true|false,
        },
        ...
    },
    sequences = {
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

## Building and testing

```bash
tarantoolctl rocks make
```

```bash
tarantoolctl rocks install luatest 0.3.0
tarantoolctl rocks install luacheck 0.25.0
make test -C build.luarocks ARGS="-V"
```
