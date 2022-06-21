<a href="https://github.com/tarantool/ddl/actions?query=workflow%3ATest">
<img src="https://github.com/tarantool/ddl/workflows/Test/badge.svg">
</a>
<a href='https://coveralls.io/github/tarantool/ddl?branch=master'>
<img src='https://coveralls.io/repos/github/tarantool/ddl/badge.svg?branch=master' alt='Coverage Status' />
</a>

# DDL

DDL module for Tarantool 1.10+

The DDL module enables you to describe data schema in a declarative YAML-based format.
It is a simpler alternative to describing data schema in Lua and doesn't require having a deep knowledge of Lua.
DDL is a built-in Cartridge module. See more details about Tarantool's data mode
in [documementation](https://www.tarantool.io/en/doc/latest/book/box/data_model/#data-schema-description-using-the-ddl-module).

## Contents

- [API](#api)
  - [Set spaces format](#set-spaces-format)
  - [Check compatibility](#check-compatibility)
  - [Get spaces format](#get-spaces-format)
- [Input data format](#input-data-format)
- [Building and testing](#building-and-testing)

## API

### Set spaces format
    `ddl.set_schema(schema)`
    - If no spaces existed before, create them.
    - If a space exists, check the space's format and indexes.
    - If the format/indexes are different from those in the database,
      return an error.
    - The module doesn't drop or alter any indexes.
    - Spaces omitted in the DDL are ignored, the module doesn't check them.

    Return values: `true` if no error, otherwise return `nil, err`

Call of function `ddl.set_schema(schema)` creates a space `_ddl_sharding_key` with two
fields: `space_name` with type `string` and `sharding_key` with type `array`.

Similarly for `sharding_func`: call of function `ddl.set_schema(schema)` creates
a space `_ddl_sharding_func` with three fields: `space_name`, `sharding_func_name` and
`sharding_func_body` with type string.

If you want to use sharding function from some module, you need to require
and set to `_G` the module with sharding function first. For example:
to use sharding functions like
[vshard.router.bucket_id_strcrc32](https://www.tarantool.io/doc/latest/reference/reference_rock/vshard/vshard_api/#router-api-bucket-id-strcrc32)
and
[vshard.router.bucket_id_mpcrc32](https://www.tarantool.io/doc/latest/reference/reference_rock/vshard/vshard_api/#router-api-bucket-id-mpcrc32)
from `vshard` module you need to require `vshard` module.

Also, you can pass your own sharding function by defining the function name in `_G`
or by specifying lua code directly in `body` field:
```lua
sharding_func = {
  body = 'function(key) return <...> end'
}
```

Your defined sharding function in `_G` should have type `function`
or `table` | `cdata` | `userdata` with `__call` metamethod.

Defined function must have a prototype according to the following rules:

**Parameters**

key (number | table | string | boolean) – a sharding key.

**Return value**

bucket identifier (number)

### Check compatibility
    `ddl.check_schema(schema)`
    - Check that a `set_schema()` call will raise no error.

    Return values: `true` if no error, otherwise return `nil, err`

### Get spaces format
    `ddl.get_schema()`
    - Scan spaces and return the database schema.

    Return values: table with space's schemas (see "Schema example")

### Get bucket id
    `ddl.bucket_id(space_name, sharding_key)`
    - Calculate bucket id for a specified space and sharding key.
    Method uses sharding function specified in DDL schema.

    Method is not transactional in the sense that it catches up
    `_ddl_sharding_func` changes immediatelly: it may see changes that're
    not committed yet and may see a state from another transaction,
    which should not be visible in the current transaction.

    Return values: bucket_id if no error, otherwise return `nil, err`

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
                            'decimal' | 'double' | 'uuid' | 'datetime'
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
                                'decimal' | 'double' | 'uuid' | 'datetime',
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
            sharding_func = 'dot.notation' | 'sharding_func_name_defined_in_G'
                            {body = 'function(key) return <...> end'},
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
            sharding_func = 'vshard.router.bucket_id_mpcrc32',
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
