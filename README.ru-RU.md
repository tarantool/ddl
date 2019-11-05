# DDL
Tarantool ddl module

## API

 - ### Set spaces format
    `ddl.set_schema(schema)`
    - Если какого-то из спейсов не существовало - модуль его создаёт
    - Если спейс существует, то модуль проверяет его формат/индексы
    - Если формат/индексы хоть немного отличается - это ошибка
    - Не удаляет и не изменят индексы
    - Спейсы, не упомянутые в ddl игнорируются и не проверяются

    Возвращаемое значение: либо true, либо nil, err.

  - ### Check compatibility
    `ddl.check_schema(schema)`
      - Проверяет, что set_schema возможен без ошибок

    Возвращаемое значение: либо true, либо nil, err.

  - ### Get spaces format
    `ddl.get_schema()`
    - Сканит спейсы, возвращает схему


## Input data fromat
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
                -- array of index paramters
                -- integer keys are used as index.id
                -- index params depend on index type
                {
                    type = 'TREE'|'HASH',
                    name = '...',
                    unique = true|false, -- hash index is always unique
                    parts = {
                        -- array of part params
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
                        -- array with only one part param
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
                        -- array with only one part param
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
