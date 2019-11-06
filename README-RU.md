# DDL

DDL-модуль для Tarantool 1.10+

## Оглавление

- [API](#api)
  - [Задание формата спейсов](#задание-формата-спейсов)
  - [Проверка совместимости](#проверка-совместимости)
  - [Возвращение текущего формата спейсов](#возвращение-текущего-формата-спейсов)
- [Входной формат спейсов](#входной-формат-спейсов)
- [Сборка и тестирование](#сборка-и-тестирование)

## API

 - ### Задание формата спейсов
    `ddl.set_schema(schema)`
    - Если какого-то из спейсов не существует, создать его.
    - Если спейс существует, то проверить его формат и индексы.
    - Если формат/индексы хоть немного отличаются, вернуть ошибку.
    - Модуль не удаляет и не изменяет индексы.
    - Спейсы, не упомянутые в DDL, игнорируются и не проверяются.

    Возвращаемое значение: либо `true`, либо `nil, err`

  - ### Проверка совместимости
    `ddl.check_schema(schema)`
    - Проверить, что вызов `set_schema()` возможен без ошибок.

    Возвращаемое значение: либо `true`, либо `nil, err`

  - ### Возвращение текущего формата спейсов
    `ddl.get_schema()`
    - Сканировать все спейсы, вернуть схему.

## Входной формат спейсов
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

## Сборка и тестирование

```bash
tarantoolctl rocks make
```

```bash
tarantoolctl rocks install luatest 0.3.0
tarantoolctl rocks install luacheck 0.25.0
make test -C build.luarocks ARGS="-V"
```
