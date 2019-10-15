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
	[space_name] = {
		engine = vinyl|memtx,
		is_local = ...,
		temporary = ...,
		format = {
			{
				name = '...',
				type = '...',
				-- unsigned | string | varbinary | integer |
				-- number | boolean | array | scalar | any | map
				is_nullable = ...
			},
			...
		},
		indexes = {
			-- array of index paramters
			-- integer keys are used as index.id
			-- index params depend on index type
			{
				name = '...',
				type = 'TREE'|'HASH',
				unique = true|false, -- HASH can't be unique
				parts = {
					-- array of part params
					{
						field = string (field name as in space format),
						path = string (path within field incl. multipath with[*],
						type = '...',
						-- unsigned | string | varbinary | integer |
						-- number | boolean | scalar
						collation = 'unicode|unicode_ci|...',
						-- to see full list of collations
						-- just run box.space._collation:select()
						is_nullable = true|false,
					}
				},
				sequence = ...,
				func = {
					name = 'string',
					body = 'string',
					is_determenistic = true|false,
					is_sandboxed = true|false,
					is_multikey = true|false,
				}
			}, {
				name = '...',
				type = 'RTREE',
				field = string (this field format must be 'array')
				dimension = number,
				distance = 'euclid'|'manhattan',
				-- can't be unique
			}, {
				name = '...',
				type = BITSET,
				field = string (this field format must be 'string')
				-- can't be unique
			},
			...
		},
	},
	...
}
```
