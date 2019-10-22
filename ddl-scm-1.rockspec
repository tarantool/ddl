package = 'ddl'
version = 'scm-1'
source  = {
    branch = 'master',
    url = 'git://github.com/tarantool/ddl.git'
}

dependencies = {
    'lua >= 5.1';
    'tarantool';
}

description = {
    summary = 'Tarantool opensource DDL module';
    homepage = 'https://github.com/tarantool/ddl';
    detailed = [[
        A ready-to-use Lua module ddl for tarantool.
        ]];
}

build = {
    type = 'none',
    install = {
        lua = {
            ['ddl'] = 'ddl.lua',
            ['ddl.utils'] = 'ddl/utils.lua',
            ['ddl.get_schema'] = 'ddl/get_schema.lua',
            ['ddl.set_schema'] = 'ddl/set_schema.lua',
            ['ddl.validator'] = 'ddl/validator.lua',
        },
    },
    copy_directories = {'doc'},
}
