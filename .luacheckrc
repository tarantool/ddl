redefined = false
exclude_files = {
    '.rocks/*'
}
new_read_globals = {
    'box',
    '_TARANTOOL',
    'tonumber64',
    'unpack',
    os = {
        fields = {
            'environ',
        }
    },
    string = {
        fields = {
            'split',
        },
    },
    table = {
        fields = {
            'maxn',
            'copy',
            'new',
            'clear',
            'move',
            'foreach',
            'sort',
            'remove',
            'foreachi',
            'deepcopy',
            'getn',
            'concat',
            'insert',
        },
    },
}
