find_program(LUACHECK luacheck
    HINTS .rocks/
    PATH_SUFFIXES bin
    DOC "Lua linter"
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(LUACHECK
    REQUIRED_VARS LUACHECK
)

mark_as_advanced(LUACHECK)
