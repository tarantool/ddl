cmake_minimum_required(VERSION 2.8 FATAL_ERROR)

project(ddl C)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})
set(CMAKE_SKIP_INSTALL_ALL_DEPENDENCY TRUE)

# Find Tarantool and Lua dependecies
set(TARANTOOL_FIND_REQUIRED ON)
find_package(Tarantool)
include_directories(${TARANTOOL_INCLUDE_DIRS})

## Testing ####################################################################
###############################################################################

enable_testing()

find_package(LuaCheck)
add_test(
  NAME luacheck
  COMMAND ${LUACHECK} .
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

find_package(LuaTest)
add_test(
  NAME luatest
  COMMAND ${LUATEST} -v
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

## Install ####################################################################
###############################################################################

install(
  FILES ${CMAKE_CURRENT_SOURCE_DIR}/ddl.lua
  DESTINATION ${TARANTOOL_INSTALL_LUADIR}/
)

file(GLOB_RECURSE LUA_FILES
  "${CMAKE_CURRENT_SOURCE_DIR}/ddl/*.lua"
)
install(
  FILES ${LUA_FILES}
  DESTINATION ${TARANTOOL_INSTALL_LUADIR}/${PROJECT_NAME}/
)
