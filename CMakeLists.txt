cmake_minimum_required(VERSION 2.8 FATAL_ERROR)

project(ddl C)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})
set(CMAKE_SKIP_INSTALL_ALL_DEPENDENCY TRUE)

# Find Tarantool and Lua dependencies
set(TARANTOOL_FIND_REQUIRED ON)
find_package(Tarantool)
include_directories(${TARANTOOL_INCLUDE_DIRS})

## Testing ####################################################################
###############################################################################

enable_testing()

find_package(LuaCheck)
add_custom_target(luacheck
  COMMAND ${LUACHECK} ${PROJECT_SOURCE_DIR}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

find_package(LuaTest)
find_package(LuaCov)
find_package(LuaCovCoveralls)

set(CODE_COVERAGE_REPORT "${PROJECT_SOURCE_DIR}/luacov.report.out")
set(CODE_COVERAGE_STATS "${PROJECT_SOURCE_DIR}/luacov.stats.out")
string(RANDOM ALPHABET 0123456789 seed)

add_custom_target(luatest
  COMMAND ${LUATEST} -v --coverage --shuffle all:${seed}
  BYPRODUCTS ${CODE_COVERAGE_STATS}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Run regression tests"
)

add_custom_target(luatest-no-coverage
  COMMAND ${LUATEST} -v --shuffle all:${seed}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Run regression tests without coverage"
)

add_custom_target(coverage
  COMMAND ${LUACOV} ${PROJECT_SOURCE_DIR} && grep -A999 '^Summary' ${CODE_COVERAGE_REPORT}
  DEPENDS ${CODE_COVERAGE_STATS}
  BYPRODUCTS ${CODE_COVERAGE_REPORT}
  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
  COMMENT "Generate code coverage stats"
)

if(DEFINED ENV{GITHUB_TOKEN})
  set(COVERALLS_COMMAND ${LUACOVCOVERALLS} --include ^ddl --verbose --repo-token $ENV{GITHUB_TOKEN})
else()
  set(COVERALLS_COMMAND ${CMAKE_COMMAND} -E echo "Skipped uploading to coveralls.io: no token.")
endif()

add_custom_target(coveralls
  # Replace absolute paths with relative ones.
  # In command line: sed -i -e 's@'"$(realpath .)"'/@@'.
  COMMAND sed -i -e "\"s@\"'${PROJECT_SOURCE_DIR}'\"/@@\"" ${CODE_COVERAGE_STATS}
  COMMAND ${COVERALLS_COMMAND}
  DEPENDS ${CODE_COVERAGE_STATS}
  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
  COMMENT "Send code coverage data to the coveralls.io service"
)

## Install ####################################################################
###############################################################################

install(
  FILES ${CMAKE_CURRENT_SOURCE_DIR}/ddl.lua
  DESTINATION ${TARANTOOL_INSTALL_LUADIR}/
)

install(
  FILES ${CMAKE_CURRENT_SOURCE_DIR}/cartridge/roles/ddl-manager.lua
  DESTINATION ${TARANTOOL_INSTALL_LUADIR}/cartridge/roles/
)

file(GLOB_RECURSE LUA_FILES
  "${CMAKE_CURRENT_SOURCE_DIR}/ddl/*.lua"
)
install(
  FILES ${LUA_FILES}
  DESTINATION ${TARANTOOL_INSTALL_LUADIR}/${PROJECT_NAME}/
)
