#!/bin/sh
# Call this script to install test dependencies

set -e

if ! [ -x "$(command -v tt)" ]; then
	TTCTL=tarantoolctl
else
	TTCTL=tt
fi

# Test dependencies:
$TTCTL rocks install luatest
$TTCTL rocks install luacov 0.13.0
$TTCTL rocks install luacheck 0.26.0

$TTCTL rocks install https://raw.githubusercontent.com/mpeterv/cluacov/master/cluacov-scm-1.rockspec
$TTCTL rocks install https://raw.githubusercontent.com/LuaDist/dkjson/master/dkjson-2.5-2.rockspec
$TTCTL rocks install https://raw.githubusercontent.com/keplerproject/luafilesystem/master/luafilesystem-scm-1.rockspec
$TTCTL rocks install https://raw.githubusercontent.com/moteus/lua-path/master/rockspecs/lua-path-scm-0.rockspec

# Most of this code is the workaround for
# https://github.com/moteus/luacov-coveralls/pull/30
# Remove it, when the pull request will be merged.
TMPDIR="$(mktemp -d)"
LUACOV_COVERALLS_ROCKSPEC_URL="https://raw.githubusercontent.com/moteus/luacov-coveralls/master/rockspecs/luacov-coveralls-scm-0.rockspec"
LUACOV_COVERALLS_ROCKSPEC_FILE="${TMPDIR}/luacov-coveralls-scm-0.rockspec"
curl -fsSL "${LUACOV_COVERALLS_ROCKSPEC_URL}" > "${LUACOV_COVERALLS_ROCKSPEC_FILE}"
sed -i -e 's@git://@git+https://@' "${LUACOV_COVERALLS_ROCKSPEC_FILE}"
$TTCTL rocks install "${LUACOV_COVERALLS_ROCKSPEC_FILE}"
rm "${LUACOV_COVERALLS_ROCKSPEC_FILE}"
rmdir "${TMPDIR}"
