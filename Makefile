bootstrap: .rocks

.rocks:
	tarantoolctl rocks install luatest 0.3.0
	tarantoolctl rocks install luacheck 0.25.0

.PHONY: lint
lint: bootstrap
	.rocks/bin/luacheck ./

.PHONY: test
test: luatest

.PHONY: luatest
luatest: bootstrap
	.rocks/bin/luatest

.PHONY: ci_prepare
ci_prepare:
	git config --global user.email "test@tarantool.io"
	git config --global user.name "Test Tarantool"