#!/usr/bin/env tarantool

local t = require('luatest')
local db = require('test.db')
local ddl = require('ddl')
local ddl_get = require('ddl.get')

local g = t.group()
g.before_all(db.init)
g.before_each(db.drop_all)

local seq_start = 1
local seq_step = 5

local test_schema = {
    spaces = {
        ['with_sequence'] = {
            engine = 'memtx',
            is_local = false,
            temporary = false,
            format = {
                {name = 'seq_id', type = 'unsigned', is_nullable = false},
                {name = 'first', type = 'string', is_nullable = false},
                {name = 'second', type = 'string', is_nullable = false},
            },
            indexes = {
                {
                    name = 'seq_index',
                    type = 'TREE',
                    unique = true,
                    parts = {
                        {is_nullable = false, path = 'seq_id', type = 'unsigned'},
                    },
                    sequence = 'seq',
                },
            },
        },
    },
    sequences = {
        ['seq'] = {
            start = seq_start,
            min = 0,
            max = 1000000000000ULL,
            cycle = true,
            cache = 0,
            step = seq_step,
        },
    },
}

local function assert_test_schema_applied()
    t.assert_not_equals(box.space['with_sequence'], nil, 'space exists')
    t.assert_not_equals(box.space['with_sequence'].index['seq_index'], nil, 'space index exists')

    local index_seq_id = box.space['with_sequence'].index['seq_index'].sequence_id
    t.assert_type(index_seq_id, 'number', 'space index uses sequence')

    local seq = box.sequence['seq']
    t.assert_not_equals(seq, nil, 'sequence exists')
    t.assert_equals(seq.start, seq_start, 'sequence is configured with proper values')
    t.assert_equals(seq.min, 0, 'sequence is configured with proper values')
    t.assert_equals(seq.max, 1000000000000ULL, 'sequence is configured with proper values')
    t.assert_equals(seq.cycle, true, 'sequence is configured with proper values')
    t.assert_equals(seq.cache, 0, 'sequence is configured with proper values')
    t.assert_equals(seq.step, seq_step, 'sequence is configured with proper values')

    local index_seq = ddl_get.get_sequence_by_id(index_seq_id)
    t.assert_equals(seq, index_seq, 'index uses expected sequence')
end

local function assert_two_test_records_can_be_inserted(opts)
    assert(type(opts.steps_before) == 'number')

    local start_id = seq_start + opts.steps_before * seq_step

    t.assert_equals(
        box.space['with_sequence']:insert{box.NULL, 'val1', 'val2'},
        {start_id, 'val1', 'val2'},
        'autoincrement space works fine for inserts'
    )
    t.assert_equals(
        box.space['with_sequence']:insert{box.NULL, 'val1', 'val2'},
        {start_id + seq_step, 'val1', 'val2'},
        'autoincrement space works fine for inserts'
    )
end

g.test_sequence_index_schema_applies_on_clean_instance = function()
    local _, err = ddl.set_schema(test_schema)
    t.assert_equals(err, nil)

    assert_test_schema_applied()
    assert_two_test_records_can_be_inserted{steps_before = 0}
end

g.test_sequence_index_schema_reapply = function()
    local _, err = ddl.set_schema(test_schema)
    t.assert_equals(err, nil)

    assert_test_schema_applied()
    assert_two_test_records_can_be_inserted{steps_before = 0}

    local _, err = ddl.set_schema(ddl.get_schema())
    t.assert_equals(err, nil)

    assert_test_schema_applied()
    assert_two_test_records_can_be_inserted{steps_before = 2}
end

function g.test_sequence_index_schema_applies_if_same_setup_already_exists()
    g.space = box.schema.space.create('with_sequence')
    g.space:format({
        {name = 'seq_id', type = 'unsigned', is_nullable = false},
        {name = 'first', type = 'string', is_nullable = false},
        {name = 'second', type = 'string', is_nullable = false},
    })

    local seq_name = 'seq'
    local seq_opts = {
        start = 1,
        min = 0,
        max = 1000000000000ULL,
        cycle = true,
        cache = 0,
        step = 5,
    }
    box.schema.sequence.create(seq_name, seq_opts)

    g.space:create_index('seq_index', {
        type = 'TREE',
        unique = true,
        parts = {{is_nullable = false, field = 'seq_id', type = 'unsigned'}},
        sequence = seq_name,
    })

    assert_two_test_records_can_be_inserted{steps_before = 0}

    local _, err = ddl.set_schema(test_schema)
    t.assert_equals(err, nil)

    assert_test_schema_applied()
    assert_two_test_records_can_be_inserted{steps_before = 2}
end

function g.test_sequence_index_schema_apply_fails_if_existing_space_index_does_not_use_sequence()
    g.space = box.schema.space.create('with_sequence')
    g.space:format({
        {name = 'seq_id', type = 'unsigned', is_nullable = false},
        {name = 'first', type = 'string', is_nullable = false},
        {name = 'second', type = 'string', is_nullable = false},
    })

    g.space:create_index('seq_index', {
        type = 'TREE',
        unique = true,
        parts = {{is_nullable = false, field = 'seq_id', type = 'unsigned'}},
        -- no sequence
    })

    local _, err = ddl.set_schema(test_schema)
    t.assert_str_contains(err, 'Incompatible schema: spaces["with_sequence"] //indexes/1/sequence ' ..
                               '(expected nil, got string)')
end

function g.test_sequence_index_schema_apply_fails_if_existing_space_index_uses_different_sequence()
    g.space = box.schema.space.create('with_sequence')
    g.space:format({
        {name = 'seq_id', type = 'unsigned', is_nullable = false},
        {name = 'first', type = 'string', is_nullable = false},
        {name = 'second', type = 'string', is_nullable = false},
    })

    box.schema.sequence.create('different_sequence')

    g.space:create_index('seq_index', {
        type = 'TREE',
        unique = true,
        parts = {{is_nullable = false, field = 'seq_id', type = 'unsigned'}},
        sequence = 'different_sequence',
    })

    local _, err = ddl.set_schema(test_schema)
    t.assert_str_contains(err, 'Incompatible schema: spaces["with_sequence"] //indexes/1/sequence ' ..
                               '(expected different_sequence, got seq)')
end

function g.test_sequence_schema_apply_fails_if_existing_sequence_has_different_options()
    g.space = box.schema.space.create('with_sequence')
    g.space:format({
        {name = 'seq_id', type = 'unsigned', is_nullable = false},
        {name = 'first', type = 'string', is_nullable = false},
        {name = 'second', type = 'string', is_nullable = false},
    })

    local seq_name = 'seq'
    local seq_opts = {
        start = 10,
        min = 10,
        max = 1000,
        cycle = false,
        cache = 0,
        step = 5,
    }
    box.schema.sequence.create(seq_name, seq_opts)

    g.space:create_index('seq_index', {
        type = 'TREE',
        unique = true,
        parts = {{is_nullable = false, field = 'seq_id', type = 'unsigned'}},
        sequence = seq_name,
    })

    local _, err = ddl.set_schema(test_schema)
    t.assert_str_contains(err, 'Incompatible schema: sequences["seq"] max ' ..
                               '(expected 1000, got 1000000000000ULL)')
end

function g.test_sequence_schema_with_defaults_apply_fails_if_existing_sequence_has_different_options()
    g.space = box.schema.space.create('with_sequence')
    g.space:format({
        {name = 'seq_id', type = 'unsigned', is_nullable = false},
        {name = 'first', type = 'string', is_nullable = false},
        {name = 'second', type = 'string', is_nullable = false},
    })

    local seq_name = 'seq'
    local seq_opts = {
        start = 10,
        min = 10,
        max = 1000,
        cycle = false,
        cache = 0,
        step = 5,
    }
    box.schema.sequence.create(seq_name, seq_opts)

    g.space:create_index('seq_index', {
        type = 'TREE',
        unique = true,
        parts = {{is_nullable = false, field = 'seq_id', type = 'unsigned'}},
        sequence = seq_name,
    })

    local test_schema = table.deepcopy(test_schema)
    test_schema.sequences['seq'] = {} -- Fill with defaults.

    local _, err = ddl.set_schema(test_schema)
    t.assert_str_contains(err, 'Incompatible schema: sequences["seq"] max '..
                               '(expected 1000, got nil and default is 9223372036854775807ULL)')
end
