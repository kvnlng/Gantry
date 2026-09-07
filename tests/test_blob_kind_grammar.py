"""The grammar for `instance_blobs.kind`, as a contract rather than a convention.

`kind` was two literals -- `'pixels'` and `'waveform'` -- until #183 needed a
third shape: pixel data that lives inside a sequence item. The column is
unconstrained `TEXT NOT NULL` and the table's only key is
`UNIQUE(instance_uid, kind)`, so a composite spelling needs no DDL change --
but it *is* a schema decision written into a UNIQUE index, and changing it
once rows exist is a migration. So it is decided once, here, for #183 and for
#277/#150-3a after it, rather than invented at each call site.

The pair under test is `parse_blob_kind` / `serialize_blob_kind`. Nothing
else may build a kind: an f-string at a call site is precisely the
"invented at the call site" failure #183 names, and it is what puts three
spellings of the same thing in one column.

**There is no escaping and none may be added.** The token alphabets are
closed and disjoint from the delimiters -- a tag is drawn from `[0-9a-f,]`
and so cannot hold `/` or `:`; an index is drawn from `[0-9]` and can hold
neither. A kind that does not match is refused, not escaped. That is the
same discipline `FLOAT_DTYPE_NAMES` applies to the dtype carrier: a string
that came back out of the store is data, and the reader allow-lists rather
than interprets.
"""

import pytest

from isocenter.persistence import parse_blob_kind, serialize_blob_kind


# The six spellings §4 of the spec enumerates -- every consumer the column
# has or is known to be getting. Kept as one list because the round-trip and
# the parse assertions must not be able to disagree about which strings are
# legal.
LEGAL_KINDS = [
    # Root blobs: the two literals the column has always held.
    ("pixels", ("pixels", (), None)),
    ("waveform", ("waveform", (), None)),
    # #183, depth 1: Icon Image Sequence item 0's Pixel Data.
    ("pixels:0088,0200/0/7fe0,0010",
     ("pixels", (("0088,0200", 0),), "7fe0,0010")),
    # #183, depth 2: an icon on Referenced Image Sequence item 3. This is
    # the spelling that proves the path loop, and the one whose thumbnail
    # is of a *different* SOP instance (PS3.3 C.7.6.16).
    ("pixels:0008,1140/3/0088,0200/0/7fe0,0010",
     ("pixels", (("0008,1140", 3), ("0088,0200", 0)), "7fe0,0010")),
    # #277 / #150 option 3a: Waveform Sequence multiplex group 2. Spellable
    # today, implemented by neither -- the point of deciding the grammar
    # once is that #277 inherits it rather than inventing a second one.
    ("waveform:5400,0100/2/5400,1010",
     ("waveform", (("5400,0100", 2),), "5400,1010")),
    # Q5: nested float pixel data. The grammar spells it; the spec
    # deliberately declined to carry it, because the shape is unreachable
    # from a conformant file.
    ("pixels:0040,0555/0/7fe0,0008",
     ("pixels", (("0040,0555", 0),), "7fe0,0008")),
]


@pytest.mark.parametrize("kind,expected", LEGAL_KINDS)
def test_every_consumers_spelling_parses_to_its_parts(kind, expected):
    """Each of the six spellings parses to (root, path, terminal_tag).

    `path` is exactly the tuple `iter_item_tree` already yields -- a tuple
    of `(sequence_tag, index)` steps in the lowercase-hex spelling used
    everywhere in the graph. That is deliberate and is the main reason to
    prefer this shape over any other: the codebase already has one answer
    to "where in the instance is this", used by `PhiFinding.entity_path`,
    `resolve_item_path` and `_rehydrate_findings`. A blob path of a
    different shape would be a second answer to the same question.
    """
    assert parse_blob_kind(kind) == expected


@pytest.mark.parametrize("kind,expected", LEGAL_KINDS)
def test_the_grammar_round_trips(kind, expected):
    """`serialize_blob_kind(*parse_blob_kind(k)) == k` for every spelling.

    The two functions are a pair or they are two grammars. A round-trip
    that loses the terminal tag, or renders an index differently from the
    way it parses, writes a key that no later read can find -- and the
    UNIQUE index means the second write lands as a *new row* rather than
    failing.
    """
    assert serialize_blob_kind(*expected) == kind


def test_the_terminal_tag_is_what_names_the_element_to_write_back():
    """Not decoration: it is how the export knows which element to create.

    Without it the writeback would have to infer the element from the
    root, and `waveform` -> (5400,1010) is only true until it is not --
    #277 already wants (5400,1010) under a path, and Q5's shape is
    (7fe0,0008) under one.
    """
    assert parse_blob_kind("pixels:0088,0200/0/7fe0,0010")[2] == "7fe0,0010"
    assert parse_blob_kind("pixels:0040,0555/0/7fe0,0008")[2] == "7fe0,0008"
    assert parse_blob_kind("waveform:5400,0100/2/5400,1010")[2] == "5400,1010"


@pytest.mark.parametrize("bad", [
    # #183's own sketch. It is NOT the grammar: the `:` already separates
    # root from path and the path's token shape is unambiguous without a
    # literal every writer must remember and every parser must check. A
    # reader who assumes the issue's spelling works must find out here.
    "pixels:seq:0088,0200/0/7fe0,0010",
    # An even token count. Positions 0, 2, 4 ... are tags and 1, 3, 5 ...
    # are indices, so a path with an even number of tokens is malformed by
    # construction -- there is a step with no index, or an index with no
    # terminal tag.
    "pixels:0088,0200/0",
    "pixels:0088,0200/0/0088,0200/0",
    # A tag where an index belongs, and an index where a tag belongs.
    "pixels:0088,0200/0088,0200/7fe0,0010",
    "pixels:0/0088,0200/7fe0,0010",
    # An unknown root. The root is an allow-list of two, not "whatever
    # precedes the colon".
    "icons:0088,0200/0/7fe0,0010",
    "pixels2:0088,0200/0/7fe0,0010",
    # Uppercase hex. Tags are lowercase-hex strings throughout the graph;
    # accepting both spellings would let one payload occupy two rows under
    # the UNIQUE index. This is also what makes SQLite's ASCII
    # case-insensitive `LIKE 'pixels:%'` prefix read safe.
    "pixels:0088,0200/0/7FE0,0010",
    "PIXELS:0088,0200/0/7fe0,0010",
    # A zero-padded or negative index. `01` and `1` would be two rows for
    # one item, and there is no negative sequence position.
    "pixels:0088,0200/01/7fe0,0010",
    "pixels:0088,0200/-1/7fe0,0010",
    # Delimiters in the wrong places, and the separators #183's prose
    # reached for.
    "pixels#0088,0200/0",
    "pixels/nested",
    "pixels:",
    "pixels::0088,0200/0/7fe0,0010",
    "pixels:0088,0200/0/7fe0,0010/",
    ":0088,0200/0/7fe0,0010",
    # A tag of the wrong width, or missing its comma.
    "pixels:088,200/0/7fe0,0010",
    "pixels:00880200/0/7fe0,0010",
    # Whitespace. `fullmatch` alone would still admit a trailing newline
    # under `$`, which is exactly how a stored key acquires an invisible
    # character.
    "pixels ",
    "pixels:0088,0200/0/7fe0,0010\n",
    "",
])
def test_a_non_conforming_kind_is_refused_not_escaped(bad):
    """Refusal is the whole mechanism -- there is no escape hatch.

    Every delimiter is outside every token's alphabet, so no legal kind can
    ever need escaping, and adding an escape would make two strings mean
    one key. The gate raises instead.
    """
    with pytest.raises(ValueError):
        parse_blob_kind(bad)


def test_the_refusal_message_carries_the_grammar():
    """A `ValueError` that only says "bad" leaves the caller guessing.

    The message is the only documentation a caller hitting the gate gets,
    and the failure it most often means is #183's `seq:` sketch.
    """
    with pytest.raises(ValueError) as excinfo:
        parse_blob_kind("pixels:seq:0088,0200/0/7fe0,0010")
    message = str(excinfo.value)
    assert "pixels:seq:0088,0200/0/7fe0,0010" in message
    assert "pixels" in message and "waveform" in message
    assert "gggg,eeee" in message


def test_serialize_refuses_to_build_a_kind_the_parser_would_reject():
    """The inverse must not be able to write what the gate would refuse.

    `serialize_blob_kind` is the only thing any writer may call, so it is
    the last place a malformed key can be stopped before it reaches a
    UNIQUE index. Left permissive, it would be the f-string it exists to
    replace.
    """
    with pytest.raises(ValueError):
        serialize_blob_kind("icons", (("0088,0200", 0),), "7fe0,0010")
    with pytest.raises(ValueError):
        serialize_blob_kind("pixels", (("0088,0200", -1),), "7fe0,0010")
    with pytest.raises(ValueError):
        serialize_blob_kind("pixels", (("0088,0200", 0),), None)
    with pytest.raises(ValueError):
        serialize_blob_kind("pixels", (), "7fe0,0010")


def test_a_root_kind_has_an_empty_path_and_no_terminal_tag():
    """`()` and None, not None and None.

    The empty tuple is what makes `for tag, index in path` a no-op at the
    root rather than a `TypeError`, so every caller can walk the path
    without first asking whether there is one.
    """
    assert parse_blob_kind("pixels") == ("pixels", (), None)
    assert serialize_blob_kind("pixels", (), None) == "pixels"


def test_the_grammar_states_no_depth_limit():
    """A depth cap here would be a second bound on the same thing.

    `populate_attrs`/`process_sequence` already recurse without one, and
    SQLite imposes no key-length limit on a b-tree index. If a bound is
    ever wanted it belongs at the parse of the *file*, not at the key.
    """
    deep = "pixels:" + "0088,0200/0/" * 40 + "7fe0,0010"
    root, path, terminal = parse_blob_kind(deep)
    assert root == "pixels"
    assert len(path) == 40
    assert terminal == "7fe0,0010"
    assert serialize_blob_kind(root, path, terminal) == deep
