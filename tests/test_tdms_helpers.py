import pytest

from fixitfelix import source, tdms_helpers


def test_correct_array_size_works_for_example(tmpdir):
    meta = source.MetaData(
        chunk_size=6,
        recurrence_size=3,  # <- 2 would be correct
        recurrence_distance=3,
        consistency_sample_size=10,
    )
    example_source = source.SourceFile.read_from_path(
        tdms_path="tests/assets/example_file.tdms", meta=meta,
    )
    res = tdms_helpers.get_maximum_array_size(example_source.tdms_operator)
    assert res == 19
