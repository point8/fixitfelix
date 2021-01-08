import pytest

import fixitfelix.fix as fix


def test_works_for_examples():
    result = fix.calculate_index_ranges_to_preserve(
        chunk_size=13, recurrence_size=3, len_data=55
    )
    assert len(result) == 4
    assert result[3] == (48, 7)


def test_works_for_edge_cases():
    result = fix.calculate_index_ranges_to_preserve(
        chunk_size=10, recurrence_size=5, len_data=60
    )
    assert len(result) == 4
    assert result[3] == (45, 10)
    result = fix.calculate_index_ranges_to_preserve(
        chunk_size=10, recurrence_size=5, len_data=61
    )
    assert len(result) == 5
    assert result[3] == (45, 10)
    assert result[4] == (60, 1)


def test_works_for_very_small_dataset():
    result = fix.calculate_index_ranges_to_preserve(
        chunk_size=8, recurrence_size=3, len_data=3
    )
    assert result == [(0, 3)]