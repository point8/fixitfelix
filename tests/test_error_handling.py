from fixitfelix import either, error_handling, source


def test_errorcode_descriptions():
    for code in error_handling.ErrorCode:
        assert code in error_handling.ERROR_DESCRIPTIONS.keys()


def test_check_recurrence_size_smaller_chunk_size():
    invalid_meta = source.MetaData(
        chunk_size=10,
        recurrence_size=12,
        recurrence_distance=1,
        consistency_sample_size=10,
    )
    assert error_handling.check_recurrence_size_smaller_chunk_size(
        meta=invalid_meta
    ) == either.Left(error_handling.ErrorCode.RECURRENCESIZE_GREATER_CHUNKSIZE)

    valid_meta = source.MetaData(
        chunk_size=12,
        recurrence_size=10,
        recurrence_distance=1,
        consistency_sample_size=10,
    )
    assert error_handling.check_recurrence_size_smaller_chunk_size(
        meta=valid_meta
    ) == either.Right(valid_meta)


def test_check_recurrence_size_nonnegative():
    invalid_meta = source.MetaData(
        chunk_size=12,
        recurrence_size=-10,
        recurrence_distance=1,
        consistency_sample_size=10,
    )
    assert error_handling.check_recurrence_size_nonnegative(
        meta=invalid_meta
    ) == either.Left(error_handling.ErrorCode.RECURRENCESIZE_NEGATIVE)

    valid_meta = source.MetaData(
        chunk_size=12,
        recurrence_size=10,
        recurrence_distance=1,
        consistency_sample_size=10,
    )
    assert error_handling.check_recurrence_size_nonnegative(
        meta=valid_meta
    ) == either.Right(valid_meta)

    valid_meta = source.MetaData(
        chunk_size=12,
        recurrence_size=0,
        recurrence_distance=1,
        consistency_sample_size=10,
    )
    assert error_handling.check_recurrence_size_nonnegative(
        meta=valid_meta
    ) == either.Right(valid_meta)


def test_check_chunksize_positive():
    invalid_meta = source.MetaData(
        chunk_size=-12,
        recurrence_size=10,
        recurrence_distance=1,
        consistency_sample_size=10,
    )
    assert error_handling.check_chunksize_positive(
        meta=invalid_meta
    ) == either.Left(error_handling.ErrorCode.CHUNKSIZE_NONPOSITIVE)

    valid_meta = source.MetaData(
        chunk_size=0,
        recurrence_size=10,
        recurrence_distance=1,
        consistency_sample_size=10,
    )
    assert error_handling.check_chunksize_positive(
        meta=valid_meta
    ) == either.Right(valid_meta)

    valid_meta = source.MetaData(
        chunk_size=12,
        recurrence_size=10,
        recurrence_distance=1,
        consistency_sample_size=10,
    )
    assert error_handling.check_chunksize_positive(
        meta=valid_meta
    ) == either.Right(valid_meta)
