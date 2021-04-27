import enum
import itertools
import pathlib
import tempfile
from typing import List, Tuple

import nptdms
import numpy as np
import pandas as pd

from fixitfelix import either, source, tdms_helpers


class ErrorCode(enum.Enum):
    LENGTHERROR = enum.auto()
    PARAMETERERROR = enum.auto()
    RECURRENCESIZE_GREATER_CHUNKSIZE = enum.auto()
    RECURRENCESIZE_NEGATIVE = enum.auto()
    CHUNKSIZE_NONPOSITIVE = enum.auto()
    DATALENGTH_NONPOSITIVE = enum.auto()
    TDMSPATH_NONEXISTENT = enum.auto()
    EXPORTPATH_NONEXISTENT = enum.auto()


ERROR_DESCRIPTIONS = {
    ErrorCode.LENGTHERROR: "Channels have different lengths",
    ErrorCode.PARAMETERERROR: "Values in Channels do not repeat as expected",
    ErrorCode.RECURRENCESIZE_GREATER_CHUNKSIZE: "Recurrence size is greater than chunk size",
    ErrorCode.RECURRENCESIZE_NEGATIVE: "Recurrence size is negative",
    ErrorCode.CHUNKSIZE_NONPOSITIVE: "Chunk size is not positive",
    ErrorCode.DATALENGTH_NONPOSITIVE: "Length of data is not positive",
    ErrorCode.TDMSPATH_NONEXISTENT: "File does not exist",
    ErrorCode.EXPORTPATH_NONEXISTENT: "Export folder does not exist",
}

# Check MetaData for consistency


def check_recurrence_size_smaller_chunk_size(
    meta: source.MetaData,
) -> either.Either:
    if meta.recurrence_size > meta.chunk_size:
        return either.Left(ErrorCode.RECURRENCESIZE_GREATER_CHUNKSIZE)
    return either.Right(meta)


def check_recurrence_size_nonnegative(meta: source.MetaData) -> either.Either:
    if meta.recurrence_size < 0:
        return either.Left(ErrorCode.RECURRENCESIZE_NEGATIVE)
    return either.Right(meta)


def check_chunksize_positive(meta: source.MetaData) -> either.Either:
    if meta.chunk_size < 0:
        return either.Left(ErrorCode.CHUNKSIZE_NONPOSITIVE)
    return either.Right(meta)


def check_meta(meta: source.MetaData) -> either.Either:
    """Combines all checks of the MetaData and returns Either[ErrorCode,source.MetaData]"""
    return (
        either.Right(meta)
        | check_recurrence_size_smaller_chunk_size
        | check_recurrence_size_nonnegative
        | check_chunksize_positive
    )


# Check TdmsFile for consistency


def check_for_same_length(tdms_operator: nptdms.TdmsFile) -> either.Either:
    """Checks whether all relevant channels of the Tdms file have the same
    length.
    """
    array_lengths = [
        [len(channel) for channel in group.channels() if len(channel) > 0]
        for group in tdms_operator.groups()
    ]
    array_lengths = np.array(array_lengths).flatten()
    all_lengths_equal = len(set(array_lengths)) == 1
    if not all_lengths_equal:
        return either.Left(ErrorCode.LENGTHERROR)
    return either.Right(tdms_operator)


def check_positive_data_length(tdms_operator: nptdms.TdmsFile) -> either.Either:
    """Checks whether the data length is positive"""
    max_length = tdms_helpers.get_maximum_array_size(tdms_operator)
    if max_length <= 0:
        return either.Left(ErrorCode.DATALENGTH_NONPOSITIVE)
    return either.Right(tdms_operator)


def check_tdms(tdms_operator: nptdms.TdmsFile) -> either.Either:
    """Combines all checks of the TdmsFile and returns Either[ErrorCode,nptdms.TdmsFile]."""
    return (
        either.Right(tdms_operator)
        | check_for_same_length
        | check_positive_data_length
    )


# Check whole SourceFile for consistency

def calculate_drop_indices(
    source_file: source.SourceFile,
) -> List[Tuple[int, int]]:
    """Calculates index positions of duplicates in file and returns them in the form of tuples
    (offset, length)

    This function is the counter part of
    fix.calculate_index_ranges_to_preserve and is only used in error handling.

    Arguments:
    source_file: Container object for the tdms file with params

    Returns:
    List of Chunk Indices that point to invalid data slices.
    """
    len_data = tdms_helpers.get_maximum_array_size(source_file.tdms_operator)
    chunk_size = source_file.meta.chunk_size
    recurrence_size = source_file.meta.recurrence_size

    offsets = np.arange(chunk_size, len_data, chunk_size + recurrence_size)
    lengths = [recurrence_size]*(len(offsets)-1) 
    lengths.append(min(recurrence_size,len_data-offsets[-1]))

    return list(zip(offsets,lengths))


def check_for_correct_repetition(
    source_file: source.SourceFile,
) -> either.Either:
    """Checks whether the meta data about the occurence of repetitions is valid
    for the Tdms file, i.e. whether repetitons really occur at the desired
    places.
    """
    # generate random test samples
    delete_ranges = np.array(calculate_drop_indices(source_file))
    number_samples_to_test = min(
        source_file.meta.consistency_sample_size, len(delete_ranges)
    )
    # np.random.choice does only take 1d arrays, so we need this
    # workaround by choosing 1d indices in range with len(delete_indices)
    chosen_deletes = np.random.choice(
        len(delete_ranges), number_samples_to_test, replace=False
    )
    delete_ranges = delete_ranges[chosen_deletes]

    # prepare all tdms channels that contain data
    all_channels = list(
        itertools.chain.from_iterable(
            [
                [c for c in group.channels() if len(c) > 0]
                for group in source_file.tdms_operator.groups()
            ]
        )
    )

    # test data of each test sample
    meta_data_suitable = False 
    for (offset, length) in delete_ranges:
        # calculate indices of the duplicates origin
        origin_offset = offset - source_file.meta.recurrence_distance
        
        # extract origin and duplicate data and compare
        duplicate_data = [
            old_channel.read_data(offset=offset, length=length) for old_channel in all_channels
        ]
        origin_data = [
            old_channel.read_data(offset=origin_offset,length=length)
            for old_channel in all_channels
        ]
        if not np.array_equal(duplicate_data, origin_data):
            meta_data_suitable = False
            break

        # extract data points around the data above
        duplicate_front_values = [
            old_channel.read_data(offset=offset-1,length=1)[0]
            for old_channel in all_channels
        ]
        duplicate_rear_values = [
            old_channel.read_data(offset=offset+length,length=1)[0]
            for old_channel in all_channels
        ]
        
        origin_front_values = [
            old_channel.read_data(offset=origin_offset-1,length=1)[0]
            for old_channel in all_channels
        ]
        origin_rear_values = [
            old_channel.read_data(offset=origin_offset+length,length=1)[0]
            for old_channel in all_channels
        ]
        # check if they are not part of duplication
        if not(np.array_equal(duplicate_front_values, origin_front_values) or np.array_equal(duplicate_rear_values, origin_rear_values)):
            meta_data_suitable = True
        
    if not meta_data_suitable:
        return either.Left(ErrorCode.PARAMETERERROR)
    return either.Right(source_file)


def check_source_file(source_file: source.SourceFile) -> either.Either:
    """Combines all checks of the SourceFile and returns Either[ErrorCode,source.SourceFile]"""
    return either.Right(source_file) | check_for_correct_repetition


def load_tdms_file(path: pathlib.Path) -> either.Either:
    """Tries to load the tdms file located at path and returns Either[ErrorCode,np.tdms.TdmsFile]"""
    try:
        return either.Right(
            nptdms.TdmsFile.open(file=path))
    except FileNotFoundError:
        return either.Left(ErrorCode.TDMSPATH_NONEXISTENT)
