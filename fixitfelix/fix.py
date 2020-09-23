import pathlib
from typing import Any, Callable, List, Tuple

import nptdms
import numpy as np
import tqdm

from fixitfelix import either, error_handling, source, tdms_helpers


def calculate_chunk_indices_to_preserve(
    chunk_size: int, recurrence_size: int, len_data: int
) -> List[Tuple[int, int]]:
    """Calculates the index ranges of valid data.

    The file correction will tack together those valid data slices.
    This function implements the rules given by OPS Ingersoll.

    Arguments:
    chunk_size: Length of the valid data slices
    recurrence_size: Length of the duplicated data slice
    len_data: Maximum length of the initial data arrays

    Returns:
    List with array ranges.
    """
    left_indices = np.arange(0, len_data, chunk_size + recurrence_size)
    right_indices = left_indices + chunk_size
    right_indices[-1] = min(len_data, right_indices[-1])
    return list(zip(left_indices, right_indices))


def prepare_data_correction(
    source_file: source.SourceFile,
) -> List[Tuple[int, int]]:
    """Prepares all parameters needed for data correction process.

    Arguments:
    source_file: Container object for the tdms file with params

    Returns:
    List of Chunk Indices that point to valid data slices.
    """
    maximum_size = tdms_helpers.get_maximum_array_size(
        source_file.tdms_operator
    )
    chunk_indices = calculate_chunk_indices_to_preserve(
        source_file.meta.chunk_size,
        source_file.meta.recurrence_size,
        maximum_size,
    )
    return chunk_indices


def combine_with_tdms(
    tdms_path: pathlib.Path,
) -> Callable[[source.MetaData], either.Either]:
    """Returns a function which combines given MetaData with the TdmsFile located at tdms_path to
    a SourceFile object after
    consistency checks of this file. The return type of the returned function is
    Either[error_handling.ErrorCode,source.SourceFile]"""
    tdms_operator = (
        error_handling.load_tdms_file(path=tdms_path)
        | error_handling.check_tdms
    )

    if isinstance(tdms_operator, either.Left):
        return lambda _: either.Left(
            error_handling.ErrorCode.TDMSPATH_NONEXISTENT
        )

    def _f(meta: source.MetaData) -> either.Either:
        return either.Right(
            source.SourceFile(tdms_operator=tdms_operator._value, meta=meta)
        )

    return _f


def check_export_path(
    path: pathlib.Path,
) -> either.Either:
    """It should not be possible to choose a nonexistent folder in the export
    path. This function checks if this is satisfied.
    Return type is Either[error_handling.ErrorCode, pathlib.Path]"""

    if not path.parent.exists():
        return either.Left(error_handling.ErrorCode.EXPORTPATH_NONEXISTENT)
    return either.Right(path)


def write_chunks_to_file(
    tdms_writer: nptdms.TdmsWriter,
    chunk_indices: List[Tuple[int, int]],
    group,
    channel,
):
    """Writes correct data slice per slice to disk.

    Arguments:
    tdms_writer: Tdms handle for the new file
    chunk_indices: Chunk Indices that point to valid data slices
    group: TDMS Group inside the old tdms file
    channel: TDMS Channel inside group
    """
    for (min_idx, max_idx) in tqdm.tqdm(chunk_indices):
        data = channel.data[min_idx:max_idx]
        new_channel = nptdms.ChannelObject(group.name, channel.name, data)
        tdms_writer.write_segment([new_channel])


def export_correct_data(
    tdms_path: pathlib.Path, meta: source.MetaData, export_path: pathlib.Path
) -> None:
    """Exports the valid data slices into a new TDMS file on disk.

    Before the export is done, all input parameters are checked for consistency are checked.
    Moreover, the function raises an execption if MetaData and TdmsFile do not match. (WIP)

    Don't mind the nested for loops, the sizes of the iterators of the first
    and second stage are very small.

    Arguments:
    tdms_path: Path to the tdms file to correct.
    meta: MetaData dict that contains all information needed for correction.
    export_path: File path for the corrected TDMS file.
    """

    p = either.Right(export_path) | check_export_path

    if isinstance(p, either.Left):
        raise Exception(error_handling.ERROR_DESCRIPTIONS.get(p._value))

    res = (
        either.Right(meta)
        | error_handling.check_meta
        | combine_with_tdms(tdms_path)
        | error_handling.check_source_file
    )

    if isinstance(res, either.Left):
        raise Exception(error_handling.ERROR_DESCRIPTIONS.get(res._value))
    source_file = res._value

    chunk_indices = prepare_data_correction(source_file)

    with nptdms.TdmsWriter(export_path) as tdms_writer:
        for group in source_file.tdms_operator.groups():
            for channel in group.channels():
                if len(channel) > 0:
                    write_chunks_to_file(
                        tdms_writer, chunk_indices, group, channel
                    )
