import pathlib
from typing import Any, Callable, List, Tuple

import nptdms
import numpy as np
import tqdm
import shutil

from fixitfelix import either, error_handling, source, tdms_helpers


def calculate_index_ranges_to_preserve(
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
    List with array ranges in the form (offset, length)
    """
    offsets = np.arange(0, len_data, chunk_size + recurrence_size)
    lengths = [chunk_size] * (len(offsets) - 1)
    lengths.append(
        min(
            chunk_size, len_data - len(lengths) * (chunk_size + recurrence_size)
        )
    )

    return list(zip(offsets, lengths))


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
    index_ranges = calculate_index_ranges_to_preserve(
        source_file.meta.chunk_size,
        source_file.meta.recurrence_size,
        maximum_size,
    )
    return index_ranges


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


def check_export_path(path: pathlib.Path,) -> either.Either:
    """It should not be possible to choose a nonexistent folder in the export
    path. This function checks if this is satisfied.
    Return type is Either[error_handling.ErrorCode, pathlib.Path]"""

    if not path.parent.exists():
        return either.Left(error_handling.ErrorCode.EXPORTPATH_NONEXISTENT)
    return either.Right(path)


def write_chunks_to_file(
    tdms_writer: nptdms.TdmsWriter,
    index_ranges: List[Tuple[int, int]],
    group,
    channel,
    usable_memory: int,
):
    """Writes correct data slice per slice to disk.

    Arguments:
    tdms_writer: Tdms handle for the new file
    index_ranges: Chunk Indices that point to valid data slices
    group: TDMS Group inside the old tdms file
    channel: TDMS Channel inside group
    """


    #standard write
    diff_check_arr = np.array([])
    for (offset,length) in tqdm.tqdm(index_ranges):
        data = channel.read_data(offset=offset,length=length)
        diff_check_arr = np.append(diff_check_arr, data)
        new_channel = nptdms.ChannelObject(group.name, channel.name, data)
        tdms_writer.write_segment([new_channel])


    index_ranges_frags = [(0, sum(index_ranges[-1]), index_ranges)]
    first_val = channel.read_data(offset=0, length=1)
    bytes_per_val = first_val.nbytes
    frag_size = len(channel)
    usable_bytes = usable_memory*1000000000

    while frag_size > usable_bytes/bytes_per_val:
        tmp_frags = []
        for index_frag in index_ranges_frags:
            print(f"frag at {index_frag[0]}")
            ranges = index_frag[2]
            new_ranges_1 = ranges[:len(ranges)//2]
            new_ranges_2 = ranges[len(ranges)//2:]
            split_offset = new_ranges_2[0][0]
            new_ranges_2 = [(offset-split_offset, length) for (offset, length) in new_ranges_2]
            tmp_frags.append((index_frag[0], index_frag[0]+split_offset, new_ranges_1))
            tmp_frags.append((index_frag[0]+split_offset, index_frag[1], new_ranges_2))
        index_ranges_frags = tmp_frags.copy()
        frag_size = index_ranges_frags[0][1]

    diff_check_arr3 = np.array([])
    for index_frag in index_ranges_frags:
        channel_data = channel.read_data(offset=index_frag[0],length=index_frag[1]-index_frag[0])
        clean_data = np.array([])
        for (offset,length) in tqdm.tqdm(index_frag[2]):
            data = channel_data[offset:offset+length]
            diff_check_arr3 = np.append(diff_check_arr3, data)
            clean_data = np.append(clean_data, data)
        new_channel = nptdms.ChannelObject(group.name, channel.name, clean_data)
        tdms_writer.write_segment([new_channel])


    
    comp2 = diff_check_arr == diff_check_arr3
    print(f"Are std read and frag read the same: {comp2.all()}")
    #comp3 = diff_check_arr3[:frag_size*2] == diff_check_arr3[frag_size*2:]
    #print(f"Is first and second half of array 3 the same: {comp3}")
    #print(f"Length of 1: {len(diff_check_arr)}")
    #print(f"Length of 2: {len(diff_check_arr2)}")
    #print(f"Length of 3: {len(diff_check_arr3)}")

    """
    if cached_read:
        channel_data = channel.read_data()
        for (offset, length) in tqdm.tqdm(index_ranges):
            data = channel_data[offset : offset + length]
            new_channel = nptdms.ChannelObject(group.name, channel.name, data)
            tdms_writer.write_segment([new_channel])
    else:
        for (offset, length) in tqdm.tqdm(index_ranges):
            data = channel.read_data(offset=offset, length=length)
            new_channel = nptdms.ChannelObject(group.name, channel.name, data)
            tdms_writer.write_segment([new_channel])
<<<<<<< Updated upstream

def preprocess(meta: source.MetaData, path: pathlib.Path) -> source.SourceFile:
=======
    """
>>>>>>> Stashed changes

    """Runs all consistency checks on given tdms file and meta data. All input parameters are checked for consistency.
    Moreover, the function raises an execption if MetaData and TdmsFile do not match.

    Arguments:
    meta: MetaData dict that contains all information needed for correction.
    path: Path to tdms file to check
    """
    res = (
        either.Right(meta)
        | error_handling.check_meta
        | combine_with_tdms(path)
        | error_handling.check_source_file
    )

    if isinstance(res, either.Left):
        raise Exception(error_handling.ERROR_DESCRIPTIONS.get(res._value))
    return res._value


def export_to_tmds(source_file: Any, export_path: pathlib.Path) -> None:
    """Exports the valid data slices into a new TDMS file on disk.

    Don't mind the nested for loops, the sizes of the iterators of the first
    and second stage are very small.

    Arguments:
    source_file: Tdms file, that passed all consistency checks
    export_path: File path for the corrected TDMS file.
    """

    index_ranges = prepare_data_correction(source_file)

    with nptdms.TdmsWriter(export_path) as tdms_writer:
        for group in source_file.tdms_operator.groups():
            for channel in group.channels():
                if len(channel) > 0:
                    write_chunks_to_file(
                        tdms_writer,
                        index_ranges,
                        group,
                        channel,
                        meta.usable_memory,
                    )


def export_correct_data(
    filename: str, meta: source.MetaData, output_file: str
) -> None:
    """ Accepts either a path to a tdms file or to a folder with just tdms files to correct.
    The name of the resulting folder or file is defined by output_file.
    If output_file is empty, the name of the resulting file or folder is the previous name with '_corrected' as suffix.
    All files in a folder will retain their previous name with '_corrected' suffix.
    In case of a folder as input all tdms files are first checked for consistency so no file is corrected before each file is checked.
    Afterwards all files are corrected and exported. This prevents cases where a later file is not valid for correction.
    If a single file is given, the file is checked and corrected immediately.

    Arguments:
    filename: Path to the tdms file or folder with tdms files to correct.
    meta: MetaData dict that contains all information needed for correction.
    output_file: File path for the corrected TDMS file or folder.
    """

    # Determines generalized export path

    path = pathlib.Path(filename)

    p = either.Right(path) | error_handling.check_input_path
    if isinstance(p, either.Left):
        raise Exception(error_handling.ERROR_DESCRIPTIONS.get(p._value))

    if output_file == "":
        name = path.with_suffix("").name + "_corrected"
        export_path = path.parent.joinpath(name)
    else:
        export_path = pathlib.Path(output_file)

    p = either.Right(export_path) | check_export_path
    if isinstance(p, either.Left):
        raise Exception(error_handling.ERROR_DESCRIPTIONS.get(p._value))

    # Directory and single file are handled seperately

    if path.is_dir():
        p = either.Right(path) | error_handling.check_dir_empty
        if isinstance(p, either.Left):
            raise Exception(error_handling.ERROR_DESCRIPTIONS.get(p._value))

        if export_path.exists():
            shutil.rmtree(export_path)
        export_path.mkdir()

        # Checks each file in folder for consistency

        files_in_dir = len(list(path.iterdir()))
        source_files = []
        for i, tdms_file in enumerate(path.iterdir()):
            print(f"Preprocess file {i+1} of {files_in_dir} at {tdms_file}")
            source_files.append(preprocess(meta=meta, path=tdms_file))

        # Corrects and exports each file in folder

        for i, tdms_file in enumerate(path.iterdir()):
            print(f"Fix file {i+1} of {files_in_dir} at {tdms_file}")
            name = tdms_file.with_suffix("").name + "_corrected.tdms"
            export_to_tmds(
                source_file=source_files[i],
                export_path=export_path.joinpath(name),
            )

    else:
        # Single file case

        name = export_path.name + ".tdms"
        export_path = export_path.parent.joinpath(name)
        source_file = preprocess(meta=meta, path=path)
        export_to_tmds(source_file=source_file, export_path=export_path)
