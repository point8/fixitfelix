import pathlib
from typing import Any, Callable, List, Tuple

import nptdms
import numpy as np
import tqdm
import os
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
    offsets = np.arange(0,len_data, chunk_size+recurrence_size)
    lengths = [chunk_size]*(len(offsets)-1) 
    lengths.append(min(chunk_size,len_data-len(lengths)*(chunk_size+recurrence_size)))

    return list(zip(offsets,lengths))
    
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
    index_ranges: List[Tuple[int, int]],
    group,
    channel,
):
    """Writes correct data slice per slice to disk.

    Arguments:
    tdms_writer: Tdms handle for the new file
    index_ranges: Chunk Indices that point to valid data slices
    group: TDMS Group inside the old tdms file
    channel: TDMS Channel inside group
    """
    for (offset,length) in tqdm.tqdm(index_ranges):
        data = channel.read_data(offset=offset,length=length)
        new_channel = nptdms.ChannelObject(group.name, channel.name, data)
        tdms_writer.write_segment([new_channel])


def preprocess(meta: source.MetaData, path: pathlib.Path) -> Any:
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
                        tdms_writer, index_ranges, group, channel
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


    #Determines generalized export path

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
        os.mkdir(export_path)


        # Checks each file in folder for consistency

        files_in_dir = len([f for f in path.iterdir()])
        file_count = 0
        pre_files = []
        for tdms_file in path.iterdir():
            pre_data = {}
            pre_data["tdms_file"] = tdms_file
            file_count += 1
            print(f"Preprocess file {file_count} of {files_in_dir} at {tdms_file}")
            name = tdms_file.with_suffix("").name + "_corrected.tdms"
            pre_data["export_file_path"] = export_path.joinpath(name)
            pre_data["source_file"] = preprocess(meta=meta, path=tdms_file)
            pre_files.append(pre_data)


        # Corrects and exports each file in folder

        for i in range(len(pre_files)):
            file_name = pre_files[i]["tdms_file"]
            print(f"Fix file {i+1} of {len(pre_files)} at {file_name}")
            export_to_tmds(source_file=pre_files[i]["source_file"], export_path=pre_files[i]["export_file_path"])
    
    
    else:
        # Single file case

        name = export_path.name + ".tdms"
        export_path = export_path.parent.joinpath(name)
        source_file = preprocess(meta=meta, path=path)
        export_to_tmds(source_file=source_file, export_path=export_path)