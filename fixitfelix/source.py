import pathlib
import tempfile
from typing import Any, NamedTuple

import nptdms


class MetaData(NamedTuple):
    recurrence_size: int
    recurrence_distance: int
    chunk_size: int
    consistency_sample_size: int
    usable_memory: int


class SourceFile:
    """Container for the tdms operator combined with meta data.

    Meta data includes arguments for file correction process.
    """

    def __init__(self, tdms_operator: nptdms.TdmsFile, meta: MetaData):
        self.tdms_operator = tdms_operator
        self.meta = meta

    @classmethod
    def read_from_path(cls, tdms_path: pathlib.Path, meta: MetaData):
        tdms_operator = nptdms.TdmsFile(
            tdms_path, memmap_dir=tempfile.gettempdir()
        )
        return cls(tdms_operator, meta)
