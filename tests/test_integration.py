import pytest
import nptdms
import numpy as np
import pathlib

from fixitfelix import fix, source


def test_processes_example_file_correct(tmpdir):
    meta = source.MetaData(
        chunk_size=6,
        recurrence_size=2,
        recurrence_distance=3,
        consistency_sample_size=10,
    )
    output_filename = pathlib.Path(tmpdir) / "output.tdms"

    fix.export_correct_data(
        tdms_path="tests/assets/example_file.tdms",
        meta=meta,
        export_path=output_filename,
    )
    tdms_operator = nptdms.TdmsFile(output_filename)
    df_result = tdms_operator.as_dataframe()
    assert len(df_result) == 15
    assert np.array_equal(
        df_result.columns.values,
        np.array(
            [
                "/'Untitled'/'D'",
                "/'Untitled'/'C'",
                "/'Untitled'/'B'",
                "/'Untitled'/'A'",
            ]
        ),
    )
    assert np.array_equal(df_result["/'Untitled'/'A'"].values, np.arange(1, 16))


def tests_fails_for_nonexistent_filepath():
    meta = source.MetaData(
        chunk_size=6,
        recurrence_size=2,
        recurrence_distance=3,
        consistency_sample_size=10,
    )
    output_filename = pathlib.Path("NonexistentPath/tdms_file.tdms")

    with pytest.raises(Exception):
        fix.export_correct_data(
            tdms_path="tests/assets/example_file.tdms",
            meta=meta,
            export_path=output_filename,
        )


def test_prechecks_recognize_wrong_parameters(tmpdir):
    meta = source.MetaData(
        chunk_size=6,
        recurrence_size=3,  # <- 2 would be correct
        recurrence_distance=3,
        consistency_sample_size=10,
    )
    output_filename = pathlib.Path(tmpdir) / "output.tdms"
    with pytest.raises(Exception):
        fix.export_correct_data(
            tdms_path="tests/assets/example_file.tdms",
            meta=meta,
            export_path=output_filename,
        )
