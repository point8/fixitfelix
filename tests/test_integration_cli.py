import nptdms
import numpy as np
import pathlib
import pytest
import subprocess

import yaml

from fixitfelix import cli

# Strictly speaking this should also reside somewhere else for the test and not touch the user's actual config
PATH_TO_TESTCONFIG = cli.PATH_TO_CONFIG

TEST_FILE_FOLDER = pathlib.Path(__file__).parent / "assets"
TEST_FILENAME = "example_file.tdms"
FILENAME_CORRECTED = "example_file_corrected.tdms"


@pytest.fixture()
def temp_dir(tmp_path_factory) -> pathlib.Path:
    tmp_dir = tmp_path_factory.mktemp("data")
    # Copy test_file to tmp_dir
    (tmp_dir / TEST_FILENAME).write_bytes(
        (TEST_FILE_FOLDER / TEST_FILENAME).read_bytes()
    )
    return tmp_dir


def run_felix(tmp_dir: pathlib.Path) -> None:
    """Run fixit with the correct paramaters. Also checks for successful run."""
    success = subprocess.run(
        [
            "fixit",
            str(tmp_dir / TEST_FILENAME),
            "--chunk_size=6",
            "--recurrence_size=2",
            "--recurrence_distance=3",
            "--consistency_sample_size=10",
        ]
    )
    assert isinstance(success, subprocess.CompletedProcess)


def check_correct_tdms_file(tmp_dir: pathlib.Path) -> None:
    """Checks if the tdms_file exists and if it contains the correct data.
    """
    # New TdmsFile exists with the correct naming
    path_to_corrected_tdms = tmp_dir / FILENAME_CORRECTED
    assert path_to_corrected_tdms.exists()

    # The corrected TdmsFile contains the desired data
    tdms_operator = nptdms.TdmsFile(path_to_corrected_tdms)
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


def test_for_correct_files(temp_dir):
    """Checks for correct content of the TdmsFile and the config yaml file"""
    # Check for_correct_tdms_file
    run_felix(temp_dir)
    check_correct_tdms_file(temp_dir)

    # Config File has been created with the used parameters

    assert PATH_TO_TESTCONFIG.exists()

    with PATH_TO_TESTCONFIG.open() as file:
        used_config = yaml.safe_load(file)
    assert used_config == {
        "chunk_size": 6,
        "recurrence_size": 2,
        "recurrence_distance": 3,
        "consistency_sample_size": 10,
    }


def test_for_used_config(temp_dir):
    """Repeats the call of fixit above but uses the written default values from
    the config.yaml file
    """
    # Make sure felix ran before. We expect it to create ~/.fixitfelix_config.yaml.
    run_felix(temp_dir)
    (temp_dir / FILENAME_CORRECTED).unlink()
    # Repeat the test to check if preferences from .fixitfelix_config.yaml are loaded correctly
    success = subprocess.run(
        ["fixit", str(temp_dir / TEST_FILENAME)], text=True, input="\n\n\n\n",
    )
    assert isinstance(success, subprocess.CompletedProcess)

    # Test whether the tdms_file was constructed correctly
    check_correct_tdms_file(temp_dir)


def teardown_module():
    """Deletes the fixitfelix_config.yaml"""
    PATH_TO_TESTCONFIG.unlink()
