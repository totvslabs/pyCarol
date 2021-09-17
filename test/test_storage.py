from pathlib import Path

from dotenv import load_dotenv
import pandas as pd

import pycarol

TEST_FILEPATH = Path("./test/artifacts/test.txt")
TEST_FILENAME = TEST_FILEPATH.name


def _setup_storage() -> pycarol.Storage:
    load_dotenv()
    login = pycarol.Carol()
    storage = pycarol.Storage(login)
    return storage


def test_save_parquet():
    storage = _setup_storage()
    empty_df = pd.DataFrame()
    storage.save(name=TEST_FILENAME, obj=empty_df, format="pickle")

    back_df = storage.load(TEST_FILENAME, format="pickle")
    assert back_df.empty, "Loaded DataFrame is different from saved one."


def test_save_joblib():
    storage = _setup_storage()
    storage.save(name=TEST_FILENAME, obj=str(TEST_FILEPATH), format="joblib")

    backpath = storage.load(TEST_FILENAME, format="joblib")
    with open(str(TEST_FILEPATH), "r") as file1:
        text1 = file1.read()

    with open(str(backpath), "r") as file2:
        text2 = file2.read()

    assert text1 == text2, "Texts are equal."


def test_save_pickle():
    storage = _setup_storage()
    storage.save(name=TEST_FILENAME, obj=str(TEST_FILEPATH), format="pickle")

    backpath = storage.load(TEST_FILENAME, format="pickle")
    with open(str(TEST_FILEPATH), "r") as file1:
        text1 = file1.read()

    with open(str(backpath), "r") as file2:
        text2 = file2.read()

    assert text1 == text2, "Texts are equal."


def test_save_file():
    storage = _setup_storage()
    storage.save(name=TEST_FILENAME, obj=str(TEST_FILEPATH), format="file")

    backpath = storage.load(TEST_FILENAME, format="file")
    with open(str(TEST_FILEPATH), "r") as file1:
        text1 = file1.read()

    with open(str(backpath), "r") as file2:
        text2 = file2.read()

    assert text1 == text2, "Texts are equal."


def test_save_other():
    storage = _setup_storage()

    try:
        storage.save(name=TEST_FILENAME, obj=str(TEST_FILEPATH), format="another")
    except ValueError:
        return

    raise Exception("Saving with unexistant format should raise exception.")


if __name__ == "__main__":
    test_save_parquet()
