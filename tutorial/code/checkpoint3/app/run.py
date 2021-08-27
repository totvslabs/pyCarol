"""Routes between all possible actions."""
import os

from dotenv import load_dotenv

from .app import train, predict

PROCESSES = {
    "bhptrainmodelnb": train.run,
    "bhppredictnb": predict.run,
}


if __name__ == "__main__":
    load_dotenv()
    ALGORITHM_NAME = os.environ.get("ALGORITHM_NAME")
    PROCESS_NAMES = list(PROCESSES.keys())
    if ALGORITHM_NAME is None:
        raise ValueError("ALGORITHM_NAME environment var does not exist.")
    if ALGORITHM_NAME not in PROCESS_NAMES:
        raise ValueError(
            f"ALGORITHM_NAME env var must be one of the following: {PROCESS_NAMES}"
        )

    PROCESSES[ALGORITHM_NAME]()
