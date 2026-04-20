import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config import AUTO_WASHED_PATH


def main():
    for file in os.listdir(AUTO_WASHED_PATH):
        if file.startswith("."):
            continue
        file_path = os.path.join(AUTO_WASHED_PATH, file)
        file_pieces = file.split("_")
        new_file = (
            "_".join(file_pieces[1:]).replace(".md", "") + "_" + file_pieces[0] + ".md"
        )
        new_file_path = os.path.join(AUTO_WASHED_PATH, new_file)

        os.rename(file_path, new_file_path)


if __name__ == "__main__":
    main()
