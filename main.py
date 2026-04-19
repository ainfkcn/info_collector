import os

FINAL_FOLDER_PATH = r"E:\\zhihu_favourite\\final_data"

def main():
    for file in os.listdir(FINAL_FOLDER_PATH):
        if file.startswith("."):
            continue
        file_path = os.path.join(FINAL_FOLDER_PATH, file)
        file_pieces = file.split("_")
        new_file = file_pieces[0][0:8] + "_" + "_".join(file_pieces[1:])
        new_file_path = os.path.join(FINAL_FOLDER_PATH, new_file)

        os.rename(file_path, new_file_path)


if __name__ == "__main__":
    main()
