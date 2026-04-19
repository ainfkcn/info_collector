import re
import os
import frontmatter
import pandas as pd

from loguru import logger
from hashlib import sha256

from zhihu_favourite.public_util import (
    ORIGIN_FOLDER_PATH,
    FINAL_FOLDER_PATH,
    DATAFRAME_COLUMNS,
    get_title,
    merge_duplicates,
)


def read_origin_data():
    new_rows = []
    for collect_date in os.listdir(ORIGIN_FOLDER_PATH):
        if collect_date.startswith("."):
            continue
        favorite_folder_path = os.path.join(ORIGIN_FOLDER_PATH, collect_date)
        for favorite_folder in os.listdir(favorite_folder_path):
            file_path = os.path.join(favorite_folder_path, favorite_folder)
            with open(file_path, "r", encoding="utf-8") as f:
                all_answers = re.sub(
                    r"---\n\n# ", "\x02# ", frontmatter.load(f).content
                )
            favorite_folder_name = re.sub(r"_.*\.md$", "", favorite_folder)
            for answer in all_answers.split("\x02"):
                # 数据清洗：去掉md中所有的url，再用纯净值求hash，排除链接干扰
                hash_answer = re.sub(r"!\[.*?\]\(.*?\)\n\n", "", answer)
                new_rows.append(
                    {
                        "hash": sha256(hash_answer.encode("utf-8")).hexdigest(),
                        "tags": [favorite_folder_name],
                        "created_time": None,
                        "edited_time": None,
                        "favorite_time_after": None,
                        "favorite_time_before": collect_date,
                        "author": None,
                        "author_id": None,
                        "censored": False,
                        # 不写入文件的跟踪变量
                        "favorite_folder": favorite_folder_name,
                        "title": get_title(answer),
                        "answer": answer,
                        "modified": False,
                        "json_str": None,
                    }
                )
    origin_df = pd.DataFrame(new_rows).sort_values(by="hash").reset_index(drop=True)
    origin_df = merge_duplicates(origin_df)
    return origin_df


def read_final_data():
    new_rows = []
    for file in os.listdir(FINAL_FOLDER_PATH):
        if file.startswith("."):
            continue
        file_path = os.path.join(FINAL_FOLDER_PATH, file)
        with open(file_path, "r", encoding="utf-8") as f:
            single_final_answer = frontmatter.load(f)
        new_rows.append(
            {
                "hash": single_final_answer["hash"],
                "tags": single_final_answer["tags"],
                "created_time": single_final_answer["created_time"],
                "edited_time": single_final_answer["edited_time"],
                "favorite_time_after": single_final_answer["favorite_time_after"],
                "favorite_time_before": single_final_answer["favorite_time_before"],
                "author": single_final_answer["author"],
                "author_id": single_final_answer["author_id"],
                "censored": single_final_answer.get("censored", False),
                # 不写入文件的跟踪变量
                "favorite_folder": None,
                "title": get_title(single_final_answer.content),
                "answer": single_final_answer.content,
                "modified": False,
                "json_str": None,
            }
        )
    return pd.DataFrame(new_rows, columns=DATAFRAME_COLUMNS)


def write_row_to_file(df, index):
    if not df.loc[index]["modified"]:
        logger.info(f"牌没有问题，跳过写入：{df.loc[index]['title']}")
        return

    file_name = f"{df.loc[index]['hash'][0:8]}_{df.loc[index]['title']}.md"
    file_path = os.path.join(FINAL_FOLDER_PATH, file_name)
    metadata = {
        "hash": df.loc[index]["hash"],
        "tags": df.loc[index]["tags"],
        "created_time": df.loc[index]["created_time"],
        "edited_time": df.loc[index]["edited_time"],
        "favorite_time_after": df.loc[index]["favorite_time_after"],
        "favorite_time_before": df.loc[index]["favorite_time_before"],
        "author": df.loc[index]["author"],
        "author_id": df.loc[index]["author_id"],
        "censored": df.loc[index]["censored"].item(),
    }
    # 把 NaN 统一转回 None，保留所有元数据字段；list 直接保留
    metadata = {
        k: (None if (v is not None and not isinstance(v, list) and pd.isna(v)) else v)
        for k, v in metadata.items()
    }
    final_md = frontmatter.Post(content=df.loc[index]["answer"], **metadata)
    frontmatter.dump(final_md, file_path)
    logger.info(f"牌没有问题，写入文件成功: {file_name}")
