import re
import os
import frontmatter
import pandas as pd

from loguru import logger

from zhihu_favourite.public.public_util import (
    DATAFRAME_COLUMNS,
    get_title,
    merge_duplicates,
    get_answer_hash,
    get_shorted_hash,
)


def read_raw_data(path):
    origin_df = pd.DataFrame(columns=DATAFRAME_COLUMNS)
    for collect_date in os.listdir(path):
        if collect_date.startswith("."):
            continue
        new_rows = []
        favorite_folder_path = os.path.join(path, collect_date)
        for favorite_folder in os.listdir(favorite_folder_path):
            favorite_folder_name = re.sub(r"_.*\.md$", "", favorite_folder)
            file_path = os.path.join(favorite_folder_path, favorite_folder)
            all_answers = re.sub(
                r"---\n\n# ", "\x02# ", frontmatter.load(file_path).content
            )
            for answer in all_answers.split("\x02"):
                answer_hash = get_answer_hash(answer)
                answer = answer.replace("pica", "picx").replace("pic1", "picx")
                new_rows.append(
                    {
                        "hash": answer_hash,
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
                        "file_path": None, # 这一项在这没用
                    }
                )
        temp_df = (
            pd.DataFrame(new_rows, columns=DATAFRAME_COLUMNS)
            .sort_values(by="hash")
            .reset_index(drop=True)
        )
        temp_df = merge_duplicates(temp_df)
        origin_df = pd.concat([origin_df, temp_df], ignore_index=True)
    origin_df = merge_duplicates(origin_df, reset_tag=True)
    return origin_df


def read_washed_data(path):
    new_rows = []
    for root, dirs, files in os.walk(path):
        for file in files:
            if not file.endswith(".md"):
                continue
            file_path = os.path.join(root, file)
            with open(file_path, "r", encoding="utf-8") as f:
                single_final_answer = frontmatter.load(f)
            try:
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
                        "file_path": file_path, # manual_washed分了多层级，必须用一次内部追踪，否则写不回原文件
                    }
                )
            except KeyError:
                continue
    return pd.DataFrame(new_rows, columns=DATAFRAME_COLUMNS)


def write_row_to_file(df, index, root_path):
    if os.path.exists(root_path) and os.path.isfile(root_path):
        file_name = f"{df.loc[index]['title']}_{get_shorted_hash(df.loc[index]['hash'])}.md"
        file_path = root_path
    else:
        os.makedirs(root_path, exist_ok=True)
        file_name = f"{df.loc[index]['title']}_{get_shorted_hash(df.loc[index]['hash'])}.md"
        file_path = os.path.join(root_path, file_name)
        
    metadata = {
        "hash": df.loc[index]["hash"],
        "tags": df.loc[index]["tags"],
        "created_time": df.loc[index]["created_time"],
        "edited_time": df.loc[index]["edited_time"],
        "favorite_time_after": df.loc[index]["favorite_time_after"],
        "favorite_time_before": df.loc[index]["favorite_time_before"],
        "author": df.loc[index]["author"],
        "author_id": df.loc[index]["author_id"],
        "censored": bool(df.loc[index]["censored"]),
    }
    # 把 NaN 统一转回 None，保留所有元数据字段；list 直接保留
    metadata = {
        k: (None if (v is not None and not isinstance(v, list) and pd.isna(v)) else v)
        for k, v in metadata.items()
    }
    if not df.loc[index]["modified"] and os.path.exists(file_path):
        logger.info(f"文件未变更，仅写入元数据：{df.loc[index]['title']}")
        final_md = frontmatter.load(file_path)
        final_md.content = final_md.content.replace("\n", "\r\n")
        final_md.metadata.update(metadata)
    else:
        final_md = frontmatter.Post(content=df.loc[index]["answer"], **metadata)
        if os.path.exists(file_path):
            logger.warning(f"文件已存在，覆盖写入：{file_path}")
    frontmatter.dump(final_md, file_path)
    logger.info(f"写入文件成功: {file_name}")
