import re
import json
import pandas as pd

from loguru import logger
from datetime import datetime

from config import RAW_PATH, MIDDLE_PATH, AUTO_WASHED_PATH, MANUAL_WASHED_PATH
from zhihu_favourite.public.io_util import (
    read_raw_data,
    read_washed_data,
    write_row_to_file,
)
from zhihu_favourite.public.public_util import drop_duplicates_from
from zhihu_favourite.public.network_util import get_json_str


def update_metadata(df, index):
    answer = df.loc[index]["answer"]
    title = df.loc[index]["title"]

    json_str = get_json_str(answer)
    if not json_str:
        return
    try:
        json_data = json.loads(json_str)
    except json.JSONDecodeError:
        logger.error(f"JSON解析失败，跳过")
        return

    # 回答
    entities = json_data["initialState"]["entities"]
    if entities["answers"]:
        answers = dict(list(entities["answers"].values())[0])

        created_time = datetime.fromtimestamp(answers["createdTime"]).strftime(
            "%Y-%m-%d"
        )
        edited_time = datetime.fromtimestamp(answers["updatedTime"]).strftime(
            "%Y-%m-%d"
        )
        favorite_time_after = created_time
        author = answers["author"]["name"]
        author_id = answers["author"]["id"]
    # 专栏
    elif entities["users"] and entities["articles"]:
        users = dict(list(entities["users"].values())[0])
        articles = dict(list(entities["articles"].values())[0])

        created_time = datetime.fromtimestamp(articles["created"]).strftime("%Y-%m-%d")
        edited_time = datetime.fromtimestamp(articles["updated"]).strftime("%Y-%m-%d")
        favorite_time_after = created_time
        author = articles["author"]["name"]
        author_id = articles["author"]["id"]
    # 想法——知乎动态
    elif entities["users"] and entities["pins"]:
        users = dict(list(entities["users"].values())[0])
        pins = dict(list(entities["pins"].values())[0])

        created_time = datetime.fromtimestamp(pins["created"]).strftime("%Y-%m-%d")
        edited_time = datetime.fromtimestamp(pins["updated"]).strftime("%Y-%m-%d")
        favorite_time_after = created_time
        author = users["name"]
        author_id = users["id"]
        if "[object Object]" in df.loc[index]["answer"]:
            answer = re.sub(
                r"\[object Object\](,\[object Object\])*",
                pins["content"][0]["content"],
                answer,
            )
            if pins["content"][0]["title"]:
                title = pins["content"][0]["title"]
            for pic in pins["content"][1:]:
                try:
                    answer += f"\n\n![image]({pic['originalUrl']})"
                except:
                    continue
    # 视频 视频内容需要手动下载，此处只能解决元数据
    elif entities["users"] and entities["zvideos"]:
        users = dict(list(entities["users"].values())[0])
        zvideos = dict(list(entities["zvideos"].values())[0])

        created_time = datetime.fromtimestamp(zvideos["publishedAt"]).strftime(
            "%Y-%m-%d"
        )
        edited_time = datetime.fromtimestamp(zvideos["updatedAt"]).strftime("%Y-%m-%d")
        favorite_time_after = created_time
        author = users["name"]
        author_id = users["id"]
    # 和谐
    else:
        logger.warning(f"小瘪三，已经和谐了，无法获取元数据")
        df.at[index, "censored"] = True
        df.at[index, "modified"] = True
        return

    df.at[index, "created_time"] = created_time
    df.at[index, "edited_time"] = edited_time
    df.at[index, "favorite_time_after"] = favorite_time_after
    # 这里为了方便考虑：未清洗数据作者名必然变化；清洗后数据只有作者名会变化
    if author != df.loc[index]["author"]:
        logger.info(f"作者改名: {df.loc[index]['author']} -> {author}")
        df.at[index, "modified"] = True
    df.at[index, "author"] = author
    df.at[index, "author_id"] = author_id
    df.at[index, "title"] = title
    df.at[index, "answer"] = answer

    logger.info(
        f"获取元数据成功: 作者: {author}, 创建时间: {created_time}, 编辑时间: {edited_time}"
    )


def exec():
    logger.info("将原始数据按回答分离")
    raw_df = read_raw_data(RAW_PATH)
    logger.info(raw_df.shape)
    washed_df = pd.concat(
        [read_washed_data(MIDDLE_PATH)],
        ignore_index=True,
    )
    logger.info(washed_df.shape)
    delta_df = drop_duplicates_from(raw_df, washed_df)
    logger.info(delta_df.shape)

    # 新导出收藏写入
    for index in delta_df.index:
        logger.info(f"——————————————————————{index}")
        logger.info(
            f"我要验牌，正在处理: {delta_df.loc[index]['favorite_folder']}"
            f" - {delta_df.loc[index]['title']}"
        )
        update_metadata(delta_df, index)
        write_row_to_file(delta_df, index, MIDDLE_PATH)


if __name__ == "__main__":
    exec()
