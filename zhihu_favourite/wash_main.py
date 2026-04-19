import os
import re
import sys
import json
import requests

from datetime import datetime
from loguru import logger
from hashlib import sha256

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from zhihu_favourite.network_util import get_json_str
from zhihu_favourite.public_util import FINAL_FOLDER_PATH, drop_duplicates_from
from zhihu_favourite.io_util import read_origin_data, read_final_data, write_row_to_file


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
                answer += f"\n\n![image]({pic['originalUrl']})"
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


def refine_final_data(final_df, index):
    # 替换空连接[]()
    if "[]()" in final_df.loc[index]["answer"]:
        final_df.at[index, "answer"] = final_df.loc[index]["answer"].replace("[]()", "")
        final_df.at[index, "modified"] = True
        logger.info("替换空连接")
    # 删除汉字或全角标点行前空格或 tab
    new_answer = re.sub(
        # 正则释义     数字 破折号 左引号     汉字       全角标点    全角英文数字
        r"^[ 　\t]+(?=[0-9\u2014\u201c\u4e00-\u9fff\u3000-\u303F\uFF00-\uFFEF])",
        "",
        final_df.loc[index]["answer"],
        flags=re.MULTILINE,
    )
    if new_answer != final_df.loc[index]["answer"]:
        final_df.at[index, "answer"] = new_answer
        final_df.at[index, "modified"] = True
        logger.info("删除前导空格")


def picture_localization(final_df, index):
    # 图片本地化
    pic_dir = os.path.join(FINAL_FOLDER_PATH, ".pic")
    os.makedirs(pic_dir, exist_ok=True)
    answer = final_df.loc[index]["answer"]
    modified = False
    for match in re.finditer(r"!\[.*?\]\((.*?)\)", answer):
        url = match.group(1)
        if not url.startswith(("http://", "https://")):
            continue  # 跳过本地链接
        if "equation" in url:
            # 取tex=部分自己拼latex
            # 先跳过吧
            continue
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
        except Exception as e:
            logger.warning(f"下载图片失败: {url}, 错误: {e}")
        pic_content = response.content
        pic_hash = sha256(pic_content).hexdigest()[:8]
        # 获取原后缀
        ext = re.sub(r"\?source=.*", "", os.path.splitext(url)[1]) or ".jpg"  # 默认jpg
        new_filename = f"{pic_hash}_{final_df.loc[index]['hash'][:8]}_{final_df.loc[index]['title']}{ext}"
        new_path = os.path.join(pic_dir, new_filename)
        with open(new_path, "wb") as f:
            f.write(pic_content)
        # 替换链接
        old_link = match.group(0)
        new_link = f"![{match.group(0)[2:-1].split(']')[0]}](.pic/{new_filename})"
        answer = answer.replace(old_link, new_link)
        modified = True
        logger.info(f"下载图片到本地：{new_filename}")
    if modified:
        final_df.at[index, "answer"] = answer
        final_df.at[index, "modified"] = True


if __name__ == "__main__":
    origin_df = read_origin_data()
    print(origin_df.shape)
    final_df = read_final_data()
    print(final_df.shape)
    delta_df = drop_duplicates_from(origin_df, final_df)
    print(delta_df.shape)

    # 新导出收藏写入
    for index in delta_df.index:
        logger.info(f"——————————————————————{index}")
        logger.info(
            f"我要验牌，正在处理: {delta_df.loc[index]['favorite_folder']}"
            f" - {delta_df.loc[index]['title']}"
        )
        update_metadata(delta_df, index)
        write_row_to_file(delta_df, index)
    # 原有收藏元数据更新核验
    for index in final_df.index:
        logger.info(f"——————————————————————{index}")
        logger.info(
            f"给我擦皮鞋，对清洗后的数据做后处理: {final_df.loc[index]['title']}"
        )
        # update_metadata(final_df, index) # 作者改名后重新同步metadata，一般不跑
        refine_final_data(final_df, index)
        picture_localization(final_df, index)
        write_row_to_file(final_df, index)
