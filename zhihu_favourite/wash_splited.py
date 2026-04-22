import os
import re
import requests
import pandas as pd

from loguru import logger
from urllib.parse import urlparse, parse_qs, unquote

from config import (
    ZHIHU_FAVOURITE_ROOT,
    MIDDLE_PATH,
    AUTO_WASHED_PATH,
    AUTO_WASHED_PASSED_PATH,
    MANUAL_WASHED_PATH,
)
from zhihu_favourite.public.io_util import (
    read_washed_data,
    write_row_to_file,
)
from zhihu_favourite.public.public_util import (
    drop_duplicates_from,
    get_hash,
    get_shorted_hash,
)


def refine_data(delta_df, index):
    answer = delta_df.loc[index]["answer"]
    # 替换空连接[]()
    new_answer = answer.replace("[]()", "")
    if new_answer != answer:
        delta_df.at[index, "modified"] = True
        logger.info("替换空连接")
    # 删除汉字或全角标点行前空格或 tab
    answer = new_answer
    new_answer = re.sub(
        # 正则释义               数字 破折号 左引号     汉字       全角标点    全角英文数字
        r"^[\u200b\u3000 \t]+(?=[0-9\u2014\u201c\u4e00-\u9fff\u3000-\u303F\uFF00-\uFFEF])",
        "",
        answer,
        flags=re.MULTILINE,
    )
    if new_answer != answer:
        delta_df.at[index, "modified"] = True
        logger.info("删除前导空格")
    # 删除多余空行
    answer = new_answer
    new_answer = re.sub(r"\n{3,}", "\n\n", answer)
    if new_answer != answer:
        delta_df.at[index, "modified"] = True
        logger.info("删除空行")
    answer = new_answer
    delta_df.at[index, "answer"] = answer


def picture_localization(delta_df, index):
    # 图片本地化
    pic_dir = os.path.join(ZHIHU_FAVOURITE_ROOT, ".pic")
    os.makedirs(pic_dir, exist_ok=True)
    answer = delta_df.loc[index]["answer"]
    modified = False
    for match in re.finditer(r"(\n\n)?!\[(.*?)\]\((.*?)\)", answer):
        url = match.group(3)
        # 已经本地化的，跳过
        if not url.startswith(("http://", "https://")):
            continue
        # 公式图片，不用下载图，直接从url里面拼latex
        elif "equation" in url:
            # 取tex=部分自己拼latex
            parsed = urlparse(url)
            query = parse_qs(parsed.query)
            tex = query.get("tex", [None])[0]
            tex = unquote(tex)

            if not tex:
                continue
            # 转换过程里遇到的后处理部分
            tex = (
                tex.replace("\\[", "")
                .replace("\\]", "")
                .replace("\\bold", "\\boldsymbol")
                .replace("\\bm", "\\boldsymbol")
                .removeprefix("\\\\")
                .removesuffix("\\\\")
            )

            new_link = f"\n\n$$\n{tex}\n$$"
            answer = answer.replace(match.group(0), new_link)
            modified = True
            logger.info(f"公式图片转换为 LaTeX 块：{tex}")
        # 真正的图片，这才需要下载
        else:
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
            except Exception as e:
                logger.error(f"下载图片失败: {url}, 错误: {e}")
                continue
            pic_content = response.content
            pic_hash = get_hash(pic_content, short=True)
            # 获取原后缀
            ext = (
                re.sub(r"\?source=.*", "", os.path.splitext(url)[1]) or ".jpg"
            )  # 默认jpg
            new_filename = f"{delta_df.loc[index]['title']}_{get_shorted_hash(delta_df.loc[index]['hash'])}_{pic_hash}{ext}"
            new_path = os.path.join(pic_dir, new_filename)
            if os.path.exists(new_path):
                logger.warning(f"图片已存在：{new_path}")
            else:
                with open(new_path, "wb") as f:
                    f.write(pic_content)
                logger.info(f"下载图片到本地：{new_filename}")
            # 替换链接
            old_link = match.group(0)
            new_link = f"\n\n![{match.group(2)}]({new_path})"
            answer = answer.replace(old_link, new_link)
            modified = True
            logger.info(f"下载图片到本地：{new_filename}")
    if modified:
        delta_df.at[index, "answer"] = answer


def exec():
    logger.info("对分离后的数据进行二次加工")
    middle_df = read_washed_data(MIDDLE_PATH)
    logger.info(middle_df.shape)
    washed_df = pd.concat(
        [read_washed_data(MANUAL_WASHED_PATH)],
        ignore_index=True,
    )
    logger.info(f"washed_df.shape: {washed_df.shape}")
    delta_df = drop_duplicates_from(middle_df, washed_df)
    logger.info(f"delta_df.shape: {delta_df.shape}")
    # 原有收藏元数据更新核验
    for index in delta_df.index:
        logger.info(f"——————————————————————{index + 1}/{delta_df.shape[0]}")
        logger.info(
            f"给我擦皮鞋，对清洗后的数据做后处理: {get_shorted_hash(delta_df.loc[index]['title'])}"
            + f"_{delta_df.loc[index]['hash']}"
        )
        refine_final_data(delta_df, index)
        picture_localization(delta_df, index)
        refine_data(delta_df, index)
        # if not delta_df.at[index, "modified"]:
        #     logger.info("这条无需处理，放入特殊目录")
        #     delta_df.at[index, "modified"] = True
        #     write_row_to_file(delta_df, index, AUTO_WASHED_PASSED_PATH)
        # else:
        write_row_to_file(delta_df, index, AUTO_WASHED_PATH)


if __name__ == "__main__":
    exec()
