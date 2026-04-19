import os
import re
import json
import frontmatter
import pandas as pd
from time import sleep
from datetime import datetime
from loguru import logger
from hashlib import sha256
from playwright.sync_api import sync_playwright


ORIGIN_FOLDER_PATH = r"E:\\zhihu_favourite\\origin_data"
FINAL_FOLDER_PATH = r"E:\\zhihu_favourite\\final_data"
DATAFRAME_COLUMNS = [
    "hash",
    "tags",
    "created_time",
    "edited_time",
    "favorite_time_after",
    "favorite_time_before",
    "author",
    "author_id",
    "censored",
    # 不写入文件的跟踪变量
    "favorite_folder",
    "title",
    "answer",
    "modified",
    "json_str",
]


def get_title(answer):
    raw_title = answer.split("\n")[0].replace("# ", "").strip()
    # 数据清洗：文件名不能包含的特殊字符替换成下划线
    title = re.sub(r'[\\/*?:"<>|]', "_", raw_title)
    # 数据清洗：有一些问题以_结尾，会导致文件名双重下划线问题
    title = title.rstrip("_")
    # 数据清洗：title中有空格的删掉空格
    title = title.replace(" ", "")
    return title


def merge_duplicates(df):
    """按hash分组合并重复行，tags列融合成不重复的列表"""
    agg_map = {col: "first" for col in DATAFRAME_COLUMNS if col != "hash"}
    agg_map["tags"] = lambda x: sorted({tag for tags_list in x for tag in tags_list})
    agg_map["favorite_time_before"] = lambda x: min(x)
    return df.groupby("hash").agg(agg_map).reset_index()


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


def drop_duplicates_from(from_df, in_df):
    if in_df.empty:
        return from_df
    return from_df[~from_df["hash"].isin(in_df["hash"])]


def get_json_str(answer, title):
    url_link = re.search(r"\[(原文|视频)链接\]\((.*?)\)", answer)
    if not url_link:
        logger.error(f"未找到链接")
        return None
    url_link = url_link.group(2)
    logger.info(f"url: {url_link}")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-web-security",
                "--disable-features=VizDisplayCompositor",
                "--disable-extensions",
                "--disable-plugins",
                "--disable-default-apps",
            ],
        )

        # 更完整的浏览器指纹伪装
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
            geolocation={"latitude": 34.69, "longitude": 135.50},
            permissions=["geolocation"],
            extra_http_headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Cache-Control": "max-age=0",
                "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                "Sec-Ch-Ua-Mobile": "?0",
                "Sec-Ch-Ua-Platform": '"Windows"',
            },
        )

        # 隐藏自动化特征
        page = context.new_page()
        page.add_init_script(
            """
            // 隐藏webdriver特征
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });

            // 隐藏chrome特征
            Object.defineProperty(navigator, 'chrome', {
                get: () => ({
                    runtime: {},
                    loadTimes: function() {},
                    csi: function() {},
                    app: {}
                }),
            });

            // 随机化插件数量
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5],
            });

            // 随机化语言
            Object.defineProperty(navigator, 'languages', {
                get: () => ['zh-CN', 'zh', 'en'],
            });
        """
        )

        try:
            script_content = None

            page.goto(url_link, wait_until="domcontentloaded", timeout=20000)

            # 从所有script标签中查找
            while not script_content:
                scripts = page.locator("script").all()
                for script in scripts:
                    try:
                        content = script.text_content()
                        if (
                            content
                            and '"initialState"' in content
                            and '"answers"' in content
                        ):
                            script_content = content
                            break
                    except:
                        continue

            if script_content:
                return script_content
            else:
                logger.warning(f"未找到初始数据")
                return None

        except Exception as e:
            logger.error(f"使用Playwright获取元数据失败: {str(e)}")
            return None
        finally:
            browser.close()


def update_metadata(df, index):
    answer = df.loc[index]["answer"]
    title = df.loc[index]["title"]

    logger.info(f"我要验牌，正在处理: {df.loc[index]['favorite_folder']} - {title}")
    json_str = get_json_str(answer, title)
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


def write_row_to_file(df, index):
    if not df.loc[index]["modified"]:
        logger.info(f"牌有问题，跳过写入：{df.loc[index]['title']}")
        return

    file_name = f"{df.loc[index]['hash']}_{df.loc[index]['title']}.md"
    file_path = os.path.join(FINAL_FOLDER_PATH, file_name)
    final_md = frontmatter.Post(
        content=df.loc[index]["answer"],
        **{
            "hash": df.loc[index]["hash"],
            "tags": df.loc[index]["tags"],
            "created_time": df.loc[index]["created_time"],
            "edited_time": df.loc[index]["edited_time"],
            "favorite_time_after": df.loc[index]["favorite_time_after"],
            "favorite_time_before": df.loc[index]["favorite_time_before"],
            "author": df.loc[index]["author"],
            "author_id": df.loc[index]["author_id"],
            "censored": df.loc[index]["censored"].item(),
        },
    )
    frontmatter.dump(final_md, file_path)
    logger.info(f"牌没有问题，写入文件成功: {file_name}")


def refine_final_data(final_df, index):
    logger.info(f"给我擦皮鞋，对清洗后的数据做后处理")
    # 替换空连接[]()
    if "[]()" in final_df.loc[index]["answer"]:
        final_df.at[index, "answer"] = final_df.loc[index]["answer"].replace("[]()", "")
        final_df.at[index, "modified"] = True
    pass


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
        update_metadata(delta_df, index)
        write_row_to_file(delta_df, index)
    # 原有收藏元数据更新核验
    for index in final_df.index:
        logger.info(f"——————————————————————{index}")
        # update_metadata(final_df, index)
        refine_final_data(final_df, index)
        write_row_to_file(final_df, index)
