import re
import json

from loguru import logger
from datetime import datetime
from playwright.sync_api import sync_playwright


def get_json_str(answer):
    url_link = re.search(r"\[(原文|视频)链接\]\((.*?)\)", answer)
    if not url_link:
        logger.error(f"未找到原文/原视频链接")
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
                logger.error(f"未找到initialState数据")
                return None

        except Exception as e:
            logger.error(f"使用Playwright获取元数据失败: {str(e)}")
            return None
        finally:
            browser.close()

def update_metadata(df, index):
    answer = df.loc[index]["answer"]
    title = df.loc[index]["title"]

    json_str = get_json_str(answer)
    if not json_str:
        return
    try:
        json_data = json.loads(json_str)
    except json.JSONDecodeError:
        logger.error(f"JSON解析失败，跳过当前文件")
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
        logger.error(f"已经和谐了，无法获取元数据")
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

def update_metadata_from_local(middle_df, washed_df, index):
    middle_index = washed_df.loc[index]["hash"]
    if middle_index not in middle_df.index:
        logger.warning(f"当前hash {middle_index} 在middle中未找到，跳过")
        washed_df.at[index, "modified"] = False
        return
    washed_df.at[index, "tags"] = middle_df.loc[middle_index]["tags"]
    washed_df.at[index, "created_time"] = middle_df.loc[middle_index]["created_time"]
    washed_df.at[index, "edited_time"] = middle_df.loc[middle_index]["edited_time"]
    washed_df.at[index, "favorite_time_before"] = middle_df.loc[middle_index]["favorite_time_before"]
    washed_df.at[index, "favorite_time_after"] = middle_df.loc[middle_index]["favorite_time_after"]
    washed_df.at[index, "author"] = middle_df.loc[middle_index]["author"]
    washed_df.at[index, "author_id"] = middle_df.loc[middle_index]["author_id"]
    washed_df.at[index, "censored"] = middle_df.loc[middle_index]["censored"]
    washed_df.at[index, "modified"] = True
