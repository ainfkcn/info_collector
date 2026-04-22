import re

from loguru import logger
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
