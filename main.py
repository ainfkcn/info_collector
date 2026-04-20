from loguru import logger

import zhihu_favourite.split_raw
import zhihu_favourite.wash_splited


def exec():
    logger.add(
        "log/data_wash.log",
        level="INFO",
        rotation="10 MB",
        encoding="utf-8",
    )
    zhihu_favourite.split_raw.exec()
    zhihu_favourite.wash_splited.exec()


if __name__ == "__main__":
    exec()
