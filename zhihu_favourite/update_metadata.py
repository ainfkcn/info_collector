import pandas as pd

from loguru import logger
from config import MANUAL_WASHED_PATH, AUTO_WASHED_PATH, MANUAL_WASHED_PATH
from zhihu_favourite.public.io_util import read_washed_data, write_row_to_file
from zhihu_favourite.public.public_util import get_shorted_hash
from zhihu_favourite.public.network_util import update_metadata_from_local


def exec():
    logger.info("刷新清洗后的数据的元信息")
    middle_df = read_washed_data(MANUAL_WASHED_PATH).set_index("hash", drop=False)
    logger.info(f"middle_df.shape: {middle_df.shape}")
    auto_washed_df = read_washed_data(AUTO_WASHED_PATH)
    logger.info(f"auto_washed_df.shape: {auto_washed_df.shape}")
    manual_washed_df = read_washed_data(MANUAL_WASHED_PATH)
    logger.info(f"manual_washed_df.shape: {manual_washed_df.shape}")
    # 原有收藏元数据更新核验
    for washed_df in [auto_washed_df, manual_washed_df]:
        for index in washed_df.index:
            logger.info(f"——————————————————————{index + 1}/{washed_df.shape[0]}")
            logger.info(
                f"刷新元信息: {washed_df.loc[index]['title']}_{get_shorted_hash(washed_df.loc[index]['hash'])}"
            )
            update_metadata_from_local(middle_df, washed_df, index)
            if washed_df.loc[index, "modified"]:
                logger.warning("元信息有更新，写入文件")
                write_row_to_file(washed_df, index, washed_df.loc[index]["file_path"])
            else:
                logger.info("元信息无更新，无需写入文件")


if __name__ == "__main__":
    exec()
