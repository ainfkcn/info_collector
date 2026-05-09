import re
import json
import pandas as pd

from loguru import logger

from config import RAW_PATH, MIDDLE_PATH
from zhihu_favourite.public.io_util import (
    read_raw_data,
    read_washed_data,
    write_row_to_file,
)
from zhihu_favourite.public.public_util import drop_duplicates_from
from zhihu_favourite.public.network_util import update_metadata


def exec(refresh_metadata=False):
    logger.info("将原始数据按回答分离")
    raw_df = read_raw_data(RAW_PATH)
    logger.info(f"raw_df.shape: {raw_df.shape}")
    if refresh_metadata:
        washed_df = pd.DataFrame()
    else:
        washed_df = pd.concat(
            [read_washed_data(MIDDLE_PATH)],
            ignore_index=True,
        )
    logger.info(f"washed_df.shape: {washed_df.shape}")
    delta_df = drop_duplicates_from(raw_df, washed_df)
    logger.info(f"delta_df.shape: {delta_df.shape}")

    # 新导出收藏写入
    for index in delta_df.index:
        logger.info(f"——————————————————————{index + 1}/{raw_df.shape[0]}")
        logger.info(
            f"正在处理: {delta_df.loc[index]['favorite_folder']}"
            f" - {delta_df.loc[index]['title']}"
        )
        update_metadata(delta_df, index)
        write_row_to_file(delta_df, index, MIDDLE_PATH)


if __name__ == "__main__":
    exec()
