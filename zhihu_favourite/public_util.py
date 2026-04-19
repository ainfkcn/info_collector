import re


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


def drop_duplicates_from(from_df, in_df):
    if in_df.empty:
        return from_df
    return from_df[~from_df["hash"].isin(in_df["hash"])]
