import re

from hashlib import sha256

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
    # 数据清洗：前文替换后，半角问号被替换成下划线，此处连同全角问号一起删除
    title = title.rstrip("？_")
    # 数据清洗：title中有空格的删掉空格
    title = title.replace(" ", "")
    return title


def merge_duplicates(df):
    """按hash分组合并重复行，tags列融合成不重复的列表"""
    agg_map = {col: "first" for col in DATAFRAME_COLUMNS if col != "hash"}
    # tag聚合成列表
    agg_map["tags"] = lambda x: sorted({tag for tags_list in x for tag in tags_list})
    # 收藏时间上限取最早的
    agg_map["favorite_time_before"] = lambda x: min(x)
    # title取最长的
    agg_map["title"] = lambda x: max(x, key=len)
    return df.groupby("hash").agg(agg_map).reset_index()


def drop_duplicates_from(from_df, in_df):
    if in_df.empty:
        return from_df
    return from_df[~from_df["hash"].isin(in_df["hash"])]


def get_hash(data, short=False):
    if short:
        return sha256(data).hexdigest()[0:8]
    return sha256(data).hexdigest()


def get_shorted_hash(hash_hex):
    return hash_hex[0:8]


def get_answer_hash(answer):
    # 获取原始链接
    answer_link = re.search(r"\[..链接\]\(.*?\)", answer).group()
    # 去掉md中所有url
    temp = re.sub(r"!?\[.*?\]\(.*?\)", "", answer)
    # 去掉顶级标题栏，防止问题变更导致答案重复
    temp = re.sub(r"# .*\n", "", temp)
    # 去掉所有链接和标题栏后重新拼接原始链接
    # 为什么不能去掉所有url：因为要保留原始链接做uuid，否则会丢内容
    temp = answer_link + temp
    # 去掉所有不重要的额外符号
    temp = re.sub(r"[\u200b\u3000\u00A0\s\t\n\r]", "", temp)
    # 4. 移除 Markdown 装饰符 (防止加粗/斜体符号变动影响 Hash)
    temp = re.sub(r"[\*\_\~\`]", "", temp)
    # 强制小写，消除英文字母大小写差异
    temp = temp.lower()
    return get_hash(temp.encode("utf-8"))
