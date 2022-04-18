import json
import os
import pprint
from pprint import pprint
from re import M
from black import List
import praw
import pyspark
import yaml
from dataclasses import dataclass, field
from typing import Dict, Tuple, List
import numpy as np
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql import SparkSession
import os


os.environ["SPARK_HOME"] = "/home/shivamanand/spark-3.2.1-bin-hadoop2.7/"
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"


class Config:
    def __init__(self):
        self.praw_config = yaml.safe_load(open("config.yaml"))["praw"]

    def load_praw(self):
        return praw.Reddit(
            client_id=self.praw_config["client_id"],
            client_secret=self.praw_config["client_secret"],
            user_agent=self.praw_config["user_agent"],
        )


@dataclass(frozen=True)
class RawAttributesToExtract:

    post: list = field(
        default_factory=lambda: [
            "title",
            "upvote_ratio",
            "score",
            "author",
            "mod_note",
            "over_18",
            "distinguished",
            "removal_reason",
            "report_reasons",
            "num_comments",
            "created_utc",
            "stickied",
            "url",
        ]
    )

    comments: list = field(default_factory=lambda: ["list1", "list2", "list3"])

    users: list = field(default_factory=lambda: ["list1", "list2", "list3"])


class UkrainePosts:

    """returns Ukraine posts data
    as a list of dicts
    """

    def __init__(self):

        self._subreddits_search_string_dict = {
            "news": ["ukraine"],
            "worldnews": ["WorldNews Live Thread"],
            "volunteersforukraine": ["ukraine"],
            "politics": ["ukraine"],
            "ukraine": [" "],
        }

        self._post_data = []

    def get_subreddit_post_data(
        self, subreddit_name: str, search_strings: str, limit: int
    ):
        """Extracts specfic subreddit's posts,  appends into _data list

        Args:
            subreddit_name (str): r/???
            search_strings (str): list of sub strings extracted post can have
            limit (int): number of posts to search in hot
        """
        subreddit = r.subreddit(subreddit_name)

        for post in subreddit.hot(limit=limit):

            if any(search_str in post.title for search_str in search_strings):

                datum = {
                    attribute: getattr(post, attribute)
                    for attribute in RawAttributesToExtract().post
                }

                self._post_data.append(datum)

    def get_data_for_all_subreddits(self):

        for sub in self._subreddits_search_string_dict:
            self.get_subreddit_post_data(
                sub, self._subreddits_search_string_dict[sub], limit=1000
            )

        return self._post_data


spark = SparkSession.builder.getOrCreate()


SparkSession.builder.appName("a").getOrCreate()

spark = SparkSession.builder.appName("Ukraine").getOrCreate()

r = Config().load_praw()

posts_data = UkrainePosts().get_data_for_all_subreddits()

schema = StructType(
    [StructField(col, StringType(), True) for col in posts_data[0].keys()]
)

posts_df = spark.createDataFrame([Row(**x) for x in posts_data], schema=schema)

posts_df

# posts_df.show()


# raw_attributes_to_extract.Post

# {attribute: getattr(post, attribute) for attribute in raw_attributes_to_extract.Post}


# def create_spark_dataframe(pandas_data_frame):
#    """
#    will return the spark dataframe input pandas dataframe
#    """
#    for col in pandas_data_frame.columns:
#         if ((pandas_data_frame[col].dtypes != np.int64) & (pandas_data_frame[col].dtypes != np.float64)):
#             pandas_data_frame[col] = pandas_data_frame[col].fillna('')

#    spark_data_frame = spark.createDataFrame(data=pandas_data_frame)
#    return spark_data_frame

# create_spark_dataframe(df)
# # ukraine_


# doc=r.submission(id=latest_id)

# {attribute: getattr(post, attribute)
# for attribute in dir(post)}


# datum ={'':doc.get(''),
#         '':doc.get(''),


# pprint(locals(reddit.submission(id=latest_id))

# vars(r.submission(id='latest_id'))#.num_comments


# for comment in r.submission(id='latest_id').comments:
#     print(comment)
#     Comment_attributes=vars(comment)
#     break

# #submission.comment is different from comment?
# #comment has _replies which not in submission comment imo

# #i20wm1q

# vars(comment) #submission comment

# vars(r.comment(id='i20wm1q')) #comment


# vars(r.comment(id='i21e8qw')._reddit)

# print(Comment_attributes.keys())

# pprint(Comment_attributes[''])


# vars(Comment_attributes)


# with open(f'x.json', 'w') as fp:
#     json.dump(dict(Comment_attributes), fp)

# ##### reddit.submission(id=latest_id).__dict__.
# # {'comment_limit': 2048,

# # 'comment_sort': 'confidence',
# # 'id': 'tbuihd',
# # '_reddit': <praw.reddit.Reddit object at 0x7f693aaa14e0>,
# # '_fetched': False,
# # '_comments_by_id': {}}

# r.submission(id=latest_id).comments  # .__dict__

# for c in r.submission(id=latest_id).comments:
#     d = c
#     break

# d.author


# spark = SparkSession.builder.getOrCreate()

# data = [["1", "2"], ["3", "4"]]

# def main():
#     r = config().load_praw()

# main()
