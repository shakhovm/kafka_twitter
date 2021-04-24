import pandas as pd
from datetime import datetime, timedelta


class Queries:
    def __init__(self, df: pd.DataFrame, current_date: datetime, n):
        self.df = df
        self.n = n
        self.current_date = current_date

    def show_all_accounts(self):
        return self.df["account"].unique()

    def number_of_tweets(self):
        statistics = []
        for i in range(3):
            statistics.append(self.df[(self.df["date"] <= self.current_date - timedelta(hours=i))
                              & (self.df["date"] >= self.current_date - timedelta(hours=i + 1))]["message"].count())
        return statistics

    def account_with_highest_number_of_tweets(self):
        return self.df[self.df["date"] >= self.current_date - timedelta(hours=3)]\
            .groupby(by='account').agg('count').sort_values(by=['date'], ascending=False)[:10].index.to_list()

    def most_producing(self):
        print(self.df["date"])
        return self.df[self.df["date"] >= self.current_date - timedelta(hours=self.n)]["account"]\
                   .value_counts()[:20].index.tolist()

    def popular_hashtags(self):

        hashtags = {}

        def add_to_dict(value):
            if value[0] == "@":
                if value not in hashtags:
                    hashtags[value] = 1
                else:
                    hashtags[value] += 1
        for message in self.df[self.df["date"] >= self.current_date - timedelta(hours=self.n)]["message"]:
            set(map(add_to_dict, message.split()))
        return list(map(lambda x: x[0], sorted(hashtags.items(), key=lambda x: x[1], reverse=True)))[:20]
        # return sorted(hashtags.items(), key=lambda x: x[1], reverse=True)

    def to_json(self):
        return {
            "task1": list(map(lambda x: x.strip('""'), self.show_all_accounts())),
            "task2": list(map(int, self.number_of_tweets())),
            "task3": list(map(lambda x: x.strip('""'), self.account_with_highest_number_of_tweets())),
            "task4": list(map(lambda x: x.strip('""'), self.most_producing())),
            "task5": self.popular_hashtags()
        }
