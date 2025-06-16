import datetime

import pandas as pd


class Agent:

    def __init__(self, data):
        self.data = data
        self.result = []

    def transform(self):
        for (name, ric), data in self.data.items():
            self.result.append(self.construct_record(name, ric, data))

        self.df = pd.DataFrame(self.result)
        self.df["timestamp_created_utc"] = datetime.datetime.utcnow()
        return self.df

    @staticmethod
    def construct_record(name, ric, data):
        record = {
            "name": name,
            "ext8_ticker": ric,
            "ticker": ric.split(".")[0],
            "exchange": ric.split(".")[-1] if "." in ric else None,
        }

        for k, v in data.items():
            for k2, v2 in v.items():
                if k == "industryComparison":
                    record[k2.lower()] = v2
                elif k == "esgScore":
                    keyname = k2.lower().replace("tr.", "")
                    if keyname != "tresg" and "tresg" in keyname:
                        keyname = keyname.replace("tresg", "")
                    if "pillar" in keyname:
                        keyname = keyname.replace("pillar", "")

                    keyname = f"esg_{keyname}"
                    record[keyname] = v2["score"] if "score" in v2 else 0
                    record[f"{keyname}_weight"] = v2["weight"] if "weight" in v2 else 0

        return record
