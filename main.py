'''
Exchange A has released new version of API. We need to compare old version and new version and decide which one we should use to have better latency.
 
We've gathered same data using both API versions (instrument ABC_USD) on a single server simultaneously
recv_time - timestamp we got data on our server
 
What is the difference between two APIs?
    - Formatting of the returned JSONs are different
    - Server time response (not recv_time) in the new api is in ms instead of μs like the old api
    - The old API has varying price level updates for some reason vs the new one which has a fixed 15
    - The new API has aggregate data (tas, tbs) while the old API has total number of orders (assumption, see Questions)

Which API has better latency and why?
    - New API has better latency on average but it might not be accurate as the server time response decreased in precision compared to the old api
    - Honestly not enough data points to give an accurate representation

How can we compare both data samples?
    - Parse the jsons and tabulate them into dataframes for analysis 
    - Run backtests with each API and see which produces better results
'''

import os
import json
import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.width", 0)

'''
Questions:
    - Why are there zeros in the quantity sections of the data? In both the old and new api.
    - Why is the old one in microseconds but the new one in miliseconds?
    - What is the currency of the price? I assume its the price of the base asset in its quote asset.
    - If measured at the same time, how come the old api has lower amount of updates?
    - Why is an orderbook depth API not returning a fixed level amount?
    - Tied to the previous question, it looks like some orderbook updates are missing?
    - What is "total" in the old api return? I assume its total number of orders. Why are some of them 0?

My thoughts:
    - I suggest using the newer API if it's compatible with existing code since
      newer APIs tend to have more support and fixes, especially if its meant to be an improvement over the status quo.
      This helps ensure access to future updates, bug fixes, and improvements.
'''

# Parse into separate bids and asks dataframes
def parse_old_api_levels(file_path: str, bids: list, asks: list):
    with open(file_path, 'r') as f:
        for num, line in enumerate(f):
            data = json.loads(line)
            recv_time = data.get("recv_time")
            content = data.get("content", {})
            content_time = content.get("datetime")
            for entry in content.get("list", []):
                if entry["orderType"] == "bid":
                    bids.append({
                        "response_idx": num + 1,
                        "datetime": content_time,
                        "recv_time": recv_time,
                        "quantity": entry["quantity"],
                        "price": entry["price"],
                    })

                else:
                    asks.append({
                        "response_idx": num + 1,
                        "datetime": content_time,
                        "recv_time": recv_time,
                        "quantity": entry["quantity"],
                        "price": entry["price"],
                    })

# Parse into separate bids and asks dataframes
def parse_new_api_levels(file_path: str, bids: list, asks: list):
    with open(file_path, 'r') as f:
        for num, line in enumerate(f):
            data = json.loads(line)
            recv_time = data.get("recv_time")

            # Convert ms to us
            tms = data.get("tms") * 1_000

            for level in data["obu"]:
                bids.append({
                    "response_idx": num + 1,
                    "datetime": tms,
                    "recv_time": recv_time,
                    "price": level["bp"],
                    "quantity": level["bs"],
                })

                asks.append({
                    "response_idx": num + 1,
                    "datetime": tms,
                    "recv_time": recv_time,
                    "price": level["ap"],
                    "quantity": level["as"],
                })

def get_old_latency(file_path: str):
    latency: list = []
    exchange_time: list = []
    received_time: list = []

    with open(file_path, 'r') as f:
        for line in f:
            data = json.loads(line)
            recv_time = data.get("recv_time")
            content = data.get("content", {})
            content_time = content.get("datetime")
            
            exchange_timestamp = pd.to_datetime(int(content_time), unit="us")
            recv_timestamp = pd.to_datetime(recv_time)
            latency_at_timestamp = (recv_timestamp - exchange_timestamp) // pd.Timedelta(microseconds=1)

            latency.append(latency_at_timestamp)
            exchange_time.append(exchange_timestamp)
            received_time.append(recv_timestamp)

    return latency, exchange_time, received_time

def get_new_latency(file_path: str):
    latency: list = []
    exchange_time: list = []
    received_time: list = []

    with open(file_path, 'r') as f:
        for line in f:
            data = json.loads(line)
            recv_time = data.get("recv_time")
            content_time = data.get("tms")
            
            exchange_timestamp = pd.to_datetime(int(content_time), unit="ms")
            recv_timestamp = pd.to_datetime(recv_time)
            latency_at_timestamp = (recv_timestamp - exchange_timestamp) // pd.Timedelta(microseconds=1)

            latency.append(latency_at_timestamp)
            exchange_time.append(exchange_timestamp)
            received_time.append(recv_timestamp)

    return latency, exchange_time, received_time

def get_latency_dataframe(old_file_path: str, new_file_path: str):
    old_latency, old_exchange_time, old_received_time = get_old_latency(old_file_path)
    new_latency, new_exchange_time, new_received_time = get_new_latency(new_file_path)

    latency_df = pd.DataFrame({
        "old_exchange_time": old_exchange_time,
        "old_received_time": old_received_time,
        "old_latency_μs": old_latency,
        "new_exchange_time": new_exchange_time,
        "new_received_time": new_received_time,
        "new_latency_μs": new_latency
    })

    latency_df["latency_diff"] = latency_df["old_latency_μs"] - latency_df["new_latency_μs"]
    latency_df.index = pd.RangeIndex(start=1, stop=len(latency_df) + 1, name="response_idx")

    latency_df["old_exchange_time"] = latency_df["old_exchange_time"].dt.time
    latency_df["old_received_time"] = latency_df["old_received_time"].dt.time
    latency_df["new_exchange_time"] = latency_df["new_exchange_time"].dt.time
    latency_df["new_received_time"] = latency_df["new_received_time"].dt.time

    return latency_df

def get_latency_summary(latency_df: pd.DataFrame):
    summary_df = latency_df[["old_latency_μs", "new_latency_μs"]].agg(["mean", "std", "median", "sum"]).round(2)
    summary_df.index = ["Average", "Std Dev", "Median", "Sum"]
    return summary_df

def get_bids_and_asks(file_path: str, is_new: bool):
    bids: list = []
    asks: list = []
    if (not is_new):
        parse_old_api_levels("old_api", bids, asks)
    else:
        parse_new_api_levels("new_api", bids, asks)

    bids_df = pd.DataFrame(bids)
    asks_df = pd.DataFrame(asks)

    bids_df["quantity"] = bids_df["quantity"].astype(float)
    asks_df["quantity"] = asks_df["quantity"].astype(float)

    bids_df["price"] = bids_df["price"].astype(int)
    asks_df["price"] = asks_df["price"].astype(int)

    bids_df["datetime"] = pd.to_datetime(bids_df["datetime"].astype("int64"), unit="us")
    asks_df["datetime"] = pd.to_datetime(asks_df["datetime"].astype("int64"), unit="us")
    bids_df["recv_time"] = pd.to_datetime(bids_df["recv_time"])
    asks_df["recv_time"] = pd.to_datetime(asks_df["recv_time"])

    bids_df = bids_df.sort_values(by=['datetime', 'price'], ascending=[True, True])
    asks_df = asks_df.sort_values(by=['datetime', 'price'], ascending=[True, False])

    bids_df = bids_df.reset_index(drop=True)
    asks_df = asks_df.reset_index(drop=True)

    bids_df["level"] = bids_df.groupby("datetime").cumcount() + 1
    asks_df["level"] = asks_df.groupby("datetime").cumcount() + 1

    return bids_df, asks_df

def get_old_aggregate(old_df_bids: pd.DataFrame, old_df_asks: pd.DataFrame):
    bids_agg = old_df_bids.groupby("response_idx")["quantity"].sum().round(4).rename("total_bids")
    asks_agg = old_df_asks.groupby("response_idx")["quantity"].sum().round(4).rename("total_asks")

    agg_df = pd.concat([bids_agg, asks_agg], axis=1).fillna(0)
    agg_df = agg_df.sort_values("response_idx").reset_index()

    return agg_df

def get_new_aggregate(file_path: str):
    records: list = []
    with open(file_path, 'r') as f:
        for num, line in enumerate(f):
            data = json.loads(line)
            tbs = data.get("tbs")
            tas = data.get("tas")

            records.append({
                "response_idx": num + 1,
                "total_bids": tbs,
                "total_asks": tas,
            })

    agg_df = pd.DataFrame(records)
    agg_df["total_bids"] = agg_df["total_bids"].astype("float32")
    agg_df["total_asks"] = agg_df["total_asks"].astype("float32")

    return agg_df

def level_count(df: pd.DataFrame):
    count_df = df.groupby("response_idx").size().reset_index(name="num_levels")

    return count_df

def merge_dataframe(bids: pd.DataFrame, asks: pd.DataFrame):
    return pd.merge(bids, asks, on=["datetime", "level", "response_idx"], how="outer", suffixes=("_ask", "_bid"))

def get_level_count_dataframe(old_df: pd.DataFrame, new_df: pd.DataFrame):
    old_level_count = level_count(old_df) 
    new_level_count = level_count(new_df) 

    return old_level_count, new_level_count

def main():
    old_api_bids_df, old_api_asks_df = get_bids_and_asks("old_api", is_new=False)
    new_api_bids_df, new_api_asks_df = get_bids_and_asks("new_api", is_new=True)
    old_api_merged_df = merge_dataframe(old_api_bids_df, old_api_asks_df)
    new_api_merged_df = merge_dataframe(new_api_bids_df, new_api_asks_df)
    
    old_agg_df = get_old_aggregate(old_api_bids_df, old_api_asks_df)
    new_agg_df = get_new_aggregate("new_api")

    old_level_count, new_level_count = get_level_count_dataframe(old_api_merged_df, new_api_merged_df)

    latency_df = get_latency_dataframe("old_api", "new_api")
    exchange_time_df = latency_df[["old_exchange_time", "new_exchange_time"]]
    recv_time_df = latency_df[["old_received_time", "new_received_time"]]
    latency_summary = get_latency_summary(latency_df)

    if not os.path.exists("result"):
        os.makedirs("result")

    print("============= Old API Results =============\n")
    print("Asks:")
    print(old_api_asks_df.to_string(index=False))
    print("\n")
    old_api_asks_df.to_csv("result/old_asks.csv", index=False)

    print("Bids:")
    print(old_api_bids_df.to_string(index=False))
    print("\n")
    old_api_bids_df.to_csv("result/old_bids.csv", index=False)

    old_api_merged_df.to_csv("result/old_api_merged.csv", index=False)

    print("============= New API Results =============")
    print("Asks:")
    print(new_api_asks_df.to_string(index=False))
    print("\n")
    new_api_asks_df.to_csv("result/new_asks.csv", index=False)

    print("Bids:")
    print(new_api_bids_df.to_string(index=False))
    print("\n")
    new_api_bids_df.to_csv("result/new_bids.csv", index=False)

    new_api_merged_df.to_csv("result/new_api_merged.csv", index=False)

    print("============= API Count Results =============")
    print("Old API Levels:")
    print(old_level_count.to_string(index=False))
    print("\n")
    old_level_count.to_csv("result/old_level_count.csv", index=False)

    print("Old API Aggregate:")
    print(old_agg_df.to_string(index=False))
    print("\n")
    old_agg_df.to_csv("result/old_api_agg.csv", index=False)

    print("New API Levels:")
    print(new_level_count.to_string(index=False))
    print("\n")
    new_level_count.to_csv("result/new_level_count.csv", index=False)

    print("New API Aggregate:")
    print(new_agg_df.to_string(index=False))
    print("\n")
    new_agg_df.to_csv("result/new_api_agg.csv", index=False)

    print("============= Latency Metrics =============\n")
    print("Latencies:")
    print(latency_df.to_string())
    latency_df.to_csv("result/latency.csv")
    print("\n")

    # Old API has consistenly earlier exchange times and received times
    print("Exchange time:")
    print(exchange_time_df.to_string())
    exchange_time_df.to_csv("result/exchange_time.csv")
    print("\n")

    print("Received time:")
    print(recv_time_df.to_string())
    recv_time_df.to_csv("result/received_time.csv")
    print("\n")

    # Not necessarily correct since date time is now miliseconds instead (loss of precision)
    print("Latency Summary:")
    print(latency_summary.to_string())
    latency_summary.to_csv("result/latency_summary.csv")


if __name__ == "__main__":
    main()
