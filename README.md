# data_analysis

Exchange A has released new version of API. We need to compare old version and new version and decide which one we should use to have better latency.
 
We've gathered same data using both API versions (instrument ABC_USD) on a single server simultaneously (recv_time - timestamp) we got data on our server
 
What is the difference between two APIs?
 - Formatting of the returned JSONs are different
 - Server time response (not recv_time) in the new api is in ms instead of μs like the old api
 - The old API has varying price level updates for some reason vs the new one

Which API has better latency and why?
 - New API has better latency on average but it might not be accurate as the server time response decreased in precision compared to the old api
 - Honestly not enough data points to give an accurate representation

How can we compare both data samples?
 - Parse the jsons and tabulate them into dataframes for analysis 
 - Run backtests with each API and see which produces better results


Questions:
 - Why are there zeros in the quantity sections of the data? In both the old and new api.
 - Why is the old one in microseconds but the new one in miliseconds?
 - What is the currency of the price? I assume its the price of the base asset in its quote asset.
 - If measured at the same time, how come the old api has lower amount of updates?
 - Why is an orderbook depth API not returning a fixed level amount?
 - Tied to the previous question, it looks like some orderbook updates are missing?
 - What is total in the old api return? Is it total number of orders? Why are some of them 0?

My thoughts:
 - I suggest using the newer API if it's compatible with existing code since newer APIs tend to have more support and fixes, especially if its meant to be an improvement over the status quo. This helps ensure access to future updates, bug fixes, and improvements.
