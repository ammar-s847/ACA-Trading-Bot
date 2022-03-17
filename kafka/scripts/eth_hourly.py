import requests, json

def eth_hourly(api_key: str) -> dict:
    url = f"https://www.alphavantage.co/query?function=CRYPTO_INTRADAY&symbol=ETH&market=USD&interval=60min&outputsize=full&apikey={api_key}"

    res = requests.get(url).json()
    ts_list = res['Time Series Crypto (60min)']

    hour_data = ts_list[list(ts_list)[1]]
    hour_data_new = dict()
    for i in hour_data.keys():
        hour_data_new[i[3:]] = hour_data[i]
    hour_data = hour_data_new
    hour_data['datetime'] = list(ts_list)[1]
    return hour_data
    
def eth_hourly_full(api_key: str) -> list:
    url = f"https://www.alphavantage.co/query?function=CRYPTO_INTRADAY&symbol=ETH&market=USD&interval=60min&outputsize=full&apikey={api_key}"

eth_hourly('68ZFL562SH8P1EZU')