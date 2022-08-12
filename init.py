import requests


if __name__ == '__main__':
    # initialize watchlist based off available bitget perp usdt contracts
    bitget_perps = requests.get('https://api.bitget.com/api/mix/v1/market/contracts?productType=umcbl').json()
    symbols = []
    if bitget_perps['msg'] == 'success':
        for contract in bitget_perps['data']:
            symbols.append(contract['symbol'].replace('_UMCBL', ''))

        with open('bitget-symbols.txt', 'w') as file:
            file.write('\n'.join(symbols))
