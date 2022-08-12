import asyncio
import asyncpg
import aiohttp
import time
import datetime

LIMIT = 20  # 20 reqs per sec


def get_symbols():
    with open('bitget-symbols.txt') as f:
        symbols = f.readlines()
    return symbols


with open('db.txt') as f:
    db_info = f.readlines()


async def fetch_perp_volume(symbol, session):
    data = None
    end_time = datetime.datetime.utcnow()
    start_time = end_time - datetime.timedelta(hours=37)

    url = f'https://api.bitget.com/api/mix/v1/market/candles?symbol={symbol}&granularity=900&' \
          f'startTime={int(start_time.timestamp() * 1e3)}&endTime={int(end_time.timestamp() * 1e3)}'  # ms times

    async with session.get(url, ssl=False) as resp:
        if resp.status != 200:
            text = await resp.text()
            print(f'F : {resp.status}\n', text, resp.url)
        else:
            data = await resp.json()
            for candle in data:
                candle[0] = datetime.datetime.fromtimestamp(int(candle[0]) // 1e3, datetime.timezone.utc)
                candle[1] = symbol.replace('_UMCBL', '')
                candle[-1] = float(candle[-1])
                del candle[2:-1]  # only keep time, symbol and USD volume from 15 min candle
    return data


async def record_perp_volume(data, pool):
    async with pool.acquire() as conn:
        for ticker_data in data:
            await conn.executemany('''
                                    INSERT INTO bitget_perps (ts, symbol, volume) VALUES ($1, $2, $3) 
                                    ON CONFLICT DO NOTHING;
                                   ''', ticker_data)
        await conn.execute("""DELETE FROM bitget_perps WHERE ts < now() - '24 hours'::interval;""")


async def perp_volume():
    tasks = []
    symbols_batch = []
    pool = await asyncpg.create_pool(user=db_info[0].strip(), password=db_info[1].strip(),
                                     host=db_info[2].strip(), database=db_info[3].strip())
    async with aiohttp.ClientSession() as session:
        symbols = get_symbols()  # watchlist of symbols to retrieve market info

        for symbol in symbols:
            symbol += '_UMCBL'
            symbols_batch.append(symbol)
            if len(symbols_batch) >= LIMIT:  # submit requests
                for sym_batch in symbols_batch:
                    tasks.append(asyncio.create_task(fetch_perp_volume(sym_batch, session)))
                symbols_batch = []
                await asyncio.sleep(1.5)  # sleep for rate limit

        if symbols_batch:  # complete final batch if any
            for sym_batch in symbols_batch:
                tasks.append(asyncio.create_task(fetch_perp_volume(sym_batch, session)))

        responses = await asyncio.gather(*tasks, return_exceptions=True)

        await record_perp_volume(responses, pool)

        await pool.close()


async def fetch_spot_volume(symbol, session):
    data_ret = []
    url = f'https://api.bitget.com/api/spot/v1/market/candles?symbol={symbol}&period=15min'

    async with session.get(url, ssl=False) as resp:
        if resp.status != 200:
            text = await resp.text()
            print(f'{resp.status} : ', text, resp.url)
        else:
            data = await resp.json()
            for candle in data['data']:
                data_ret.append([datetime.datetime.fromtimestamp(int(candle['ts']) // 1e3, datetime.timezone.utc),
                                 symbol.replace('_SPBL', ''),
                                 float(candle['usdtVol'])])
    return data_ret


async def record_spot_volume(data, pool):
    async with pool.acquire() as conn:
        for ticker_data in data:
            if ticker_data:
                await conn.executemany('''
                                        INSERT INTO bitget_spot (ts, symbol, volume) VALUES ($1, $2, $3) 
                                        ON CONFLICT DO NOTHING;
                                       ''', ticker_data)
        await conn.execute("""DELETE FROM bitget_spot WHERE ts < now() - '24 hours'::interval;""")


async def spot_volume():
    tasks = []
    symbols_batch = []

    pool = await asyncpg.create_pool(user=db_info[0].strip(), password=db_info[1].strip(),
                                     host=db_info[2].strip(), database=db_info[3].strip())

    async with aiohttp.ClientSession() as session:
        symbols = get_symbols()

        for symbol in symbols:
            symbol += '_SPBL'
            symbols_batch.append(symbol)
            if len(symbols_batch) >= LIMIT:  # submit requests
                for sym_batch in symbols_batch:
                    tasks.append(asyncio.create_task(fetch_spot_volume(sym_batch, session)))
                symbols_batch = []
                await asyncio.sleep(1.5)  # sleep for rate limit

        if symbols_batch:  # complete final batch if any
            for sym_batch in symbols_batch:
                tasks.append(asyncio.create_task(fetch_spot_volume(sym_batch, session)))

        responses = await asyncio.gather(*tasks, return_exceptions=True)

        await record_spot_volume(responses, pool)

        await pool.close()


async def main():
    await perp_volume()
    await spot_volume()


def run():
    asyncio.run(main(), debug=True)


if __name__ == '__main__':
    start = time.monotonic()
    # asyncio.run(main(), debug=True)
    run()
    print('done in :', time.monotonic() - start)
