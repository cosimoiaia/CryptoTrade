import csv
import os
from datetime import datetime, timedelta
from multiprocessing import Pool
from typing import Generator
from time import sleep

import requests
from pydantic import BaseModel
from pytz import UTC

from _list_of_currency_pairs import currency_pairs
from config import (
    end_date,
    format_exc,
    logging,
    price_by_minutes_path,
    price_by_minutes_pool_size,
    start_date_4_populate as start_date,
)

binance_kline_url = "https://api.binance.com/api/v3/klines"


class KLine(BaseModel):
    open_time: int
    open_price: str
    high_price: str
    low_price: str
    close_price: str
    volume: str
    close_time: int
    quote_volume: str
    trades: int
    taker_base_volume: str
    taker_quote_volume: str
    unused: str

    @classmethod
    def from_raw_data(cls, data):
        return KLine(
            open_time=data[0],
            open_price=data[1],
            high_price=data[2],
            low_price=data[3],
            close_price=data[4],
            volume=data[5],
            close_time=data[6],
            quote_volume=data[7],
            trades=data[8],
            taker_base_volume=data[9],
            taker_quote_volume=data[10],
            unused=data[11],
        )


class BinanceKLine:
    _binance_max_page_size = 1000

    def __init__(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime = datetime.utcnow(),
        interval: timedelta = timedelta(minutes=1),
    ):
        self.symbol = symbol
        self.interval = interval
        self.start_date = start_date
        self.end_date = end_date
        self.interval_as_seconds = int(self.interval.total_seconds())
        self.start_date_as_timestamp = int(self.start_date.timestamp())
        self.end_date_as_timestamp = int(self.end_date.timestamp())
        self.items_count = int(
            (self.end_date - self.start_date).total_seconds()
            / self.interval.total_seconds()
        )

    def generate_klines(self) -> Generator[KLine, None, None]:
        last_date = self.start_date_as_timestamp + self.interval_as_seconds
        i = 0
        while True:
            if i % 10 == 0:
                logging.info(f"{self.symbol} - Page {i}")
            binance_data = self._get_binance_page(
                self.symbol,
                last_date,
                self.interval_as_seconds,
            )
            i += 1
            for kline in binance_data:
                k = KLine.from_raw_data(kline)
                last_date = int(k.open_time / 1000)
                if last_date > self.end_date_as_timestamp:
                    return
                yield k

    def _get_binance_page(
        self,
        symbol: str,
        start_timestamp: int,
        interval_as_seconds: int,
    ):
        intervals = {
            1: "1s",
            60: "1m",
            120: "3m",
            300: "5m",
            900: "15m",
            1800: "30m",
            3600: "1h",
            7200: "2h",
            14400: "4h",
            21600: "6h",
            28800: "8h",
            43200: "12h",
            86400: "1d",
            259200: "3d",
            604800: "1w",
            2592000: "1M",
        }
        params = {
            "symbol": symbol,
            "startTime": start_timestamp * 1000,
            "interval": intervals[interval_as_seconds],
            "limit": self._binance_max_page_size,
        }
        for sleep_time in (1, 2, 3, 5, 8, 13, 21, 34, 55, 89):
            try:
                response = requests.get(binance_kline_url, params=params)
                break
            except:
                sleep(sleep_time)
        else:
            raise Exception("All waits is do nothing")
        if response.status_code != 200:
            logging.error(
                f"Binance sent not successed status({response.status_code}) whit message {response.text}"
            )
            raise Exception(response.text)
        data = response.json()
        return data


def xreadlines_reverse(f, blksz=524288):
    f.seek(0, os.SEEK_END)
    size = f.tell()
    line = ""
    for i in range(size // blksz, -1, -1):
        f.seek(blksz * i)
        block = f.read(blksz)
        lines = block.split("\n")
        if len(lines) > 1:
            for i in lines[1:][::-1]:
                if i:
                    yield i + line
                    line = ""
        line = lines[0] + line


def populate_for_pair(currency_pair: str):
    try:
        file = price_by_minutes_path.joinpath(f"{currency_pair}.csv")
        logging.info(
            f"{currency_pair} | Let's start processing the {file.name} file..."
        )
        if not file.exists():
            with open(file, "w") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [
                        "",
                        "Open time",
                        "Open",
                        "High",
                        "Low",
                        "Close",
                        "Volume",
                        "Close time",
                        "Quote asset volume",
                        "Number of trades",
                        "Taker buy base asset volume",
                        "Taker buy quote asset volume",
                        "Ignore",
                    ]
                )
            last_line_num = 0
            open_time = start_date
        else:
            with open(file, "r") as f:
                last_line = next(xreadlines_reverse(f, blksz=4096), None)
            if last_line is not None:
                last_line_num, open_time, *_ = last_line.split(",")
                open_time = datetime.strptime(open_time, "%Y-%m-%d %H:%M:%S").replace(
                    tzinfo=UTC
                )
                open_time += timedelta(minutes=1)
            else:
                last_line_num = 0
                open_time = start_date

        logging.debug(
            f"{currency_pair} | The latest data in this file is for {open_time}"
        )
        generator = BinanceKLine(currency_pair, open_time, end_date).generate_klines()
        line_num = int(last_line_num)
        with open(file, "a") as f:
            writer = csv.writer(f)
            logging.debug(f"{currency_pair} | Write to the end file...")
            for kline in generator:
                open_time = datetime.utcfromtimestamp(kline.open_time / 1000)
                close_time = datetime.utcfromtimestamp(kline.close_time / 1000)
                line_num += 1
                writer.writerow(
                    [
                        line_num,
                        open_time.strftime("%Y-%m-%d %H:%M:%S"),
                        kline.open_price,
                        kline.high_price,
                        kline.low_price,
                        kline.close_price,
                        kline.volume,
                        close_time.strftime("%Y-%m-%d %H:%M:%S"),
                        kline.quote_volume,
                        kline.trades,
                        kline.taker_base_volume,
                        kline.taker_quote_volume,
                        kline.unused,
                    ]
                )
        logging.info(f"{currency_pair} | Complete processing file {file.name}")
    except:
        print(format_exc())


if __name__ == "__main__":
    with Pool(price_by_minutes_pool_size) as p:
        p.map(populate_for_pair, currency_pairs)
