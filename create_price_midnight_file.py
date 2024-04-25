import csv
import os
from datetime import datetime, timedelta
from math import ceil
from pathlib import Path
from time import sleep
from typing import Generator

import requests
from pydantic import BaseModel
from config import open_price_file, start_date, end_date, logging
from _list_of_currency_pairs import currency_pairs

binance_kline_url = "https://api.binance.com/api/v3/klines"


class KLine(BaseModel):
    open_time: datetime
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
            open_time=datetime.utcfromtimestamp(data[0] / 1000),
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

        self.__last_time_req = datetime.fromtimestamp(0)

    def generate_klines(self) -> Generator[KLine, None, None]:
        for i in range(ceil(self.items_count / self._binance_max_page_size)):
            start_date = self.start_date_as_timestamp + (
                self.interval_as_seconds * (self._binance_max_page_size * i)
            )
            binance_data = self._get_binance_page(
                self.symbol,
                start_date,
                self.interval_as_seconds,
            )
            for kline in binance_data:
                yield KLine.from_raw_data(kline)

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
        delta = datetime.utcnow() - self.__last_time_req
        if delta < timedelta(seconds=1):
            sleep(delta.total_seconds())
        response = requests.get(binance_kline_url, params=params)
        self.__last_time_req = datetime.utcnow()
        if response.status_code != 200:
            raise Exception()
        data = response.json()
        return data


def main():

    iterator = 0
    delta = timedelta(days=1)
    with open(open_price_file, "a") as f:
        for symbol in currency_pairs:
            generator = BinanceKLine(
                symbol, start_date, end_date, interval=delta
            ).generate_klines()
            for kline in generator:
                f.write(
                    f"{iterator},{kline.open_price},{symbol},{kline.open_time.date().strftime('%Y-%m-%d')}\n"
                )
                iterator += 1
                if iterator % 1000 == 0:
                    logging.info(f"iterator={iterator}")


if __name__ == "__main__":
    main()
