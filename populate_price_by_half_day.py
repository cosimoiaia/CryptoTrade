# -*- coding: utf-8 -*-

import logging
from traceback import format_exc
from typing import cast
from multiprocessing import Pool
from pathlib import Path

import pandas as pd
import talib
from pytz import UTC

from config import (
    end_date,
    interval_mins,
    logging,
    price_by_half_day_path,
    price_by_half_day_pool_size,
    price_by_minutes_path,
    start_date_4_populate as start_date,
    timeperiod_list,
)


def load_and_process_csv(file_path: Path) -> pd.DataFrame:
    logging.info("Loading data...")
    data = cast(pd.DataFrame, pd.read_csv(file_path))
    if "Unnamed: 0" in data.columns:
        data = data.drop("Unnamed: 0", axis=1)
    data = data[data["Ignore"] == 0]  # type: ignore
    data = data.drop(columns=["Ignore", "Close time"])  # type: ignore
    #  This format assumes that the date format in the CSV files will always match this pattern.
    #  If there's any variation in the date format in the CSV files, this could lead to parsing errors.
    data["Open time"] = pd.to_datetime(  # type: ignore
        data["Open time"], format="%Y/%m/%d %H:%M"  # type: ignore
    ).dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )  # type: ignore
    return cast(pd.DataFrame, data)


def fill_missing_values(data: pd.DataFrame):
    data["Volume"] = data["Volume"].fillna(0)  # type: ignore
    data["Quote asset volume"] = data["Quote asset volume"].fillna(0)  # type: ignore
    data["Number of trades"] = data["Number of trades"].fillna(0)  # type: ignore
    data["Taker buy base asset volume"] = data["Taker buy base asset volume"].fillna(0)  # type: ignore
    data["Taker buy quote asset volume"] = data["Taker buy quote asset volume"].fillna(  # type: ignore
        0
    )
    data["Close"] = data["Close"].ffill()  # type: ignore
    data["Open"] = data["Open"].fillna(data["Close"])  # type: ignore
    data["High"] = data["High"].fillna(data["Close"])  # type: ignore
    data["Low"] = data["Low"].fillna(data["Close"])  # type: ignore
    data = data.dropna(how="any", axis=0)  # type: ignore
    return data


def calculate_indicators(data: pd.DataFrame):
    """Feat"""
    # Note: open and volume are not used
    # keeping them for legacy code tracking
    # open = data["Open"]
    close = data["Close"]
    high = data["High"]
    low = data["Low"]
    # volume = data["Volume"]

    data["CPG24"] = (close - close.shift(24)) / (close.shift(24))  # type: ignore
    data["CPG48"] = (close - close.shift(48)) / (close.shift(48))  # type: ignore
    data["CPG72"] = (close - close.shift(72)) / (close.shift(72))  # type: ignore
    data["CPG96"] = (close - close.shift(96)) / (close.shift(96))  # type: ignore
    data["CPG1"] = (close - close.shift(1)) / (close.shift(1))  # type: ignore
    data["APO"] = talib.APO(close)  # type: ignore
    data["CCI_30"] = talib.CCI(high, low, close, timeperiod=30)  # type: ignore

    for tp in timeperiod_list:
        data[f"CMO_{tp}"] = talib.CMO(close, timeperiod=tp)  # type: ignore
    for tp in timeperiod_list:
        data[f"DEMA_{tp}"] = talib.DEMA(close, timeperiod=tp)  # type: ignore
    for tp in timeperiod_list:
        data[f"EMA_{tp}"] = talib.EMA(close, timeperiod=tp)  # type: ignore

    data["HT_TRENDLINE"] = talib.HT_TRENDLINE(close)  # type: ignore

    data["MACD"], data["MACDSIGNAL"], data["MACDHIST"] = talib.MACD(close)  # type: ignore
    data["MACDEXT"], data["MACDEXTSIGNAL"], data["MACDEXTHIST"] = talib.MACDEXT(close)  # type: ignore
    data["MACDFIX"], _, data["MACDFIXHIST"] = talib.MACDFIX(close)  # type: ignore

    data["MIDPOINT_20"] = talib.MIDPOINT(close, timeperiod=20)  # type: ignore
    data["MIDPRICE_20"] = talib.MIDPRICE(high, low, timeperiod=20)  # type: ignore
    data["MINUS_DI_20"] = talib.MINUS_DI(high, low, close, timeperiod=20)  # type: ignore

    for tp in timeperiod_list:
        data[f"MOM_{tp}"] = talib.MOM(close, timeperiod=tp)  # type: ignore

    data["PLUS_DI_10"] = talib.PLUS_DI(high, low, close, timeperiod=10)  # type: ignore
    data["PLUS_DI_30"] = talib.PLUS_DI(high, low, close, timeperiod=30)  # type: ignore
    data["PPO"] = talib.PPO(close)  # type: ignore

    for tp in timeperiod_list:
        data[f"ROCP_{tp}"] = talib.ROCP(close, timeperiod=tp)  # type: ignore
    for tp in timeperiod_list:
        data[f"ROCR_{tp}"] = talib.ROCR(close, timeperiod=tp)  # type: ignore
    for tp in timeperiod_list:
        data[f"ROCR100_{tp}"] = talib.ROCR100(close, timeperiod=tp)  # type: ignore
    for tp in timeperiod_list:
        data[f"ROC_{tp}"] = talib.ROC(close, timeperiod=tp)  # type: ignore
    for tp in timeperiod_list:
        data[f"RSI_{tp}"] = talib.RSI(close, timeperiod=tp)  # type: ignore

    data["T3_20"] = talib.T3(close, timeperiod=20)  # type: ignore

    for tp in timeperiod_list:
        data[f"TRIMA_{tp}"] = talib.TRIMA(close, timeperiod=tp)  # type: ignore

    data["TRIX_10"] = talib.TRIX(close, timeperiod=10)  # type: ignore
    data["TRIX_20"] = talib.TRIX(close, timeperiod=20)  # type: ignore

    for tp in timeperiod_list:
        data[f"WMA_{tp}"] = talib.WMA(close, timeperiod=tp)  # type: ignore

    data["ULTOSC_20"] = talib.ULTOSC(  # type: ignore
        high, low, close, timeperiod1=20, timeperiod2=20 * 2, timeperiod3=20 * 4
    )
    data["ULTOSC_30"] = talib.ULTOSC(  # type: ignore
        high, low, close, timeperiod1=30, timeperiod2=30 * 2, timeperiod3=30 * 4
    )

    _, data["middleband_SMA_10"], _ = talib.BBANDS(  # type: ignore
        close, timeperiod=10, nbdevup=2, nbdevdn=2, matype=0
    )
    _, data["middleband_SMA_20"], _ = talib.BBANDS(  # type: ignore
        close, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0
    )
    data["lowerband_SMA_30"], _, _ = talib.BBANDS(  # type: ignore
        close, timeperiod=30, nbdevup=2, nbdevdn=2, matype=0
    )

    return data


def populate_by_currency_pair(raw_file: Path):
    try:
        logging.debug(f"Processing: {raw_file}")
        data = load_and_process_csv(raw_file)

        # Convert all dates in the df in dt format
        data["Open time"] = pd.to_datetime(data["Open time"], format="%Y/%m/%d %H:%M")  # type: ignore
        # data["Open time"] = data["Open time"].dt.tz_localize(None)

        logging.debug(data.iloc[0]["Open time"])
        file_start_date = data.iloc[0]["Open time"].replace(tzinfo=UTC)
        file_end_date = data.iloc[-1]["Open time"].replace(tzinfo=UTC)
        logging.debug(f"start_date: {file_start_date}")
        logging.debug(f"end_date: {file_end_date}")
        if file_start_date < start_date or file_end_date > end_date:
            logging.debug(
                f"Conditions not met for {file_start_date} > {start_date} "
                f"or {file_end_date} < {end_date}"
            )
            logging.info(f"date out of range, skipping file: {raw_file}...")
            return
        # Create the full time given the start date and end date
        timestamp = pd.date_range(
            start=start_date, end=end_date, freq=interval_mins, tz="UTC"
        )
        logging.info(f"Timeframe: {timestamp}")

        new_data = pd.DataFrame({"Open time": timestamp})
        new_data["Open time"] = new_data["Open time"].dt.tz_localize(None)  # type: ignore
        new_data = new_data.merge(data, on="Open time", how="left")
        new_data = fill_missing_values(new_data)
        new_data = calculate_indicators(new_data)

        save_file_path = price_by_half_day_path.joinpath(f"{interval_mins}_{raw_file.name}")
        logging.debug(f"Saving results to: {save_file_path}")
        new_data.to_csv(save_file_path, index=False)
        logging.info(f"Finished file: {raw_file}")
    except:
        logging.error(f"Error {raw_file}")
        logging.error(format_exc())


if __name__ == "__main__":
    with Pool(price_by_half_day_pool_size) as p:
        p.map(populate_by_currency_pair, price_by_minutes_path.iterdir())
