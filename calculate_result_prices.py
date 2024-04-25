# -*- coding: utf-8 -*-
from datetime import timedelta
import os
from typing import cast

import pandas as pd
import numpy as np
import logging
from config import (
    training_output_path,
    result_prices_path,
    start_date,
    end_date,
    price_by_half_day_path,
    interval_mins,
)


def main():
    # Ensure the result folder exists.
    os.makedirs(result_prices_path, exist_ok=True)

    logging.info(f"Start date: {start_date}")
    logging.info(f"End date: {end_date}")

    smape_pair = pd.DataFrame()

    for i in range((end_date - start_date).days):
        date = (start_date + timedelta(days=i)).replace(tzinfo=None)

        training_output_file = training_output_path.joinpath(
            f"{date.strftime('%Y-%m-%d')}_smape_result-test.csv"
        )
        # the change in price is equal to (price_t-growth_period - prince_t)/(prince_t)
        growth_period = 72

        # load pair info (e.g., "token_number.token_name.csv")
        ## Example:
        #    0,720min_BTCUSDT.csv,"[[-0.31704575  0.06900746 -0.52229637 -0.24277563 -0.0310903   0.47418264]
        #    [-0.45837167 -0.21395266 -0.47799063  0.36277884 -0.08389079  0.41672185]]",2023-12-06
        smape_pair = cast(pd.DataFrame, pd.read_csv(training_output_file))

        final_result = pd.DataFrame()
        for i in range(0, len(smape_pair)):
            # Example
            pair_name = smape_pair.loc[i, "pair"].split(".")[0].split("_")[1]

            # get the predicted change in price for the next few days.
            # Convert the string to a NumPy array
            predicted_return = np.fromstring(
                smape_pair.loc[i, "smape"].split("]\n [")[0].strip("[["), sep=" "
            )

            # load the data file where you can see the past price and price on target date at 00:00:00
            target_file = cast(
                pd.DataFrame,
                pd.read_csv(
                    price_by_half_day_path.joinpath(f"{interval_mins}_{pair_name}.csv"),
                    index_col="Open time",
                    parse_dates=True,
                ),
            )
            # target_file = cast(pd.DataFrame, target_file)
            target_last_index = target_file.index.get_loc(date)
            # true price is the price on the target date at 00:00:00
            true_price_on_target_date_at_midnight = target_file.iloc[
                target_last_index
            ].Close
            # load the previous price so that we can use them to calculate the predicted price
            # along with the predicted change in price
            logging.info(f"Token name: {pair_name}")
            previous_price = target_file.iloc[
                target_last_index
                - growth_period : target_last_index
                + 6
                - growth_period
            ].Close.tolist()
            predicted_price = [
                l * m + l for l, m in zip(previous_price, predicted_return)
            ]

            # Continue on predicted price and actual price on target date at 00:00:00 for each token
            price_prd_max = max(predicted_price[1:6])

            # calculated maximum predicted return based on predicted prices
            return_pred = (price_prd_max - predicted_price[0]) / predicted_price[0]

            logging.info(f"Token Name: {pair_name} Prediction: {return_pred}")
            # sold price is the price you are going to sell the token at within the next few days
            sold_price = (
                true_price_on_target_date_at_midnight * return_pred
                + true_price_on_target_date_at_midnight
            )
            str_final = {
                "date": date,
                "token_name": pair_name,
                "price_to_be_sold": sold_price,
                "predicted_return": return_pred,
                "price_0": true_price_on_target_date_at_midnight,
            }
            final_result = pd.concat(
                [final_result, pd.DataFrame([str_final])], ignore_index=True
            )
        logging.info(f"Saving trading prices...")
        # final_result.to_excel(
        #     result_prices_path.joinpath(
        #         f"{date.strftime('%Y-%m-%d')}_trading_price.xlsx"
        #     )
        # )
        final_result.to_csv(
            result_prices_path.joinpath(
                f"{date.strftime('%Y-%m-%d')}_trading_price.csv"
            )
        )


if __name__ == "__main__":
    main()
