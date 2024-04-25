from traceback import format_exc
import pandas as pd
from typing import cast, Tuple, List

from pathlib import Path
import numpy as np
from datetime import datetime
from sklearn.preprocessing import StandardScaler
from dateutil.relativedelta import relativedelta
import datetime
from tsai.all import (
    set_seed,
    flatten_check,
    torch,
    combine_split_data,
    TSStandardize,
    TSForecaster,
    TSTPlus,
    mse,
    mae,
    matplotlib,
)

import datetime
import pandas as pd
from sklearn.preprocessing import StandardScaler
from datetime import datetime, timedelta
import logging

from config import (
    price_by_half_day_path,
    seed,
    target_column,
    X_period,
    mins,
    minimum_length_of_days_for_validation_testing,
    num_layers,
    feedforward,
    dropout,
    epochs,
    start_date,
    end_date,
    x_columns,
    period,
    training_models_path,
    training_output_path,
)


matplotlib.pyplot.set_loglevel(level="warning")  # type: ignore
# Model parameters
set_seed(seed, True)


def run_train_for_period():
    work_date = start_date.replace(tzinfo=None)
    while work_date <= end_date.replace(tzinfo=None):
        try:
            run_train_for_date(work_date)
        except Exception as e:
            logging.error(format_exc())
            raise e
        work_date += timedelta(days=1)


def run_train_for_date(work_date: datetime):

    logging.info(f"Loading training data from: {price_by_half_day_path}")

    x_train = np.array([])
    y_train = np.array([])
    x_valid = np.array([])
    y_valid = np.array([])
    # Save the separated couples to calculate smape after predictions
    xy_valid = []

    for file in price_by_half_day_path.iterdir():
        logging.debug(f"Processing: {file}")
        x_train_s, y_train_s, x_valid_s, y_valid_s, x_test_s = prepare_training_data(
            file, work_date
        )
        xy_valid.append(
            {
                "name": file.name,
                "x_valid": x_valid_s,
                "y_valid": y_valid_s,
                "x_test": x_test_s,
            }
        )
        if len(x_train) == 0:
            x_train = x_train_s
            y_train = y_train_s
            x_valid = x_valid_s
            y_valid = y_valid_s
        else:
            x_train = np.concatenate([x_train_s, x_train])
            y_train = np.concatenate([y_train_s, y_train])
            x_valid = np.concatenate([x_valid_s, x_valid])
            y_valid = np.concatenate([y_valid_s, y_valid])
    X, y, splits = combine_split_data([x_train, x_valid], [y_train, y_valid])
    # Now we can train the model
    logging.info("Running training...")
    batch_tfms = TSStandardize(by_sample=True, by_var=True)
    fcst = TSForecaster(
        X,
        y,
        splits=splits,
        batch_tfms=batch_tfms,
        bs=128,
        arch=TSTPlus,
        metrics=[mse, mae, smape],
        # device='cuda',
        arch_config={
            "dropout": dropout,
            "fc_dropout": 0.8,
            "d_model": 16,
            "n_layers": num_layers,
            "d_ff": feedforward,
        },
    )
    lr_max = fcst.lr_find()
    logging.info(f"lf_max = {lr_max.valley}")

    # run the training for #epochs
    fcst.fit_one_cycle(epochs, lr_max.valley)
    a, _, _ = fcst.get_X_preds(x_valid)
    cal = smape(a, torch.tensor(y_valid))
    cal = cal.numpy()

    # Training done. Save the model for later use
    model_path = training_models_path.joinpath(f"{work_date.strftime('%Y-%m-%d')}.pkl")
    logging.info(f"Saving the model in {model_path}")
    fcst.export(model_path)

    smape_pair = pd.DataFrame()
    smape_pair_test = pd.DataFrame()
    # calculate the smape against the validation set
    logging.info("Calculating SMAPE...")

    for pair in xy_valid:
        a, _, _ = fcst.get_X_preds(pair["x_valid"])
        cal = smape(a, torch.tensor(pair["y_valid"]))
        cal = cal.numpy()
        da = {
            "pair": pair["name"],
            "smape": cal,
            "end_test_date": work_date,
        }
        smape_pair = smape_pair.append(da, ignore_index=True)

        a, _, _ = fcst.get_X_preds(pair["x_test"])
        da = {
            "pair": pair["name"],
            "smape": a.numpy(),
            "actual_date": work_date,
        }
        smape_pair_test = smape_pair_test.append(da, ignore_index=True)

    logging.info("Saving csv...")
    date_string = work_date.strftime("%Y-%m-%d")
    # smape_pair.to_csv(
    #     training_output_path.joinpath(f"{date_string}_smape_result-validation.csv")
    # )
    smape_pair_test.to_csv(
        training_output_path.joinpath(f"{date_string}_smape_result-test.csv")
    )
    # smape_pair.to_excel(
    #     training_output_path.joinpath(f"{date_string}_predict_result-validation.xlsx")
    # )
    smape_pair_test.to_excel(
        training_output_path.joinpath(f"{date_string}_predict_result-test.xlsx")
    )


def prepare_training_data(filename: Path, work_date: datetime):
    """This function will load the processes data
    create the training, test and validation set
    """
    train_period = relativedelta(months=2)
    # Prepare dates
    end_validation_date = work_date - timedelta(days=1)
    start_validation_date = end_validation_date - timedelta(
        days=minimum_length_of_days_for_validation_testing
    )

    start_train_date = start_validation_date - train_period
    end_train_date = start_validation_date - timedelta(days=1)

    start_test_date = work_date - timedelta(
        days=minimum_length_of_days_for_validation_testing
    )
    end_test_date = work_date
    # Load the data from csv directly here
    data = pd.read_csv(filename, index_col="Open time", parse_dates=True)
    data = cast(pd.DataFrame, data)

    # select table with some features
    temp_table = cast(pd.DataFrame, data[x_columns]).sort_values("Open time")
    temp_table = cast(pd.DataFrame, temp_table)
    cols = temp_table.columns.drop(target_column)

    # normalization
    index = temp_table.index[
        (temp_table.index >= start_train_date) & (temp_table.index <= end_train_date)
    ]
    scaler = StandardScaler().fit(temp_table.loc[index, cols])

    # Applied scale and saved into data warehouse
    temp_table[cols] = scaler.transform(temp_table[cols])
    # transfering data into time series input and output
    input_cols = [c for c in temp_table.columns]
    # Isolates the indexes for the training timeframe
    ## Train frames
    x_train, y_train = get_xy_frames(
        temp_table, input_cols, start_train_date, end_train_date
    )
    ## Validate frames
    x_valid, y_valid = get_xy_frames(
        temp_table, input_cols, start_validation_date, end_validation_date, k=6
    )
    ## Test frames
    # Isolates the indexed for the testing/predictions timeframe
    x_test, y_test = get_xy_frames(
        temp_table, input_cols, start_test_date, end_test_date
    )
    return (
        np.array(x_train),
        np.array(y_train),
        np.array(x_valid),
        np.array(y_valid),
        np.array(x_test),
    )


def get_xy_frames(
    df: pd.DataFrame,
    input_cols: List[str],
    start_date: datetime,
    end_date: datetime,
    k: int = 0,
) -> Tuple[List, List]:
    x_data = []
    y_data = []
    delta = timedelta(minutes=(period - 1) * mins)
    minimum_index = df.index.get_loc(start_date)
    maximum_index = df.index.get_loc(end_date - delta)
    if type(minimum_index) == slice:
        minimum_index = minimum_index.stop
    if type(maximum_index) == slice:
        maximum_index = maximum_index.start
    for i in range(minimum_index, maximum_index + 1):
        x_frame = df.iloc[i - k : i + X_period - k]
        y_frame = df.iloc[i + X_period : i + period]
        x_data.append(x_frame[input_cols].values.T)
        y_data.append(y_frame[target_column].values)
    return x_data, y_data


def smape(inp, targ):
    """Mean absolute error between `inp` and `targ`."""

    inp, targ = flatten_check(inp, targ)
    return torch.mean(torch.abs(inp - targ) / (torch.abs(inp) + torch.abs(targ)))


if __name__ == "__main__":
    ## For today
    now = datetime.now()
    today = datetime.combine(now.date(), datetime.min.time())

    run_train_for_date(today)
    ## Or for period
    # # now = datetime(year=2023, month=1, day=1)
    # run_train_for_period()
