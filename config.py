import logging
from datetime import datetime, timedelta
from pathlib import Path
from dateutil.relativedelta import relativedelta
from traceback import format_exc
from typing import Callable
from FTFeatures import SelectedFeatures

from pytz import UTC

# Logging
## Change this from DEBUG to INFO if you don't want debug msg in prod

logging.basicConfig(level=logging.INFO)

# Directories
BUCKET_ROOT = Path("./data")

price_by_minutes_path = BUCKET_ROOT.joinpath("0_by_minutes")
price_by_half_day_path = BUCKET_ROOT.joinpath("1_by_half_day")
training_models_path = BUCKET_ROOT.joinpath("2_training_models")
training_output_path = BUCKET_ROOT.joinpath("3_training_output")
result_prices_path = BUCKET_ROOT.joinpath("4_result_prices")
backtest_result_path = BUCKET_ROOT.joinpath("5_backtest_result")
visualization_path = BUCKET_ROOT.joinpath("6_visualization_path")
max_price_file = BUCKET_ROOT.joinpath("agg_max_price.csv")
open_price_file = BUCKET_ROOT.joinpath("agg_open_price.csv")

# Pool sizes
price_by_minutes_pool_size = 5
price_by_half_day_pool_size = 10

# Dates

start_date = datetime(year=2020, month=4, day=1, hour=0, minute=0, second=0).replace(
    tzinfo=UTC
)
end_date = datetime(year=2023, month=12, day=7, hour=0, minute=0, second=0).replace(
    tzinfo=UTC
)
# end_date = datetime.combine(
#     datetime.utcnow().replace(tzinfo=UTC), datetime.min.time(), tzinfo=UTC
# )

# Model's parameters

timeperiod_list = [10, 20, 30]
interval_mins = "720min"

num_layers = 2
feedforward = 128
dropout = 0.3
epochs = 25
seed = 77

# Features parameters (don't change this)
target_column = "CPG72"
X_period = 48  # window size for x
Y_period = 6  # steps for y
period = X_period + Y_period
mins = 720
days = 720 / 60 / 24 * Y_period
x_columns = SelectedFeatures + [target_column]


# Date parameters
minimum_length_of_days_for_validation_testing = (
    27  # eight days is the minimum days for validation
)

start_date_4_populate = (
    start_date
    - timedelta(days=1)
    - timedelta(days=minimum_length_of_days_for_validation_testing)
    - relativedelta(months=2)
    - timedelta(days=20)
)

# Others
def exceptor(func: Callable):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except:
            logging.error(format_exc())

    return wrapper
