from pathlib import Path

from config import (
    price_by_half_day_path,
    price_by_minutes_path,
    training_models_path,
    training_output_path,
    result_prices_path,
    max_price_file,
    open_price_file,
)


def clean_directory(directory: Path):
    for file in directory.iterdir():
        file.unlink()


directories_for_clean = (
    # price_by_minutes_path,
    # price_by_half_day_path,
    # training_models_path,
    # training_output_path,
    result_prices_path,
)
for dir in directories_for_clean:
    clean_directory(dir)
