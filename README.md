# CryptoTrade

A cryptocurrency trading system that implements backtesting and price prediction using machine learning models. The system is designed to analyze historical cryptocurrency data, make price predictions, and simulate trading strategies through backtesting.

## Project Structure

The project consists of several key components:

- **Data Collection and Processing**
  - `populate_price_by_minutes.py`: Collects price data at minute intervals
  - `populate_price_by_half_day.py`: Aggregates price data into half-day intervals
  - `_clean_data.py`: Data cleaning utilities
  - `_exchange.py`: Exchange interface and order management

- **Price Analysis and Prediction**
  - `training_model.py`: Machine learning model for price prediction
  - `FTFeatures.py`: Feature engineering for the prediction model
  - `calculate_result_prices.py`: Calculates predicted prices and returns
  - `create_max_price_file.py`: Creates aggregated maximum price data
  - `create_price_midnight_file.py`: Creates aggregated opening price data

- **Backtesting**
  - `run_backtesting.py`: Implements backtesting simulation
  - `visualization.ipynb`: Jupyter notebook for visualizing results

## Features

1. **Data Collection and Processing**
   - Collects historical cryptocurrency price data
   - Processes data at different time intervals (minutes and half-days)
   - Cleans and prepares data for analysis

2. **Price Prediction**
   - Implements a transformer-based deep learning model for price prediction using the TS-AI library
   - Uses technical indicators and features for prediction
   - Generates trading signals based on predicted price movements
   - Leverages time series analysis capabilities of TS-AI for accurate price forecasting

3. **Backtesting**
   - Simulates trading strategies using historical data
   - Tracks portfolio performance and balance
   - Calculates trading fees and transaction costs
   - Generates detailed performance reports

4. **Visualization**
   - Provides tools for visualizing trading results
   - Analyzes performance metrics and trading patterns

## Setup and Installation

1. **Prerequisites**
   - Python 3.8 or higher
   - Docker (optional, for containerized deployment)

2. **Installation**
   ```bash
   # Clone the repository
   git clone [repository-url]
   cd CryptoTrade

   # Create and activate virtual environment (recommended)
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate

   # Install dependencies
   pip install -r requirements.txt
   ```

3. **Configuration**
   - Update `config.py` with your desired settings:
     - Date ranges for analysis
     - Model parameters
     - Data directories
     - Trading parameters

## Usage

1. **Data Collection**
   ```bash
   # Collect minute-level price data
   python populate_price_by_minutes.py

   # Aggregate data into half-day intervals
   python populate_price_by_half_day.py
   ```

2. **Price Prediction**
   ```bash
   # Train the transformer-based prediction model
   python training_model.py

   # Calculate predicted prices
   python calculate_result_prices.py
   ```

3. **Backtesting**
   ```bash
   # Run backtesting simulation
   python run_backtesting.py
   ```

4. **Visualization**
   - Open and run `visualization.ipynb` in Jupyter Notebook

## Docker Support

The project includes a Dockerfile for containerized deployment:
```bash
# Build the Docker image
docker build -t cryptotrade .

# Run the container
docker run -it cryptotrade
```

## Data Structure

The project organizes data in the following directory structure:
- `data/0_by_minutes/`: Minute-level price data
- `data/1_by_half_day/`: Half-day aggregated data
- `data/2_training_models/`: Trained model files
- `data/3_training_output/`: Model training outputs
- `data/4_result_prices/`: Predicted price results
- `data/5_backtest_result/`: Backtesting results
- `data/6_visualization_path/`: Visualization data


## License

Copyright Cosimo Iaia