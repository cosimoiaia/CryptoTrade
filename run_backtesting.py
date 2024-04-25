import csv
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional, List, Dict, Set

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal
from pydantic import BaseModel

from _exchange import CreateOrder, Exchange, Order
from config import backtest_result_path, result_prices_path, start_date, end_date
from _list_of_currency_pairs import currency_pairs


class Transaction(BaseModel):
    pair: str
    price: Decimal
    amount: Decimal
    date: datetime
    side: Literal["buy", "sell"]
    predict_price: Optional[Decimal]
    current_price: Decimal

    @property
    def fee(self) -> Decimal:
        fee_rate = Decimal(0.002)
        if self.side == "buy":
            return fee_rate * self.amount
        else:
            return fee_rate * (self.amount * self.price)


class Data(BaseModel):
    date: datetime
    pair: str
    usdt_balance: Decimal
    pair_balance: Decimal
    total_balance: Decimal
    current_price: Decimal
    predict_price: Optional[Decimal]
    max_price: Decimal
    price_percentage_change: Decimal
    orders: List[Order]


class Balance(BaseModel):
    usdt: Decimal
    pair: Decimal


class BackTest:
    def __init__(
        self,
        start_date: datetime,
        end_date: datetime,
        fee_rate: Decimal = Decimal("0.00"),
    ):
        self.start_date = start_date
        self.end_date = end_date
        self.fee_rate = fee_rate
        self.pairs = self._get_pairs()
        self.exchange = Exchange()
        for pair in self.pairs:
            self.exchange.deposit(pair, Decimal(1000))
        self.date_range = self._create_date_range()
        self.transactions: dict[
            str, list[Transaction]
        ] = self._generate_start_transactions()
        self.calendar: dict[datetime, dict[str, Data]] = {}

    def run(self):
        counter = 0
        for pair in self.pairs:
            print(f"pair={pair}")
            counter += 1
            start_price = self.exchange.get_max_price(pair, self.date_range[0])
            for date in self.date_range:
                predictions = self._get_prediction(date)
                data = self.calculate_pair_for_date(date, pair, predictions)
                balance = self.exchange.get_balance(pair)
                currrent_price = self.exchange.get_market_price(pair, date)
                max_price = self.exchange.get_max_price(pair, date)
                total = balance.usdt + balance.pair * currrent_price
                orders = self.exchange.get_orders(pair, date)
                self.calendar[date] = self.calendar.get(date, {})
                self.calendar[date][pair] = Data(
                    date=date,
                    pair=pair,
                    usdt_balance=balance.usdt,
                    pair_balance=balance.pair,
                    total_balance=total,
                    current_price=currrent_price,
                    predict_price=predictions.get(pair),
                    max_price=max_price,
                    price_percentage_change=currrent_price / start_price,
                    orders=orders,
                )
            # set_trace()

        t = Decimal(0)
        c = 0
        for pair in self.pairs:
            c += 1
            date = self.date_range[-1]
            balance = self.exchange.get_balance(pair)
            total = balance.usdt + balance.pair * self.exchange.get_market_price(
                pair, date
            )
            t += total

    def calculate_pair_for_date(
        self, date: datetime, pair: str, date_predictions: Dict[str, Decimal]
    ):
        prediction = date_predictions.get(pair, None)
        balance = self.exchange.get_balance(pair)
        last_order = self.exchange.get_last_order(pair)
        if (
            last_order
            and last_order.updated_at <= date
            and last_order.status == "cancelled"
        ):
            # Create market sell order
            last_order = self.exchange.create_order(
                CreateOrder(
                    pair=pair,
                    amount=balance.pair,
                    price=None,
                    side="sell",
                    type="market",
                    created_at=date,
                )
            )
            balance = self.exchange.get_balance(pair)
        if not prediction:
            return
        if last_order is None:
            # Buy tokens and create sell limit order
            order = self.exchange.create_order(
                CreateOrder(
                    pair=pair,
                    amount=balance.usdt,
                    price=None,
                    side="buy",
                    type="market",
                    created_at=date,
                )
            )
            balance = self.exchange.get_balance(pair)
            order = self.exchange.create_order(
                CreateOrder(
                    pair=pair,
                    amount=balance.pair,
                    price=prediction,
                    side="sell",
                    type="limit",
                    created_at=date,
                )
            )
        elif last_order.status == "cancelled":
            # Do nothing
            ...
        elif last_order.status == "closed":
            if last_order.side == "sell" and last_order.updated_at <= date:
                order = self.exchange.create_order(
                    CreateOrder(
                        pair=pair,
                        amount=balance.usdt,
                        price=None,
                        side="buy",
                        type="market",
                        created_at=date,
                    )
                )
                balance = self.exchange.get_balance(pair)
                order = self.exchange.create_order(
                    CreateOrder(
                        pair=pair,
                        amount=balance.pair,
                        price=prediction,
                        side="sell",
                        type="limit",
                        created_at=date,
                    )
                )

    def _generate_start_balances(self):
        return {i: Balance(usdt=Decimal(1000), pair=Decimal(0)) for i in self.pairs}

    def _generate_start_transactions(self):
        return {i: [] for i in self.pairs}

    def _create_date_range(self) -> List[datetime]:
        result = []
        date = start_date
        while date <= end_date:
            result += [date]
            date += timedelta(days=1)
        return result

    def _get_pairs(self) -> Set[str]:
        return set(currency_pairs)

    def _get_prediction(self, date: datetime) -> Dict[str, Decimal]:
        file = result_prices_path.joinpath(date.strftime("%Y-%m-%d_trading_price.csv"))
        if not file.exists():
            return {}

        with open(
            result_prices_path.joinpath(date.strftime("%Y-%m-%d_trading_price.csv")),
            "r",
        ) as f:
            lines = f.read().split("\n")[1:-1]
        predictions = {}
        for line in lines:
            try:
                (
                    _,
                    # _,
                    _date,
                    _token_name,
                    _price_to_be_sold,
                    _predicted_return,
                    _price_0,
                ) = line.split(",")
            except Exception as e:
                print(line)
                raise e
            if Decimal(_predicted_return) < 0.01:
                continue
            predictions[_token_name] = Decimal(_price_to_be_sold)
        return predictions

    def to_csv(self, headers: Optional[List[str]] = None):
        if headers is None:
            headers = [
                "Date",
                "Pair",
                "USDT balance",
                "Pair balance",
                "Total balance",
                "Current price",
                "Predict price",
                "Max price",
                "Price percentage change",
                "Sell Market Order amount",
                "Sell Market Order price",
                "Buy Market Order amount",
                "Buy Market Order price",
                "Sell Limit Order amount",
                "Sell Limit Order price",
            ]
        for apair in self.pairs:
            with open(backtest_result_path.joinpath(f"{apair}.csv"), "w") as f:
                writer = csv.DictWriter(f, dialect="excel", fieldnames=headers)
                writer.writeheader()
                for date, pairs in self.calendar.items():
                    for pair, data in pairs.items():
                        if pair != apair:
                            continue
                        sell_market = next(
                            (
                                i
                                for i in data.orders
                                if i.side == "sell" and i.type == "market"
                            ),
                            None,
                        )
                        buy_market = next(
                            (
                                i
                                for i in data.orders
                                if i.side == "buy" and i.type == "market"
                            ),
                            None,
                        )
                        sell_limit = next(
                            (
                                i
                                for i in data.orders
                                if i.side == "sell" and i.type == "limit"
                            ),
                            None,
                        )
                        writer.writerow(
                            {
                                "Date": date.strftime("%Y-%m-%d"),
                                "Pair": pair,
                                "USDT balance": data.usdt_balance,
                                "Pair balance": data.pair_balance,
                                "Total balance": data.total_balance,
                                "Current price": data.current_price,
                                "Predict price": data.predict_price,
                                "Max price": data.max_price,
                                "Price percentage change": data.price_percentage_change,
                                "Sell Market Order amount": sell_market.amount
                                if sell_market
                                else None,
                                "Sell Market Order price": sell_market.price
                                if sell_market
                                else None,
                                "Buy Market Order amount": buy_market.amount
                                if buy_market
                                else None,
                                "Buy Market Order price": buy_market.price
                                if buy_market
                                else None,
                                "Sell Limit Order amount": sell_limit.amount
                                if sell_limit
                                else None,
                                "Sell Limit Order price": sell_limit.price
                                if sell_limit
                                else None,
                            }
                        )


if __name__ == "__main__":

    test = BackTest(start_date=start_date, end_date=end_date)
    test.run()
    test.to_csv()
