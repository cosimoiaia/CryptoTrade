from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional, Dict, List

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal
from pydantic import BaseModel
from config import max_price_file, open_price_file


class Balance(BaseModel):
    usdt: Decimal
    pair: Decimal


class CreateOrder(BaseModel):
    pair: str
    amount: Decimal
    price: Optional[Decimal]
    side: Literal["buy", "sell"]
    type: Literal["market", "limit"]
    created_at: datetime


class Order(CreateOrder):
    price: Decimal
    fee: Decimal
    updated_at: datetime
    status: Literal["open", "closed", "cancelled"]


class Exchange:
    fee_rate = Decimal("0.002")
    accounts: Dict[str, Balance] = {}
    order_history: Dict[str, List[Order]] = {}

    def __init__(self, fee_rate: Optional[Decimal] = None) -> None:
        if fee_rate is not None:
            self.fee_rate = fee_rate
        self.max_prices = self._get_max_prices()
        self.open_prices = self._get_open_prices()

    def get_balance(self, pair: str) -> Balance:
        return self.accounts[pair]

    def get_last_order(self, pair: str) -> Optional[Order]:
        if not self.order_history.get(pair, []):
            return None
        return self.order_history[pair][-1]

    def get_market_price(self, pair: str, date: datetime) -> Decimal:
        return self.open_prices[f"{pair}:{date.strftime('%Y-%m-%d')}"]

    def get_max_price(self, pair: str, date: datetime) -> Decimal:
        return self.max_prices[f"{pair}:{date.strftime('%Y-%m-%d')}"]

    def get_orders(self, pair: str, date: datetime) -> List[Order]:
        return [i for i in self.order_history.get(pair, []) if i.updated_at == date]

    def create_order(self, order: CreateOrder):
        if (order.side, order.type) == ("buy", "limit"):
            raise Exception("must not be")
        elif (order.side, order.type) == ("buy", "market"):
            price = self.open_prices[
                f"{order.pair}:{order.created_at.strftime('%Y-%m-%d')}"
            ]
            status = "closed"
            amount = order.amount / price
            new_order = Order(
                pair=order.pair,
                amount=amount,
                price=price,
                side=order.side,
                type=order.type,
                created_at=order.created_at,
                status=status,
                updated_at=order.created_at,
                fee=self.fee_rate * amount,
            )
        elif (order.side, order.type) == ("sell", "limit"):
            if order.price is None:
                raise Exception("Must not be")
            for i in range(3):
                date = order.created_at + timedelta(days=i)
                max_price = self.max_prices[f"{order.pair}:{date.strftime('%Y-%m-%d')}"]
                if max_price >= order.price:
                    updated_at = date
                    status = "closed"
                    break
            else:
                updated_at = order.created_at + timedelta(days=3)
                status = "cancelled"
            new_order = Order(
                pair=order.pair,
                amount=order.amount,
                price=order.price,
                side=order.side,
                type=order.type,
                created_at=order.created_at,
                status=status,
                updated_at=updated_at,
                fee=self.fee_rate * (order.amount * order.price),
            )
        elif (order.side, order.type) == ("sell", "market"):
            price = self.open_prices[
                f"{order.pair}:{order.created_at.strftime('%Y-%m-%d')}"
            ]
            status = "closed"
            new_order = Order(
                pair=order.pair,
                amount=order.amount,
                price=price,
                side=order.side,
                type=order.type,
                created_at=order.created_at,
                status=status,
                updated_at=order.created_at,
                fee=self.fee_rate * (order.amount * price),
            )
        else:
            raise Exception("must not be")
        # print(f"Order {order.side} {order.type} {order.amount}")
        if new_order.pair not in self.order_history:
            self.order_history[new_order.pair] = []
        self.order_history[new_order.pair] += [new_order]

        if new_order.side == "buy":
            self.accounts[new_order.pair].usdt -= new_order.price * new_order.amount
            self.accounts[new_order.pair].pair += new_order.amount - new_order.fee
        elif new_order.status == "closed" and new_order.side == "sell":
            self.accounts[new_order.pair].usdt += (
                new_order.price * new_order.amount
            ) - new_order.fee
            self.accounts[new_order.pair].pair -= new_order.amount

    def _get_max_prices(self) -> Dict[str, Decimal]:
        with open(max_price_file, "r") as f:
            lines = f.read().split("\n")[:-1]
        data = [i.split(",") for i in lines]
        return {f"{i[2]}:{i[3]}": Decimal(i[1]) for i in data}

    def _get_open_prices(self) -> Dict[str, Decimal]:
        with open(open_price_file, "r") as f:
            lines = f.read().split("\n")[:-1]
        data = [i.split(",") for i in lines]
        return {f"{i[2]}:{i[3]}": Decimal(i[1]) for i in data}

    def deposit(self, pair: str, amount: Decimal):
        if not pair in self.accounts:
            self.accounts[pair] = Balance(usdt=Decimal(0), pair=Decimal(0))
        self.accounts[pair].usdt += amount
