from datetime import datetime, timezone
from hypersocket import Hypersocket
import asyncio
import logging
import eth_account
import os
from dotenv import load_dotenv
from hypersocket import TOB, Side
from math import floor, log10
from collections import defaultdict
import time
from copy import copy
from quotes import Quotes
from dataclasses import dataclass
from analytics import AnalyticGatherer


def round_price(price: float, sig_figs: int = 5) -> float:
    if price == 0:
        return 0
    decimals = sig_figs - 1 - floor(log10(abs(price)))
    return round(price, max(decimals, 0))


@dataclass
class LiteFill:
    side: Side
    px: float
    sz: float
    oid: str
    time: float


class PerpQuoter:

    def __init__(self, ws: Hypersocket):
        self.ws = ws
        self.quotes = Quotes()
        self.name = "BTC"
        self.quote_size = 0.01
        self.max_position_size = 0.1
        self.spread = 0.0001
        self.tif = "Alo"
        self.deviation_threshold = 0.0001

        self.positions: defaultdict[str, float] = defaultdict(float)
        self.order_state: defaultdict[Side, dict[int, dict]] = defaultdict(dict)

        self.analytics = AnalyticGatherer()
        self.order_limit_prices = {}

    def calc_quote_size(self):
        cur_position = abs(self.positions[self.name])
        if cur_position + self.quote_size > self.max_position_size:
            return max(0, self.max_position_size - cur_position)
        return self.quote_size

    async def trading_loop(self, queue: asyncio.Queue):
        logging.info("Starting trading loop...")
        while True:
            if not self.positions:
                await asyncio.sleep(0.1)
                continue

            latest_quote = await queue.get()
            self.quotes.add(latest_quote)
            await self.manage_order()

            await asyncio.sleep(3)

    async def manage_order(self):
        logging.info(f"Managing orders...")

        if self.quotes.len() > 1:
            if self.quotes.last_relative_change("mid") > self.deviation_threshold:
                logging.info(f"Mid price deviation of {self.quotes.last_relative_change('mid'):.4%} > threshold, adjusting orders")
                await self.update_existing_orders(self.quotes.bid(), self.quotes.ask())
            else:
                logging.info(f"Mid price deviation of {self.quotes.last_relative_change('mid'):.4%} < threshold, no order adjustment needed")

        for side, price in zip([Side.Bid, Side.Ask], [self.quotes.bid(), self.quotes.ask()]):
            if not self.order_state[side]:
                logging.info(f"No existing {side} order, placing new")
                await self.place_new_order(side, price)

    async def cancel_order(self, oid: str):
        logging.info(f"Cancelling order {oid}")
        response = await self.ws.cancel(oid)
        logging.info(f"Cancel response: {response}")
        if response["status"] == "ok":
            logging.info(f"Order {oid} cancelled successfully")
        else:
            logging.error(f"Error cancelling order {oid}: {response}")

    async def update_existing_orders(self, bid: float, ask: float):
        logging.info("Updating existing orders due to mid price deviation")
        cp_order_state = copy(self.order_state)      # to avoid race conditions

        for side, orders in cp_order_state.items():
            for oid, details in orders.items():
                match details["status"]:
                    case "resting" | "open":
                        new_price = round_price(ask if side == "A" else bid)
                        logging.info(f"Modifying {side} order {oid} to {new_price}")
                        response = await self.ws.modify_order(
                            oid=oid,
                            coin=self.name,
                            is_buy=side == "B",
                            price=new_price,
                            size=self.calc_quote_size(),
                            order_type={"limit": {"tif": self.tif}},
                        )
                        logging.info(f"Modify response: {response}")
                    case "cancelled" | "filled":
                        logging.error(f"Order {oid} is {details['status']}. Should have been removed. Exiting...")
                        raise Exception
                    case _:
                        logging.warning(f"Updating order: Order {oid} has status {details['status']}. No action taken.")

    async def place_new_order(self, side: Side, price: float):
        price = round_price(price)
        size = self.calc_quote_size()
        logging.info(f"Placing new {side} order for {self.name}@{price}, qty {size}")

        response = await self.ws.order(self.name, side == "B", price, size, {"limit": {"tif": self.tif}})
        logging.info(f"Order response: {response}")
        payload = response["response"]["payload"]

        if payload["status"] == "ok":
            order_details = payload["response"]["data"]["statuses"][0]
            logging.info(f"Order details: {order_details}")

            if "error" in order_details:
                logging.error(f"Order placed with error: {order_details['error']}")
                return
            
            state = next(iter(order_details))
            details = order_details[state]
            oid = details["oid"]
            self.order_state[side] = {oid: {"status": state}}
            logging.info(f"order state: {self.order_state}")
        else:
            raise Exception("Error placing new order")

    async def update_orders(self, queue: asyncio.Queue):
        while True:
            orders = await queue.get()
            logging.info(f"updating orders: {orders}")

            for ou in orders:
                self.analytics.record_order_update((ou.order.side, ou.order.limit_px, ou.order.sz, ou.order.oid, ou.status))
                side = ou.order.side
                oid = ou.order.oid
                self.order_limit_prices[oid] = ou.order.limit_px

                match ou.status:
                    case "resting" | "open":
                        self.order_state[side].setdefault(oid, {})["status"] = ou.status
                    case "cancelled" | "filled":
                        self.order_state[side].pop(oid, None)
                    case _:
                        logging.info(f"order update for {oid=}, status={ou.status}. Nothing done")
            logging.info(f"Updated order state: {self.order_state}")

    async def update_positions(self, queue: asyncio.Queue):
        while True:
            p = await queue.get()
            logging.info(f"updating positions: {p}")
            if p:
                for pos in p:
                    if pos.coin == self.name:
                        self.positions[pos.coin] = pos.szi
                        self.analytics.record_position(pos.szi)
                    else:
                        logging.warning(f"Received position for coin != {self.name}, ignoring...")
            else:
                self.positions[self.name] = 0.0
            logging.info(f"Updated positions: {self.positions}")

    async def update_user_events(self, queue: asyncio.Queue):
        while True:
            ue = await queue.get()
            logging.info(f"updating user events: {ue}")
            if latest_fills := ue.get("data"):
                for f in latest_fills:
                    self.analytics.record_fill(
                        LiteFill(f.side, f.px, f.sz, f.oid, f.time), 
                        self.quotes, 
                        self.order_limit_prices)
                    logging.info(f"New {f.side} fill at {f.time}: {f.sz}@{f.px}")

    async def log_analytics(self):
        while True:
            logging.info(self.analytics)
            await asyncio.sleep(30)


async def main():
    log_format = "%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("app.log", mode="w"),
        ],
    )
    logging.info("Launching main...")
    account = eth_account.Account.from_key(os.getenv("HL_API_SECRET"))
    address = os.getenv("HL_ACCOUNT_ADDRESS")

    ws = Hypersocket(os.getenv("HL_API_SECRET"))
    await ws.connect()
    quoter = PerpQuoter(ws)


    bbo_queue = await ws.subscribe({"type": "bbo", "coin": quoter.name})
    user_events_queue = await ws.subscribe({"type": "userEvents", "user": address})
    order_updates_queue = await ws.subscribe({"type": "orderUpdates", "user": address})
    ch_queue = await ws.subscribe({"type": "clearinghouseState", "user": address})

    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(ws.wait_for_response_handler())
            tg.create_task(quoter.trading_loop(bbo_queue))
            tg.create_task(quoter.update_orders(order_updates_queue))
            tg.create_task(quoter.update_positions(ch_queue))
            tg.create_task(quoter.update_user_events(user_events_queue))
            tg.create_task(quoter.log_analytics())

    except* KeyboardInterrupt:
        logging.info("KeyboardInterrupt received, shutting down...")
    except* Exception as eg:
        for exc in eg.exceptions:
            logging.error(f"Task failed: {exc}", exc_info=exc)
    finally:
        await ws.close()
        logging.info("Websocket connection closed. Exiting main.")


if __name__ == "__main__":
    load_dotenv()
    asyncio.run(main())
