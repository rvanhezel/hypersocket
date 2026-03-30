from hypersocket import Hypersocket
import asyncio
import logging
import eth_account
import os
from dotenv import load_dotenv
from hypersocket import TOB, Side
from math import floor, log10
from collections import defaultdict


NAME = "BTC"
QUOTE_SIZE = 0.1
MAX_POSITION_SIZE = 0.5
SPREAD = 0.0001
TIF = "Alo"
PREVIOUS_MID = None
DEVIATION_THRESHOLD = 0.001

positions: defaultdict[str, float] = defaultdict(float)   
order_state: defaultdict[Side, dict[int, dict]] = defaultdict(dict)


async def trading_loop(queue: asyncio.Queue, ws: Hypersocket):
    logging.info("Starting trading loop...")
    while True:
        if not positions:
            await asyncio.sleep(0.1)
            continue

        latest_quote = await queue.get()
        await manage_order(latest_quote, ws)

        await asyncio.sleep(3)


async def manage_order(quote: TOB, ws: Hypersocket):
    global PREVIOUS_MID

    logging.info(f"Managing orders based on quote: {quote}")
    mid = (quote.bid.px + quote.ask.px) / 2
    bid = (mid * (1 - SPREAD))
    ask = (mid * (1 + SPREAD))

    if PREVIOUS_MID:
        if abs(mid - PREVIOUS_MID) / PREVIOUS_MID > DEVIATION_THRESHOLD:
            logging.info(f"Mid price deviation of {abs(mid - PREVIOUS_MID) / PREVIOUS_MID:.4%} > threshold, adjusting orders")
            await update_existing_orders(ws, bid, ask)
        else:
            logging.info(f"Mid price deviation of {abs(mid - PREVIOUS_MID) / PREVIOUS_MID:.4%} < threshold, no order adjustment needed")

    for side, price in zip([Side.Bid, Side.Ask], [bid, ask]):
        if not order_state[side]:
            logging.info(f"No existing {side} order, placing new")
            await place_new_order(ws, side, NAME, price)
    
    PREVIOUS_MID = mid
    logging.info(f"Updated PREVIOUS_MID to {PREVIOUS_MID}") 


async def cancel_order(ws: Hypersocket, oid: str):
    logging.info(f"Cancelling order {oid}")
    response = await ws.cancel(oid)
    logging.info(f"Cancel response: {response}")
    if response["status"] == "ok":
        logging.info(f"Order {oid} cancelled successfully")
    else:
        logging.error(f"Error cancelling order {oid}: {response}")


async def update_existing_orders(ws: Hypersocket, bid: float, ask: float):
    logging.info("Updating existing orders due to mid price deviation")

    for side, orders in order_state.items():
        for oid, details in orders.items():
            match details["status"]:
                case "resting" | "open":
                    new_price = round_price(ask if side == "A" else bid)
                    logging.info(f"Modifying {side} order {oid} to {new_price}")
                    response = ws.modify_order(
                        oid=oid,
                        name=NAME,
                        is_buy=side == "B",
                        sz=QUOTE_SIZE,
                        limit_px=new_price,
                        order_type={"limit": {"tif": TIF}},
                    )
                    logging.info(f"Modify response: {response}")
                case "cancelled" | "filled":
                    logging.error(f"Order {oid} is {details['status']}. SHould have been removed. Exiiting...")
                    raise Exception
                case _:
                    logging.info(f"Order {oid} has status {details['status']}. No action taken.")
    

def round_price(price: float, sig_figs: int = 5) -> float:
    if price == 0:
        return 0
    decimals = sig_figs - 1 - floor(log10(abs(price)))
    return round(price, max(decimals, 0))


async def place_new_order(ws: Hypersocket, side: Side, coin: str, price: float):
    price = round_price(price)
    logging.info(f"Placing new {side} order for {coin}@{price}, qty {QUOTE_SIZE}")

    response = await ws.order(coin, side == "B", price, QUOTE_SIZE, {"limit": {"tif": TIF}})
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
        order_state[side] = {
            oid: {"status": state}
        }
        logging.info(f"order state: {order_state}")
    else:
        raise Exception("Error placing new order")


async def update_orders(queue: asyncio.Queue):
    while True:
        orders = await queue.get()
        logging.info(f"updating orders: {orders}")
        for ou in orders:
            side = ou.order.side
            oid = ou.order.oid
            match ou.status:
                case "resting" | "open":
                    order_state[side].setdefault(oid, {})["status"] = ou.status
                case "cancelled" | "filled":
                    order_state[side].pop(oid, None)
                case _:
                    logging.info(f"order update for {oid=}, status={ou.status}. Nothing done")
        logging.info(f"Updated order state: {order_state}")


async def update_positions(queue: asyncio.Queue):
    global positions
    while True:
        p = await queue.get()
        logging.info(f"updating positions: {p}")
        if p:
            for pos in p:
                if pos.coin == NAME:
                    positions[pos.coin] = pos.szi
                else:
                    logging.warning(f"Received position for coin != {NAME}, ignoring...")
        else:
            positions[NAME] = 0.0
        logging.info(f"Updated positions: {positions}")


async def update_user_events(queue: asyncio.Queue):
    while True:
        ue = await queue.get()
        logging.info(f"updating user events: {ue}")


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

    bbo_queue = await ws.subscribe({"type": "bbo", "coin": NAME})
    user_events_queue = await ws.subscribe({"type": "userEvents", "user": address})
    order_updates_queue = await ws.subscribe({"type": "orderUpdates", "user": address})
    ch_queue = await ws.subscribe({"type": "clearinghouseState", "user": address})

    try: 

        async with asyncio.TaskGroup() as tg:
            tg.create_task(ws.wait_for_response_handler())
            tg.create_task(trading_loop(bbo_queue, ws))
            tg.create_task(update_orders(order_updates_queue))
            tg.create_task(update_positions(ch_queue))
            tg.create_task(update_user_events(user_events_queue))

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

