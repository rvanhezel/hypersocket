import asyncio
import logging
import websockets
import json
from dataclasses import dataclass
from enum import StrEnum
import time
from hyperliquid.utils.signing import sign_l1_action, get_timestamp_ms, float_to_wire
from eth_account import Account


# MAINNET = "wss://api.hyperliquid.xyz/ws"
TESTNET = "wss://api.hyperliquid-testnet.xyz/ws"
URL = TESTNET


class Side(StrEnum):
    Bid = "B"
    Ask = "A"

@dataclass
class BBO:
    px: float
    sz: float
    n: float

@dataclass
class TOB:
    name: str
    bid: BBO
    ask: BBO

@dataclass
class Order:
    coin: str
    side: str
    limit_px: float
    sz: float
    oid: int
    timestamp: int
    orig_sz: float
    cloid: str | None = None

@dataclass
class OrderUpdate:
    order: Order
    status: str
    status_timestamp: int

@dataclass
class Fill:
    coin: str
    px: float
    sz: float
    side: str
    dir: str
    closed_pnl: float
    hash: str
    oid: int
    crossed: bool
    fee: float
    fee_token: str
    start_position: float
    time: int
    tid: int
    builder_fee: float | None = None

@dataclass
class CumFunding:
    all_time: float
    since_change: float
    since_open: float

@dataclass
class Leverage:
    type: str
    value: int
    raw_usd: float

@dataclass
class Position:
    coin: str
    entry_px: float
    szi: float
    unrealized_pnl: float
    position_value: float
    margin_used: float
    liquidation_px: float | None
    max_leverage: int
    return_on_equity: float
    cum_funding: CumFunding
    leverage: Leverage


class Hypersocket:

    def __init__(self, pkey: str | None = None):
        self._ws = None
        self._request_futures: dict[int, asyncio.Future] = {}
        self._request_queues: dict[int, asyncio.Queue] = {}
        self._req_id = 0
        self.wallet = Account.from_key(pkey) if pkey else None

        if self.wallet:
            logging.info(f"Initialized Hypersocket with wallet address: {self.wallet.address}")

    def _prepare_future(self, key: str):
        future = asyncio.Future()
        self._request_futures[key] = future
        return future
    
    def _prepare_queue(self, key: str):
        queue = asyncio.Queue()
        self._request_queues[key] = queue
        return queue
    
    async def _response_handler(self):
        async for message in self._ws:
            # logging.info(f"<- {message=}")
            msg = json.loads(message)
            if channel := msg.get("channel"):
                match channel:
                    case "post":
                        key = msg["data"]["id"]
                        future = self._request_futures.pop(key)
                        future.set_result(msg["data"])
                    case "clearinghouseState":
                        logging.info("Response handler: received clearinghouseState update")
                        self._dispatch_clearinghouse_state(msg["data"])
                    case "orderUpdates":
                        logging.info("Response handler: received orderUpdate")
                        self._dispatch_order_updates(msg["data"])
                    case "userEvents":
                        logging.info("Response handler: received userEvent")
                        self._dispatch_user_events(msg["data"])
                    case "bbo":
                        logging.info("Response handler: received bbo")
                        self._dispatch_bbo(msg["data"])
                    case "subscriptionResponse":
                        logging.info(f"Response handler: subscription response: {msg}")
                        if subdata := msg["data"]["subscription"]:
                            suffix_key = "coin" if "coin" in subdata else "user"
                            key = self._key(subdata["type"], subdata[suffix_key])
                            future = self._request_futures.pop(key)
                            future.set_result(True)
                        else:
                            logging.error("Received subscription response without subscriuption data.")
                    case "error":
                        raise Exception(f"Response handler receivefd error. Need to define logic: {channel}")
                        #ws.close etc
                    case _:
                        logging.info(f"Response handler: unhandled channel: {channel}")
            else:
                logging.warning("Received ws message with no channel")

    def _dispatch_order_updates(self, data: dict):
        logging.info(f"Dispatching order update: data received: {data}")
        updates = []
        for item in data:
            o = item["order"]
            updates.append(OrderUpdate(
                order=Order(
                    coin=o["coin"],
                    side=o["side"],
                    limit_px=float(o["limitPx"]),
                    sz=float(o["sz"]),
                    oid=int(o["oid"]),
                    timestamp=int(o["timestamp"]),
                    orig_sz=float(o["origSz"]),
                    cloid=o.get("cloid"),
                ),
                status=item["status"],
                status_timestamp=int(item["statusTimestamp"]),
            ))
        queue = next((q for k, q in self._request_queues.items() if k.startswith("ORDERUPDATES:")), None)
        if queue:
            queue.put_nowait(updates)
        else:
            logging.error("uninitialized orderUpdates queue")

    def _dispatch_user_events(self, data: dict):
        logging.info(f"Dispatching user event: data received: {data}")
        key = self._key("userEvents", data["user"])
        if queue := self._request_queues.get(key):
            for event in data["events"]:
                if "fills" in event:
                    fills = [
                        Fill(
                            coin=f["coin"],
                            px=float(f["px"]),
                            sz=float(f["sz"]),
                            side=f["side"],
                            dir=f["dir"],
                            closed_pnl=float(f["closedPnl"]),
                            hash=f["hash"],
                            oid=int(f["oid"]),
                            crossed=f["crossed"],
                            fee=float(f["fee"]),
                            fee_token=f["feeToken"],
                            start_position=float(f["startPosition"]),
                            time=int(f["time"]),
                            tid=int(f["tid"]),
                            builder_fee=float(f["builderFee"]) if f.get("builderFee") else None,
                        )
                        for f in event["fills"]
                    ]
                    queue.put_nowait({"type": "fills", "data": fills})
                else:
                    queue.put_nowait(event)
        else:
            logging.error(f"uninitialized userEvents queue for {key=}")

    def _dispatch_clearinghouse_state(self, data: dict):
        logging.info(f"Dispatching clearinghouse state: data received: {data}")

        chs = data["clearinghouseState"]
        key = self._key("clearinghouseState", data["user"])
        if queue := self._request_queues.get(key):
            positions = []
            for item in chs.get("assetPositions", []):
                p = item["position"]
                cf = p["cumFunding"]
                lev = p["leverage"]
                liq_px = p.get("liquidationPx")
                positions.append(Position(
                    coin=p["coin"],
                    entry_px=float(p["entryPx"]),
                    szi=float(p["szi"]),
                    unrealized_pnl=float(p["unrealizedPnl"]),
                    position_value=float(p["positionValue"]),
                    margin_used=float(p["marginUsed"]),
                    liquidation_px=float(liq_px) if liq_px else None,
                    max_leverage=int(p["maxLeverage"]),
                    return_on_equity=float(p["returnOnEquity"]),
                    cum_funding=CumFunding(
                        all_time=float(cf["allTime"]),
                        since_change=float(cf["sinceChange"]),
                        since_open=float(cf["sinceOpen"]),
                    ),
                    leverage=Leverage(
                        type=lev["type"],
                        value=int(lev["value"]),
                        raw_usd=float(lev["rawUsd"]),
                    ),
                ))
            queue.put_nowait(positions)
        else:
            logging.error(f"uninitialized clearinghouseState queue for {key=}")

    def _dispatch_bbo(self, data: dict):
        coin = data["coin"].upper()
        key = self._key("bbo", coin)
        if bbo:= data["bbo"]:
            if queue:= self._request_queues.get(key):
                queue.put_nowait(
                    TOB(
                        name=coin,
                        bid=BBO(
                            px=float(bbo[0].get("px")),
                            sz=float(bbo[0].get("sz")),
                            n=float(bbo[0].get("n"))
                        ),
                        ask=BBO(
                            px=float(bbo[1].get("px")),
                            sz=float(bbo[1].get("sz")),
                            n=float(bbo[1].get("n"))
                        )
                    )
                )
            else:
                logging.error(f"uninitialized queue for message {key=}")

    async def connect(self):
        self._ws = await websockets.connect(URL)
        self.response_task = asyncio.create_task(self._response_handler())

    async def close(self):
        self.response_task.cancel()
        if self._ws:
            await self._ws.close()

    def _key(self, prefix: str, suffix: str):
        return f"{prefix}:{suffix}".upper()

    async def subscribe(self, feed: dict):
        if "type" not in feed:
            logging.error("Incorrect feed for subscription. 'type' missing.")

        key = self._key(feed["type"], feed.get("coin") or feed.get("user"))
        future = self._prepare_future(key)
        queue = self._prepare_queue(key)
        body = {
            "method": "subscribe",
            "subscription": feed
        }
        await self._send(body)
        await future
        return queue

    async def _send(self, msg: dict):
        logging.info(f"-> {msg=}")
        if self._ws:
            await self._ws.send(json.dumps(msg))

    async def _unsubscribe(self, feed: dict):
        if "type" not in feed:
            logging.error("Incorrect feed for unsubscription. 'type' missing.")
        body = {
            "method": "unsubscribe",
            "subscription": feed
        }
        await self._send(body)

    def _next_req_id(self):
        self._req_id += 1
        return self._req_id

    async def _request(self, payload, id = None):
        req_id = id if id else self._next_req_id()
        future = self._prepare_future(req_id)

        body = {
            "method": "post",
            "id": req_id ,
            "request": {
                "type": "action",
                "payload": payload
            }
        }
        await self._send(body)
        return await future
    
    async def order(self, 
                    coin: str, 
                    is_buy: bool, 
                    price: float, 
                    size: float, 
                    order_type: dict, 
                    reduce_only: bool = False, 
                    cloid: str = None):
        order = {
            "a": 3,
            "b": is_buy,
            "p": float_to_wire(price),
            "s": float_to_wire(size),
            "r": reduce_only,
            "t": order_type
        }
        if cloid:
            order["c"] = cloid

        action = {
                "type": "order",
                "orders": [order],
                "grouping": "na"
            }
        nonce = get_timestamp_ms()
        payload = {
            "action": action,
            "nonce": nonce,
            "signature": sign_l1_action(
            self.wallet, action, None, nonce, None, False
            )
        }
        return await self._request(payload)

