from quotes import Quotes



class AnalyticGatherer:

    def __init__(self):
        self.fills = []
        self.order_updates = []
        self.positions_history = []

    def record_fill(self, fill, quotes: Quotes | None = None, order_limit_prices: dict[int, float] | None = None):
        fill_time = fill.time
        before, after = None, None
        if quotes:
            quote_before = quotes.quote_before(fill_time)
            quote_after = quotes.quote_after(fill_time)

            match fill.side:
                case "B":
                    before = quote_before.bid.px if quote_before else None
                    after = quote_after.bid.px if quote_after else None
                case "A":
                    before = quote_before.ask.px if quote_before else None  
                    after = quote_after.ask.px if quote_after else None 
                case _:
                    before = None 

        self.fills.append({
            "fill": fill,
            "quote_before": before,   
            "quote_after": after,
            "order_price": order_limit_prices.get(fill.oid)
        })

    def record_order_update(self, order_update):
        self.order_updates.append(order_update)

    def record_position(self, position):
        self.positions_history.append(position)

    def __repr__(self):
        return (
            f"AnalyticGatherer:\n"
            f"  fills={self.fills}\n"
            f"  order_updates={self.order_updates}\n"
            f"  positions_history={self.positions_history}"
        )
