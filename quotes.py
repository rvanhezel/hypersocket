from datetime import datetime, timezone
import logging  
from hypersocket import TOB


class Quotes:

    def __init__(self):
        self.quotes: list[TOB] = []

    def add(self, bbo):
        self.quotes.append((self._ts(), bbo))

    def _ts(self):
        return datetime.now(tz=timezone.utc)
    
    def empty(self):
        return len(self.quotes) == 0
    
    def last_relative_change(self, side: str):
        if len(self.quotes) < 2:
            return None
        match side:
            case "bid":
                return (self.quotes[-1][1].bid.px / self.quotes[-2][1].bid.px - 1)
            case "ask":
                return (self.quotes[-1][1].ask.px / self.quotes[-2][1].ask.px - 1)
            case "mid":
                mid1 = (self.quotes[-1][1].bid.px + self.quotes[-1][1].ask.px) / 2
                mid2 = (self.quotes[-2][1].bid.px + self.quotes[-2][1].ask.px) / 2
                return (mid1 / mid2 - 1)
            case _:
                raise ValueError("Invalid side")
        return (bbo - self.quotes[0][1]) / self.quotes[0][1]
    
    def mid(self, offset: int = -1):
        if self.empty():
            return None
        return (self.quotes[offset][1].bid.px + self.quotes[offset][1].ask.px) / 2
    
    def bid(self, offset: int = -1):
        if self.empty():
            return None
        return self.quotes[offset][1].bid.px

    def ask(self, offset: int = -1):
        if self.empty():
            return None
        return self.quotes[offset][1].ask.px
    
    def len(self):
        return len(self.quotes)
    
    def quote_before(self, timestamp: datetime):
        for ts, quote in reversed(self.quotes):
            if ts < timestamp:
                return quote
        return None
    
    def quote_after(self, timestamp: datetime):
        for ts, quote in self.quotes:
            if ts > timestamp:
                return quote
        return None