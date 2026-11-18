"""Prior-session context uses only history available before the session."""
import unittest
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from desk_broker import MinuteBar
from session_context import build_context, previous_session

ET = ZoneInfo("America/New_York")


class FakeHistory:
    def minute_bars(self, symbols, start, end):
        symbol = symbols[0]
        bars = []
        day = date(2026, 10, 1)
        while day < date(2026, 11, 17):
            if day.weekday() < 5:
                opening = datetime.combine(day, datetime.min.time(), tzinfo=ET).replace(hour=9, minute=30)
                for minute in range(31):
                    bars.append(MinuteBar(symbol, opening + timedelta(minutes=minute), 100, 101, 99, 100, 10))
            day += timedelta(days=1)
        close = datetime(2026, 11, 16, 15, 59, tzinfo=ET)
        bars.append(MinuteBar(symbol, close, 100, 101, 99, 100.5, 10))
        return {symbol: bars}


class ContextTest(unittest.TestCase):
    def test_context_uses_previous_close_and_complete_opening_ranges(self):
        now = datetime(2026, 11, 17, 8, 0, tzinfo=ET)
        result = build_context(FakeHistory(), now.date(), now)
        self.assertEqual(result["previous_session"], "2026-11-16")
        self.assertEqual(result["symbols"]["QQQ"]["prior_close"], 100.5)
        self.assertAlmostEqual(result["symbols"]["SPY"]["orb_width_median"], .02)

    def test_previous_session_skips_exchange_holidays(self):
        self.assertEqual(previous_session(date(2026, 11, 27)), date(2026, 11, 25))
        with self.assertRaises(ValueError):
            previous_session(date(2026, 1, 2))


if __name__ == "__main__":
    unittest.main()
