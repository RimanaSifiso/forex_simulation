import requests
import pandas as pd
from dateutil import parser
from datetime import datetime as dt
from infrastructure.exceptions import NullCandles, RequestError, InvalidFunctionArguments


class DataCollector:
    CANDLE_COUNT = 4000
    INCREMENTS = {
        "M5": 5 * CANDLE_COUNT,
        "M1": CANDLE_COUNT,
        "M30": 30 * CANDLE_COUNT,
        "M15": 15 * CANDLE_COUNT,
        "H1": 60 * CANDLE_COUNT,
        "H4": 60 * 4 * CANDLE_COUNT,
        "D": 60 * 24 * CANDLE_COUNT,
    }
    granularity_list = list(INCREMENTS.keys())

    def __init__(self, api_key, account_id, oanda_url):
        self.API_KEY = api_key
        self.ACCOUNT_ID = account_id
        self.OANDA_URL = oanda_url
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.API_KEY}",
            "Content-Type": "application/json",
        })
        self.tradeable_instruments = self.get_tradeable_instruments()
        self.instruments = self.get_instruments_names()

    def get_tradeable_instruments(self) -> list:
        try:
            url = f"{self.OANDA_URL}/accounts/{self.ACCOUNT_ID}/instruments"
            response = self.session.get(url)
            tradeable_instruments = response.json()["instruments"] if "instruments" in response.json() else []
            return tradeable_instruments
        except requests.exceptions.RequestException as e:
            raise RequestError(e)

    def get_instruments_names(self) -> list | None:
        return [i["name"] for i in self.tradeable_instruments] if len(self.tradeable_instruments) > 0 else None

    @staticmethod
    def calculate_candles_between(from_date: str, to_date: str, granularity: str) -> int:
        """
        Calculate the number of candles between two dates based on the specified granularity.

        :param from_date: Start date in 'YYYY-MM-DDTHH:MM:SSZ' or 'YYYY-MM-DD' format.
        :param to_date: End date in 'YYYY-MM-DDTHH:MM:SSZ' or 'YYYY-MM-DD' format.
        :param granularity: Granularity (timeframe) as a string, e.g., 'M1', 'M5', 'M30', 'H1', 'H2', 'H4', 'D', 'W'.
        :return: Number of candles between the two dates.
        """

        # Convert string dates to datetime objects
        date_format = "%Y-%m-%dT%H:%M:%SZ" if "T" in from_date else "%Y-%m-%d"
        dt1 = dt.strptime(from_date, date_format)
        dt2 = dt.strptime(to_date, date_format)

        if dt1 >= dt2:
            raise ValueError("from_date must be earlier than to_date")

        # Match the granularity and calculate the time delta in minutes
        match granularity:
            case "M1":
                delta_minutes = 1
            case "M5":
                delta_minutes = 5
            case "M30":
                delta_minutes = 30
            case "H1":
                delta_minutes = 60
            case "H2":
                delta_minutes = 120
            case "H4":
                delta_minutes = 240
            case "D":
                delta_minutes = 1440  # 24 hours
            case "W":
                delta_minutes = 10080  # 7 days
            case _:
                raise ValueError(f"Unsupported granularity: {granularity}")

        # Calculate the time difference in minutes
        total_minutes = int((dt2 - dt1).total_seconds() / 60)

        # Calculate the number of candles
        candles_count = total_minutes // delta_minutes

        return candles_count

    def fetch_candles(self, pair_name: str, start: str, end: str | None = None, granularity: str = 'H1',
                      count: int = 10, price: str = 'MBA', use_count: bool = False) -> list[dict]:
        """
        Fetches candles for a given pair name. Note that this function assumes that 'requests' library is installed and that
        tradeable instruments are available in its scope
        :param pair_name: pair to request, e.g. 'EUR_USD'
        :param start: date to start fetching candles for, e.g. from '2020-01-01'
        :param end: date to end fetching candles for, e.g. from '2020-01-01'
        :param granularity: e.g. 'M1', 'M5', 'H1', 'H4'
        :param count: Number of candles to fetch from the date specified in 'start'
        :param price: string representation of price, e.g. 'MBA' or 'B' for bid
        :param use_count: whether to use count or end date in 'end'
        :return returns a dict with candles data as per the specification in the API documentation.
        """

        # --------------------- parameter checking ---------------------------------------------------------------------
        if pair_name not in self.instruments:  # NOTE: instruments_names is a list, and it is assumed to be available
            # in the scope
            raise InvalidFunctionArguments(
                function_name="fetch_candles",
                arguments=pair_name,
                message="Pair name must be in 'tradeable_instruments' list")

        if granularity not in DataCollector.granularity_list:
            raise InvalidFunctionArguments(
                function_name="fetch_candles",
                arguments=granularity,
                message=f"Granularity must be one of {DataCollector.granularity_list}")
        try:
            date_format = "%Y-%m-%dT%H:%M:%SZ"
            start = dt.strftime(parser.parse(start), date_format)
            end = dt.strftime(parser.parse(end), date_format) if end is not None else None
        except TypeError:
            raise InvalidFunctionArguments(
                function_name="fetch_candles",
                arguments=[start, end],
                message="Start date must be in 'YYYY-MM-DD' or 'yyyy-mm-ddTh:m:s' format")

        if price not in ["MBA", "B", "A", "M"]:
            raise InvalidFunctionArguments(
                function_name="fetch_candles", arguments=price, message="Price must be one of ['MBA', 'B', 'A', 'M']")

        if count not in range(1, DataCollector.CANDLE_COUNT + 1):
            raise InvalidFunctionArguments(function_name="fetch_candles",
                                           arguments=count, message="Count must be in range(1, 5000)")

        # --------------------- end of parameter checking ---------------------------------------------------------
        # if we reach here, all should be good with the provided parameters, proceed with the function operation

        request_url = f"{self.OANDA_URL}/instruments/{pair_name}/candles"
        params = {'granularity': granularity, 'price': price, 'from': start}
        if use_count and end is None:
            params["count"] = count
        else:
            params["to"] = end
        res = self.session.get(request_url, params=params)
        # sometimes the data can return with success but will not have 'candles' data in it. We will check for this
        if not res.ok:
            raise RequestError(request_obj=res, message=res.text)
        elif 'candles' not in res.json():
            raise NullCandles(res)

        return res.json()['candles']

    def collect_large_candle_data(self, pair: str, granularity: str, date_from: str, date_to: str,
                                  price: str = 'MBA') -> list | None:
        """
        Collects large candle data for a given pair and granularity.
        :param pair: pair to request, e.g. 'EUR_USD'
        :param granularity: granularity of candles to fetch
        :param date_from: date from which to start fetching candles for, e.g. from '2020-01-01'
        :param date_to: date to end fetching candles for, e.g. from '2020-01-01'
        :param price: price representation of price, e.g. 'MBA' or 'B' for bid
        :return: list of candles data as per the specification in the API documentation.
        """

        # --------------- checking parameters --------------------
        from_date: dt
        to_date: dt
        if pair not in self.instruments:
            raise InvalidFunctionArguments(function_name="collect_large_candle_data",
                                           arguments=pair, message="Pair must be in 'tradeable_instruments' list")

        if granularity not in DataCollector.granularity_list:
            raise InvalidFunctionArguments(
                function_name="collect_large_candle_data",
                arguments=granularity, message=f"Granularity must be one of {DataCollector.granularity_list}")

        try:
            from_date = parser.parse(date_from)
            to_date = parser.parse(date_to)
        except TypeError:
            raise InvalidFunctionArguments(
                function_name="collect_large_candle_data",
                arguments=[date_from, date_to], message="dates must be in 'YYYY-MM-DD' or 'yyyy-mm-ddTh:m:s' format")

        if price not in ["MBA", "B", "A", "M"]:
            raise InvalidFunctionArguments(function_name="collect_large_candle_data",
                                           arguments=price, message="Price must be one of ['MBA', 'B', 'A', 'M']")

        candles: list = []
        date_format = "%Y-%m-%dT%H:%M:%SZ"

        # ------------- subroutines ------------------------------------
        def get_candles(p_count: int, start_date: dt) -> list | None:
            try:
                return self.fetch_candles(
                    pair_name=pair,
                    start=dt.strftime(start_date, date_format),
                    granularity=granularity,
                    price=price,
                    use_count=True,
                    count=p_count
                )
            except InvalidFunctionArguments as e:
                # TODO: Apply appropriate error handling and do some logging
                print(f"Invalid arguments: {e}")
                return None
            except RequestError as e:
                # TODO: Apply appropriate error handling and do some logging
                print(f"Request failed: {e}")
                return None
            except NullCandles as e:
                # TODO: Apply appropriate error handling and do some logging
                print(f"No candle data returned: {e}")
                return None

        # ------------ end subroutines --------------------------------
        try:

            count = self.calculate_candles_between(
                from_date=dt.strftime(from_date, date_format),
                to_date=dt.strftime(to_date, date_format),
                granularity=granularity,
            ) + 1
        except ValueError as e:
            # TODO: Apply appropriate error handling and do some logging
            print(f"Value error at function calculate_candles_between: {e}")
            return None

        if count <= DataCollector.CANDLE_COUNT:
            candles.extend(get_candles(p_count=count, start_date=from_date))
        else:
            progress = 0
            while count > 0:
                print(f"Now collecting candles for {pair} candles. Progress: {progress} candles so far")
                current_count = min(count,  DataCollector.CANDLE_COUNT)
                candles.extend(get_candles(p_count=current_count, start_date=from_date))
                count -= current_count
                from_date = parser.parse(candles[-1]["time"])
                progress += len(candles)
                if from_date >= to_date:
                    break

        return candles

    def run(self):
        pairs = ['EUR_USD', 'GBP_USD', 'USD_JPY', 'USD_CHF', 'GBP_JPY']
        for pair in pairs:
            if pair in self.instruments:
                print(f'Collecting data for {pair} with granularity H1')
                print("------------------------------------------------")
                candles = self.collect_large_candle_data(
                    pair=pair,
                    granularity="H1",
                    date_from="2016-01-01T00:00:00Z",
                    date_to="20-08-25T00:00:00Z",
                )
                candles_df = pd.DataFrame(candles)
                print(f'Now Saving data for {pair} with granularity H1')
                candles_df.to_csv(f"../data/instruments/{pair}_H1.csv")
