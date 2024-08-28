class NullCandles(Exception):
    def __init__(self, candles_data, message="Data has no candles."):
        self.candles_data = candles_data
        self.message = message
        super().__init__(self.message)


class RequestError(Exception):
    def __init__(self, request_obj, message="Request error"):
        self.request_obj = request_obj
        self.message = message
        super().__init__(self.message)


class InvalidFunctionArguments(Exception):
    def __init__(self, function_name, arguments, message="Invalid function arguments"):
        self.function_name = function_name
        self.arguments = arguments
        self.message = message
        super().__init__(self.message)

