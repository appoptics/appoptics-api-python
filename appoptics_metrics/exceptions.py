class ClientError(Exception):
    """4xx client exceptions"""
    def __init__(self, code, error_payload=None):
        self.code = code
        self.error_payload = error_payload
        Exception.__init__(self, self.error_message())

    def error_message(self):
        return "[%s] %s" % (self.code, self._parse_error_message())

    # See https://docs.appoptics.com/api/#response-codes-amp-errors
    # Examples:
    # {
    #   "errors": {
    #     "params": {
    #       "name":["is not present"],
    #       "start_time":["is not a number"]
    #      }
    #    }
    #  }
    #
    #
    # {
    #   "errors": {
    #     "request": [
    #       "Please use secured connection through https!",
    #       "Please provide credentials for authentication."
    #     ]
    #   }
    # }
    #
    #
    # {
    #   "errors": {
    #     "request": "The requested data resolution is unavailable for the
    #                   given time range. Please try a different resolution."
    #   }
    # }
    #
    #
    # Rate limiting example:
    # {
    #     u'request_time': 1467306906,
    #     u'error': u'You have hit the API limit for measurements
    #     [measure:raw_rate]. Contact: support@appoptics.com to adjust this limit.'
    # }
    def _parse_error_message(self):
        if isinstance(self.error_payload, str):
            # Payload is just a string
            return self.error_payload
        elif isinstance(self.error_payload, dict):
            # The API could return 'errors' or just 'error' with a flat message
            if 'error' in self.error_payload:
                return self.error_payload['error']
            elif 'message' in self.error_payload:
                return self.error_payload['message']
            else:
                payload = self.error_payload['errors']
                messages = []
                if isinstance(payload, list):
                    return payload
                for key in payload:
                    error_list = payload[key]
                    if isinstance(error_list, str):
                        # The error message is a scalar string, just tack it on
                        msg = "%s: %s" % (key, error_list)
                        messages.append(msg)
                    elif isinstance(error_list, list):
                        for error_message in error_list:
                            msg = "%s: %s" % (key, error_message)
                            messages.append(msg)
                    elif isinstance(error_list, dict):
                        for k in error_list:
                            # e.g. "params: time: "
                            msg = "%s: %s: " % (key, k)
                            msg += self._flatten_error_message(error_list[k])
                            messages.append(msg)
                return ", ".join(messages)

    def _flatten_error_message(self, error_msg):
        if isinstance(error_msg, str):
            return error_msg
        elif isinstance(error_msg, list):
            # Join with commas
            return ", ".join(error_msg)
        elif isinstance(error_msg, dict):
            # Flatten out the dict
            for k in error_msg:
                messages = ", ".join(error_msg[k])
                return "%s: %s" % (k, messages)


class BadRequest(ClientError):
    """400 Forbidden"""
    def __init__(self, msg=None):
        ClientError.__init__(self, 400, msg)


class Unauthorized(ClientError):
    """401 Unauthorized"""
    def __init__(self, msg=None):
        ClientError.__init__(self, 401, msg)


class Forbidden(ClientError):
    """403 Forbidden"""
    def __init__(self, msg=None):
        ClientError.__init__(self, 403, msg)


class NotFound(ClientError):
    """404 Forbidden"""
    def __init__(self, msg=None):
        ClientError.__init__(self, 404, msg)

CODES = {
    400: BadRequest,
    401: Unauthorized,
    403: Forbidden,
    404: NotFound
}


# https://docs.appoptics.com/api/#http-status-codes
def get(code, resp_data):
    if code in CODES:
        return CODES[code](resp_data)
    else:
        return ClientError(code, resp_data)
