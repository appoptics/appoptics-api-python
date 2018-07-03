import re
import six
import platform
import time
import logging
import os
from six.moves import http_client
from six.moves import map
from six import string_types
import urllib
import base64
import json
import email.message
from appoptics_metrics import exceptions
from appoptics_metrics.queue import Queue
from appoptics_metrics.metrics import Gauge, Metric
from appoptics_metrics.alerts import Alert, Service
from appoptics_metrics.annotations import Annotation
from appoptics_metrics.spaces import Space, Chart

__version__ = "5.0.0"

# Defaults
HOSTNAME = "api.appoptics.com"
BASE_PATH = "/v1/"
DEFAULT_TIMEOUT = 10

log = logging.getLogger("appoptics-metrics")

# Alias HTTPSConnection so the tests can mock it out.
HTTPSConnection = http_client.HTTPSConnection
HTTPConnection = http_client.HTTPConnection

# Alias urlencode, it moved between py2 and py3.
try:
    urlencode = urllib.parse.urlencode  # py3
except AttributeError:
    urlencode = urllib.urlencode        # py2


def sanitize_metric_name(metric_name):
    disallowed_character_pattern = r"(([^A-Za-z0-9.:\-_]|[\[\]]|\s)+)"
    max_metric_name_length = 255
    return re.sub(disallowed_character_pattern, '-', metric_name)[:max_metric_name_length]


def sanitize_no_op(metric_name):
    """
    Default behavior, some people want the error
    """
    return metric_name


class AppOpticsConnection(object):
    """AppOptics API Connection.
    Usage:
    conn = AppOpticsConnection(api_key)
    conn.list_metrics()
    [...]
    """

    def __init__(self, api_key, hostname=HOSTNAME, base_path=BASE_PATH, sanitizer=sanitize_no_op,
                 protocol="https", tags=None):
        """Create a new connection to AppOptics Metrics.
        Doesn't actually connect yet or validate until you make a request.

        :param api_key: The API Key (token) to use to authenticate
        :type api_key: str
        """
        tags = tags or {}
        try:
            self.api_key = api_key.encode('ascii')
        except Exception:
            raise TypeError("AppOptics only supports ascii for the credentials")

        if protocol not in ["http", "https"]:
            raise ValueError("Unsupported protocol: {}".format(protocol))

        self.custom_ua = None
        self.protocol = protocol
        self.hostname = hostname
        self.base_path = base_path
        # these two attributes ared used to control fake server errors when doing
        # unit testing.
        self.fake_n_errors = 0
        self.backoff_logic = lambda backoff: backoff * 2
        self.sanitize = sanitizer
        self.timeout = DEFAULT_TIMEOUT
        self.tags = dict(tags)

    def _compute_ua(self):
        if self.custom_ua:
            return self.custom_ua
        else:
            # http://en.wikipedia.org/wiki/User_agent#Format
            # AppOptics-metrics/1.0.3 (ruby; 1.9.3p385; x86_64-darwin11.4.2) direct-faraday/0.8.4
            ua_chunks = []  # Set user agent
            ua_chunks.append("appoptics_metrics/" + __version__)
            p = platform
            system_info = (p.python_version(), p.machine(), p.system(), p.release())
            ua_chunks.append("(python; %s; %s-%s%s)" % system_info)
            return ' '.join(ua_chunks)

    def __getattr__(self, attr):
        def handle_undefined_method(*args):
            if re.search('dashboard|instrument', attr):
                six.print_("We have deprecated support for instruments and dashboards.")
                six.print_("https://github.com/appoptics/appoptics-api-python")
                six.print_("")
            raise NotImplementedError()
        return handle_undefined_method

    def _set_headers(self, headers):
        """ set headers for request """
        if headers is None:
            headers = {}
        headers['Authorization'] = b"Basic " + base64.b64encode(self.api_key + b":").strip()
        headers['User-Agent'] = self._compute_ua()
        return headers

    def _url_encode_params(self, params=None):
        params = params or {}
        if not isinstance(params, dict):
            raise Exception("You must pass in a dictionary!")
        params_list = []
        for k, v in params.items():
            if isinstance(v, list):
                params_list.extend([(k + '[]', x) for x in v])
            else:
                params_list.append((k, v))
        return urlencode(params_list)

    def _make_request(self, conn, path, headers, query_props, method):
        """ Perform the an https request to the server """
        uri = self.base_path + path
        body = None
        if query_props:
            if method == "POST" or method == "DELETE" or method == "PUT":
                body = json.dumps(query_props)
                headers['Content-Type'] = "application/json"
            else:
                uri += "?" + self._url_encode_params(query_props)
        log.info("method=%s uri=%s" % (method, uri))
        if body is None:
            log.info("body(->): %s" % body)
        else:
            log.info("body(->): %s" % json.dumps(json.loads(body), indent=4, sort_keys=True))

        conn.request(method, uri, body=body, headers=headers)

        return conn.getresponse()

    def _process_response(self, resp, backoff):
        """ Process the response from the server """
        success = True
        resp_data = None
        log.info("status code(<-): %s" % resp.status)

        not_a_server_error = resp.status < 500

        if not_a_server_error:
            resp_data = _decode_body(resp)
            a_client_error = resp.status >= 400
            if a_client_error:
                raise exceptions.get(resp.status, resp_data)
            return resp_data, success, backoff
        else:  # A server error, wait and retry
            backoff = self.backoff_logic(backoff)
            log.info("%s: waiting %s before re-trying" % (resp.status, backoff))
            time.sleep(backoff)
            return None, not success, backoff

    def _parse_tags_params(self, tags):
        result = {}
        for k, v in tags.items():
            result["tags[%s]" % k] = v
        return result

    def _mexe(self, path, method="GET", query_props=None, p_headers=None):
        """Internal method for executing a command.
           If we get server errors we exponentially wait before retrying
        """
        conn = self._setup_connection()
        headers = self._set_headers(p_headers)
        success = False
        backoff = 1
        resp_data = None
        while not success:
            resp = self._make_request(conn, path, headers, query_props, method)
            try:
                resp_data, success, backoff = self._process_response(resp, backoff)
            except http_client.ResponseNotReady:
                conn.close()
                conn = self._setup_connection()
        conn.close()
        return resp_data

    def _do_we_want_to_fake_server_errors(self):
        return self.fake_n_errors > 0

    def _setup_connection(self):
        connection_class = HTTPSConnection if self.protocol == "https" else HTTPConnection

        if self._do_we_want_to_fake_server_errors():
            return connection_class(self.hostname, fake_n_errors=self.fake_n_errors)
        else:
            return connection_class(self.hostname, timeout=self.timeout)

    def _parse(self, resp, name, cls):
        """Parse to an object"""
        if name in resp:
            return [cls.from_dict(self, m) for m in resp[name]]
        else:
            return resp

    def get_tags(self):
        """
        Get a shallow copy of the top-level tag set
        :return:
        """
        return dict(self.tags)

    def set_tags(self, d):
        """
        Define the top-level tag set for posting measurements
        :param d:
        :return:
        """
        self.tags = dict(d)    # Create a copy

    def add_tags(self, d):
        """
        Add to the top-level tag set
        :param d:
        :return:
        """
        self.tags.update(d)

    def _get_paginated_results(self, entity, klass, **query_props):
        """
        Return all items for a "list" request
        :param entity:
        :param klass:
        :param query_props:
        :return:
        """
        resp = self._mexe(entity, query_props=query_props)

        results = self._parse(resp, entity, klass)
        for result in results:
            yield result

        length = resp.get('query', {}).get('length', 0)
        offset = query_props.get('offset', 0) + length
        total = resp.get('query', {}).get('total', length)
        if offset < total and length > 0:
            query_props.update({'offset': offset})
            for result in self._get_paginated_results(entity, klass, **query_props):
                yield result

    #
    # Metrics
    #
    def list_metrics(self, **query_props):
        """List a page of metrics"""
        resp = self._mexe("metrics", query_props=query_props)
        return self._parse(resp, "metrics", Metric)

    def list_all_metrics(self, **query_props):
        return self._get_paginated_results("metrics", Metric, **query_props)

    def submit_measurement(self, name, value, **query_props):
        """
        submit_measurement is an alias for submit()
        :param name:
        :param value:
        :param query_props:
        :return:
        """
        return self.submit(name, value, **query_props)

    def submit(self, name, value, **query_props):
        # silently ignore `type` for measurements submission
        query_props.pop("type", None)

        if 'tags' in query_props or self.get_tags():
            self.submit_tagged(name, value, **query_props)
        else:  # at least one `tags` is required
            raise Exception('At least one tag is needed.')

    def submit_tagged(self, name, value, **query_props):
        payload = {'measurements': []}
        payload['measurements'].append(self.create_tagged_payload(name, value, **query_props))
        self._mexe("measurements", method="POST", query_props=payload)

    def create_tagged_payload(self, name, value, **query_props):
        """Create the measurement for forwarding to AppOptics"""
        measurement = {
            'name': self.sanitize(name),
            'value': value
        }
        if 'tags' in query_props:
            inherit_tags = query_props.pop('inherit_tags', False)
            if inherit_tags:
                tags = query_props.pop('tags', {})
                measurement['tags'] = dict(self.get_tags(), **tags)
        elif self.tags:
            measurement['tags'] = self.tags

        for k, v in query_props.items():
            measurement[k] = v
        return measurement

    def get_metric(self, name, **query_props):
        """
        get_metric is the API to fetch a metric.
        :param name:
        :param query_props:
        :return:
        """
        return self.get(name, **query_props)

    def get(self, name, **query_props):
        """
        get is used to retrieve metrics from the server.
        :param name:
        :param query_props:
        :return:
        """
        resp = self._mexe("metrics/%s" % self.sanitize(name), method="GET", query_props=query_props)
        if resp['type'] == 'gauge':
            return Gauge.from_dict(self, resp)
        else:
            raise Exception('The server sent me something that is not a Gauge.')

    def get_measurements(self, name, **query_props):
        """
        get_measurements retrieves measurements from a specific metric
        :param name:
        :param query_props:
        :return:
        """
        return self.get_tagged(name, **query_props)

    def get_tagged(self, name, **query_props):
        """
        get_tagged is used to retrieve measurements from a specific metric.
        :param name:
        :param query_props:
        :return:
        """
        if 'resolution' not in query_props:
            # Default to raw resolution
            query_props['resolution'] = 1
        if 'start_time' not in query_props and 'duration' not in query_props:
            raise Exception("You must provide 'start_time' or 'duration'")
        if 'start_time' in query_props and 'end_time' in query_props and 'duration' in query_props:
            raise Exception("It is an error to set 'start_time', 'end_time' and 'duration'")

        if 'tags' in query_props:
            parsed_tags = self._parse_tags_params(query_props.pop('tags'))
            query_props.update(parsed_tags)

        return self._mexe("measurements/%s" % self.sanitize(name), method="GET", query_props=query_props)

    def get_composite(self, compose, **query_props):
        if self.get_tags():
            return self.get_composite_tagged(compose, **query_props)
        else:
            if 'resolution' not in query_props:
                # Default to raw resolution
                query_props['resolution'] = 1
            if 'start_time' not in query_props:
                raise Exception("You must provide a 'start_time'")
            query_props['compose'] = compose
            return self._mexe('metrics', method="GET", query_props=query_props)

    def get_composite_tagged(self, compose, **query_props):
        if 'resolution' not in query_props:
            # Default to raw resolution
            query_props['resolution'] = 1
        if 'start_time' not in query_props:
            raise Exception("You must provide a 'start_time'")
        query_props['compose'] = compose
        return self._mexe('measurements', method="GET", query_props=query_props)

    def create_composite(self, name, compose, **query_props):
        query_props['composite'] = compose
        query_props['type'] = 'composite'
        return self.update(name, **query_props)

    def create_metric(self, name, type="gauge", **props):
        """
        create_metric creates a new metric with the provided name and properties
        :param name:
        :param type:
        :param props:
        :return:
        """
        return self.create(name, type, **props)

    def create(self, name, type="gauge", **props):
        props.update({'name': name})
        props.update({'type': type})
        return self._mexe("metrics/%s" % self.sanitize(name), method="PUT", query_props=props)

    def update_metric(self, name, **query_props):
        """
        update_metric updates the properties of a metric
        :param name:
        :param query_props:
        :return:
        """
        return self.update(name, **query_props)

    def update(self, name, **query_props):
        return self._mexe("metrics/%s" % self.sanitize(name), method="PUT", query_props=query_props)

    def delete_metric(self, names):
        """
        delete_metric deletes one or multiple metrics
        :param names:
        :return:
        """
        return self.delete(names)

    def delete(self, names):
        if isinstance(names, six.string_types):
            names = self.sanitize(names)
        else:
            names = list(map(self.sanitize, names))
        path = "metrics/%s" % names
        payload = {}
        if not isinstance(names, string_types):
            payload = {'names': names}
            path = "metrics"
        return self._mexe(path, method="DELETE", query_props=payload)

    #
    # Annotations
    #
    def list_annotation_streams(self, **query_props):
        """List all annotation streams"""
        return self._get_paginated_results('annotations', Annotation, **query_props)

    def get_annotation_stream(self, name, **query_props):
        """Get an annotation stream (add start_date to query props for events)"""
        resp = self._mexe("annotations/%s" % name, method="GET", query_props=query_props)
        return Annotation.from_dict(self, resp)

    def get_annotation(self, name, id, **query_props):
        """Get a specific annotation event by ID"""
        resp = self._mexe("annotations/%s/%s" % (name, id), method="GET", query_props=query_props)
        return Annotation.from_dict(self, resp)

    def update_annotation_stream(self, name, **query_props):
        """Update an annotation streams metadata"""
        payload = Annotation(self, name).get_payload()
        for k, v in query_props.items():
            payload[k] = v
        resp = self._mexe("annotations/%s" % name, method="PUT", query_props=payload)
        return Annotation.from_dict(self, resp)

    def post_annotation(self, name, **query_props):
        """ Create an annotation event on :name. """
        """ If the annotation stream does not exist, it will be created automatically. """
        resp = self._mexe("annotations/%s" % name, method="POST", query_props=query_props)
        return resp

    def delete_annotation_stream(self, name, **query_props):
        """delete an annotation stream """
        resp = self._mexe("annotations/%s" % name, method="DELETE", query_props=query_props)
        return resp

    #
    # Alerts
    #
    def create_alert(self, name, **query_props):
        """Create a new alert"""
        payload = Alert(self, name, **query_props).get_payload()
        resp = self._mexe("alerts", method="POST", query_props=payload)
        return Alert.from_dict(self, resp)

    def update_alert(self, alert, **query_props):
        """Update an existing alert"""
        payload = alert.get_payload()
        for k, v in query_props.items():
            payload[k] = v
        resp = self._mexe("alerts/%s" % alert._id,
                          method="PUT", query_props=payload)
        return resp

    # Delete an alert by name (not by id)
    def delete_alert(self, name):
        """delete an alert"""
        alert = self.get_alert(name)
        if alert is None:
            return None
        resp = self._mexe("alerts/%s" % alert._id, method="DELETE")
        return resp

    def get_alert(self, name):
        """Get specific alert"""
        resp = self._mexe("alerts", query_props={'name': name})
        alerts = self._parse(resp, "alerts", Alert)
        if len(alerts) > 0:
            return alerts[0]
        return None

    def list_alerts(self, active_only=True, **query_props):
        """List all alerts (default to active only)"""
        return self._get_paginated_results("alerts", Alert, **query_props)

    def list_services(self, **query_props):
        # Note: This API currently does not have the ability to
        # filter by title, type, etc
        return self._get_paginated_results("services", Service, **query_props)

    #
    # Spaces
    #
    def list_spaces(self, **query_props):
        """List all spaces"""
        return self._get_paginated_results("spaces", Space, **query_props)

    def get_space(self, id, **query_props):
        """Get specific space by ID"""
        resp = self._mexe("spaces/%s" % id,
                          method="GET", query_props=query_props)
        return Space.from_dict(self, resp)

    def find_space(self, name):
        if type(name) is int:
            raise ValueError("This method expects name as a parameter, %s given" % name)
        """Find specific space by Name"""
        spaces = self.list_spaces(name=name)
        # Find the Space by name (case-insensitive)
        # This returns the first space found matching the name
        for space in spaces:
            if space.name and space.name.lower() == name.lower():
                # Now use the ID to hydrate the space attributes (charts)
                return self.get_space(space.id)

        return None

    def update_space(self, space, **query_props):
        """Update an existing space (API currently only allows update of name"""
        payload = space.get_payload()
        for k, v in query_props.items():
            payload[k] = v
        resp = self._mexe("spaces/%s" % space.id,
                          method="PUT", query_props=payload)
        return resp

    def create_space(self, name, **query_props):
        payload = Space(self, name).get_payload()
        for k, v in query_props.items():
            payload[k] = v
        resp = self._mexe("spaces", method="POST", query_props=payload)
        return Space.from_dict(self, resp)

    def delete_space(self, id):
        """delete a space"""
        resp = self._mexe("spaces/%s" % id, method="DELETE")
        return resp

    #
    # Charts
    #
    def list_charts_in_space(self, space, **query_props):
        """List all charts from space"""
        resp = self._mexe("spaces/%s/charts" % space.id, query_props=query_props)
        # "charts" is not in the response, but make this
        # actually return Chart objects
        charts = self._parse({"charts": resp}, "charts", Chart)
        # Populate space ID
        for chart in charts:
            chart.space_id = space.id
        return charts

    def get_chart(self, chart_id, space_or_space_id, **query_props):
        """Get specific chart by ID from Space"""
        space_id = None
        if type(space_or_space_id) is int:
            space_id = space_or_space_id
        elif type(space_or_space_id) is Space:
            space_id = space_or_space_id.id
        else:
            raise ValueError("Space parameter is invalid")
        # TODO: Add better handling around 404s
        resp = self._mexe("spaces/%s/charts/%s" % (space_id, chart_id), method="GET", query_props=query_props)
        resp['space_id'] = space_id
        return Chart.from_dict(self, resp)

    def find_chart(self, name, space):
        """
        Find a chart by name in a space. Return the first match, so if multiple
        charts have the same name, you'll only get the first one
        :param name:
        :param space:
        :return:
        """
        charts = self.list_charts_in_space(space)
        for chart in charts:
            if chart.name and chart.name.lower() == name.lower():
                # Now use the ID to hydrate the chart attributes (streams)
                return self.get_chart(chart.id, space)
        return None

    def create_chart(self, name, space, **query_props):
        """Create a new chart in space"""
        payload = Chart(self, name).get_payload()
        for k, v in query_props.items():
            payload[k] = v
        resp = self._mexe("spaces/%s/charts" % space.id, method="POST", query_props=payload)
        resp['space_id'] = space.id
        return Chart.from_dict(self, resp)

    def update_chart(self, chart, space, **query_props):
        """Update an existing chart"""
        payload = chart.get_payload()
        for k, v in query_props.items():
            payload[k] = v
        resp = self._mexe("spaces/%s/charts/%s" % (space.id, chart.id),
                          method="PUT",
                          query_props=payload)
        return resp

    def delete_chart(self, chart_id, space_id, **query_props):
        """delete a chart from a space"""
        resp = self._mexe("spaces/%s/charts/%s" % (space_id, chart_id), method="DELETE")
        return resp

    #
    # Queue
    #
    def new_queue(self, **kwargs):
        return Queue(self, **kwargs)

    #
    # misc
    #
    def set_timeout(self, timeout):
        self.timeout = timeout


def connect(api_key=None, hostname=HOSTNAME, base_path=BASE_PATH, sanitizer=sanitize_no_op,
            protocol="https", tags=None):
    """
    Connect to AppOptics Metrics
    """
    api_key = api_key if api_key else os.getenv('APPOPTICS_TOKEN', '')

    return AppOpticsConnection(api_key, hostname, base_path, sanitizer=sanitizer, protocol=protocol, tags=tags)


def _decode_body(resp):
    """
    Read and decode HTTPResponse body based on charset and content-type
    """
    body = resp.read()
    # if body is None:
    log.info("body(<-): %s" % body)
    # else:
    #     log.info("body(<-): %s" % json.dumps(json.loads(body), indent=4, sort_keys=True))

    if not body:
        return None

    decoded_body = body.decode(_getcharset(resp))
    content_type = _get_content_type(resp)

    if content_type == "application/json":
        resp_data = json.loads(decoded_body)
    else:
        resp_data = decoded_body

    return resp_data


def _getcharset(resp, default='utf-8'):
    """
    Extract the charset from an HTTPResponse.
    """
    # In Python 3, HTTPResponse is a subclass of email.message.Message, so we
    # can use get_content_chrset. In Python 2, however, it's not so we have
    # to be "clever".
    if hasattr(resp, 'headers'):
        return resp.headers.get_content_charset(default)
    else:
        m = email.message.Message()
        m['content-type'] = resp.getheader('content-type')
        return m.get_content_charset(default)


def _get_content_type(resp):
    """
    Get Content-Type header ignoring parameters
    """
    parts = resp.getheader('content-type', "application/json").split(";")
    return parts[0]
