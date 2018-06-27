class Metric(object):
    """AppOptics Metric Base class"""

    def __init__(self, connection, name, attributes=None, period=None, description=None):
        self.connection = connection
        self.name = name
        self.attributes = attributes or {}
        self.period = period
        self.description = description
        self.measurements = {}
        self.query = {}
        self.composite = None

    def __getitem__(self, name):
        return self.attributes[name]

    def get(self, name, default=None):
        return self.attributes.get(name, default)

    @classmethod
    def from_dict(cls, connection, data):
        """Returns a metric object from a dictionary item,
        which is usually from AppOptics's API"""

        metric_type = data.get('type')

        if metric_type == "gauge":
            cls = Gauge
        elif metric_type == "composite":
            # Since we don't have a formal Composite class, use Gauge for now
            cls = Gauge

        obj = cls(connection, data['name'])
        obj.period = data['period']
        obj.attributes = data['attributes']
        obj.description = data['description'] if 'description' in data else None
        obj.measurements = data['measurements'] if 'measurements' in data else {}
        obj.query = data['query'] if 'query' in data else {}
        obj.composite = data.get('composite', None)
        obj.source_lag = data.get('source_lag', None)

        return obj

    def __repr__(self):
        return "%s<%s>" % (self.__class__.__name__, self.name)


class Gauge(Metric):
    """AppOptics Gauge metric"""
    def add(self, value, **params):
        """Add a new measurement to this gauge"""
        return self.connection.submit(self.name, value, type="gauge", **params)

    def what_am_i(self):
        return 'gauges'
