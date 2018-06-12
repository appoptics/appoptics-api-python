class Annotation(object):
    """AppOptics Annotation Stream Base class"""

    def __init__(self, connection, name, display_name=None):
        self.connection = connection
        self.name = name
        self.display_name = display_name
        self.events = {}
        self.query = {}

    def __repr__(self):
        return "%s<%s>" % (self.__class__.__name__, self.name)

    @classmethod
    def from_dict(cls, connection, data):
        """Returns a metric object from a dictionary item,
        which is usually from AppOptics's API"""
        obj = cls(connection, data['name'])
        obj.display_name = data['display_name'] if 'display_name' in data else None
        obj.events = data['events'] if 'events' in data else None
        obj.query = data['query'] if 'query' in data else {}
        return obj

    def get_payload(self):
        return {'name': self.name, 'display_name': self.display_name}
