import time

class Aggregator(object):
    """ Implements client-side *gauge* aggregation to reduce the number of measurements
    submitted.
    Specify a period (default: None) and the aggregator will automatically
    floor the measure_times to that interval.
    """

    def __init__(self, connection, **args):
        self.connection = connection
        # Global source for all 'legacy' metrics sent into the aggregator
        self.source = args.get('source')
        # Global tags, which apply to MD metrics only
        self.tags = dict(args.get('tags', {}))
        self.measurements = {}
        self.tagged_measurements = {}
        self.period = args.get('period')
        self.measure_time = args.get('time')

    # Get a shallow copy of the top-level tag set
    def get_tags(self):
        return dict(self.tags)

    # Define the top-level tag set for posting measurements
    def set_tags(self, d):
        self.tags = dict(d)    # Create a copy

    # Add one or more top-level tags for posting measurements
    def add_tags(self, d):
        self.tags.update(d)

    def add(self, name, value):
        if name not in self.measurements:
            self.measurements[name] = {
                'count': 1,
                'sum': value,
                'min': value,
                'max': value
            }
        else:
            m = self.measurements[name]
            m['sum'] += value
            m['count'] += 1
            if value < m['min']:
                m['min'] = value
            if value > m['max']:
                m['max'] = value

        return self.measurements

    def add_tagged(self, name, value):
        if name not in self.tagged_measurements:
            self.tagged_measurements[name] = {
                'count': 1,
                'sum': value,
                'min': value,
                'max': value
            }
        else:
            m = self.tagged_measurements[name]
            m['sum'] += value
            m['count'] += 1
            if value < m['min']:
                m['min'] = value
            if value > m['max']:
                m['max'] = value

        return self.tagged_measurements

    def to_payload(self):
        # Map measurements into AppOptics POST (array) format
        # {
        #     'gauges': [
        #         {'count': 1, 'max': 42, 'sum': 42, 'name': 'foo', 'min': 42}
        #     ]
        #    'measure_time': 1418838418 (optional)
        #    'source': 'mysource' (optional)
        # }
        # Note: hash format would work too, but the mocks aren't currently set up
        # for the hash format :-(
        # i.e. result = {'gauges': dict(self.measurements)}

        body = []
        for metric_name in self.measurements:
            # Create a clone so we don't change self.measurements
            vals = dict(self.measurements[metric_name])
            vals["name"] = metric_name
            body.append(vals)

        result = {'measurements': body}
        if self.source:
            result['source'] = self.source

        mt = self.floor_measure_time()
        if mt:
            result['time'] = mt

        return result

    def to_md_payload(self):
        # Map measurements into AppOptics MD POST format
        # {
        #     'measures': [
        #         {'count': 1, 'max': 42, 'sum': 42, 'name': 'foo', 'min': 42}
        #     ]
        #    'time': 1418838418 (optional)
        #    'tags': {'hostname': 'myhostname'} (optional)
        # }

        body = []
        for metric_name in self.tagged_measurements:
            # Create a clone so we don't change self.tagged_measurements
            vals = dict(self.tagged_measurements[metric_name])
            vals["name"] = metric_name
            body.append(vals)

        result = {'measurements': body}
        if self.tags:
            result['tags'] = self.tags

        mt = self.floor_measure_time()
        if mt:
            result['time'] = mt

        return result

    # Get/set the measure time if it is ever queried, that way you'll know the measure_time
    # that was submitted, and we'll guarantee the same measure_time for all measurements
    # extracted into a queue
    def get_measure_time(self):
        mt = self.floor_measure_time()
        if mt:
            self.measure_time = mt
        return self.measure_time

    # Return floored measure time if period is set
    # otherwise return user specified value if set
    # otherwise return none
    def floor_measure_time(self):
        if self.period:
            mt = None
            if self.measure_time:
                # Use user-specified time
                mt = self.measure_time
            else:
                # Grab wall time
                mt = int(time.time())
            return mt - (mt % self.period)
        elif self.measure_time:
            # Use the user-specified value with no flooring
            return self.measure_time

    def clear(self):
        self.measurements = {}
        self.tagged_measurements = {}
        self.measure_time = None

    def submit(self):
        # Submit any legacy or tagged measurements to API
        # This will actually return an empty 200 response (no body)
        if self.measurements:
            self.connection._mexe("measurements",
                                  method="POST",
                                  query_props=self.to_payload())
        if self.tagged_measurements:
            self.connection._mexe("measurements",
                                  method="POST",
                                  query_props=self.to_md_payload())
        # Clear measurements
        self.clear()
