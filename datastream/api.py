from __future__ import absolute_import

import inspect

from . import exceptions, utils

class Granularity(object):
    class _Base(object):
        class _BaseMetaclass(type):
            def __lt__(self, other):
                return self._order < other._order

            def __gt__(self, other):
                return self._order > other._order

            def __le__(self, other):
                return self._order <= other._order

            def __ge__(self, other):
                return self._order >= other._order

            def __eq__(self, other):
                return self._order == other._order

            def __str__(self):
                return self.__name__.lower()

        __metaclass__ = _BaseMetaclass

        @utils.class_property
        def name(cls):
            return cls.__name__.lower()

        @classmethod
        def __str__(cls):
            return cls.__name__.lower()

    class Seconds(_Base):
        _order = 0

    class Minutes(_Base):
        _order = -1

    class Hours(_Base):
        _order = -2

    class Days(_Base):
        _order = -3

    @utils.class_property
    def values(cls):
        if not hasattr(cls, '_values'):
            cls._values = tuple(sorted([
                getattr(cls, name) for name in cls.__dict__ if \
                    name != 'values' and inspect.isclass(getattr(cls, name)) and \
                    getattr(cls, name) is not cls._Base and issubclass(getattr(cls, name), cls._Base)
            ], reverse=True))
        return cls._values

# We want initial letters to be unique
assert len(set(granularity.name.lower()[0] for granularity in Granularity.values)) == len(Granularity.values)

# _order values should be unique
assert len(set(granularity._order for granularity in Granularity.values)) == len(Granularity.values)

assert Granularity.Seconds > Granularity.Minutes > Granularity.Hours > Granularity.Days

class Metric(object):
    def __init__(self, all_tags):
        tags = []
        for tag in all_tags:
            try:
                self.id = tag['metric_id']
                continue
            except (ValueError, KeyError, TypeError):
                pass

            try:
                self.value_downsamplers = tag['value_downsamplers']
                continue
            except (ValueError, KeyError, TypeError):
                pass

            try:
                self.time_downsamplers = tag['time_downsamplers']
                continue
            except (ValueError, KeyError, TypeError):
                pass

            try:
                self.highest_granularity = tag['highest_granularity']
                continue
            except (ValueError, KeyError, TypeError):
                pass

            tags.append(tag)

        self.tags = tags

        if not hasattr(self, 'id'):
            raise ValueError("Supplied tags are missing 'metric_id'.")
        if not hasattr(self, 'value_downsamplers'):
            raise ValueError("Supplied tags are missing 'value_downsamplers'.")
        if not hasattr(self, 'time_downsamplers'):
            raise ValueError("Supplied tags are missing 'time_downsamplers'.")
        if not hasattr(self, 'highest_granularity'):
            raise ValueError("Supplied tags are missing 'highest_granularity'.")

RESERVED_TAGS = (
    'metric_id',
    'value_downsamplers',
    'time_downsamplers',
    'highest_granularity',
)

VALUE_DOWNSAMPLERS = {
    'mean': 'm', # average of all datapoints
    'median': 'e', # median of all datapoints
    'sum': 's', # sum of all datapoints
    'min': 'l', # minimum value of all dataponts (key mnemonic: l for lower)
    'max': 'u', # maximum value of all datapoints (key mnemonic: u for upper)
    'sum_squares': 'q', # sum of squares of all datapoints
    'std_dev': 'd', # standard deviation of all datapoints
    'count': 'c', # number of all datapoints
    'most_often': 'o', # the most often occurring value of all datapoints (key mnemonic: o for often)
    'least_often': 'r', # the least often occurring value of all datapoints (key mnemonic: r for rare)
    'frequencies': 'f', # for each value number of occurrences in all datapoints
}

# Count of timestamps is the same as count of values
TIME_DOWNSAMPLERS = {
    'mean': 'm', # average of all timestamps
    'median': 'e', # median of all timestamps
    'first': 'a', # the first timestamp of all datapoints (key mnemonic: a is the first in the alphabet)
    'last': 'z', # the last timestamp of all datapoints (key mnemonic: z is the last in the alphabet)
    'intervals_mean': 'i', # average of all interval lengths (key mnemonic: i for interval)
    'intervals_median': 'n', # median of all interval lengths (key mnemonic: mediaN)
    'intervals_min': 'l', # minimum of all interval lengths (key mnemonic: l for lower)
    'intervals_max': 'u', # maximum of all interval lengths (key mnemonic: u for upper)
    'intervals_sum_squares': 'q', # sum of squares of all interval lengths
    'intervals_std_dev': 'd', # standard deviation of all interval lengths
}

class Datastream(object):
    Granularity = Granularity
    Metric = Metric
    RESERVED_TAGS = RESERVED_TAGS
    VALUE_DOWNSAMPLERS = VALUE_DOWNSAMPLERS
    TIME_DOWNSAMPLERS = TIME_DOWNSAMPLERS

    # TODO: Implement support for callback
    def __init__(self, backend, callback=None):
        """
        Class constructor.

        :param backend: Backend instance
        :param callback: Callback to call when new datapoint is inserted or downsampled
        """

        self.backend = backend
        self.backend.set_callback(callback)

    def ensure_metric(self, query_tags, tags, value_downsamplers, highest_granularity):
        """
        Ensures that a specified metric exists.

        :param query_tags: Tags which uniquely identify a metric
        :param tags: Tags that should be used (together with `query_tags`) to create a
                     metric when it doesn't yet exist
        :param value_downsamplers: A set of names of value downsampler functions for this metric
        :param highest_granularity: Predicted highest granularity of the data the metric
                                    will store, may be used to optimize data storage
        :return: A metric identifier
        """

        if highest_granularity not in Granularity.values:
            raise exceptions.UnsupportedGranularity("'highest_granularity' is not a valid value: '%s'" % highest_granularity)

        unsupported_downsamplers = list(set(value_downsamplers) - set(VALUE_DOWNSAMPLERS.keys()))
        if len(unsupported_downsamplers) > 0:
            raise exceptions.UnsupportedDownsampler("Unsupported value downsampler(s): %s" % unsupported_downsamplers)

        return self.backend.ensure_metric(query_tags, tags, value_downsamplers, highest_granularity)

    def get_tags(self, metric_id):
        """
        Returns the tags for the specified metric.

        :param metric_id: Metric identifier
        :return: A list of tags for the metric
        """

        return self.backend.get_tags(metric_id)

    def update_tags(self, metric_id, tags):
        """
        Updates metric tags with new tags, overriding existing ones.

        :param metric_id: Metric identifier
        :param tags: A list of new tags
        """

        return self.backend.update_tags(metric_id, tags)

    def remove_tag(self, metric_id, tag):
        """
        Removes metric tag.

        :param metric_id: Metric identifier
        :param tag: Tag value to remove
        """

        return self.backend.remove_tag(metric_id, tag)

    def clear_tags(self, metric_id):
        """
        Removes (clears) all non-readonly metric tags.

        Care should be taken that some tags are set immediately afterwards which uniquely
        identify a metric to be able to use query the metric, in for example, `ensure_metric`.

        :param metric_id: Metric identifier
        """

        return self.backend.clear_tags(metric_id)

    def find_metrics(self, query_tags=None):
        """
        Finds all metrics matching the specified query tags.

        :param query_tags: Tags that should be matched to metrics
        :return: A list of matched metric descriptors
        """

        return self.backend.find_metrics(query_tags)

    def insert(self, metric_id, value):
        """
        Inserts a data point into the data stream.

        :param metric_id: Metric identifier
        :param value: Metric value
        """

        return self.backend.insert(metric_id, value)

    def get_data(self, metric_id, granularity, start, end=None, value_downsamplers=None, time_downsamplers=None):
        """
        Retrieves data from a certain time range and of a certain granularity.

        :param metric_id: Metric identifier
        :param granularity: Wanted granularity
        :param start: Time range start
        :param end: Time range end (optional)
        :param value_downsamplers: The list of downsamplers to limit datapoint values to (optional)
        :param time_downsamplers: The list of downsamplers to limit timestamp values to (optional)
        :return: A list of datapoints
        """

        if granularity not in Granularity.values:
            raise exceptions.UnsupportedGranularity("'granularity' is not a valid value: '%s'" % granularity)

        if value_downsamplers is not None:
            unsupported_downsamplers = set(value_downsamplers) - set(VALUE_DOWNSAMPLERS.keys())
            if len(unsupported_downsamplers) > 0:
                raise exceptions.UnsupportedDownsampler("Unsupported value downsampler(s): %s" % unsupported_downsamplers)

        if time_downsamplers is not None:
            unsupported_downsamplers = set(time_downsamplers) - set(TIME_DOWNSAMPLERS.keys())
            if len(unsupported_downsamplers) > 0:
                raise exceptions.UnsupportedDownsampler("Unsupported time downsampler(s): %s" % unsupported_downsamplers)

        return self.backend.get_data(metric_id, granularity, start, end, value_downsamplers, time_downsamplers)

    def downsample_metrics(self, query_tags=None):
        """
        Requests the backend to downsample all metrics matching the specified
        query tags.

        :param query_tags: Tags that should be matched to metrics
        """

        return self.backend.downsample_metrics(query_tags)
