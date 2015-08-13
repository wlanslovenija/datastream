import collections
import datetime
import inspect

import pytz

from . import exceptions, utils


class Granularity(object):
    class _Base(object):
        # To be overridden.
        _order = None
        _key = None
        _round_rule = None
        _name = None
        _duration = None

        class _BaseMetaclass(type):
            def __lt__(cls, other):
                return cls._order < other._order

            def __gt__(cls, other):
                return cls._order > other._order

            def __le__(cls, other):
                return cls._order <= other._order

            def __ge__(cls, other):
                return cls._order >= other._order

            def __eq__(cls, other):
                return cls._order == other._order

            def __str__(cls):
                return cls._name

        __metaclass__ = _BaseMetaclass

        @utils.class_property
        def name(cls): # pylint: disable=no-self-argument
            return cls._name

        @utils.class_property
        def key(cls): # pylint: disable=no-self-argument
            return cls._key

        @classmethod
        def __str__(cls):
            return cls._name

        @classmethod
        def duration_in_seconds(cls):
            return cls._duration

        @classmethod
        def round_timestamp(cls, timestamp):
            if timestamp.utcoffset() is not None:
                timestamp = timestamp - timestamp.utcoffset()

            time_values = {}
            for atom in cls._round_rule:
                if isinstance(atom, basestring):
                    time_values[atom] = getattr(timestamp, atom)
                else:
                    time_values[atom[0]] = getattr(timestamp, atom[0]) // atom[1] * atom[1]

            return datetime.datetime(tzinfo=pytz.utc, **time_values)

    class Seconds(_Base):
        _order = 0
        _key = 's'
        _round_rule = ('year', 'month', 'day', 'hour', 'minute', 'second')
        _name = 'seconds'
        _duration = 1

    class Seconds10(_Base):
        _order = -1
        _key = 'S'
        _round_rule = ('year', 'month', 'day', 'hour', 'minute', ('second', 10))
        _name = '10seconds'
        _duration = 10

    class Minutes(_Base):
        _order = -10
        _key = 'm'
        _round_rule = ('year', 'month', 'day', 'hour', 'minute')
        _name = 'minutes'
        _duration = 60

    class Minutes10(_Base):
        _order = -11
        _key = 'M'
        _round_rule = ('year', 'month', 'day', 'hour', ('minute', 10))
        _name = '10minutes'
        _duration = 600

    class Hours(_Base):
        _order = -20
        _key = 'h'
        _round_rule = ('year', 'month', 'day', 'hour')
        _name = 'hours'
        _duration = 3600

    class Hours6(_Base):
        _order = -21
        _key = 'H'
        _round_rule = ('year', 'month', 'day', ('hour', 6))
        _name = '6hours'
        _duration = 21600

    class Days(_Base):
        _order = -30
        _key = 'd'
        _round_rule = ('year', 'month', 'day')
        _name = 'days'
        _duration = 86400

    @utils.class_property
    def values(cls): # pylint: disable=no-self-argument
        if not hasattr(cls, '_values'):
            cls._values = tuple(sorted(
                [
                    getattr(cls, name)
                    for name in cls.__dict__
                    if name != 'values' and inspect.isclass(getattr(cls, name)) and getattr(cls, name) is not cls._Base and issubclass(getattr(cls, name), cls._Base)
                ],
                reverse=True
            ))
        return cls._values

# We want granularity keys to be unique
assert len(set(granularity.key for granularity in Granularity.values)) == len(Granularity.values)

# We want all keys to be of length == 1
assert all((len(granularity.key) == 1 for granularity in Granularity.values))

# _order values should be unique
assert len(set(granularity._order for granularity in Granularity.values)) == len(Granularity.values)

assert Granularity.Seconds > Granularity.Seconds10 > Granularity.Minutes > Granularity.Minutes10 > Granularity.Hours > Granularity.Hours6 > Granularity.Days


class Stream(object):
    def __init__(self, all_tags):
        self.tags = all_tags.copy()

        try:
            self.id = all_tags['stream_id']
            self.value_downsamplers = all_tags['value_downsamplers']
            self.time_downsamplers = all_tags['time_downsamplers']
            self.highest_granularity = all_tags['highest_granularity']
            self.pending_backprocess = bool(all_tags['pending_backprocess'])
            self.latest_datapoint = all_tags['latest_datapoint']
            self.earliest_datapoint = all_tags['earliest_datapoint']
            self.downsampled_until = all_tags['downsampled_until']
            self.value_type = all_tags['value_type']
            self.value_type_options = all_tags['value_type_options']

            if 'derived_from' in all_tags:
                self.derived_from = all_tags['derived_from']
                del self.tags['derived_from']
            if 'contributes_to' in all_tags:
                self.contributes_to = all_tags['contributes_to']
                del self.tags['contributes_to']

            del self.tags['stream_id']
            del self.tags['value_downsamplers']
            del self.tags['time_downsamplers']
            del self.tags['highest_granularity']
            del self.tags['pending_backprocess']
            del self.tags['latest_datapoint']
            del self.tags['earliest_datapoint']
            del self.tags['downsampled_until']
            del self.tags['value_type']
            del self.tags['value_type_options']
        except KeyError, exception:
            raise ValueError("Supplied tags are missing %s." % exception)

RESERVED_TAGS = (
    'stream_id',
    'value_downsamplers',
    'time_downsamplers',
    'highest_granularity',
    'derived_from',
    'contributes_to',
    'pending_backprocess',
    'latest_datapoint',
    'earliest_datapoint',
    'downsampled_until',
    'value_type',
    'value_type_options',
)

VALUE_DOWNSAMPLERS = {
    'mean': 'm', # average of all datapoints
    'sum': 's', # sum of all datapoints
    'min': 'l', # minimum value of all dataponts (key mnemonic: l for lower)
    'max': 'u', # maximum value of all datapoints (key mnemonic: u for upper)
    'sum_squares': 'q', # sum of squares of all datapoints
    'std_dev': 'd', # standard deviation of all datapoints
    'count': 'c', # number of all datapoints
    'median': 'n', # middle-ranked value of all datapoints
    'most_often': 'o', # the most often occurring value of all datapoints (key mnemonic: o for often)
    'least_often': 'r', # the least often occurring value of all datapoints (key mnemonic: r for rare)
    'frequencies': 'f', # for each value number of occurrences in all datapoints
}

# Count of timestamps is the same as count of values
TIME_DOWNSAMPLERS = {
    'mean': 'm', # average of all timestamps
    'first': 'a', # the first timestamp of all datapoints (key mnemonic: a is the first in the alphabet)
    'last': 'z', # the last timestamp of all datapoints (key mnemonic: z is the last in the alphabet)
    'intervals_mean': 'i', # average of all interval lengths (key mnemonic: i for interval)
    'intervals_min': 'l', # minimum of all interval lengths (key mnemonic: l for lower)
    'intervals_max': 'u', # maximum of all interval lengths (key mnemonic: u for upper)
    'intervals_sum_squares': 'q', # sum of squares of all interval lengths
    'intervals_std_dev': 'd', # standard deviation of all interval lengths
}

DERIVE_OPERATORS = {
    'sum': 'SUM', # sum of multiple streams
    'derivative': 'DERIVATIVE', # derivative of a stream
    'counter_reset': 'COUNTER_RESET', # generates a counter reset stream
    'counter_derivative': 'COUNTER_DERIVATIVE', # derivative of a monotonically increasing counter stream
}

VALUE_TYPES = (
    'numeric',
    'graph',
    'nominal',
)


class ResultsBase(object):
    def batch_size(self, batch_size): # pylint: disable=unused-argument
        # Ignore by default, this is just for optimization
        return

    def count(self):
        raise NotImplementedError

    def __len__(self):
        return self.count()

    def __iter__(self):
        raise NotImplementedError

    def __getitem__(self, key):
        raise NotImplementedError

    def _get_backend_cursor(self):
        # Very internal. Just for debugging and testing.
        raise NotImplementedError


class Streams(ResultsBase):
    pass


class Datapoints(ResultsBase):
    pass


class Datastream(object):
    Granularity = Granularity
    Stream = Stream
    RESERVED_TAGS = RESERVED_TAGS
    VALUE_DOWNSAMPLERS = VALUE_DOWNSAMPLERS
    TIME_DOWNSAMPLERS = TIME_DOWNSAMPLERS
    DERIVE_OPERATORS = DERIVE_OPERATORS
    VALUE_TYPES = VALUE_TYPES

    def __init__(self, backend):
        """
        Initializes the Datastream API.

        :param backend: Backend instance
        """

        self.backend = backend

    def _switch_database(self, database_name):
        # Very internal. Just for debugging and testing.
        self.backend._switch_database(database_name)

    def _check_query_tags(self, tags):
        for key, value in tags.iteritems():
            # One should use nested dicts and not `__`.
            if '__' in key:
                raise exceptions.ReservedTagNameError("Reserved tag name used: %s" % key)

            if isinstance(value, collections.Mapping):
                self._check_query_tags(value)

    def _check_tags(self, tags):
        for key, value in tags.iteritems():
            if key in RESERVED_TAGS:
                raise exceptions.ReservedTagNameError("Reserved tag name used: %s" % key)

        self._check_query_tags(tags)

    def ensure_stream(self, query_tags, tags, value_downsamplers, highest_granularity, derive_from=None, derive_op=None, derive_args=None, value_type=None, value_type_options=None):
        """
        Ensures that a specified stream exists.

        :param query_tags: A dictionary of tags which uniquely identify a stream
        :param tags: A dictionary of tags that should be used (together with `query_tags`) to create a
                     stream when it doesn't yet exist
        :param value_downsamplers: A set of names of value downsampler functions for this stream
        :param highest_granularity: Predicted highest granularity of the data the stream
                                    will store, may be used to optimize data storage
        :param derive_from: Create a derivate stream
        :param derive_op: Derivation operation
        :param derive_args: Derivation operation arguments
        :param value_type: Optional value type (defaults to `numeric`)
        :param value_type_options: Options specific to the value type
        :return: A stream identifier
        """

        self._check_query_tags(query_tags or {})
        self._check_tags(tags or {})

        if highest_granularity not in Granularity.values:
            raise exceptions.UnsupportedGranularity("'highest_granularity' is not a valid value: '%s'" % highest_granularity)

        unsupported_downsamplers = list(set(value_downsamplers) - set(VALUE_DOWNSAMPLERS.keys()))
        if len(unsupported_downsamplers) > 0:
            raise exceptions.UnsupportedDownsampler("Unsupported value downsampler(s): %s" % unsupported_downsamplers)

        if derive_from is not None:
            if not isinstance(derive_from, (list, tuple)):
                derive_from = [derive_from]
            if derive_op is None:
                raise ValueError("Missing 'derive_op' argument")
            elif derive_op not in DERIVE_OPERATORS:
                raise exceptions.UnsupportedDeriveOperator("Unsupported derive operator: %s" % derive_op)
            if derive_args is None:
                derive_args = {}

        if value_type is None:
            value_type = 'numeric'
        if value_type not in VALUE_TYPES:
            raise exceptions.UnsupportedValueType("Unsupported value type: %s" % value_type)
        if value_type_options is None:
            if value_type == 'numeric':
                value_type_options = {'high_accuracy': False}
            else:
                value_type_options = {}
        elif not isinstance(value_type_options, dict):
            raise TypeError("Value type options must be a dictionary")

        return self.backend.ensure_stream(query_tags, tags, value_downsamplers, highest_granularity, derive_from, derive_op, derive_args, value_type, value_type_options)

    def get_tags(self, stream_id):
        """
        Returns the tags for the specified stream.

        :param stream_id: Stream identifier
        :return: A dictionary of tags for the stream
        """

        return self.backend.get_tags(stream_id)

    def update_tags(self, stream_id, tags):
        """
        Updates stream tags with new tags, overriding existing ones.

        :param stream_id: Stream identifier
        :param tags: A dictionary of new tags
        """

        self._check_tags(tags)

        self.backend.update_tags(stream_id, tags)

    def remove_tag(self, stream_id, tag):
        """
        Removes a stream tag.

        :param stream_id: Stream identifier
        :param tag: Dictionary describing the tag(s) to remove (values are ignored)
        """

        self._check_tags(tag)

        self.backend.remove_tag(stream_id, tag)

    def clear_tags(self, stream_id):
        """
        Removes (clears) all non-readonly stream tags.

        Care should be taken that some tags are set immediately afterwards which uniquely
        identify a stream to be able to query the stream, in for example, `ensure_stream`.

        :param stream_id: Stream identifier
        """

        self.backend.clear_tags(stream_id)

    def find_streams(self, query_tags=None):
        """
        Finds all streams matching the specified query tags.

        :param query_tags: Tags that should be matched to streams
        :return: A `Streams` iterator over matched stream descriptors
        """

        self._check_query_tags(query_tags or {})

        return self.backend.find_streams(query_tags)

    def append(self, stream_id, value, timestamp=None, check_timestamp=True):
        """
        Appends a datapoint into the datastream.

        :param stream_id: Stream identifier
        :param value: Datapoint value
        :param timestamp: Datapoint timestamp, must be equal or larger (newer) than the latest one, monotonically increasing (optional)
        :param check_timestamp: Check if timestamp is equal or larger (newer) than the latest one (default: true)
        :return: A dictionary containing `stream_id`, `granularity`, and `datapoint`
        """

        if timestamp is not None and timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=pytz.utc)

        # TODO: Should we limit timestamp to max(timestamp, datetime.datetime.utcfromtimestamp(0)) and min(timestamp, datetime.datetime.utcfromtimestamp(2147483647))

        return self.backend.append(stream_id, value, timestamp, check_timestamp)

    def get_data(self, stream_id, granularity, start=None, end=None, start_exclusive=None, end_exclusive=None, reverse=False, value_downsamplers=None, time_downsamplers=None):
        """
        Retrieves data from a certain time range and of a certain granularity.

        :param stream_id: Stream identifier
        :param granularity: Wanted granularity
        :param start: Time range start, including the start
        :param end: Time range end, excluding the end (optional)
        :param start_exclusive: Time range start, excluding the start
        :param end_exclusive: Time range end, excluding the end (optional)
        :param reverse: Should datapoints be returned in oldest to newest order (false), or in reverse (true)
        :param value_downsamplers: The list of downsamplers to limit datapoint values to (optional)
        :param time_downsamplers: The list of downsamplers to limit timestamp values to (optional)
        :return: A `Datapoints` iterator over datapoints
        """

        # TODO: Do we want to allow user to specify order of datapoints returned?

        if (start is None) == (start_exclusive is None):
            raise AttributeError("One and only one time range start must be specified.")

        if end is not None and end_exclusive is not None:
            raise AttributeError("Only one time range end can be specified.")

        if start is not None and start.tzinfo is None:
            start = start.replace(tzinfo=pytz.utc)

        if end is not None and end.tzinfo is None:
            end = end.replace(tzinfo=pytz.utc)

        if start_exclusive is not None and start_exclusive.tzinfo is None:
            start_exclusive = start_exclusive.replace(tzinfo=pytz.utc)

        if end_exclusive is not None and end_exclusive.tzinfo is None:
            end_exclusive = end_exclusive.replace(tzinfo=pytz.utc)

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

        return self.backend.get_data(stream_id, granularity, start, end, start_exclusive, end_exclusive, reverse, value_downsamplers, time_downsamplers)

    def downsample_streams(self, query_tags=None, until=None, return_datapoints=False, filter_stream=None):
        """
        Requests the backend to downsample all streams matching the specified
        query tags. Once a time range has been downsampled, new datapoints
        cannot be added to it anymore.

        :param query_tags: Tags that should be matched to streams
        :param until: Timestamp until which to downsample, not including datapoints
                      at a timestamp (optional, otherwise all until the current time)
        :param return_datapoints: Should newly downsampled datapoints be returned, this can
                                  potentially create a huge temporary list and memory consumption
                                  when downsampling many streams and datapoints
        :param filter_stream: An optional callable which returns false for streams that should be skipped
        :return: A list of dictionaries containing `stream_id`, `granularity`, and `datapoint`
                 for each datapoint created while downsampling, if `return_datapoints` was set
        """

        self._check_query_tags(query_tags or {})

        if until is not None and until.tzinfo is None:
            until = until.replace(tzinfo=pytz.utc)

        return self.backend.downsample_streams(query_tags, until, return_datapoints, filter_stream)

    def backprocess_streams(self, query_tags=None):
        """
        Requests the backend to backprocess any derived streams.

        :param query_tags: Tags that should be matched to streams
        """

        self._check_query_tags(query_tags or {})

        return self.backend.backprocess_streams(query_tags)

    def delete_streams(self, query_tags=None):
        """
        Deletes datapoints for all streams matching the specified
        query tags. If no query tags are specified, all datastream-related
        data is deleted from the backend.

        :param query_tags: Tags that should be matched to streams
        """

        self._check_query_tags(query_tags or {})

        self.backend.delete_streams(query_tags)
