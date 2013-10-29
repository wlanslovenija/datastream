import calendar
import datetime
import inspect
import os
import struct
import time
import uuid
import warnings

import pytz

import pymongo
from bson import objectid

import mongoengine

from .. import api, exceptions, utils

DATABASE_ALIAS = 'datastream'

# The largest integer that can be stored in MongoDB; larger values need to use floats
MAXIMUM_INTEGER = 2 ** 63 - 1

ZERO_TIMEDELTA = datetime.timedelta()
ONE_SECOND_TIMEDELTA = datetime.timedelta(seconds=1)


class DownsamplersBase(object):
    """
    Base class for downsampler containers.
    """

    class _Base(object):
        """
        Base class for downsamplers.
        """

        name = None
        key = None

        def initialize(self):
            pass

        def update(self, datum):
            pass

        def finish(self, output):
            pass

        def postprocess(self, values):
            pass

    @utils.class_property
    def values(cls):
        if not hasattr(cls, '_values'):
            cls._values = tuple([
                getattr(cls, name)
                for name in cls.__dict__
                if name != 'values' and inspect.isclass(getattr(cls, name)) and getattr(cls, name) is not cls._Base and issubclass(getattr(cls, name), cls._Base)
            ])

        return cls._values


class ValueDownsamplers(DownsamplersBase):
    """
    A container of downsampler classes for datapoint values.
    """

    class _Base(DownsamplersBase._Base):
        """
        Base class for value downsamplers.
        """

        def __init__(self):
            self.key = api.VALUE_DOWNSAMPLERS[self.name]

    class Count(_Base):
        """
        Counts the number of datapoints.
        """

        name = 'count'

        def initialize(self):
            self.count = 0

        def update(self, datum):
            self.count += 1

        def finish(self, output):
            assert self.key not in output
            output[self.key] = self.count

    class Sum(_Base):
        """
        Sums the datapoint values.
        """

        name = 'sum'

        def initialize(self):
            self.sum = 0

        def update(self, datum):
            try:
                self.sum += datum
            except TypeError:
                warnings.warn(exceptions.InvalidValueWarning("Unsupported non-numeric value for 'sum' downsampler."))

        def finish(self, output):
            assert self.key not in output
            output[self.key] = float(self.sum) if self.sum > MAXIMUM_INTEGER else self.sum

    class SumSquares(_Base):
        """
        Sums the squared datapoint values.
        """

        name = 'sum_squares'

        def initialize(self):
            self.sum = 0

        def update(self, datum):
            try:
                self.sum += datum * datum
            except TypeError:
                warnings.warn(exceptions.InvalidValueWarning("Unsupported non-numeric value for 'sum' downsampler."))

        def finish(self, output):
            assert self.key not in output
            output[self.key] = float(self.sum) if self.sum > MAXIMUM_INTEGER else self.sum

    class Min(_Base):
        """
        Stores the minimum of the datapoint values.
        """

        name = 'min'

        def initialize(self):
            self.min = None

        def update(self, datum):
            if self.min is None:
                self.min = datum
            else:
                self.min = min(self.min, datum)

        def finish(self, output):
            assert self.key not in output
            output[self.key] = self.min

    class Max(_Base):
        """
        Stores the maximum of the datapoint values.
        """

        name = 'max'

        def initialize(self):
            self.max = None

        def update(self, datum):
            if self.max is None:
                self.max = datum
            else:
                self.max = max(self.max, datum)

        def finish(self, output):
            assert self.key not in output
            output[self.key] = self.max

    class Mean(_Base):
        """
        Computes the mean from sum and count (postprocess).
        """

        name = 'mean'
        dependencies = ('sum', 'count')

        def postprocess(self, values):
            assert 'm' not in values
            values[self.key] = float(values[api.VALUE_DOWNSAMPLERS['sum']]) / values[api.VALUE_DOWNSAMPLERS['count']]

    class StdDev(_Base):
        """
        Computes the standard deviation from sum, count and sum squares
        (postprocess).
        """

        name = 'std_dev'
        dependencies = ('sum', 'count', 'sum_squares')

        def postprocess(self, values):
            n = float(values[api.VALUE_DOWNSAMPLERS['count']])
            s = float(values[api.VALUE_DOWNSAMPLERS['sum']])
            ss = float(values[api.VALUE_DOWNSAMPLERS['sum_squares']])
            assert self.key not in values

            if n == 1:
                values[self.key] = 0
            else:
                values[self.key] = (n * ss - s ** 2) / (n * (n - 1))


class TimeDownsamplers(DownsamplersBase):
    """
    A container of downsampler classes for datapoint timestamps.
    """

    class _Base(DownsamplersBase._Base):
        """
        Base class for time downsamplers.
        """

        def __init__(self):
            self.key = api.TIME_DOWNSAMPLERS[self.name]

        def _to_datetime(self, timestamp):
            return datetime.datetime.fromtimestamp(int(timestamp), pytz.utc)

    class Mean(_Base):
        """
        Computes the mean timestamp.
        """

        name = 'mean'

        def initialize(self):
            self.count = 0
            self.sum = 0

        def update(self, datum):
            self.count += 1
            self.sum += datum

        def finish(self, output):
            assert self.key not in output
            output[self.key] = self._to_datetime(float(self.sum) / self.count)

    class First(_Base):
        """
        Stores the first timestamp in the interval.
        """

        name = 'first'

        def initialize(self):
            self.first = None

        def update(self, datum):
            if self.first is None:
                self.first = datum

        def finish(self, output):
            assert self.key not in output
            output[self.key] = self._to_datetime(self.first)

    class Last(_Base):
        """
        Stores the last timestamp in the interval.
        """

        name = 'last'

        def initialize(self):
            self.last = None

        def update(self, datum):
            self.last = datum

        def finish(self, output):
            assert self.key not in output
            output[self.key] = self._to_datetime(self.last)


class DerivationOperators(object):
    """
    A container for derivation operator classes.
    """

    class _Base(object):
        """
        Base class for derivation operators.
        """

        name = None

        def __init__(self, backend, dst_stream, **parameters):
            """
            Class constructor.

            :param backend: A valid MongoDB backend instance
            :param dst_stream: Derived stream instance
            """

            self._backend = backend
            self._stream = dst_stream

        @classmethod
        def get_parameters(cls, src_streams, dst_stream, **arguments):
            """
            Performs validation of the supplied operator parameters and returns
            their database representation that will be used when calling the
            update method.

            :param src_streams: Source stream descriptors
            :param dst_stream: Future destination stream descriptor (not yet saved)
            :param **arguments: User-supplied arguments
            :return: Database representation of the parameters
            """

            return arguments

        def update(self, src_stream, timestamp, value, name=None):
            """
            Called when a new datapoint is added to one of the source streams.

            :param src_stream: Source stream instance
            :param timestamp: Newly inserted datapoint timestamp
            :param value: Newly inserted datapoint value
            :param name: Stream name when specified
            """

            raise NotImplementedError

    @classmethod
    def get(cls, operator):
        if not hasattr(cls, '_values'):
            cls._values = {
                getattr(cls, name).name: getattr(cls, name)
                for name in cls.__dict__
                if name != 'get' and inspect.isclass(getattr(cls, name)) and getattr(cls, name) is not cls._Base and issubclass(getattr(cls, name), cls._Base)
            }

        return cls._values[operator]

    class Sum(_Base):
        """
        Computes the sum of multiple streams.
        """

        name = 'sum'

        @classmethod
        def get_parameters(cls, src_streams, dst_stream, **arguments):
            """
            Performs validation of the supplied operator parameters and returns
            their database representation that will be used when calling the
            update method.

            :param src_streams: Source stream descriptors
            :param dst_stream: Future destination stream descriptor (not yet saved)
            :param **arguments: User-supplied arguments
            :return: Database representation of the parameters
            """

            # Ensure that source stream granularity matches our highest granularity
            for stream_dsc in src_streams:
                granularity = stream_dsc.get('granularity', stream_dsc['stream'].highest_granularity)
                if granularity != dst_stream.highest_granularity:
                    raise exceptions.IncompatibleGranularities

            return super(DerivationOperators.Sum, cls).get_parameters(src_streams, dst_stream, **arguments)

        def update(self, src_stream, timestamp, value, name=None):
            """
            Called when a new datapoint is added to one of the source streams.

            :param src_stream: Source stream instance
            :param timestamp: Newly inserted datapoint timestamp
            :param value: Newly inserted datapoint value
            :param name: Stream name when specified
            """

            # Handle streams with lower granularities
            if isinstance(value, dict):
                # TODO: This is probably not right, currently only the mean is taken into account
                try:
                    value = float(value[api.VALUE_DOWNSAMPLERS['sum']]) / value[api.VALUE_DOWNSAMPLERS['count']]
                    timestamp = timestamp[api.TIME_DOWNSAMPLERS['last']]
                except KeyError:
                    pass

            # First ensure that we have a numeric value, as we can't do anything with
            # other values
            if not isinstance(value, (int, float)):
                warnings.warn(exceptions.InvalidValueWarning("Unsupported non-numeric value for 'sum' operator."))
                return

            rounded_ts = self._stream.highest_granularity.round_timestamp(timestamp)
            ts_key = rounded_ts.strftime("%Y%m%d%H%M%S")
            # TODO: This uses pymongo because MongoEngine can't handle multiple levels of dynamic fields
            # (in addition ME can't handle queries with field names that look like numbers)
            db = mongoengine.connection.get_db(DATABASE_ALIAS)
            db.streams.update({'_id': self._stream.id}, {
                '$set': {('derive_state.%s.%s' % (ts_key, src_stream.id)): value}
            }, w=1)
            self._stream.reload()

            if ts_key not in self._stream.derive_state:
                # This might happen when being updated from multiple threads and another thread has
                # also handled this update
                return

            if len(self._stream.derive_state[ts_key]) == len(self._stream.derived_from.stream_ids):
                # Note that this append may be called multiple times with the same timestamp when
                # multiple threads are calling update
                try:
                    self._backend._append(self._stream, sum(self._stream.derive_state[ts_key].values()), rounded_ts)
                except exceptions.InvalidTimestamp:
                    pass

                # Any keys that are less than or equal to ts_key are safe to remove as we have just
                # appended something, and no threads can insert data between datapoints in the past
                unset = {}
                for key in self._stream.derive_state.keys():
                    if key <= ts_key:
                        unset['derive_state.%s' % key] = ''

                db.streams.update({'_id': self._stream.id}, {'$unset': unset}, w=1)

    class Derivative(_Base):
        """
        Computes the derivative of a stream.
        """

        name = 'derivative'

        @classmethod
        def get_parameters(cls, src_streams, dst_stream, **arguments):
            """
            Performs validation of the supplied operator parameters and returns
            their database representation that will be used when calling the
            update method.

            :param src_streams: Source stream descriptors
            :param dst_stream: Future destination stream descriptor (not yet saved)
            :param **arguments: User-supplied arguments
            :return: Database representation of the parameters
            """

            # The derivative operator supports only one source stream
            if len(src_streams) > 1:
                raise exceptions.InvalidOperatorArguments

            # The highest granularity of the source stream must match ours
            stream_dsc = src_streams[0]
            granularity = stream_dsc.get('granularity', stream_dsc['stream'].highest_granularity)
            if granularity != dst_stream.highest_granularity:
                raise exceptions.IncompatibleGranularities

            return super(DerivationOperators.Derivative, cls).get_parameters(src_streams, dst_stream, **arguments)

        def update(self, src_stream, timestamp, value, name=None):
            """
            Called when a new datapoint is added to one of the source streams.

            :param src_stream: Source stream instance
            :param timestamp: Newly inserted datapoint timestamp
            :param value: Newly inserted datapoint value
            :param name: Stream name when specified
            """

            # First ensure that we have a numeric value, as we can't do anything with
            # other values
            if not isinstance(value, (int, float)):
                warnings.warn(exceptions.InvalidValueWarning("Unsupported non-numeric value for 'derivative' operator."))
                return

            if self._stream.derive_state is not None:
                # We already have a previous value, compute derivative
                delta = float((timestamp - self._stream.derive_state['t']).total_seconds())
                derivative = (value - self._stream.derive_state['v']) / delta
                self._backend._append(self._stream, derivative, timestamp)

            self._stream.derive_state = {'v': value, 't': timestamp}
            self._stream.save()

    class CounterReset(_Base):
        """
        Computes the counter reset stream.
        """

        name = 'counter_reset'

        @classmethod
        def get_parameters(cls, src_streams, dst_stream, **arguments):
            """
            Performs validation of the supplied operator parameters and returns
            their database representation that will be used when calling the
            update method.

            :param src_streams: Source stream descriptors
            :param dst_stream: Future destination stream descriptor (not yet saved)
            :param **arguments: User-supplied arguments
            :return: Database representation of the parameters
            """

            # The counter reset operator supports only one source stream
            if len(src_streams) > 1:
                raise exceptions.InvalidOperatorArguments

            return super(DerivationOperators.CounterReset, cls).get_parameters(src_streams, dst_stream, **arguments)

        def update(self, src_stream, timestamp, value, name=None):
            """
            Called when a new datapoint is added to one of the source streams.

            :param src_stream: Source stream instance
            :param timestamp: Newly inserted datapoint timestamp
            :param value: Newly inserted datapoint value
            :param name: Stream name when specified
            """

            # First ensure that we have a numeric value, as we can't do anything with
            # other values
            if not isinstance(value, (int, float)):
                warnings.warn(exceptions.InvalidValueWarning("Unsupported non-numeric value for 'counter_reset' operator."))
                return

            if self._stream.derive_state is not None:
                # We already have a previous value, check what value needs to be inserted
                # TODO: Add a configurable maximum counter value so overflows can be detected
                if self._stream.derive_state['v'] > value:
                    self._backend._append(self._stream, 1, timestamp)

            self._stream.derive_state = {'v': value, 't': timestamp}
            self._stream.save()

    class CounterDerivative(_Base):
        """
        Computes the derivative of a monotonically increasing counter stream.
        """

        name = 'counter_derivative'

        def __init__(self, backend, dst_stream, max_value=None, **parameters):
            """
            Class constructor.
            """

            self._max_value = max_value

            super(DerivationOperators.CounterDerivative, self).__init__(backend, dst_stream, **parameters)

        @classmethod
        def get_parameters(cls, src_streams, dst_stream, **arguments):
            """
            Performs validation of the supplied operator parameters and returns
            their database representation that will be used when calling the
            update method.

            :param src_streams: Source stream descriptors
            :param dst_stream: Future destination stream descriptor (not yet saved)
            :param **arguments: User-supplied arguments
            :return: Database representation of the parameters
            """

            # We require exactly two input streams, the data stream and the reset stream
            if len(src_streams) != 2:
                raise exceptions.InvalidOperatorArguments("'counter_derivative' requires exactly two input streams!")

            # The reset stream must be first and named "reset", the data stream must be second
            if src_streams[0].get('name', None) != "reset":
                raise exceptions.InvalidOperatorArguments("'counter_derivative' requires 'reset' to be the first input stream!")

            if src_streams[1].get('name', None) is not None:
                raise exceptions.InvalidOperatorArguments("'counter_derivative' requires an unnamed data stream!")

            return super(DerivationOperators.CounterDerivative, cls).get_parameters(src_streams, dst_stream, **arguments)

        def update(self, src_stream, timestamp, value, name=None):
            """
            Called when a new datapoint is added to one of the source streams.

            :param src_stream: Source stream instance
            :param timestamp: Newly inserted datapoint timestamp
            :param value: Newly inserted datapoint value
            :param name: Stream name when specified
            """

            # First ensure that we have a numeric value, as we can't do anything with
            # other values
            if not isinstance(value, (int, float)):
                warnings.warn(exceptions.InvalidValueWarning("Unsupported non-numeric value for 'counter_derivative' operator."))
                return

            if name is None:
                # A data value has just been added
                if self._stream.derive_state is not None:
                    # We already have a previous value, compute derivative
                    v1 = self._stream.derive_state['v']
                    vdelta = value - v1
                    if v1 > value:
                        # Treat this as an overflow
                        if self._max_value is not None:
                            vdelta = self._max_value - v1 + value
                        else:
                            # Treat this as a reset since we lack the maximum value setting
                            warnings.warn(exceptions.InvalidValueWarning("Assuming reset as maximum value is not set even when reset stream said nothing."))
                            vdelta = None

                    if vdelta is not None:
                        tdelta = float((timestamp - self._stream.derive_state['t']).total_seconds())
                        derivative = vdelta / tdelta
                        self._backend._append(self._stream, derivative, timestamp)

                self._stream.derive_state = {'v': value, 't': timestamp}
            elif name == "reset" and value == 1:
                # A reset stream marker has just been added, reset state
                self._stream.derive_state = None

            self._stream.save()


class GranularityField(mongoengine.StringField):
    def __init__(self, **kwargs):
        kwargs.update({
            'choices': api.Granularity.values,
        })
        super(GranularityField, self).__init__(**kwargs)

    def to_python(self, value):
        try:
            return getattr(api.Granularity, value)
        except TypeError:
            if issubclass(value, api.Granularity._Base):
                return value

            raise

    def to_mongo(self, value):
        return value.__name__

    def validate(self, value):
        # No need for any special validation and no need for StringField validation
        pass


class Datapoints(api.Datapoints):
    def __init__(self, stream, cursor=None):
        self.stream = stream
        self.cursor = cursor

    def batch_size(self, batch_size):
        self.cursor.batch_size(batch_size)

    def count(self):
        if self.cursor is None:
            return 0

        return self.cursor.count()

    def __iter__(self):
        if self.cursor is None:
            return

        for datapoint in self.cursor:
            yield self.stream._format_datapoint(datapoint)

    def __getitem__(self, key):
        if self.cursor is None:
            raise IndexError

        if isinstance(key, slice):
            return Datapoints(self.stream, cursor=self.cursor.__getitem__(key))
        elif isinstance(key, int):
            return self.stream._format_datapoint(self.cursor.__getitem__(key))
        else:
            raise AttributeError


class DownsampleState(mongoengine.EmbeddedDocument):
    timestamp = mongoengine.DateTimeField()

    meta = dict(
        allow_inheritance=False,
    )


class DerivedStreamDescriptor(mongoengine.EmbeddedDocument):
    stream_ids = mongoengine.ListField(mongoengine.IntField())
    op = mongoengine.StringField()
    args = mongoengine.DynamicField()


class ContributesToStreamDescriptor(mongoengine.EmbeddedDocument):
    name = mongoengine.StringField()
    granularity = GranularityField()
    op = mongoengine.StringField()
    args = mongoengine.DynamicField()


class Stream(mongoengine.Document):
    # Sequence field uses integer which is much smaller than ObjectId value
    # Because stream id is stored in each datapoint it is important that it is small
    id = mongoengine.SequenceField(primary_key=True, db_alias=DATABASE_ALIAS)
    # But externally we want fancy UUID IDs which do not allow enumeration of all streams
    external_id = mongoengine.UUIDField(binary=True)
    value_downsamplers = mongoengine.ListField(mongoengine.StringField(
        choices=[downsampler.name for downsampler in ValueDownsamplers.values],
    ))
    downsample_state = mongoengine.MapField(mongoengine.EmbeddedDocumentField(DownsampleState))
    highest_granularity = GranularityField()
    derived_from = mongoengine.EmbeddedDocumentField(DerivedStreamDescriptor)
    derive_state = mongoengine.DynamicField()
    contributes_to = mongoengine.MapField(mongoengine.EmbeddedDocumentField(ContributesToStreamDescriptor))
    pending_backprocess = mongoengine.BooleanField()
    tags = mongoengine.ListField(mongoengine.DynamicField())

    meta = dict(
        db_alias=DATABASE_ALIAS,
        collection='streams',
        indexes=['tags', 'external_id'],
        allow_inheritance=False,
    )


class Backend(object):
    value_downsamplers = set([downsampler.name for downsampler in ValueDownsamplers.values])
    time_downsamplers = set([downsampler.name for downsampler in TimeDownsamplers.values])

    # We are storing timestamp into an ObjectID where timestamp is a 32-bit signed value
    # In fact timestamp is a signed value so we could support also timestamps before the epoch,
    # but queries get a bit more complicated because less and greater operators do not see
    # ObjectIDs as signed values, so we should construct a more complicated query for datapoints
    # before the epoch and after the epoch
    # TODO: Support datapoints with timestamp before the epoch
    #_min_timestamp = datetime.datetime.fromtimestamp(-2**31, tz=pytz.utc)
    _min_timestamp = datetime.datetime.fromtimestamp(0, tz=pytz.utc)
    _max_timestamp = datetime.datetime.fromtimestamp(2 ** 31 - 1, tz=pytz.utc)

    def __init__(self, database_name, **connection_settings):
        """
        Initializes the MongoDB backend.

        :param database_name: MongoDB database name
        :param connection_settings: Extra connection settings as defined for `mongoengine.register_connection`
        """

        connection_settings.setdefault('tz_aware', True)

        # Setup the database connection to MongoDB
        mongoengine.connect(database_name, DATABASE_ALIAS, **connection_settings)

        assert \
            set(
                sum(
                    [
                        [downsampler.name] + list(getattr(downsampler, 'dependencies', ()))
                        for downsampler in ValueDownsamplers.values
                    ],
                    []
                )
            ) <= set(api.VALUE_DOWNSAMPLERS.keys())

        assert \
            set(
                sum(
                    [
                        [downsampler.name] + list(getattr(downsampler, 'dependencies', ()))
                        for downsampler in TimeDownsamplers.values
                    ],
                    []
                )
            ) <= set(api.TIME_DOWNSAMPLERS.keys())

        # Ensure indices on datapoints collections (these collections are not defined through MongoEngine)
        db = mongoengine.connection.get_db(DATABASE_ALIAS)
        for granularity in api.Granularity.values:
            collection = getattr(db.datapoints, granularity.name)
            collection.ensure_index([
                ('m', pymongo.ASCENDING),
                ('_id', pymongo.ASCENDING),
            ])
            collection.ensure_index([
                ('m', pymongo.ASCENDING),
                ('_id', pymongo.DESCENDING),
            ])

        # Only for testing, don't use!
        self._test_callback = None

        # Used only to artificially advance time when testing, don't use!
        self._time_offset = ZERO_TIMEDELTA

    def _process_tags(self, tags):
        """
        Checks that reserved tags are not used and converts dicts to their
        hashable counterparts, so they can be used in set operations.
        """

        converted_tags = set()

        for tag in tags:
            if isinstance(tag, dict):
                for reserved in api.RESERVED_TAGS:
                    if reserved in tag:
                        raise exceptions.ReservedTagNameError

                # Convert dicts to hashable dicts so they can be used in set
                # operations
                tag = utils.hashabledict(tag)

            converted_tags.add(tag)

        return converted_tags

    def ensure_stream(self, query_tags, tags, value_downsamplers, highest_granularity, derive_from, derive_op, derive_args):
        """
        Ensures that a specified stream exists.

        :param query_tags: Tags which uniquely identify a stream
        :param tags: Tags that should be used (together with `query_tags`) to create a
                     stream when it doesn't yet exist
        :param value_downsamplers: A set of names of value downsampler functions for this stream
        :param highest_granularity: Predicted highest granularity of the data the stream
                                    will store, may be used to optimize data storage
        :param derive_from: Create a derivate stream
        :param derive_op: Derivation operation
        :param derive_args: Derivation operation arguments
        :return: A stream identifier
        """

        try:
            stream = Stream.objects.get(tags__all=query_tags)
        except Stream.DoesNotExist:
            # Create a new stream
            stream = Stream()
            stream.external_id = uuid.uuid4()

            # Some downsampling functions don't need to be stored in the database but
            # can be computed on the fly from other downsampled values
            value_downsamplers = set(value_downsamplers)
            for downsampler in ValueDownsamplers.values:
                if downsampler.name in value_downsamplers and hasattr(downsampler, 'dependencies'):
                    value_downsamplers.update(downsampler.dependencies)

            if not value_downsamplers <= self.value_downsamplers:
                raise exceptions.UnsupportedDownsampler(
                    "Unsupported value downsampler(s): %s" % list(value_downsamplers - self.value_downsamplers),
                )

            # This should already be checked at the API level
            assert highest_granularity in api.Granularity.values

            stream.value_downsamplers = list(value_downsamplers)
            stream.highest_granularity = highest_granularity
            stream.tags = list(self._process_tags(query_tags).union(self._process_tags(tags)))

            # Setup source stream metadata for derivate streams
            if derive_from is not None:
                # Validate that all source streams exist and resolve their internal ids
                derive_stream_ids = []
                derive_stream_dscs = []
                for stream_dsc in derive_from:
                    if not isinstance(stream_dsc, dict):
                        stream_dsc = {'stream': stream_dsc}

                    try:
                        src_stream = Stream.objects.get(external_id=uuid.UUID(stream_dsc['stream']))

                        # One can't specify a granularity higher than the stream's highest one
                        if stream_dsc.get('granularity', src_stream.highest_granularity) > src_stream.highest_granularity:
                            raise exceptions.IncompatibleGranularities

                        # If any of the input streams already holds some data, we pause our stream; there
                        # is a potential race condition here, but this should not result in great loss
                        try:
                            self.get_data(unicode(src_stream.external_id), src_stream.highest_granularity, self._min_timestamp)[0]
                            stream.pending_backprocess = True
                        except IndexError:
                            pass

                        stream_dsc = stream_dsc.copy()
                        stream_dsc['stream'] = src_stream

                        derive_stream_ids.append(src_stream.id)
                        derive_stream_dscs.append(stream_dsc)
                    except Stream.DoesNotExist:
                        raise exceptions.StreamNotFound

                # Validate and convert operator parameters
                derive_operator = DerivationOperators.get(derive_op)
                derive_args = derive_operator.get_parameters(derive_stream_dscs, stream, **derive_args)

                derived = DerivedStreamDescriptor()
                derived.stream_ids = derive_stream_ids
                derived.op = derive_op
                derived.args = derive_args
                stream.derived_from = derived

            # Initialize downsample state
            if highest_granularity != api.Granularity.values[-1]:
                for granularity in api.Granularity.values[api.Granularity.values.index(highest_granularity) + 1:]:
                    state = DownsampleState()
                    # TODO: Or maybe: granularity.round_timestamp(datetime.datetime.now(pytz.utc) + self._time_offset)
                    state.timestamp = None
                    stream.downsample_state[granularity.name] = state

            stream.save()

            if derive_from is not None:
                # Now that the stream has been saved, update all dependent streams' metadata
                try:
                    for stream_dsc in derive_stream_dscs:
                        src_stream = stream_dsc['stream']
                        src_stream.contributes_to[str(stream.id)] = ContributesToStreamDescriptor(
                            name=stream_dsc.get('name', None),
                            granularity=stream_dsc.get('granularity', src_stream.highest_granularity),
                            op=stream.derived_from.op,
                            args=stream.derived_from.args,
                        )
                        src_stream.save()
                except:
                    # Update has failed, we have to undo everything and remove this stream
                    try:
                        Stream.objects.filter(id__in=stream.derived_from.stream_ids).update(
                            **{('unset__contributes_to__%s' % stream.id): ''}
                        )
                    finally:
                        stream.delete()

                    raise
        except Stream.MultipleObjectsReturned:
            raise exceptions.MultipleStreamsReturned

        return unicode(stream.external_id)

    def _map_derived_from(self, stream):
        """
        Maps derived from descriptor so it can be returned as a tag.

        :param stream: Stream instance to map from
        :return: Mapped descriptor
        """

        stream_ids = []
        for stream_id in stream.derived_from.stream_ids:
            sstream = Stream.objects.get(id=int(stream_id))
            stream_ids.append(unicode(sstream.external_id))

        return {
            'stream_ids': stream_ids,
            'op': stream.derived_from.op,
            'args': stream.derived_from.args,
        }

    def _map_contributes_to(self, stream):
        """
        Maps contributes to descriptor so it can be returned as a tag.

        :param stream: Stream instance to map from
        :return: Mapped descriptor
        """

        result = {}
        for stream_id, descriptor in stream.contributes_to.items():
            stream = Stream.objects.get(id=int(stream_id))
            result[unicode(stream.external_id)] = {
                'op': descriptor.op,
                'args': descriptor.args,
            }

        return result

    def _get_stream_tags(self, stream):
        """
        Returns a stream descriptor in the form of tags.
        """

        tags = stream.tags
        tags += [
            {'stream_id': unicode(stream.external_id)},
            {'value_downsamplers': stream.value_downsamplers},
            {'time_downsamplers': self.time_downsamplers},
            {'highest_granularity': stream.highest_granularity},
            {'pending_backprocess': bool(stream.pending_backprocess)}
        ]

        if stream.derived_from:
            tags += [{'derived_from': self._map_derived_from(stream)}]
        if stream.contributes_to:
            tags += [{'contributes_to': self._map_contributes_to(stream)}]

        return tags

    def get_tags(self, stream_id):
        """
        Returns the tags for the specified stream.

        :param stream_id: Stream identifier
        :return: A list of tags for the stream
        """

        try:
            stream = Stream.objects.get(external_id=uuid.UUID(stream_id))
            tags = self._get_stream_tags(stream)
        except Stream.DoesNotExist:
            raise exceptions.StreamNotFound

        return tags

    def update_tags(self, stream_id, tags):
        """
        Updates stream tags with new tags, overriding existing ones.

        :param stream_id: Stream identifier
        :param tags: A list of new tags
        """

        if not Stream.objects(external_id=uuid.UUID(stream_id)).update(set__tags=list(self._process_tags(tags))):
            # Returned count is 1 if stream is found even if nothing changed, this is what we want
            raise exceptions.StreamNotFound

    def remove_tag(self, stream_id, tag):
        """
        Removes stream tag.

        :param stream_id: Stream identifier
        :param tag: Tag value to remove
        """

        if not Stream.objects(external_id=uuid.UUID(stream_id)).update(pull__tags=tag):
            # Returned count is 1 if stream is found even if nothing removed, this is what we want
            raise exceptions.StreamNotFound

    def clear_tags(self, stream_id):
        """
        Removes (clears) all non-readonly stream tags.

        Care should be taken that some tags are set immediately afterwards which uniquely
        identify a stream to be able to query the stream, in for example, `ensure_stream`.

        :param stream_id: Stream identifier
        """

        if not Stream.objects(external_id=uuid.UUID(stream_id)).update(set__tags=[]):
            # Returned count is 1 if stream is found even if nothing changed, this is what we want
            raise exceptions.StreamNotFound

    def _get_stream_queryset(self, query_tags):
        """
        Returns a queryset that matches the specified stream tags.

        :param query_tags: Tags that should be matched to streams
        :return: A filtered queryset
        """

        if query_tags is None:
            query_tags = []

        query_set = Stream.objects.all()
        for tag in query_tags[:]:
            if isinstance(tag, dict):
                if 'stream_id' in tag:
                    query_set = query_set.filter(external_id=uuid.UUID(tag['stream_id']))
                    query_tags.remove(tag)

        if not query_tags:
            return query_set
        else:
            return query_set.filter(tags__all=query_tags)

    def find_streams(self, query_tags=None):
        """
        Finds all streams matching the specified query tags.

        :param query_tags: Tags that should be matched to streams
        :return: A list of matched stream descriptors
        """

        query_set = self._get_stream_queryset(query_tags)
        return [self._get_stream_tags(m) for m in query_set]

    def _supported_timestamp_range(self, timestamp):
        """
        Checks if timestamp is in supported range for MongoDB. Otherwise raises InvalidTimestamp exception.
        """

        if timestamp is None or self._min_timestamp <= timestamp <= self._max_timestamp:
            return

        raise exceptions.InvalidTimestamp("Timestamp is out of range: %s" % timestamp)

    def _force_timestamp_range(self, timestamp):
        return max(self._min_timestamp, min(self._max_timestamp, timestamp))

    def _timestamp_after_downsampled(self, stream, timestamp):
        """
        We require that once a period has been downsampled, no new datapoint can be added there.
        """

        for granularity in api.Granularity.values[api.Granularity.values.index(stream.highest_granularity) + 1:]:
            state = stream.downsample_state.get(granularity.name, None)
            rounded_timestamp = granularity.round_timestamp(timestamp)
            if state is None or state.timestamp is None or rounded_timestamp >= state.timestamp:
                continue
            else:
                raise exceptions.InvalidTimestamp("Stream '%s' at granularity '%s' has already been downsampled until '%s' and datapoint timestamp falls into that range: %s" % (stream.external_id, granularity, state.timestamp, timestamp))

    def _process_contributes_to(self, stream, timestamp, value, granularity):
        """
        Processes stream contributions to other streams.

        :param stream: Stream whose contributions to check
        :param timestamp: Timestamp of the datapoint that was inserted
        :param value: Value of the datapoint that was inserted
        :param granularity: Granularity of the current datapoint
        """

        if not stream.contributes_to:
            return

        # Update any streams we are contributing to
        for stream_id, descriptor in stream.contributes_to.iteritems():
            if descriptor.granularity != granularity:
                continue

            try:
                derived_stream = Stream.objects.get(id=int(stream_id))
                # Skip streams that are waiting to be backprocessed
                if derived_stream.pending_backprocess:
                    continue
            except Stream.DoesNotExist:
                warnings.warn(exceptions.InternalInconsistencyWarning("Invalid stream reference to '%s' in contributes_to!" % stream_id))
                continue

            derive_operator = DerivationOperators.get(descriptor.op)(self, derived_stream, **descriptor.args)
            derive_operator.update(stream, timestamp, value, name=descriptor.name)

    def _append(self, stream, value, timestamp=None, check_timestamp=True):
        """
        Appends a datapoint into the datastream.

        :param stream: Stream instance
        :param value: Datapoint value
        :param timestamp: Datapoint timestamp, must be equal or larger (newer) than the latest one, monotonically increasing (optional)
        :param check_timestamp: Check if timestamp is equal or larger (newer) than the latest one (default: true)
        :return: A dictionary containing `stream_id`, `granularity`, and `datapoint`
        """

        self._supported_timestamp_range(timestamp)

        # Append the datapoint into appropriate granularity
        db = mongoengine.connection.get_db(DATABASE_ALIAS)
        collection = getattr(db.datapoints, stream.highest_granularity.name)
        if timestamp is None and self._time_offset == ZERO_TIMEDELTA:
            datapoint = {'m': stream.id, 'v': value}
        else:
            object_id = self._generate_object_id(timestamp)

            if check_timestamp:
                # TODO: There is a race condition here, between check and insert
                latest_timestamp = self._last_timestamp(stream)
                if object_id.generation_time < latest_timestamp:
                    raise exceptions.InvalidTimestamp("Datapoint timestamp must be equal or larger (newer) than the latest one '%s': %s" % (latest_timestamp, object_id.generation_time))

            # We always check this because it does not require database access
            self._timestamp_after_downsampled(stream, object_id.generation_time)

            datapoint = {'_id': object_id, 'm': stream.id, 'v': value}

        datapoint['_id'] = collection.insert(datapoint, w=1)

        if timestamp is None and self._time_offset == ZERO_TIMEDELTA:
            # When timestamp is not specified, database generates one, so we check it here
            try:
                self._timestamp_after_downsampled(stream, datapoint['_id'].generation_time)
            except exceptions.InvalidTimestamp:
                # Cleanup
                collection.remove(datapoint['_id'], w=1)

                raise

        # Process contributions to other streams
        self._process_contributes_to(stream, datapoint['_id'].generation_time, value, stream.highest_granularity)

        ret = {
            'stream_id': str(stream.external_id),
            'granularity': stream.highest_granularity,
            'datapoint': self._format_datapoint(datapoint),
        }

        # Call test callback after everything
        if self._test_callback is not None:
            self._test_callback(**ret)

        return ret

    def append(self, stream_id, value, timestamp=None, check_timestamp=True):
        """
        Appends a datapoint into the datastream.

        :param stream_id: Stream identifier
        :param value: Datapoint value
        :param timestamp: Datapoint timestamp, must be equal or larger (newer) than the latest one, monotonically increasing (optional)
        :param check_timestamp: Check if timestamp is equal or larger (newer) than the latest one (default: true)
        :return: A dictionary containing `stream_id`, `granularity`, and `datapoint`
        """

        stream = None
        try:
            stream = Stream.objects.get(external_id=uuid.UUID(stream_id))
        except Stream.DoesNotExist:
            raise exceptions.StreamNotFound

        # Appending is not allowed for derived streams
        if stream.derived_from is not None:
            raise exceptions.AppendToDerivedStreamNotAllowed

        return self._append(stream, value, timestamp, check_timestamp)

    def _format_datapoint(self, datapoint):
        """
        Formats a datapoint so it is suitable for user output.

        :param datapoint: Raw datapoint from MongoDB database
        :return: A properly formatted datapoint
        """

        return {
            't': datapoint.get('t', datapoint['_id'].generation_time),
            'v': datapoint['v']
        }

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

        try:
            stream = Stream.objects.get(external_id=uuid.UUID(stream_id))
        except Stream.DoesNotExist:
            raise exceptions.StreamNotFound

        # This should already be checked at the API level
        assert granularity in api.Granularity.values

        if granularity > stream.highest_granularity:
            granularity = stream.highest_granularity

        if granularity == stream.highest_granularity:
            # On highest granularity downsamplers are not used
            value_downsamplers = None
            time_downsamplers = None

        downsamplers = []

        if value_downsamplers is not None:
            value_downsamplers = set(value_downsamplers)

            # This should already be checked at the API level
            assert value_downsamplers <= self.value_downsamplers, value_downsamplers - self.value_downsamplers

            downsamplers += ['v.%s' % api.VALUE_DOWNSAMPLERS[d] for d in value_downsamplers]

        if time_downsamplers is not None:
            time_downsamplers = set(time_downsamplers)

            # This should already be checked at the API level
            assert time_downsamplers <= self.time_downsamplers, time_downsamplers - self.time_downsamplers

            downsamplers += ['t.%s' % api.TIME_DOWNSAMPLERS[d] for d in time_downsamplers]

        if downsamplers == []:
            downsamplers = None

        # Get the datapoints
        db = mongoengine.connection.get_db(DATABASE_ALIAS)
        collection = getattr(db.datapoints, granularity.name)

        # Round the start time to include the first granular value of the
        # corresponding interval (ie. values from 15:00 to 16:00 are stored at
        # the time 15:00 for hours granularity)
        if start_exclusive is not None:
            # This should already be checked at the API level
            assert start is None

            # We add one second and use non-strict greater-than to skip all
            # possible ObjectId values in a given "start" timestamp
            try:
                start_timestamp = granularity.round_timestamp(start_exclusive) + ONE_SECOND_TIMEDELTA
            except OverflowError:
                return Datapoints(self)
        else:
            start_timestamp = granularity.round_timestamp(start)

        if start_timestamp > self._max_timestamp:
            return Datapoints(self)

        start_timestamp = self._force_timestamp_range(start_timestamp)

        time_query = {
            '$gte': objectid.ObjectId.from_datetime(start_timestamp),
        }

        if end is not None or end_exclusive is not None:
            overflow = False

            # No need to round the end time as the last granularity is
            # automatically included
            if end_exclusive is not None:
                # This should already be checked at the API level
                assert end is None

                end_timestamp = end_exclusive
            else:
                # We add one second and use strict less-than to cover all
                # possible ObjectId values in a given "end" timestamp
                try:
                    end_timestamp = end + ONE_SECOND_TIMEDELTA
                except OverflowError:
                    overflow = True

            if not overflow:
                if end_timestamp <= self._min_timestamp:
                    return Datapoints(self)

                end_timestamp = self._force_timestamp_range(end_timestamp)

                # Optimization
                if end_timestamp <= start_timestamp:
                    return Datapoints(self)

                time_query.update({
                    '$lt': objectid.ObjectId.from_datetime(end_timestamp),
                })

        datapoints = collection.find({
            'm': stream.id,
            '_id': time_query,
        }, downsamplers).sort('_id', -1 if reverse else 1)

        return Datapoints(self, datapoints)

    def delete_streams(self, query_tags=None):
        """
        Deletes datapoints for all streams matching the specified
        query tags. If no query tags are specified, all downstream-related
        data is deleted from the backend.

        :param query_tags: Tags that should be matched to streams
        """

        if query_tags is None:
            db = mongoengine.connection.get_db(DATABASE_ALIAS)
            for granularity in api.Granularity.values:
                collection = getattr(db.datapoints, granularity.name)
                collection.drop()
            db.streams.drop()
        else:
            # TODO: Implement
            raise NotImplementedError

    def _last_timestamp(self, stream=None):
        """
        Returns timestamp of the last datapoint among all streams, or of the specified stream.
        """

        db = mongoengine.connection.get_db(DATABASE_ALIAS)

        if stream:
            granularity = stream.highest_granularity.name
            query = {'m': stream.id}
        else:
            # TODO: This is not necessary true, there could be a newer datapoint among streams which do not have highest granularity == api.Granularity.values[0] granularity
            granularity = api.Granularity.values[0].name
            query = {}

        collection = getattr(db.datapoints, granularity)

        try:
            # _id is indexed descending, first value is the last inserted
            return collection.find(query).sort('_id', -1).next()['_id'].generation_time
        except StopIteration:
            return datetime.datetime.min.replace(tzinfo=pytz.utc)

    def downsample_streams(self, query_tags=None, until=None):
        """
        Requests the backend to downsample all streams matching the specified
        query tags. Once a time range has been downsampled, new datapoints
        cannot be added to it anymore.

        :param query_tags: Tags that should be matched to streams
        :param until: Timestamp until which to downsample, not including datapoints
                      at a timestamp (optional, otherwise all until the current time)
        :return: A list of dictionaries containing `stream_id`, `granularity`, and `datapoint`
                 for each datapoint created while downsampling
        """

        if until is None:
            # TODO: Hm, this is not completely correct, because client time could be different than server time, we should allow use where client does not have to specify any timestamp and everything is done on the server
            until = datetime.datetime.now(pytz.utc) + self._time_offset

        new_datapoints = []

        for stream in self._get_stream_queryset(query_tags):
            new_datapoints += self._downsample_check(stream, until)

        return new_datapoints

    def _downsample_check(self, stream, until_timestamp):
        """
        Checks if we need to perform any stream downsampling. In case it is needed,
        we perform downsampling.

        :param stream: Stream instance
        :param until_timestamp: Timestamp of the newly inserted datum
        """

        new_datapoints = []

        for granularity in api.Granularity.values[api.Granularity.values.index(stream.highest_granularity) + 1:]:
            state = stream.downsample_state.get(granularity.name, None)
            rounded_timestamp = granularity.round_timestamp(until_timestamp)
            # TODO: Why "can't compare offset-naive and offset-aware datetimes" is sometimes thrown here?
            if state is None or state.timestamp is None or rounded_timestamp > state.timestamp:
                new_datapoints += self._downsample(stream, granularity, rounded_timestamp)

        return new_datapoints

    def _generate_object_id(self, timestamp=None):
        """
        Generates a new ObjectId.

        :param timestamp: Desired timestamp (optional)
        """

        oid = objectid.EMPTY

        # 4 bytes current time
        if timestamp is not None:
            oid += struct.pack('>i', int(calendar.timegm(timestamp.utctimetuple())))
        else:
            # We add possible _time_offset which is used in debugging to artificially change time
            oid += struct.pack('>i', int(time.time() + self._time_offset.total_seconds()))

        # 3 bytes machine
        oid += objectid.ObjectId._machine_bytes

        # 2 bytes pid
        oid += struct.pack('>H', os.getpid() % 0xFFFF)

        # 3 bytes inc
        objectid.ObjectId._inc_lock.acquire()
        oid += struct.pack('>i', objectid.ObjectId._inc)[1:4]
        objectid.ObjectId._inc = (objectid.ObjectId._inc + 1) % 0xFFFFFF
        objectid.ObjectId._inc_lock.release()

        return objectid.ObjectId(oid)

    def _generate_timed_stream_object_id(self, timestamp, stream_id):
        """
        Generates a unique ObjectID for a specific timestamp and stream identifier.

        :param timestamp: Desired timestamp
        :param stream_id: 8-byte packed stream identifier
        :return: A valid object identifier
        """

        oid = objectid.EMPTY
        # 4 bytes timestamp
        oid += struct.pack('>i', int(calendar.timegm(timestamp.utctimetuple())))
        # 8 bytes of packed stream identifier
        oid += stream_id
        return objectid.ObjectId(oid)

    def _downsample(self, stream, granularity, until_timestamp):
        """
        Performs downsampling on the given stream and granularity.

        :param stream: Stream instance
        :param granularity: Lower granularity to downsample into
        :param until_timestamp: Timestamp until which to downsample, not including datapoints
                                at a timestamp, rounded to the specified granularity
        """

        assert granularity.round_timestamp(until_timestamp) == until_timestamp
        assert stream.downsample_state.get(granularity.name, None) is None or stream.downsample_state.get(granularity.name).timestamp is None or until_timestamp > stream.downsample_state.get(granularity.name).timestamp

        self._supported_timestamp_range(until_timestamp)

        db = mongoengine.connection.get_db(DATABASE_ALIAS)

        # Determine the interval (one or more granularity periods) that needs downsampling
        datapoints = getattr(db.datapoints, stream.highest_granularity.name)
        state = stream.downsample_state[granularity.name]
        if state.timestamp is not None:
            datapoints = datapoints.find({
                'm': stream.id,
                '_id': {
                    '$gte': objectid.ObjectId.from_datetime(state.timestamp),
                    # It is really important that current_timestamp is correctly rounded for given granularity,
                    # because we want that only none or all datapoints for granularity periods are selected
                    '$lt': objectid.ObjectId.from_datetime(until_timestamp),
                },

            })
        else:
            # All datapoints should be selected as we obviously haven't done any downsampling yet
            # Initially, for the first granularity period, _downsample function is called for every new datapoint,
            # but query here is returning no datapoints until datapoint for next granularity period is appended and
            # its rounded timestamp moves to that next granularity period
            datapoints = datapoints.find({
                'm': stream.id,
                '_id': {
                    # It is really important that current_timestamp is correctly rounded for given granularity,
                    # because we want that only none or all datapoints for granularity periods are selected
                    '$lt': objectid.ObjectId.from_datetime(until_timestamp),
                },
            })

        # Construct downsampler instances
        value_downsamplers = []
        for downsampler in ValueDownsamplers.values:
            if downsampler.name in stream.value_downsamplers:
                value_downsamplers.append(downsampler())

        time_downsamplers = []
        for downsampler in TimeDownsamplers.values:
            time_downsamplers.append(downsampler())

        # Pack stream identifier to be used for object id generation
        stream_id = struct.pack('>Q', stream.id)

        new_datapoints = []

        def store_downsampled_datapoint(timestamp):
            value = {}
            time = {}
            for x in value_downsamplers:
                x.finish(value)
            for x in time_downsamplers:
                x.finish(time)

            for x in value_downsamplers:
                x.postprocess(value)
                x.initialize()
            for x in time_downsamplers:
                x.postprocess(time)
                x.initialize()

            # Insert downsampled datapoint
            point_id = self._generate_timed_stream_object_id(timestamp, stream_id)
            datapoint = {'_id': point_id, 'm': stream.id, 'v': value, 't': time}

            # We want to process each granularity period only once, we want it to fail if there is an error in this
            # TODO: We should probably create some API function which reprocesses everything and fixes any inconsistencies
            downsampled_points.insert(datapoint, w=1)

            # Process contributions to other streams
            self._process_contributes_to(stream, datapoint['t'], value, granularity)

            new_datapoints.append({
                'stream_id': str(stream.external_id),
                'granularity': granularity,
                'datapoint': self._format_datapoint(datapoint),
            })

        downsampled_points = getattr(db.datapoints, granularity.name)
        current_granularity_period_timestamp = None
        for datapoint in datapoints.sort('_id', 1):
            ts = datapoint['_id'].generation_time
            new_granularity_period_timestamp = granularity.round_timestamp(ts)
            if current_granularity_period_timestamp is None:
                # TODO: Use generator here, not concatenation
                for x in value_downsamplers + time_downsamplers:
                    x.initialize()
            elif current_granularity_period_timestamp != new_granularity_period_timestamp:
                # All datapoints for current granularity period have been processed, we store the new datapoint
                # This happens when interval ("datapoints" query) contains multiple not-yet-downsampled granularity periods
                store_downsampled_datapoint(current_granularity_period_timestamp)

            current_granularity_period_timestamp = new_granularity_period_timestamp

            # Update all downsamplers for the current datapoint
            for x in value_downsamplers:
                x.update(datapoint['v'])
            for x in time_downsamplers:
                x.update(int(calendar.timegm(ts.utctimetuple())))

        # Store the last downsampled datapoint as well
        # The "datapoints" query above assures that just none or all datapoints for each granularity period
        # is retrieved so we know that whole granularity period has been processed and we can now store its datapoint here
        if current_granularity_period_timestamp is not None:
            store_downsampled_datapoint(current_granularity_period_timestamp)

            # At the end, update the timestamp until which we have processed everything
            state.timestamp = until_timestamp
            stream.save()

        # And call test callback for all new datapoints
        if self._test_callback is not None:
            for kwargs in new_datapoints:
                self._test_callback(**kwargs)

        return new_datapoints

    def _backprocess_stream(self, stream):
        """
        Performs backprocessing of a single stream.

        :param stream: Stream to backprocess
        """

        stream.reload()
        if not stream.pending_backprocess:
            return

        # TODO: Clear derived stream on all granularities (we should probably add truncate_stream to API)

        # Obtain the list of dependent streams and backprocess any pending streams
        src_streams = []
        for sstream in Stream.objects.filter(id__in=stream.derived_from.stream_ids):
            if sstream.pending_backprocess:
                self._backprocess_stream(sstream)

            descriptor = sstream.contributes_to[str(stream.id)]
            src_streams.append((
                sstream,
                descriptor,
                # TODO: Implement a _get_data that operates directly on Stream instances
                iter(self.get_data(unicode(sstream.external_id), descriptor.granularity, self._min_timestamp))
            ))

        # Ensure that streams are in the proper order
        src_streams.sort(key=lambda x: stream.derived_from.stream_ids.index(x[0].id))

        # Apply streams in order until they all run out of datapoints
        def get_timestamp(datapoint):
            if isinstance(datapoint['t'], datetime.datetime):
                return datapoint['t']

            return datapoint['t'][api.TIME_DOWNSAMPLERS['last']]

        lookahead = {}
        while True:
            # Determine the current point in time so we can properly apply datapoints
            current_ts = None
            for sstream, descriptor, data in src_streams:
                datapoint = lookahead.get(sstream)
                if datapoint is None:
                    try:
                        datapoint = data.next()
                    except StopIteration:
                        continue

                ts = get_timestamp(datapoint)
                if current_ts is None or ts < current_ts:
                    current_ts = ts
                lookahead[sstream] = datapoint

            if current_ts is None:
                break

            # Apply datapoints in proper order
            for sstream, descriptor, data in src_streams:
                datapoint = lookahead.get(sstream)
                if datapoint is not None:
                    derive_operator = DerivationOperators.get(descriptor.op)(self, stream, **descriptor.args)

                    while get_timestamp(datapoint) <= current_ts:
                        derive_operator.update(sstream, datapoint['t'], datapoint['v'], name=descriptor.name)
                        try:
                            datapoint = data.next()
                        except StopIteration:
                            datapoint = None
                            break

                    lookahead[sstream] = datapoint

        # Mark the derived stream as ready for normal processing (unpause)
        stream.pending_backprocess = False
        stream.save()

    def backprocess_streams(self, query_tags=None):
        """
        Performs backprocessing of any derived streams that are marked as
        'pending backprocess'.

        :param query_tags: Tags that should be matched to streams
        """

        # Select all streams that are pending backprocessing
        for stream in self._get_stream_queryset(query_tags).filter(pending_backprocess=True):
            self._backprocess_stream(stream)
