import calendar
import collections
import datetime
import decimal
import inspect
import math
import numbers
import os
import struct
import sys
import threading
import time as time_module
import uuid
import warnings

import pytz

import pymongo
from bson import objectid, son

import mongoengine

from .. import api, exceptions, utils

DATABASE_ALIAS = 'datastream'

# The largest/smallest integer that can be stored in MongoDB; larger values need to use floats/strings
MAXIMUM_INTEGER = 2 ** 63 - 1
MINIMUM_INTEGER = -2 ** 63

ZERO_TIMEDELTA = datetime.timedelta()
ONE_SECOND_TIMEDELTA = datetime.timedelta(seconds=1)

DECIMAL_PRECISION = 64

MAINTENANCE_LOCK_DURATION = 120
DOWNSAMPLE_SAFETY_MARGIN = 10


def deserialize_numeric_value(value, use_decimal=False):
    """
    Attempts to parse a value as a number and raises a type error in case
    a conversion is not possible.
    """

    if value is None:
        return

    if use_decimal:
        # Compute using arbitrary precision numbers.
        with decimal.localcontext() as ctx:
            ctx.prec = DECIMAL_PRECISION

            if isinstance(value, (int, long)):
                return value
            elif isinstance(value, float):
                decimal_value = +float_to_decimal(value)
                int_value = int(decimal_value)
                if int_value == decimal_value:
                    return int_value
                else:
                    return decimal_value
            elif isinstance(value, decimal.Decimal):
                return value
            elif isinstance(value, basestring):
                try:
                    return int(value)
                except ValueError:
                    try:
                        return +decimal.Decimal(value)
                    except decimal.InvalidOperation:
                        raise TypeError
            else:
                raise TypeError
    else:
        # Compute using floats.
        if isinstance(value, (int, long, float)):
            return value
        elif isinstance(value, decimal.Decimal):
            return float(value)
        else:
            raise TypeError


def serialize_numeric_value(value, use_decimal=False):
    """
    Attempts to properly store a numeric value and raises a type error in case
    a conversion is not possible.
    """

    if value is None:
        return
    elif isinstance(value, float):
        return value

    if use_decimal:
        # Compute using arbitrary precision numbers.
        if isinstance(value, (int, long)):
            if (value > MAXIMUM_INTEGER or value < MINIMUM_INTEGER):
                return str(value)
            else:
                return value
        elif isinstance(value, decimal.Decimal):
            int_value = int(value)
            if MINIMUM_INTEGER <= value <= MAXIMUM_INTEGER and int_value == value:
                return int_value
            else:
                float_value = float(value)
                if value == float_to_decimal(float_value):
                    return float_value
                else:
                    return str(value)
        else:
            raise TypeError
    else:
        # Compute using floats.
        if isinstance(value, (int, long)):
            if value > MAXIMUM_INTEGER or value < MINIMUM_INTEGER:
                return float(value)

            return value
        elif isinstance(value, decimal.Decimal):
            int_value = int(value)
            if MINIMUM_INTEGER <= value <= MAXIMUM_INTEGER and int_value == value:
                return int_value

            return float(value)
        else:
            raise TypeError


def middle_timestamp(dt, granularity):
    """
    Returns a timestamp that is located in the middle of the granularity
    bucket.

    :param dt: Timestamp rounded to specific granularity
    :param granularity: Granularity instance
    :return: Timestamp in the middle of the granularity bucket
    """

    return dt + datetime.timedelta(seconds=granularity.duration_in_seconds() // 2)


def total_seconds(delta):
    if sys.version_info < (2, 7):
        return (delta.microseconds + (delta.seconds + delta.days * 24 * 3600) * 1e6) / 1e6
    else:
        return delta.total_seconds()


def float_to_decimal(f):
    if sys.version_info < (2, 7):
        # See http://docs.python.org/release/2.6.7/library/decimal.html#decimal-faq
        n, d = f.as_integer_ratio()
        numerator, denominator = decimal.Decimal(n), decimal.Decimal(d)
        ctx = decimal.Context(prec=60)
        result = ctx.divide(numerator, denominator)
        while ctx.flags[decimal.Inexact]:
            ctx.flags[decimal.Inexact] = False
            ctx.prec *= 2
            result = ctx.divide(numerator, denominator)
        return result
    else:
        return decimal.Decimal(f)


def to_decimal(v):
    if isinstance(v, float):
        return float_to_decimal(v)
    else:
        return decimal.Decimal(v)


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
        supports_types = ()

        def initialize(self):
            pass

        def update(self, time, value):
            pass

        def finish(self, output, timestamp, granularity):
            pass

        def postprocess(self, values):
            pass

    @utils.class_property
    def values(cls): # pylint: disable=no-self-argument
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

        def __init__(self, stream):
            self.key = api.VALUE_DOWNSAMPLERS[self.name]
            self.stream = stream

    class Count(_Base):
        """
        Counts the number of datapoints.
        """

        name = 'count'
        supports_types = ('numeric', 'graph', 'nominal')

        def initialize(self):
            self.count = 0

        def update(self, time, value):
            if isinstance(value, dict) and self.key in value:
                value = value[self.key]
                if value is None:
                    value = 0
            else:
                value = 1 if value is not None else 0

            self.count += value

        def finish(self, output, timestamp, granularity):
            assert self.key not in output
            output[self.key] = self.stream.serialize_numeric_value(self.count)

    class Sum(_Base):
        """
        Sums the datapoint values.
        """

        name = 'sum'
        supports_types = ('numeric',)

        def initialize(self):
            self.sum = None

        def update(self, time, value):
            if isinstance(value, dict) and self.key in value:
                value = value[self.key]

            if value is None:
                return

            if self.sum is None:
                self.sum = 0

            try:
                if self.stream.value_type_options.get('high_accuracy', False):
                    with decimal.localcontext() as ctx:
                        ctx.prec = DECIMAL_PRECISION
                        self.sum += deserialize_numeric_value(value, use_decimal=True)
                else:
                    self.sum += deserialize_numeric_value(value)
            except TypeError:
                warnings.warn(exceptions.InvalidValueWarning("Unsupported non-numeric value '%s' for 'sum' downsampler." % repr(value)))

        def finish(self, output, timestamp, granularity):
            assert self.key not in output
            output[self.key] = self.stream.serialize_numeric_value(self.sum)

    class SumSquares(_Base):
        """
        Sums the squared datapoint values.
        """

        name = 'sum_squares'
        supports_types = ('numeric',)

        def initialize(self):
            self.sum = None

        def update(self, time, value):
            if isinstance(value, dict) and self.key in value:
                value = value[self.key]
                square = False
            else:
                square = True

            if value is None:
                return

            if self.sum is None:
                self.sum = 0

            try:
                if self.stream.value_type_options.get('high_accuracy', False):
                    with decimal.localcontext() as ctx:
                        ctx.prec = DECIMAL_PRECISION
                        value = deserialize_numeric_value(value, use_decimal=True)
                        self.sum += value * value if square else value
                else:
                    value = deserialize_numeric_value(value)
                    self.sum += value * value if square else value
            except TypeError:
                warnings.warn(exceptions.InvalidValueWarning("Unsupported non-numeric value '%s' for 'sum_squares' downsampler." % repr(value)))

        def finish(self, output, timestamp, granularity):
            assert self.key not in output
            output[self.key] = self.stream.serialize_numeric_value(self.sum)

    class Min(_Base):
        """
        Stores the minimum of the datapoint values.
        """

        name = 'min'
        supports_types = ('numeric',)

        def initialize(self):
            self.min = None

        def update(self, time, value):
            if isinstance(value, dict) and self.key in value:
                value = value[self.key]

            if value is None:
                return

            try:
                value = self.stream.deserialize_numeric_value(value)
            except TypeError:
                warnings.warn(exceptions.InvalidValueWarning("Unsupported non-numeric value '%s' for 'min' downsampler." % repr(value)))
                return

            if self.min is None:
                self.min = value
            else:
                self.min = min(self.min, value)

        def finish(self, output, timestamp, granularity):
            assert self.key not in output
            output[self.key] = self.stream.serialize_numeric_value(self.min)

    class Max(_Base):
        """
        Stores the maximum of the datapoint values.
        """

        name = 'max'
        supports_types = ('numeric',)

        def initialize(self):
            self.max = None

        def update(self, time, value):
            if isinstance(value, dict) and self.key in value:
                value = value[self.key]

            if value is None:
                return

            try:
                value = self.stream.deserialize_numeric_value(value)
            except TypeError:
                warnings.warn(exceptions.InvalidValueWarning("Unsupported non-numeric value '%s' for 'max' downsampler." % repr(value)))
                return

            if self.max is None:
                self.max = value
            else:
                self.max = max(self.max, value)

        def finish(self, output, timestamp, granularity):
            assert self.key not in output
            output[self.key] = self.stream.serialize_numeric_value(self.max)

    class Mean(_Base):
        """
        Computes the mean from sum and count (postprocess).
        """

        name = 'mean'
        dependencies = ('sum', 'count')
        supports_types = ('numeric',)

        def postprocess(self, values):
            assert 'm' not in values
            n = self.stream.deserialize_numeric_value(values[api.VALUE_DOWNSAMPLERS['count']])

            if n > 0:
                s = self.stream.deserialize_numeric_value(values[api.VALUE_DOWNSAMPLERS['sum']])

                if s is None:
                    values[self.key] = None
                    return

                if self.stream.value_type_options.get('high_accuracy', False):
                    with decimal.localcontext() as ctx:
                        ctx.prec = DECIMAL_PRECISION
                        values[self.key] = serialize_numeric_value(to_decimal(s) / n, use_decimal=True)
                else:
                    values[self.key] = serialize_numeric_value(float(s) / n)
            else:
                values[self.key] = None

    class StdDev(_Base):
        """
        Computes the standard deviation from sum, count and sum squares
        (postprocess).
        """

        name = 'std_dev'
        dependencies = ('sum', 'count', 'sum_squares')
        supports_types = ('numeric',)

        def postprocess(self, values):
            assert self.key not in values
            n = self.stream.deserialize_numeric_value(values[api.VALUE_DOWNSAMPLERS['count']])

            if n == 0:
                values[self.key] = None
            elif n == 1:
                values[self.key] = 0
            else:
                s = self.stream.deserialize_numeric_value(values[api.VALUE_DOWNSAMPLERS['sum']])
                ss = self.stream.deserialize_numeric_value(values[api.VALUE_DOWNSAMPLERS['sum_squares']])

                if s is None or ss is None:
                    values[self.key] = None
                    return

                try:
                    if self.stream.value_type_options.get('high_accuracy', False):
                        with decimal.localcontext() as ctx:
                            ctx.prec = DECIMAL_PRECISION
                            try:
                                e_x = to_decimal(s) / n
                                e_x *= e_x
                                e_x2 = to_decimal(ss) / n
                                e_diff = e_x2 - e_x
                                if abs(e_diff) < decimal.Decimal('0.00000001'):
                                    e_diff = decimal.Decimal(0)

                                values[self.key] = serialize_numeric_value(e_diff.sqrt(), use_decimal=True)
                            except decimal.InvalidOperation:
                                raise ValueError
                    else:
                        e_x = float(s) / n
                        e_x *= e_x
                        e_x2 = float(ss) / n
                        e_diff = e_x2 - e_x
                        if abs(e_diff) < 10e-7:
                            e_diff = 0

                        values[self.key] = serialize_numeric_value(math.sqrt(e_diff))
                except ValueError:
                    warnings.warn(exceptions.InvalidValueWarning("Failed to compute standard deviation, setting to zero (stream %d)." % self.stream.pk))
                    values[self.key] = 0


class TimeDownsamplers(DownsamplersBase):
    """
    A container of downsampler classes for datapoint timestamps.
    """

    class _Base(DownsamplersBase._Base):
        """
        Base class for time downsamplers.
        """

        def __init__(self, stream):
            self.key = api.TIME_DOWNSAMPLERS[self.name]
            self.stream = stream

        def _from_datetime(self, dt):
            return int(calendar.timegm(dt.utctimetuple()))

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

        def update(self, time, value):
            if isinstance(time, dict) and self.key in time:
                time = self._from_datetime(time[self.key])
                count = value[api.VALUE_DOWNSAMPLERS['count']]

                self.count += count
                self.sum += time * count
            else:
                self.count += 1 if value is not None else 0
                self.sum += self._from_datetime(time) if value is not None else 0

        def finish(self, output, timestamp, granularity):
            assert self.key not in output
            if self.count > 0:
                output[self.key] = self._to_datetime(float(self.sum) / self.count)
            else:
                output[self.key] = middle_timestamp(timestamp, granularity)

    class First(_Base):
        """
        Stores the first timestamp in the interval.
        """

        name = 'first'

        def initialize(self):
            self.first = None

        def update(self, time, value):
            if isinstance(time, dict) and self.key in time:
                time = time[self.key]

            if self.first is None:
                self.first = time

        def finish(self, output, timestamp, granularity):
            assert self.key not in output
            output[self.key] = self.first

    class Last(_Base):
        """
        Stores the last timestamp in the interval.
        """

        name = 'last'

        def initialize(self):
            self.last = None

        def update(self, time, value):
            if isinstance(time, dict) and self.key in time:
                time = time[self.key]

            self.last = time

        def finish(self, output, timestamp, granularity):
            assert self.key not in output
            output[self.key] = self.last


class DerivationOperators(object):
    """
    A container for derivation operator classes.
    """

    class _Base(object):
        """
        Base class for derivation operators.
        """

        name = None
        supports_types = ()

        def __init__(self, backend, dst_stream, **parameters):
            """
            Class constructor.

            :param backend: A valid MongoDB backend instance
            :param dst_stream: Derived stream instance
            """

            self._backend = backend
            self._stream = dst_stream
            self._appended = False

        def _append(self, *args, **kwargs):
            """
            A wrapper around the backend's _append which also updates the local
            appended status.
            """

            self._appended = True
            self._backend._append(*args, **kwargs)

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

        def update_last_timestamp(self, timestamp):
            """
            Updates the last timestamp even if the derivation operator did not insert
            any new datapoints.

            :param timestamp: Timestamp of the last processed datapoint
            """

            try:
                self._backend._update_last_timestamp(self._stream, timestamp, True)
            except exceptions.InvalidTimestamp:
                # This might happen in case of concurrent inserts.
                pass

    @classmethod
    def get(cls, operator):
        if not hasattr(cls, '_values'):
            cls._values = dict([
                (getattr(cls, name).name, getattr(cls, name))
                for name in cls.__dict__
                if name != 'get' and inspect.isclass(getattr(cls, name)) and getattr(cls, name) is not cls._Base and issubclass(getattr(cls, name), cls._Base)
            ])

        return cls._values[operator]

    class Sum(_Base):
        """
        Computes the sum of multiple streams.
        """

        name = 'sum'
        supports_types = ('numeric',)

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

            for stream_dsc in src_streams:
                # Ensure that the input streams are of correct type.
                if stream_dsc['stream'].value_type != 'numeric':
                    raise exceptions.IncompatibleTypes("All streams for 'sum' must be of 'numeric' type!")

                # Ensure that source stream granularity matches our highest granularity.
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
                    count = src_stream.deserialize_numeric_value(value[api.VALUE_DOWNSAMPLERS['count']])
                    value = src_stream.deserialize_numeric_value(value[api.VALUE_DOWNSAMPLERS['sum']])

                    if src_stream.value_type_options.get('high_accuracy', False):
                        value = to_decimal(value) / count
                    else:
                        value = float(value) / count

                    timestamp = timestamp[api.TIME_DOWNSAMPLERS['last']]
                except KeyError:
                    pass

            # First ensure that we have a numeric value, as we can't do anything with other values
            if value is not None:
                try:
                    value = src_stream.deserialize_numeric_value(value)
                except TypeError:
                    warnings.warn(exceptions.InvalidValueWarning("Unsupported non-numeric value '%s' for 'sum' operator." % repr(value)))
                    return

            rounded_ts = self._stream.highest_granularity.round_timestamp(timestamp)
            ts_key = rounded_ts.strftime("%Y%m%d%H%M%S")
            # TODO: This uses pymongo because MongoEngine can't handle multiple levels of dynamic fields
            # (in addition ME can't handle queries with field names that look like numbers)
            db = mongoengine.connection.get_db(DATABASE_ALIAS)
            db.streams.update({'_id': self._stream.id}, {
                '$set': {('derive_state.%s.%s' % (ts_key, src_stream.id)): self._stream.serialize_numeric_value(value)}
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
                    values = [self._stream.deserialize_numeric_value(x) for x in self._stream.derive_state[ts_key].values() if x is not None]
                    s = sum(values) if values else None
                    self._append(self._stream, s, rounded_ts)
                except exceptions.InvalidTimestamp:
                    pass

                # Any keys that are less than or equal to ts_key are safe to remove as we have just
                # appended something, and no threads can insert data between datapoints in the past
                unset = {}
                for key in self._stream.derive_state.keys():
                    if key <= ts_key:
                        unset['derive_state.%s' % key] = ''

                db.streams.update({'_id': self._stream.id}, {'$unset': unset}, w=1)

        def update_last_timestamp(self, timestamp):
            """
            Updates the last timestamp even if the derivation operator did not insert
            any new datapoints.

            :param timestamp: Timestamp of the last processed datapoint
            """

            # Do not update the last timestamp in case of the sum operator. Sum needs to collect
            # multiple input datapoints before creating one output datapoint and if we update the
            # timestamp too soon, actual datapoints will not be inserted.
            pass

    class Derivative(_Base):
        """
        Computes the derivative of a stream.
        """

        name = 'derivative'
        supports_types = ('numeric',)

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

            if src_streams[0]['stream'].value_type != 'numeric':
                raise exceptions.IncompatibleTypes("The data stream for 'derivative' must be of 'numeric' type!")

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

            if value is None:
                # In case a null value is passed, we carry it on to the derived stream
                self._append(self._stream, None, timestamp)
                self._stream.derive_state = None
                self._stream.save()
                return

            # First ensure that we have a numeric value, as we can't do anything with other values
            try:
                value = src_stream.deserialize_numeric_value(value)
            except TypeError:
                warnings.warn(exceptions.InvalidValueWarning("Unsupported non-numeric value '%s' for 'derivative' operator." % repr(value)))
                return

            if self._stream.derive_state is not None:
                # We already have a previous value, compute derivative
                delta = total_seconds(timestamp - self._stream.derive_state['t'])

                if delta != 0:
                    if self._stream.value_type_options.get('high_accuracy', False):
                        derivative = to_decimal(value - src_stream.deserialize_numeric_value(self._stream.derive_state['v'])) / to_decimal(delta)
                    else:
                        derivative = float(value - src_stream.deserialize_numeric_value(self._stream.derive_state['v'])) / delta
                    self._append(self._stream, derivative, timestamp)
                else:
                    warnings.warn(exceptions.InvalidValueWarning("Zero time-delta in derivative computation (stream %s)!" % src_stream.id))

            self._stream.derive_state = {'v': src_stream.serialize_numeric_value(value), 't': timestamp}
            self._stream.save()

    class CounterReset(_Base):
        """
        Computes the counter reset stream.
        """

        name = 'counter_reset'
        supports_types = ('nominal',)

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

            # The counter reset operator supports only one source stream.
            if len(src_streams) > 1:
                raise exceptions.InvalidOperatorArguments

            if src_streams[0]['stream'].value_type != 'numeric':
                raise exceptions.IncompatibleTypes("The data stream for 'counter_reset' must be of 'numeric' type!")

            return super(DerivationOperators.CounterReset, cls).get_parameters(src_streams, dst_stream, **arguments)

        def update(self, src_stream, timestamp, value, name=None):
            """
            Called when a new datapoint is added to one of the source streams.

            :param src_stream: Source stream instance
            :param timestamp: Newly inserted datapoint timestamp
            :param value: Newly inserted datapoint value
            :param name: Stream name when specified
            """

            if value is None:
                return

            # First ensure that we have a numeric value, as we can't do anything with other values
            try:
                value = src_stream.deserialize_numeric_value(value)
            except TypeError:
                warnings.warn(exceptions.InvalidValueWarning("Unsupported non-numeric value '%s' for 'counter_reset' operator." % repr(value)))
                return

            if self._stream.derive_state is not None:
                # We already have a previous value, check what value needs to be inserted
                # TODO: Add a configurable maximum counter value so overflows can be detected
                if src_stream.deserialize_numeric_value(self._stream.derive_state['v']) > value:
                    self._append(self._stream, 1, timestamp)

            self._stream.derive_state = {'v': src_stream.serialize_numeric_value(value), 't': timestamp}
            self._stream.save()

    class CounterDerivative(_Base):
        """
        Computes the derivative of a monotonically increasing counter stream.
        """

        name = 'counter_derivative'
        supports_types = ('numeric',)

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
            if src_streams[0].get('name', None) != 'reset':
                raise exceptions.InvalidOperatorArguments("'counter_derivative' requires 'reset' to be the first input stream!")

            if src_streams[1].get('name', None) is not None:
                raise exceptions.InvalidOperatorArguments("'counter_derivative' requires an unnamed data stream!")

            if src_streams[1]['stream'].value_type != 'numeric':
                raise exceptions.IncompatibleTypes("The unnamed data stream for 'counter_derivative' must be of 'numeric' type!")

            return super(DerivationOperators.CounterDerivative, cls).get_parameters(src_streams, dst_stream, **arguments)

        def update(self, src_stream, timestamp, value, name=None):
            """
            Called when a new datapoint is added to one of the source streams.

            :param src_stream: Source stream instance
            :param timestamp: Newly inserted datapoint timestamp
            :param value: Newly inserted datapoint value
            :param name: Stream name when specified
            """

            # First ensure that we have a numeric value, as we can't do anything with other values
            if value is None:
                # In case a null value is passed, we carry it on to the derived stream
                self._append(self._stream, None, timestamp)
                self._stream.derive_state = None
                self._stream.save()
                return

            try:
                value = src_stream.deserialize_numeric_value(value)
            except TypeError:
                warnings.warn(exceptions.InvalidValueWarning("Unsupported non-numeric value '%s' for 'counter_derivative' operator." % repr(value)))
                return

            if name is None:
                # A data value has just been added
                if self._stream.derive_state is not None:
                    # We already have a previous value, compute derivative
                    v1 = src_stream.deserialize_numeric_value(self._stream.derive_state['v'])
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
                        tdelta = total_seconds(timestamp - self._stream.derive_state['t'])
                        if tdelta != 0:
                            if self._stream.value_type_options.get('high_accuracy', False):
                                derivative = vdelta / to_decimal(tdelta)
                            else:
                                derivative = float(vdelta) / tdelta
                            self._append(self._stream, derivative, timestamp)
                        else:
                            warnings.warn(exceptions.InvalidValueWarning("Zero time-delta in derivative computation (stream %s)!" % src_stream.id))

                self._stream.derive_state = {'v': src_stream.serialize_numeric_value(value), 't': timestamp}
            elif name == "reset" and value:
                # Value may be any value which evaluates to true. This signals that state should be reset.
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


class Streams(api.Streams):
    def __init__(self, datastream, queryset=None):
        self.datastream = datastream
        self.queryset = queryset

    def batch_size(self, batch_size):
        if self.queryset is not None:
            self.queryset._cursor_obj.batch_size(batch_size)

    def count(self):
        if self.queryset is None:
            return 0

        return self.queryset.count()

    def __iter__(self):
        if self.queryset is None:
            return

        for stream in self.queryset:
            yield self.datastream._get_stream_tags(stream)

    def __getitem__(self, key):
        if self.queryset is None:
            raise IndexError

        if isinstance(key, slice):
            return Streams(self.datastream, queryset=self.queryset.__getitem__(key))
        elif isinstance(key, (int, long)):
            return self.datastream._get_stream_tags(self.queryset.__getitem__(key))
        else:
            raise TypeError

    def _get_backend_cursor(self):
        if self.queryset is None:
            return None

        return self.queryset._cursor_obj


class Datapoints(api.Datapoints):
    def __init__(self, datastream, stream, cursor=None, empty_time=False):
        self.datastream = datastream
        self.stream = stream
        # Cursor might be None when we are optimizing a query and know in advance that there will be no datapoints.
        # Then we return Datapoints object with cursor set to None. Behavior of such object should be the same
        # as if we would do a query which would be returning no values.
        self.cursor = cursor
        self.empty_time = empty_time

    def batch_size(self, batch_size):
        if self.cursor is not None:
            self.cursor.batch_size(batch_size)

    def count(self):
        if self.cursor is None:
            return 0

        return self.cursor.count(with_limit_and_skip=True)

    def __iter__(self):
        if self.cursor is None:
            return

        for datapoint in self.cursor:
            yield self.datastream._format_datapoint(self.stream, datapoint, self.empty_time)

    def __getitem__(self, key):
        if isinstance(key, slice):
            if self.cursor is None:
                # We just make a copy of the object, but do not raise an exception. This is to behave the same as
                # cursor would behave. Even if there are no values returned by the query, it does not raise an exception
                # when doing a slice. It raises an exception only when you are accessing the concrete index of the cursor.
                return Datapoints(self.datastream, self.stream, cursor=None, empty_time=self.empty_time)
            else:
                return Datapoints(self.datastream, self.stream, cursor=self.cursor.__getitem__(key), empty_time=self.empty_time)
        elif isinstance(key, (int, long)):
            if self.cursor is None:
                # Doing a concrete index access, there are no values, so raise an exception.
                raise IndexError
            else:
                return self.datastream._format_datapoint(self.stream, self.cursor.__getitem__(key), self.empty_time)
        else:
            raise TypeError

    def _get_backend_cursor(self):
        return self.cursor


class DownsampleState(mongoengine.EmbeddedDocument):
    timestamp = mongoengine.DateTimeField()

    meta = dict(
        allow_inheritance=False,
    )


class DerivedStreamDescriptor(mongoengine.EmbeddedDocument):
    stream_ids = mongoengine.ListField(mongoengine.IntField())
    external_stream_ids = mongoengine.ListField(mongoengine.UUIDField(binary=True))
    op = mongoengine.StringField()
    args = mongoengine.DynamicField()


class ContributesToStreamDescriptor(mongoengine.EmbeddedDocument):
    name = mongoengine.StringField()
    granularity = GranularityField()
    op = mongoengine.StringField()
    args = mongoengine.DynamicField()


class LastDatapoint(mongoengine.Document):
    stream_id = mongoengine.IntField(db_field='s')
    insertion_ts = mongoengine.DateTimeField(db_field='i')
    datapoint_ts = mongoengine.DateTimeField(db_field='d')

    meta = {
        'db_alias': DATABASE_ALIAS,
        'indexes': [
            {'fields': ['stream_id', 'insertion_ts']},
            {'fields': ['insertion_ts'], 'expireAfterSeconds': DOWNSAMPLE_SAFETY_MARGIN},
        ]
    }


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
    downsample_count = mongoengine.IntField(default=0)
    highest_granularity = GranularityField()
    derived_from = mongoengine.EmbeddedDocumentField(DerivedStreamDescriptor)
    derive_state = mongoengine.DynamicField()
    contributes_to = mongoengine.MapField(mongoengine.EmbeddedDocumentField(ContributesToStreamDescriptor))
    pending_backprocess = mongoengine.BooleanField()
    earliest_datapoint = mongoengine.DateTimeField()
    latest_datapoint = mongoengine.DateTimeField()
    value_type = mongoengine.StringField()
    value_type_options = mongoengine.DynamicField()
    tags = mongoengine.DictField()

    # Maintenance operations lock
    _lock_mt = mongoengine.DateTimeField(default=datetime.datetime.min)

    meta = dict(
        db_alias=DATABASE_ALIAS,
        collection='streams',
        indexes=['tags', 'external_id'],
        allow_inheritance=False,
    )

    def serialize_numeric_value(self, value):
        return serialize_numeric_value(
            value,
            use_decimal=self.value_type_options.get('high_accuracy', False),
        )

    def deserialize_numeric_value(self, value):
        return deserialize_numeric_value(
            value,
            use_decimal=self.value_type_options.get('high_accuracy', False),
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
        self.connection_settings = connection_settings

        self._switch_database(database_name)

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
        server_info = db.command('buildinfo')
        self._tokumx = 'tokumxVersion' in server_info

        for granularity in api.Granularity.values:
            if self._tokumx:
                # Create a primary key index to avoid seconday indices. Use LZMA compression.
                try:
                    db.create_collection(
                        'datapoints.%s' % granularity.name,
                        primaryKey=son.SON([('m', 1), ('_id', 1)]),
                        compression='lzma',
                    )
                except (pymongo.errors.CollectionInvalid, pymongo.errors.OperationFailure):
                    # Collection already exists.
                    pass
            else:
                collection = getattr(db.datapoints, granularity.name)
                collection.ensure_index([
                    ('m', pymongo.ASCENDING),
                    ('_id', pymongo.ASCENDING),
                ])

        # TODO: For some reason the indexes don't get created unless calling ensure_indexes
        Stream.ensure_indexes()
        LastDatapoint.ensure_indexes()

        # Only for testing, don't use!
        self._test_callback = None

        # Used only to artificially advance time when testing, don't use!
        self._time_offset = ZERO_TIMEDELTA

        # Used only for concurrency tests
        self._test_concurrency = threading.local()

    def _switch_database(self, database_name):
        # Very internal. Just for debugging and testing.

        # There can currently be only one connection to the database at a given moment, because
        # stupid Mongoengine links all documents to the database alias. Even more, if a connection
        # already exists with the same alias, then mongoengine.connect just returns the old connection,
        # even if it is connected to a different database than requested! So to make sure, we first
        # disconnect here, so that we get connected to the right database.
        mongoengine.connection.disconnect(DATABASE_ALIAS)

        # Settings are kept stored with old database name. Delete everything.
        if DATABASE_ALIAS in mongoengine.connection._connection_settings:
            del mongoengine.connection._connection_settings[DATABASE_ALIAS]

        # And documents store collections with a link to old database. Clear that.
        LastDatapoint._collection = None
        Stream._collection = None

        # Setup the database connection to MongoDB.
        mongoengine.connect(database_name, DATABASE_ALIAS, **self.connection_settings)

    def ensure_stream(self, query_tags, tags, value_downsamplers, highest_granularity, derive_from, derive_op, derive_args, value_type, value_type_options):
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

        try:
            stream = Stream.objects.get(**self._get_tag_query_dict(None, query_tags))

            # If a stream already exists and the tags have changed, we update them
            combined_tags = query_tags.copy()
            combined_tags.update(tags)
            if combined_tags != stream.tags:
                stream.tags = combined_tags
                stream.save()

            if value_type != stream.value_type:
                raise exceptions.InconsistentStreamConfiguration(
                    "You cannot change a stream's value type"
                )

            if value_type_options != stream.value_type_options:
                raise exceptions.InconsistentStreamConfiguration(
                    "You cannot change a stream's value type options"
                )

            # If a stream already exists and the derive inputs and/or operator have changed,
            # we raise an exception
            if (derive_from and not stream.derived_from) or (not derive_from and stream.derived_from):
                raise exceptions.InconsistentStreamConfiguration(
                    "You cannot change a derived stream into a non-derived one or vice-versa"
                )

            if derive_from:
                if derive_op != stream.derived_from.op:
                    raise exceptions.InconsistentStreamConfiguration(
                        "You cannot modify a derived stream's operator from '%s' to '%s'" % (stream.derived_from.op, derive_op)
                    )

                external_stream_ids = set()
                for stream_dsc in derive_from:
                    if not isinstance(stream_dsc, dict):
                        stream_dsc = {'stream': stream_dsc}

                    external_stream_ids.add(uuid.UUID(stream_dsc['stream']))

                if external_stream_ids != set(stream.derived_from.external_stream_ids):
                    raise exceptions.InconsistentStreamConfiguration(
                        "You cannot modify a derived stream's input streams"
                    )
        except Stream.DoesNotExist:
            # Create a new stream
            stream = Stream()
            stream.external_id = uuid.uuid4()

            value_downsamplers = set(value_downsamplers)
            # Ensure that the count downsampler is always included when downsampling is
            # requested; otherwise the mean time downsampler will fail.
            if value_downsamplers:
                value_downsamplers.add('count')

            # Some downsampling functions don't need to be stored in the database but
            # can be computed on the fly from other downsampled values
            for downsampler in ValueDownsamplers.values:
                if downsampler.name in value_downsamplers and hasattr(downsampler, 'dependencies'):
                    value_downsamplers.update(downsampler.dependencies)

            if not value_downsamplers <= self.value_downsamplers:
                raise exceptions.UnsupportedDownsampler(
                    "Unsupported value downsampler(s): %s" % list(value_downsamplers - self.value_downsamplers),
                )

            for downsampler in ValueDownsamplers.values:
                if downsampler.name not in value_downsamplers:
                    continue

                if value_type not in downsampler.supports_types:
                    raise exceptions.UnsupportedDownsampler(
                        "Downsampler '%s' does not support type '%s'" % (downsampler.name, value_type)
                    )

            # This should already be checked at the API level
            assert highest_granularity in api.Granularity.values

            stream.value_downsamplers = list(value_downsamplers)
            stream.highest_granularity = highest_granularity
            stream.value_type = value_type
            # TODO: Should we perform per-type options validation?
            stream.value_type_options = value_type_options
            stream.tags = query_tags.copy()
            stream.tags.update(tags)

            # Ensure there is an index on the query tags
            db = mongoengine.connection.get_db(DATABASE_ALIAS)
            index_spec = []
            for tag in sorted(self._get_tag_query_dict(None, query_tags).keys()):
                index_spec.append((tag.replace('__', '.'), pymongo.ASCENDING))
            db.streams.ensure_index(index_spec)

            # Setup source stream metadata for derived streams
            if derive_from is not None:
                # Validate that all source streams exist and resolve their internal ids
                derive_stream_ids = []
                derive_external_stream_ids = []
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

                        derive_external_stream_ids.append(src_stream.external_id)
                        derive_stream_ids.append(src_stream.id)
                        derive_stream_dscs.append(stream_dsc)
                    except Stream.DoesNotExist:
                        raise exceptions.StreamNotFound

                # Validate and convert operator parameters
                derive_operator = DerivationOperators.get(derive_op)
                if value_type not in derive_operator.supports_types:
                    raise exceptions.UnsupportedDeriveOperator(
                        "Derivation operator '%s' does not support type '%s'" % (
                            derive_operator.name,
                            value_type,
                        )
                    )
                derive_args = derive_operator.get_parameters(derive_stream_dscs, stream, **derive_args)

                derived = DerivedStreamDescriptor()
                derived.stream_ids = derive_stream_ids
                derived.external_stream_ids = derive_external_stream_ids
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
        tags.update({
            'stream_id': unicode(stream.external_id),
            'value_downsamplers': stream.value_downsamplers,
            'time_downsamplers': self.time_downsamplers,
            'highest_granularity': stream.highest_granularity,
            'pending_backprocess': bool(stream.pending_backprocess),
            'earliest_datapoint': stream.earliest_datapoint,
            'latest_datapoint': stream.latest_datapoint,
            'downsampled_until': dict([
                (g.name, getattr(stream.downsample_state.get(g.name, None), 'timestamp', None))
                for g in api.Granularity.values[api.Granularity.values.index(stream.highest_granularity) + 1:]
            ]),
            'value_type': stream.value_type,
            'value_type_options': stream.value_type_options,
        })

        if stream.derived_from:
            tags.update({'derived_from': self._map_derived_from(stream)})
        if stream.contributes_to:
            tags.update({'contributes_to': self._map_contributes_to(stream)})

        return tags

    def get_tags(self, stream_id):
        """
        Returns the tags for the specified stream.

        :param stream_id: Stream identifier
        :return: A dictionary of tags for the stream
        """

        try:
            stream = Stream.objects.get(external_id=uuid.UUID(stream_id))
            tags = self._get_stream_tags(stream)
        except Stream.DoesNotExist:
            raise exceptions.StreamNotFound

        return tags

    def _get_tag_query_dict(self, prefix, tags, value=None):
        """
        Generates a dictionary suitable for a MongoEngine query.

        :param prefix: Prefix of all keys
        :param tags: A dictionary of tag values
        :param value: Optional value to use instead of values in tags
        """

        ops = {}

        if prefix is None:
            prefix = 'tags'

        for k, v in tags.iteritems():
            if isinstance(v, collections.Mapping):
                v = v if value is None else value
                ops.update(self._get_tag_query_dict('%s__%s' % (prefix, k), v))
            else:
                v = v if value is None else value
                ops['%s__%s' % (prefix, k)] = v

        return ops

    def update_tags(self, stream_id, tags):
        """
        Updates stream tags with new tags, overriding existing ones.

        :param stream_id: Stream identifier
        :param tags: A dictionary of new tags
        """

        if not Stream.objects(external_id=uuid.UUID(stream_id)).update(**self._get_tag_query_dict('set__tags', tags)):
            # Returned count is 1 if stream is found even if nothing changed, this is what we want
            raise exceptions.StreamNotFound

    def remove_tag(self, stream_id, tag):
        """
        Removes stream tag.

        :param stream_id: Stream identifier
        :param tag: Dictionary describing the tag(s) to remove (values are ignored)
        """

        if not Stream.objects(external_id=uuid.UUID(stream_id)).update(**self._get_tag_query_dict('unset__tags', tag, 1)):
            # Returned count is 1 if stream is found even if nothing removed, this is what we want
            raise exceptions.StreamNotFound

    def clear_tags(self, stream_id):
        """
        Removes (clears) all non-readonly stream tags.

        Care should be taken that some tags are set immediately afterwards which uniquely
        identify a stream to be able to query the stream, in for example, `ensure_stream`.

        :param stream_id: Stream identifier
        """

        if not Stream.objects(external_id=uuid.UUID(stream_id)).update(set__tags={}):
            # Returned count is 1 if stream is found even if nothing changed, this is what we want
            raise exceptions.StreamNotFound

    def _get_stream_queryset(self, query_tags, allow_stream_id=True):
        """
        Returns a queryset that matches the specified stream tags.

        :param query_tags: Tags that should be matched to streams
        :return: A filtered queryset
        """

        if query_tags is None:
            query_tags = {}
        else:
            query_tags = query_tags.copy()

        query_set = Stream.objects.all()

        if allow_stream_id and 'stream_id' in query_tags:
            query_set = query_set.filter(external_id=uuid.UUID(query_tags['stream_id']))
            del query_tags['stream_id']

        if not query_tags:
            return query_set
        else:
            return query_set.filter(**self._get_tag_query_dict(None, query_tags))

    def find_streams(self, query_tags=None):
        """
        Finds all streams matching the specified query tags.

        :param query_tags: Tags that should be matched to streams
        :return: A `Streams` iterator over matched stream descriptors
        """

        # We do not specially process stream_id in find_streams. One should use get_tags
        # instead. We force order by _id so that it is predictable and pagination works
        # as expected. We do not provide API for custom sorting anyway.
        return Streams(self, self._get_stream_queryset(query_tags, False).order_by('_id'))

    def _supported_timestamp_range(self, timestamp):
        """
        Checks if timestamp is in supported range for MongoDB. Otherwise raises InvalidTimestamp exception.
        """

        if timestamp is None or self._min_timestamp <= timestamp <= self._max_timestamp:
            return

        raise exceptions.InvalidTimestamp("Timestamp is out of range: %s" % timestamp)

    def _force_timestamp_range(self, timestamp):
        return max(self._min_timestamp, min(self._max_timestamp, timestamp))

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
            if not derive_operator._appended:
                # The derivation operator did not update its stream. We still update the timestamp to
                # properly handle downsampling.
                derive_operator.update_last_timestamp(timestamp)

    def _update_last_timestamp(self, stream, timestamp, check_timestamp):
        object_id = self._generate_object_id(timestamp)

        for i in xrange(10):
            if check_timestamp:
                if stream.latest_datapoint and object_id.generation_time < stream.latest_datapoint:
                    raise exceptions.InvalidTimestamp("Datapoint timestamp must be equal or larger (newer) than the latest one '%s': %s" % (stream.latest_datapoint, object_id.generation_time))

            # Update last datapoint metadata.
            timestamp_check_time = datetime.datetime.now(pytz.utc)
            mstream = Stream._get_collection().find_and_modify(
                {'_id': stream.pk, 'latest_datapoint': stream.latest_datapoint},
                {'$set': {'latest_datapoint': object_id.generation_time}},
                fields={'_id': 1},
            )
            if not mstream:
                stream.reload()
                continue
            else:
                stream.latest_datapoint = object_id.generation_time
                ld = LastDatapoint(
                    stream_id=stream.pk,
                    insertion_ts=timestamp_check_time,
                    datapoint_ts=object_id.generation_time
                )
                ld.save()
                break
        else:
            raise exceptions.StreamAppendContended

        if stream.earliest_datapoint is None:
            stream.earliest_datapoint = stream.latest_datapoint
            stream.save()

        return object_id, timestamp_check_time

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

        if stream.value_type == 'numeric':
            # If the value is too big to store into MongoDB as an integer and is not already a float,
            # convert it to string and write a string into the database
            if isinstance(value, numbers.Number):
                value = stream.serialize_numeric_value(value)
            elif isinstance(value, dict):
                # Assume that we are inserting a downsampled datapoint. Users can use this when the source
                # of their values already provides information from multiple samples. For example, pinging
                # over the Internet sends multiple packets and then returns min, max, mean times.
                # By storing directly min, max, and mean values, no information is lost.
                valid_keys = set()
                for ds_name in stream.value_downsamplers:
                    key = api.VALUE_DOWNSAMPLERS[ds_name]
                    valid_keys.add(key)
                    if key not in value:
                        raise ValueError("Missing datapoint value for downsampler '%s'!" % ds_name)
                    else:
                        value[key] = stream.serialize_numeric_value(value[key])

                for key in value:
                    if key not in valid_keys:
                        raise ValueError("Unknown downsampled datapoint key '%s'!" % key)
            elif value is None:
                pass
            else:
                raise TypeError("Streams of type 'numeric' may only accept numbers or downsampled datapoints!")
        elif stream.value_type == 'nominal':
            # We allow arbitrary values to be stored for nominal values. But do remember that they are stored
            # as-is in the database so repeating the same huge value multiple times will be stored multiple
            # times. If values will be repeating it is better to instead store only some small keys representing
            # them.
            pass
        elif stream.value_type == 'graph':
            if isinstance(value, dict):
                try:
                    vertices = value['v']
                except KeyError:
                    raise ValueError("Graph must contain a list of vertices under key 'v'!")

                try:
                    edges = value['e']
                except KeyError:
                    raise ValueError("Graph must contain a list of edges under key 'e'!")

                # Validate that node identifiers are not duplicated
                vertices_set = set()
                for vertex in vertices:
                    try:
                        vid = vertex['i']
                        if vid in vertices_set:
                            raise ValueError("Duplicate vertex identifier '%s'!" % vid)
                        vertices_set.add(vid)
                    except KeyError:
                        raise ValueError("Graph vertices must contain a unique id under key 'i'!")

                # Validates edges
                for edge in edges:
                    try:
                        vertex_from = edge['f']
                        if vertex_from not in vertices_set:
                            raise ValueError("Invalid source vertex identifier '%s'!" % vertex_from)
                    except KeyError:
                        raise ValueError("Graph edges must contain source vertex id under key 'f'!")

                    try:
                        vertex_to = edge['t']
                        if vertex_to not in vertices_set:
                            raise ValueError("Invalid destination vertex identifier '%s'!" % vertex_to)
                    except KeyError:
                        raise ValueError("Graph edges must contain destination vertex id under key 't'!")
            elif value is None:
                pass
            else:
                raise TypeError("Streams of type 'graph' may only accept dictionary datapoints!")
        else:
            raise TypeError("Unsupported stream value type: %s" % stream.value_type)

        object_id, timestamp_check_time = self._update_last_timestamp(stream, timestamp, check_timestamp)

        if getattr(self._test_concurrency, 'sleep', False):
            time_module.sleep(DOWNSAMPLE_SAFETY_MARGIN // 2)

        # Append the datapoint into appropriate granularity
        db = mongoengine.connection.get_db(DATABASE_ALIAS)
        collection = getattr(db.datapoints, stream.highest_granularity.name)
        datapoint = {'_id': object_id, 'm': stream.id, 'v': value}
        collection.insert(datapoint, w=1)

        if total_seconds(datetime.datetime.now(pytz.utc) - timestamp_check_time) > DOWNSAMPLE_SAFETY_MARGIN:
            warnings.warn(exceptions.DownsampleConsistencyNotGuaranteed("Downsample safety margin of %d seconds exceeded in append." % DOWNSAMPLE_SAFETY_MARGIN))

        # Process contributions to other streams
        self._process_contributes_to(stream, object_id.generation_time, value, stream.highest_granularity)

        ret = {
            'stream_id': str(stream.external_id),
            'granularity': stream.highest_granularity,
            'datapoint': self._format_datapoint(stream, datapoint),
        }

        # Call test callback after everything
        if callable(self._test_callback):
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

    def _format_datapoint(self, stream, datapoint, empty_time=False):
        """
        Formats a datapoint so it is suitable for user output.

        :param stream: Stream descriptor
        :param datapoint: Raw datapoint from MongoDB database
        :param empty_time: Should there be not time value in the output? (default: false)
        :return: A properly formatted datapoint
        """

        result = {}

        if not empty_time:
            result['t'] = datapoint.get('t', datapoint['_id'].generation_time)

        if 'v' in datapoint:
            result['v'] = datapoint['v']

            if isinstance(result['v'], dict):
                for k, v in result['v'].iteritems():
                    try:
                        result['v'][k] = stream.deserialize_numeric_value(v)
                    except TypeError:
                        pass
            else:
                try:
                    result['v'] = stream.deserialize_numeric_value(result['v'])
                except TypeError:
                    pass

        return result

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

        # We explicitly set _id so that if both value_downsamplers and time_downsamplers we do not return everything
        downsamplers = ['_id']
        empty_time = False

        if value_downsamplers is not None:
            value_downsamplers = set(value_downsamplers)

            # This should already be checked at the API level
            assert value_downsamplers <= self.value_downsamplers, value_downsamplers - self.value_downsamplers

            downsamplers += ['v.%s' % api.VALUE_DOWNSAMPLERS[d] for d in value_downsamplers]
        else:
            # Else we want full 'v'
            downsamplers += ['v']

        if time_downsamplers is not None:
            time_downsamplers = set(time_downsamplers)

            # This should already be checked at the API level
            assert time_downsamplers <= self.time_downsamplers, time_downsamplers - self.time_downsamplers

            downsamplers += ['t.%s' % api.TIME_DOWNSAMPLERS[d] for d in time_downsamplers]

            empty_time = len(time_downsamplers) == 0
        else:
            # Else we want full 't'
            downsamplers += ['t']

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
                return Datapoints(self, stream)
        else:
            start_timestamp = granularity.round_timestamp(start)

        if start_timestamp > self._max_timestamp:
            return Datapoints(self, stream)

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
                    return Datapoints(self, stream)

                end_timestamp = self._force_timestamp_range(end_timestamp)

                # Optimization
                if end_timestamp <= start_timestamp:
                    return Datapoints(self, stream)

                time_query.update({
                    '$lt': objectid.ObjectId.from_datetime(end_timestamp),
                })

        datapoints = collection.find({
            'm': stream.id,
            '_id': time_query,
        }, downsamplers).sort('_id', -1 if reverse else 1)

        return Datapoints(self, stream, datapoints, empty_time)

    def delete_streams(self, query_tags=None):
        """
        Deletes datapoints for all streams matching the specified
        query tags. If no query tags are specified, all datastream-related
        data is deleted from the backend.

        :param query_tags: Tags that should be matched to streams
        """

        db = mongoengine.connection.get_db(DATABASE_ALIAS)
        if query_tags is None:
            for granularity in api.Granularity.values:
                collection = getattr(db.datapoints, granularity.name)
                collection.drop()
            db.streams.drop()
        else:
            have_streams = True
            while have_streams:
                have_streams = False
                any_removed = False

                for stream in self._get_stream_queryset(query_tags):
                    have_streams = True
                    if stream.contributes_to:
                        continue

                    any_removed = True

                    if stream.derived_from:
                        # Remove dependencies from all source streams
                        Stream.objects.filter(id__in=stream.derived_from.stream_ids).update(
                            **{('unset__contributes_to__%s' % stream.id): ''}
                        )

                    stream.delete()
                    for granularity in api.Granularity.values:
                        collection = getattr(db.datapoints, granularity.name)
                        collection.remove({'m': stream.id})

                if have_streams and not any_removed:
                    raise exceptions.OutstandingDependenciesError(
                        "Unable to remove stream as derived streams depend on it"
                    )

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

        if until is None:
            # TODO: Hm, this is not completely correct, because client time could be different than server time, we should allow use where client does not have to specify any timestamp and everything is done on the server
            until = datetime.datetime.now(pytz.utc) + self._time_offset

        new_datapoints = []

        for stream in self._get_stream_queryset(query_tags).filter(value_downsamplers__not__size=0):
            if callable(filter_stream) and not filter_stream(stream):
                continue

            result = self._downsample_check(stream, until, return_datapoints)
            if return_datapoints:
                new_datapoints += result

        return new_datapoints

    def _downsample_check(self, stream, until_timestamp, return_datapoints):
        """
        Checks if we need to perform any stream downsampling. In case it is needed,
        we perform downsampling.

        :param stream: Stream instance
        :param until_timestamp: Timestamp of the newly inserted datum
        :param return_datapoints: Should the added datapoints be stored
        """

        new_datapoints = []

        # Lock the stream for downsampling
        now = datetime.datetime.now(pytz.utc)
        locked_until = now + datetime.timedelta(seconds=MAINTENANCE_LOCK_DURATION)
        locked_stream = Stream._get_collection().find_and_modify(
            {"_id": stream.pk, "_lock_mt": {"$lt": now}, "downsample_count": stream.downsample_count},
            {"$set": {"_lock_mt": locked_until}, "$inc": {"downsample_count": 1}},
            fields={'_id': 1},
        )
        if not locked_stream:
            # Skip downsampling of this stream as we have failed to acquire the lock
            return new_datapoints

        last_granularity = None
        try:
            # Last datapoint timestamp of one higher granularity
            if stream.latest_datapoint:
                now += self._time_offset
                higher_last_ts = LastDatapoint.objects(
                    stream_id=stream.pk,
                    insertion_ts__gte=now - datetime.timedelta(seconds=DOWNSAMPLE_SAFETY_MARGIN),
                ).order_by('datapoint_ts').scalar('datapoint_ts').first() or stream.latest_datapoint
            else:
                higher_last_ts = self._min_timestamp

            for granularity in api.Granularity.values[api.Granularity.values.index(stream.highest_granularity) + 1:]:
                last_granularity = granularity
                state = stream.downsample_state.get(granularity.name, None)
                rounded_timestamp = granularity.round_timestamp(min(until_timestamp, higher_last_ts))
                # TODO: Why "can't compare offset-naive and offset-aware datetimes" is sometimes thrown here?
                if state is None or state.timestamp is None or rounded_timestamp > state.timestamp:
                    try:
                        result, locked_until = self._downsample(stream, granularity, rounded_timestamp, return_datapoints, locked_until)
                        if return_datapoints:
                            new_datapoints += result
                    except exceptions.InvalidTimestamp:
                        break

                higher_last_ts = stream.downsample_state[granularity.name].timestamp or higher_last_ts
        except:
            # Only unlock the stream but do not save the descriptor as it might be corrupted
            Stream.objects(pk=stream.pk).update(set___lock_mt=datetime.datetime.min)
            # Warn that an exception ocurred during processing of a stream
            warnings.warn(exceptions.DatastreamWarning("Unhandled exception during downsampling of stream '%s', granularity '%s'." % (repr(stream.pk), repr(last_granularity))))
            raise
        else:
            # Ensure that the stream is unlocked and all changes are saved
            stream._lock_mt = datetime.datetime.min
            stream.save()

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
            oid += struct.pack('>i', int(time_module.time() + total_seconds(self._time_offset)))

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

    def _downsample(self, stream, granularity, until_timestamp, return_datapoints, locked_until):
        """
        Performs downsampling on the given stream and granularity.

        :param stream: Stream instance
        :param granularity: Lower granularity to downsample into
        :param until_timestamp: Timestamp until which to downsample, not including datapoints
                                at a timestamp, rounded to the specified granularity
        :param return_datapoints: Should the added datapoints be stored
        :param locked_until: Timestamp when the maintenance lock on this datastream expires
        """

        assert granularity.round_timestamp(until_timestamp) == until_timestamp
        assert stream.downsample_state.get(granularity.name, None) is None or stream.downsample_state.get(granularity.name).timestamp is None or until_timestamp > stream.downsample_state.get(granularity.name).timestamp

        self._supported_timestamp_range(until_timestamp)

        db = mongoengine.connection.get_db(DATABASE_ALIAS)

        # Determine the interval (one or more granularity periods) that needs downsampling
        higher_granularity = api.Granularity.values[api.Granularity.values.index(granularity) - 1]
        datapoints = getattr(db.datapoints, higher_granularity.name)
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
                value_downsamplers.append(downsampler(stream))

        time_downsamplers = []
        for downsampler in TimeDownsamplers.values:
            time_downsamplers.append(downsampler(stream))

        # Pack stream identifier to be used for object id generation
        stream_id = struct.pack('>Q', stream.id)

        new_datapoints = []

        def store_downsampled_datapoint(timestamp, locked_until):
            # Check if we need to lengthen the lock
            now = datetime.datetime.now(pytz.utc)
            if locked_until < now:
                # Lock has expired while we were processing; abort immediately
                raise exceptions.LockExpiredMidMaintenance
            elif total_seconds(locked_until - now) <= MAINTENANCE_LOCK_DURATION // 2:
                locked_until = now + datetime.timedelta(seconds=MAINTENANCE_LOCK_DURATION)
                Stream.objects(pk=stream.pk).update(set___lock_mt=locked_until)

            value = {}
            time = {}
            for x in value_downsamplers:
                x.finish(value, timestamp, granularity)
            for x in time_downsamplers:
                x.finish(time, timestamp, granularity)

            for x in value_downsamplers:
                x.postprocess(value)
                x.initialize()
            for x in time_downsamplers:
                x.postprocess(time)
                x.initialize()

            # Insert downsampled datapoint
            point_id = self._generate_timed_stream_object_id(timestamp, stream_id)
            datapoint = {'_id': point_id, 'm': stream.id, 'v': value, 't': time}

            # TODO: We should probably create some API function which reprocesses everything and fixes any inconsistencies
            downsampled_points.update({'_id': point_id}, datapoint, upsert=True, w=1)

            # Process contributions to other streams
            self._process_contributes_to(stream, datapoint['t'], value, granularity)

            if return_datapoints:
                new_datapoints.append({
                    'stream_id': str(stream.external_id),
                    'granularity': granularity,
                    'datapoint': self._format_datapoint(stream, datapoint),
                })

            return locked_until

        # TODO: Use generator here, not concatenation
        for x in value_downsamplers + time_downsamplers:
            x.initialize()

        downsampled_points = getattr(db.datapoints, granularity.name)
        current_granularity_period_timestamp = None
        current_null_bucket = state.timestamp
        for datapoint in datapoints.sort('_id', 1):
            ts = datapoint['_id'].generation_time
            new_granularity_period_timestamp = granularity.round_timestamp(ts)
            if current_null_bucket is None:
                # Current bucket obviously has a datapoint, so the next bucket is a candidate
                # for being the first bucket that has no datapoints
                current_null_bucket = granularity.round_timestamp(
                    new_granularity_period_timestamp + datetime.timedelta(seconds=granularity.duration_in_seconds())
                )

            if current_granularity_period_timestamp is not None and \
               current_granularity_period_timestamp != new_granularity_period_timestamp:
                # All datapoints for current granularity period have been processed, we store the new datapoint
                # This happens when interval ("datapoints" query) contains multiple not-yet-downsampled granularity periods
                locked_until = store_downsampled_datapoint(current_granularity_period_timestamp, locked_until)

            current_granularity_period_timestamp = new_granularity_period_timestamp

            # Insert NULL values into empty buckets
            while current_null_bucket < current_granularity_period_timestamp:
                for x in value_downsamplers:
                    x.update(middle_timestamp(current_null_bucket, granularity), None)
                for x in time_downsamplers:
                    x.update(middle_timestamp(current_null_bucket, granularity), None)

                locked_until = store_downsampled_datapoint(current_null_bucket, locked_until)

                # Move to next bucket
                current_null_bucket = granularity.round_timestamp(
                    current_null_bucket + datetime.timedelta(seconds=granularity.duration_in_seconds())
                )

            # Move to next candidate NULL bucket
            current_null_bucket = granularity.round_timestamp(
                current_granularity_period_timestamp + datetime.timedelta(seconds=granularity.duration_in_seconds())
            )

            # Update all downsamplers for the current datapoint
            ts = datapoint.get('t', datapoint['_id'].generation_time)
            for x in value_downsamplers:
                x.update(ts, datapoint['v'])
            for x in time_downsamplers:
                x.update(ts, datapoint['v'])

        if current_granularity_period_timestamp is not None:
            locked_until = store_downsampled_datapoint(current_granularity_period_timestamp, locked_until)

            # Insert NULL values into empty buckets
            while current_null_bucket < until_timestamp:
                for x in value_downsamplers:
                    x.update(middle_timestamp(current_null_bucket, granularity), None)
                for x in time_downsamplers:
                    x.update(middle_timestamp(current_null_bucket, granularity), None)

                locked_until = store_downsampled_datapoint(current_null_bucket, locked_until)

                # Move to next bucket
                current_null_bucket = granularity.round_timestamp(
                    current_null_bucket + datetime.timedelta(seconds=granularity.duration_in_seconds())
                )

            # At the end, update the timestamp until which we have processed everything
            state.timestamp = until_timestamp

        # And call test callback for all new datapoints
        if self._test_callback is not None:
            for kwargs in new_datapoints:
                self._test_callback(**kwargs)

        return new_datapoints, locked_until

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
