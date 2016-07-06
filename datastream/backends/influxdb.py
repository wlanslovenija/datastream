from __future__ import absolute_import

import base64
import collections
import copy
import datetime
import dateutil.parser
import influxdb
import inspect
import json
import numbers
import os
import psycopg2
import psycopg2.extras
import pytz
import sys
import uuid
import warnings

try:
    import lzma
except ImportError:
    from backports import lzma

from .. import api, exceptions

# 1 second larger than the UNIX epoch is required due to issue https://github.com/influxdb/influxdb/issues/2703.
INFLUXDB_MINIMUM_TIMESTAMP = datetime.datetime(1970, 1, 1, 0, 0, 1, tzinfo=pytz.utc)
# Upper bound due to issue https://github.com/influxdata/influxdb/issues/6740.
INFLUXDB_MAXIMUM_TIMESTAMP = datetime.datetime(2049, 12, 9, 23, 13, 56, tzinfo=pytz.utc)

GRANULARITY_MAP = {
    '10seconds': '10s',
    'minutes': '1m',
    '10minutes': '10m',
    'hours': '1h',
    '6hours': '6h',
    'days': '1d',
}
VALUE_DOWNSAMPLER_MAP = {
    'mean': 'MEAN(%s)',
    'sum': 'SUM(%s)',
    'min': 'MIN(%s)',
    'max': 'MAX(%s)',
    'std_dev': 'STDDEV(%s)',
    'count': 'COUNT(%s)',
}
VALUE_DOWNSAMPLER_TYPE_MAP = {
    'numeric': VALUE_DOWNSAMPLER_MAP.keys(),
    'nominal': ['count'],
    'graph': ['count'],
}


def total_seconds(delta):
    if sys.version_info < (2, 7):
        return (delta.microseconds + (delta.seconds + delta.days * 24 * 3600) * 1e6) / 1e6
    else:
        return delta.total_seconds()


class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime.datetime):
            return {'$type': 'datetime', 'value': o.isoformat()}
        elif isinstance(o, uuid.UUID):
            return str(o)

        return super(JSONEncoder, self).default(o)


class JSONDecoder(json.JSONDecoder):
    def __init__(self):
        super(JSONDecoder, self).__init__(object_hook=self.dict_to_object)

    def dict_to_object(self, value):
        if '$type' not in value:
            return value

        if value['$type'] == 'datetime':
            dt = dateutil.parser.parse(value['value'])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=pytz.utc)
            return dt
        else:
            raise TypeError(value['$type'])


class PostgresJson(psycopg2.extras.Json):
    def dumps(self, obj):
        return JSONEncoder().encode(obj)


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

        @classmethod
        def get_parameters(cls, src_streams, dst_stream, arguments):
            """
            Performs validation of the supplied operator parameters and returns
            their database representation that will be used when calling the
            update method.

            :param src_streams: Source stream descriptors
            :param dst_stream: Future destination stream descriptor (not yet saved)
            :param arguments: User-supplied arguments
            :return: Database representation of the parameters
            """

            return arguments

        def __init__(self, backend, dst_stream, arguments):
            """
            Constructs a derivation operator.
            """

            self.backend = backend
            self.dst_stream = dst_stream
            self.arguments = arguments
            self.emitted_points = []

        def update(self, src_stream, datapoint):
            """
            Called for each datapoint processed from a source stream.

            :param src_stream: Source stream descriptor
            :param datapoint: Datapoint (may be downsampled)
            """

            pass

        def emit(self, value, timestamp):
            """
            Emits a datapoint.
            """

            self.emitted_points.append({
                'stream_id': self.dst_stream.uuid,
                'value': value,
                'timestamp': timestamp,
            })

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
        def get_parameters(cls, src_streams, dst_stream, arguments):
            """
            Performs validation of the supplied operator parameters and returns
            their database representation that will be used when calling the
            update method.

            :param src_streams: Source stream descriptors
            :param dst_stream: Future destination stream descriptor (not yet saved)
            :param arguments: User-supplied arguments
            :return: Database representation of the parameters
            """

            for stream_dsc in src_streams:
                # Ensure that the input streams are of correct type.
                if stream_dsc['stream'].value_type != 'numeric':
                    raise exceptions.IncompatibleTypes("All streams for 'sum' must be of 'numeric' type!")

                # Ensure that source stream granularity matches our highest granularity.
                if stream_dsc['granularity'] != dst_stream.highest_granularity:
                    raise exceptions.IncompatibleGranularities

            return super(DerivationOperators.Sum, cls).get_parameters(src_streams, dst_stream, arguments)

        def update(self, src_stream, datapoint):
            """
            Called for each datapoint processed from a source stream.

            :param src_stream: Source stream descriptor
            :param datapoint: Datapoint (may be downsampled)
            """

            # Handle streams with lower granularities.
            if isinstance(datapoint['v'], dict):
                # TODO: This is probably not right, currently only the mean is taken into account.
                value = datapoint['v'][api.VALUE_DOWNSAMPLERS['mean']]
                timestamp = datapoint['t'][api.TIME_DOWNSAMPLERS['mean']]
            else:
                value = datapoint['v']
                timestamp = datapoint['t']

            # Update internal state.
            rounded_ts = self.dst_stream.highest_granularity.round_timestamp(timestamp)
            bucket_key = rounded_ts.strftime("%Y%m%d%H%M%S")
            bucket = self.dst_stream.derive_state.setdefault(bucket_key, {})
            bucket[src_stream['stream']] = value
            if len(bucket) == len(self.dst_stream.derived_from['streams']):
                values = [x for x in bucket.values() if x is not None]
                self.emit(float(sum(values)) if values else None, rounded_ts)

                # Remove the bucket and all previous buckets as datapoints cannot be appended in the past.
                for key in self.dst_stream.derive_state.keys():
                    if key <= bucket_key:
                        del self.dst_stream.derive_state[key]
            elif len(self.dst_stream.derive_state) > 50:
                # Prevent stream metadata from filling up by removing the oldest bucket.
                buckets = self.dst_stream.derive_state.keys()
                buckets.sort()
                del self.dst_stream.derive_state[buckets[0]]

    class Derivative(_Base):
        """
        Computes the derivative of a stream.
        """

        name = 'derivative'
        supports_types = ('numeric',)

        @classmethod
        def get_parameters(cls, src_streams, dst_stream, arguments):
            """
            Performs validation of the supplied operator parameters and returns
            their database representation that will be used when calling the
            update method.

            :param src_streams: Source stream descriptors
            :param dst_stream: Future destination stream descriptor (not yet saved)
            :param arguments: User-supplied arguments
            :return: Database representation of the parameters
            """

            # The derivative operator supports only one source stream.
            if len(src_streams) > 1:
                raise exceptions.InvalidOperatorArguments

            if src_streams[0]['stream'].value_type != 'numeric':
                raise exceptions.IncompatibleTypes("The data stream for 'derivative' must be of 'numeric' type!")

            # The highest granularity of the source stream must match ours.
            stream_dsc = src_streams[0]
            if stream_dsc['granularity'] != dst_stream.highest_granularity:
                raise exceptions.IncompatibleGranularities

            return super(DerivationOperators.Derivative, cls).get_parameters(src_streams, dst_stream, arguments)

        def update(self, src_stream, datapoint):
            """
            Called for each datapoint processed from a source stream.

            :param src_stream: Source stream descriptor
            :param datapoint: Datapoint (may be downsampled)
            """

            # TODO: Handle downsampled source streams.
            if datapoint['v'] is None:
                # In case a null value is passed, we carry it on to the derived stream
                self.emit(None, datapoint['t'])
                self.dst_stream.derive_state = {}
                return

            if self.dst_stream.derive_state:
                # We already have a previous value, compute the derivative.
                delta = total_seconds(datapoint['t'] - self.dst_stream.derive_state['t'])

                if delta != 0:
                    derivative = float(datapoint['v'] - self.dst_stream.derive_state['v']) / delta
                    self.emit(derivative, datapoint['t'])
                else:
                    warnings.warn(exceptions.InvalidValueWarning("Zero time-delta in derivative computation (stream %s)!" % src_stream['stream']))

            # Store previous datapoint in derive state.
            self.dst_stream.derive_state = datapoint

    class CounterReset(_Base):
        """
        Computes the counter reset stream.
        """

        name = 'counter_reset'
        supports_types = ('nominal',)

        @classmethod
        def get_parameters(cls, src_streams, dst_stream, arguments):
            """
            Performs validation of the supplied operator parameters and returns
            their database representation that will be used when calling the
            update method.

            :param src_streams: Source stream descriptors
            :param dst_stream: Future destination stream descriptor (not yet saved)
            :param arguments: User-supplied arguments
            :return: Database representation of the parameters
            """

            # The counter reset operator supports only one source stream.
            if len(src_streams) > 1:
                raise exceptions.InvalidOperatorArguments

            if src_streams[0]['stream'].value_type != 'numeric':
                raise exceptions.IncompatibleTypes("The data stream for 'counter_reset' must be of 'numeric' type!")

            # The highest granularity of the source stream must match ours.
            stream_dsc = src_streams[0]
            if stream_dsc['granularity'] != dst_stream.highest_granularity:
                raise exceptions.IncompatibleGranularities

            return super(DerivationOperators.CounterReset, cls).get_parameters(src_streams, dst_stream, arguments)

        def update(self, src_stream, datapoint):
            """
            Called for each datapoint processed from a source stream.

            :param src_stream: Source stream descriptor
            :param datapoint: Datapoint (may be downsampled)
            """

            # TODO: Handle downsampled source streams.
            if datapoint['v'] is None:
                return

            if self.dst_stream.derive_state:
                # We already have a previous value, check what value needs to be inserted.
                # TODO: Add a configurable maximum counter value so overflows can be detected.
                if self.dst_stream.derive_state['v'] > datapoint['v']:
                    self.emit(1, datapoint['t'])

            self.dst_stream.derive_state = {'v': datapoint['v']}

    class CounterDerivative(_Base):
        """
        Computes the derivative of a monotonically increasing counter stream.
        """

        name = 'counter_derivative'
        supports_types = ('numeric',)

        @classmethod
        def get_parameters(cls, src_streams, dst_stream, arguments):
            """
            Performs validation of the supplied operator parameters and returns
            their database representation that will be used when calling the
            update method.

            :param src_streams: Source stream descriptors
            :param dst_stream: Future destination stream descriptor (not yet saved)
            :param arguments: User-supplied arguments
            :return: Database representation of the parameters
            """

            # We require exactly two input streams, the data stream and the reset stream.
            if len(src_streams) != 2:
                raise exceptions.InvalidOperatorArguments("'counter_derivative' requires exactly two input streams!")

            # The reset stream must be first and named "reset", the data stream must be second.
            if src_streams[0].get('name', None) != 'reset':
                raise exceptions.InvalidOperatorArguments("'counter_derivative' requires 'reset' to be the first input stream!")

            if src_streams[1].get('name', None) is not None:
                raise exceptions.InvalidOperatorArguments("'counter_derivative' requires an unnamed data stream!")

            if src_streams[1]['stream'].value_type != 'numeric':
                raise exceptions.IncompatibleTypes("The unnamed data stream for 'counter_derivative' must be of 'numeric' type!")

            if arguments.get('max_value', None) is not None:
                try:
                    arguments['max_value'] = float(arguments['max_value'])
                except (ValueError, TypeError):
                    raise exceptions.InvalidOperatorArguments("max_value must be a float!")

            return super(DerivationOperators.CounterDerivative, cls).get_parameters(src_streams, dst_stream, arguments)

        def update(self, src_stream, datapoint):
            """
            Called for each datapoint processed from a source stream.

            :param src_stream: Source stream descriptor
            :param datapoint: Datapoint (may be downsampled)
            """

            # TODO: Handle downsampled source streams.
            if datapoint['v'] is None:
                # In case a null value is passed, we carry it on to the derived stream
                self.emit(None, datapoint['t'])
                self.dst_stream.derive_state = {}
                return

            if src_stream['name'] == 'reset' and datapoint['v']:
                # Handle reset stream. Value may be any value which evaluates to true, which
                # signals that state should be reset.
                self.dst_stream.derive_state = {}
            elif not src_stream['name']:
                # Handle data stream.
                if self.dst_stream.derive_state:
                    # We already have a previous value, compute the derivative.
                    value = datapoint['v']
                    v1 = self.dst_stream.derive_state['v']
                    vdelta = value - v1
                    if v1 > value:
                        # Treat this as an overflow.
                        if self.arguments.get('max_value', None) is not None:
                            vdelta = self.arguments['max_value'] - v1 + value
                        else:
                            # Treat this as a reset since we lack the maximum value setting.
                            warnings.warn(exceptions.InvalidValueWarning("Assuming reset as maximum value is not set even when reset stream said nothing."))
                            vdelta = None

                    if vdelta is not None:
                        tdelta = total_seconds(datapoint['t'] - self.dst_stream.derive_state['t'])

                        if tdelta != 0:
                            derivative = float(vdelta) / tdelta
                            self.emit(derivative, datapoint['t'])
                        else:
                            warnings.warn(exceptions.InvalidValueWarning("Zero time-delta in derivative computation (stream %s)!" % src_stream['stream']))

                # Store previous datapoint in derive state.
                self.dst_stream.derive_state = datapoint


class ResultSetIteratorMixin(object):
    def __init__(self, backend, query):
        self._backend = backend
        self._influxdb = backend._influxdb
        self._query = query
        self._results = None
        self._batch = None

    def _clone(self):
        return copy.copy(self)

    def _evaluate(self):
        if self._results is not None:
            return

        if self._query is None:
            self._results = []
        else:
            self._results = list(self._influxdb.query(self._query.strip()).get_points())

    def count(self):
        self._evaluate()
        return len(self._results)

    def batch(self, size):
        clone = self._clone()
        clone._batch = size
        return clone

    def no_batch(self):
        clone = self._clone()
        clone._batch = None
        return clone

    def __iter__(self):
        if self._batch is not None:
            # Batched operation.
            batch_offset = 0
            while True:
                batch = self.no_batch()[batch_offset:batch_offset + self._batch]
                if not batch:
                    break

                for item in batch:
                    yield item
                batch_offset += self._batch
        else:
            # Evaluate immediately.
            self._evaluate()

            for item in self._results:
                point = self._format_datapoint(item)
                if point is None:
                    continue

                yield point

    def __getitem__(self, key):
        clone = self._clone()

        if isinstance(key, slice):
            if clone._query is not None:
                if key.stop is not None:
                    limit = key.stop
                    if key.start is not None:
                        limit -= key.start

                    assert limit >= 0
                    clone._query += ' LIMIT %d' % limit

                if key.start is not None:
                    assert key.start >= 0
                    clone._query += ' OFFSET %d' % key.start

            clone._evaluate()
            return clone
        elif isinstance(key, (int, long)):
            if clone._query is not None:
                assert key >= 0
                clone._query += ' LIMIT 1 OFFSET %d' % key

            clone._evaluate()
            return list(clone)[0]
        else:
            raise TypeError


class Streams(ResultSetIteratorMixin, api.Streams):
    def __init__(self, backend, query_tags, raw=False):
        super(Streams, self).__init__(backend, '')

        self._query_tags = query_tags
        self._raw = raw

    def _evaluate(self):
        if self._results is not None:
            return

        with self._backend._metadata:
            with self._backend._metadata.cursor() as cursor:
                if self._query_tags is None:
                    cursor.execute('SELECT * FROM datastream.streams' + self._query)
                else:
                    cursor.execute('SELECT * FROM datastream.streams WHERE tags @> %s' + self._query, (
                        PostgresJson(self._query_tags),
                    ))

                self._results = cursor.fetchall()

    def _format_datapoint(self, datapoint):
        stream = StreamDescriptor(*datapoint)
        if self._raw:
            return stream

        return self._backend._get_tags(stream)


class Datapoints(ResultSetIteratorMixin, api.Datapoints):
    def __init__(self, backend, stream, query, value_downsamplers, time_downsamplers):
        super(Datapoints, self).__init__(backend, query)

        self._stream = stream
        self.value_downsamplers = value_downsamplers
        self.time_downsamplers = time_downsamplers

    def _deserialize_value(self, value):
        if self._stream.value_type == 'graph':
            try:
                return json.loads(lzma.decompress(base64.b64decode(value)))
            except (ValueError, TypeError, lzma.LZMAError):
                return json.loads(value)
        elif self._stream.value_type == 'nominal':
            return json.loads(value)

        return value

    def _format_datapoint(self, datapoint):
        result = {}

        if 'value' in datapoint or 'value_null' in datapoint:
            if datapoint.get('value_null', False):
                result['v'] = None
            else:
                result['v'] = self._deserialize_value(datapoint['value'])

            result['t'] = dateutil.parser.parse(datapoint['time'])
            if result['t'].tzinfo is None:
                result['t'] = result['t'].replace(tzinfo=pytz.utc)
        else:
            value = {}
            for key, aggregate_value in datapoint.items():
                # If there is only one point, return standard deviation as zero, to distinguish
                # it from the case where there are no datapoints.
                if key == 'std_dev' and aggregate_value is None and datapoint['count'] == 1:
                    aggregate_value = 0

                if key in self.value_downsamplers:
                    value[api.VALUE_DOWNSAMPLERS[key]] = aggregate_value

            if value:
                result['v'] = value

            # Encode timestamp differently for aggregations (we only support the mean time downsampler).
            if 'mean' in self.time_downsamplers:
                result['t'] = {
                    'm': dateutil.parser.parse(datapoint['time']),
                }

                if result['t']['m'].tzinfo is None:
                    result['t']['m'] = result['t']['m'].replace(tzinfo=pytz.utc)

        return result


class StreamDescriptor(object):
    def __init__(self, uuid, tags, pending_backprocess=False):
        tags = copy.deepcopy(tags)
        self.uuid = str(uuid)
        self.value_downsamplers = tags.pop('value_downsamplers')
        self.highest_granularity = getattr(api.Granularity, tags.pop('highest_granularity'))
        self.value_type = tags.pop('value_type')
        self.value_type_options = tags.pop('value_type_options')
        self.derived_from = tags.pop('derived_from', None)
        self.derive_state = tags.pop('derive_state', {})
        self.pending_backprocess = pending_backprocess
        self.tags = tags

        if self.derived_from is not None:
            for src_stream in self.derived_from['streams']:
                src_stream['granularity'] = getattr(api.Granularity, src_stream['granularity'])

    def get_tags(self):
        if self.derived_from is not None:
            derived_from = copy.deepcopy(self.derived_from)
            for src_stream in derived_from['streams']:
                src_stream['granularity'] = src_stream['granularity'].__name__
        else:
            derived_from = None

        tags = {
            'value_downsamplers': self.value_downsamplers,
            'highest_granularity': self.highest_granularity.__name__,
            'value_type': self.value_type,
            'value_type_options': self.value_type_options,
            'derived_from': derived_from,
        }
        tags.update(copy.deepcopy(self.tags))

        if self.derive_state:
            tags['derive_state'] = copy.deepcopy(self.derive_state)

        return tags

    def matches_tags(self, query_tags):
        for key, value in query_tags.items():
            if self.tags.get(key, None) != value:
                return False

        return True


class Backend(object):
    """
    InfluxDB backend.
    """

    requires_downsampling = False
    requires_derived_stream_backprocess = False
    downsampled_always_exist = True
    downsampled_timestamps_start_bucket = True
    value_downsamplers = set(VALUE_DOWNSAMPLER_MAP.keys())
    time_downsamplers = set(['mean'])

    def __init__(self, connection_influxdb, connection_metadata):
        """
        Initializes the InfluxDB backend.

        :param connection_influxdb: InfluxDB connection parameters
        :param connection_metadata: PostgreSQL metadata store connection parameters
        """

        self._streams = {}
        self._connection = {
            'influxdb': connection_influxdb,
            'metadata': connection_metadata,
        }
        self._database = connection_influxdb['database']

        self._creator_pid = None
        self._connect()

        self._create_schema()

    def _connect(self):
        # Ensure that the connections are recreated after process fork.
        if os.getpid() == self._creator_pid:
            return

        self._influxdb_db = influxdb.InfluxDBClient(**self._connection['influxdb'])
        self._metadata_db = psycopg2.connect(**self._connection['metadata'])
        self._creator_pid = os.getpid()

        psycopg2.extras.register_default_jsonb(self._metadata_db, loads=JSONDecoder().decode)

    @property
    def _metadata(self):
        self._connect()
        return self._metadata_db

    @property
    def _influxdb(self):
        self._connect()
        return self._influxdb_db

    def _create_schema(self):
        # Ensure that the metadata storage table exists.
        with self._metadata:
            with self._metadata.cursor() as cursor:
                cursor.execute('CREATE SCHEMA IF NOT EXISTS datastream')

                # Streams table.
                cursor.execute('''CREATE TABLE IF NOT EXISTS datastream.streams (
                    id uuid PRIMARY KEY,
                    tags jsonb,
                    pending_backprocess boolean NOT NULL DEFAULT false
                )''')

                # Create a GIN index if one doesn't yet exist.
                cursor.execute('SELECT to_regclass(\'datastream.streams_tags\')')
                if cursor.fetchone()[0] is None:
                    cursor.execute('CREATE INDEX streams_tags ON datastream.streams USING GIN (tags jsonb_path_ops)')

                # Dependencies table.
                cursor.execute('''CREATE TABLE IF NOT EXISTS datastream.dependencies (
                    src_stream uuid NOT NULL REFERENCES datastream.streams (id) ON DELETE RESTRICT,
                    dst_stream uuid NOT NULL REFERENCES datastream.streams (id) ON DELETE RESTRICT,
                    options jsonb,

                    UNIQUE (src_stream, dst_stream)
                )''')

                cursor.execute('SELECT to_regclass(\'datastream.dependencies_src_stream\')')
                if cursor.fetchone()[0] is None:
                    cursor.execute('CREATE INDEX dependencies_src_stream ON datastream.dependencies (src_stream)')

                cursor.execute('SELECT to_regclass(\'datastream.dependencies_dst_stream\')')
                if cursor.fetchone()[0] is None:
                    cursor.execute('CREATE INDEX dependencies_dst_stream ON datastream.dependencies (dst_stream)')

        # Ensure that the passed in database exists.
        self._influxdb.query('CREATE DATABASE IF NOT EXISTS "%s"' % self._database)

    def _get_where(self, query_tags):
        where = []
        for key, value in query_tags.items():
            # TODO: More proper escaping.
            key = key.replace('\"', '\\"')
            value = value.replace('\'', '\\\'')
            where.append('"%s" = \'%s\'' % (key, value))

        return where

    def ensure_stream(self, query_tags, tags, value_downsamplers, highest_granularity, derive_from, derive_op, derive_args, value_type, value_type_options, derive_backprocess):
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
        :param derive_backprocess: Should a derived stream be backprocessed
        :return: A stream identifier
        """

        value_downsamplers = list(value_downsamplers)
        tags = tags.copy()
        tags.update(query_tags)

        with self._metadata:
            with self._metadata.cursor() as cursor:
                cursor.execute('SELECT * FROM datastream.streams WHERE tags @> %s LIMIT 1', (PostgresJson(query_tags),))
                stream = cursor.fetchone()

                if stream is not None:
                    # Stream already exists.
                    stream = StreamDescriptor(*stream)
                    old_tags = stream.tags
                    stream.tags = copy.deepcopy(tags)

                    # Enforce metadata change constraints.
                    if value_type != stream.value_type:
                        raise exceptions.InconsistentStreamConfiguration(
                            "You cannot change a stream's value type"
                        )

                    if value_type_options != stream.value_type_options:
                        raise exceptions.InconsistentStreamConfiguration(
                            "You cannot change a stream's value type options"
                        )

                    # If a stream already exists and the derive inputs and/or operator have changed,
                    # we raise an exception.
                    if (derive_from and not stream.derived_from) or (not derive_from and stream.derived_from):
                        raise exceptions.InconsistentStreamConfiguration(
                            "You cannot change a derived stream into a non-derived one or vice-versa"
                        )

                    if derive_from:
                        if derive_op != stream.derived_from['op']:
                            raise exceptions.InconsistentStreamConfiguration(
                                "You cannot modify a derived stream's operator from '%s' to '%s'" % (stream.derived_from['op'], derive_op)
                            )

                        existing_src_streams = [descriptor['stream'] for descriptor in stream.derived_from['streams']]
                        src_streams = []
                        for stream_dsc in derive_from:
                            if isinstance(stream_dsc, dict):
                                src_streams.append(str(stream_dsc['stream']))
                            else:
                                src_streams.append(str(stream_dsc))

                        if src_streams != existing_src_streams:
                            raise exceptions.InconsistentStreamConfiguration(
                                "You cannot modify a derived stream's input streams"
                            )

                    if stream.tags != old_tags:
                        self._update_metadata(stream)
                else:
                    # Create a new stream.
                    tags.update({
                        'value_downsamplers': value_downsamplers,
                        'highest_granularity': highest_granularity.__name__,
                        'value_type': value_type,
                        'value_type_options': value_type_options,
                    })
                    stream = StreamDescriptor(str(uuid.uuid4()), tags)

                    # Validate value downsamplers.
                    supported_downsamplers = VALUE_DOWNSAMPLER_TYPE_MAP[value_type]
                    for downsampler in value_downsamplers:
                        if downsampler not in supported_downsamplers:
                            raise exceptions.UnsupportedDownsampler(
                                "Downsampler '%s' does not support type '%s'" % (downsampler, value_type)
                            )

                    # Setup source stream metadata for derived streams.
                    if derive_from is not None:
                        # Validate that all source streams exist.
                        src_streams = []
                        for stream_dsc in derive_from:
                            if not isinstance(stream_dsc, dict):
                                stream_dsc = {'stream': stream_dsc}

                            src_stream = self._get_stream(stream_dsc['stream'])

                            # One can't specify a granularity higher than the stream's highest one.
                            if stream_dsc.get('granularity', src_stream.highest_granularity) > src_stream.highest_granularity:
                                raise exceptions.IncompatibleGranularities

                            # If any of the input streams already holds some data, we pause our stream.
                            if not stream.pending_backprocess and derive_backprocess:
                                try:
                                    self.get_data(src_stream.uuid, src_stream.highest_granularity)[0]
                                    stream.pending_backprocess = True
                                except IndexError:
                                    pass

                            src_streams.append({
                                'name': stream_dsc.get('name', None),
                                'granularity': stream_dsc.get('granularity', src_stream.highest_granularity),
                                'stream': src_stream,
                            })

                        # Validate and convert operator parameters.
                        derive_operator = DerivationOperators.get(derive_op)
                        if value_type not in derive_operator.supports_types:
                            raise exceptions.UnsupportedDeriveOperator(
                                "Derivation operator '%s' does not support type '%s'" % (
                                    derive_operator.name,
                                    value_type,
                                )
                            )
                        derive_args = derive_operator.get_parameters(src_streams, stream, derive_args)

                        # Update stream tags.
                        stream.derived_from = {
                            'streams': [
                                {
                                    'name': descriptor['name'],
                                    'granularity': descriptor['granularity'],
                                    'stream': descriptor['stream'].uuid,
                                }
                                for descriptor in src_streams
                            ],
                            'op': derive_op,
                            'args': derive_args,
                        }

                    # Create the stream.
                    cursor.execute('INSERT INTO datastream.streams VALUES(%(id)s, %(tags)s, %(pending_backprocess)s)', {
                        'id': stream.uuid,
                        'tags': PostgresJson(stream.get_tags()),
                        'pending_backprocess': stream.pending_backprocess,
                    })

                    # Setup stream dependencies.
                    if derive_from is not None:
                        for stream_dsc in src_streams:
                            src_stream = stream_dsc['stream']
                            cursor.execute('INSERT INTO datastream.dependencies VALUES (%s, %s, %s)', (
                                src_stream.uuid,
                                stream.uuid,
                                PostgresJson({
                                    'name': stream_dsc.get('name', None),
                                    'granularity': stream_dsc.get('granularity', src_stream.highest_granularity).__name__,
                                    'op': derive_op,
                                    'args': derive_args,
                                }),
                            ))

        self._streams[stream.uuid] = stream
        return stream.uuid

    def _get_stream(self, stream_id):
        try:
            return self._streams[stream_id]
        except KeyError:
            with self._metadata:
                return self._get_stream_by_id(stream_id)

    def _get_stream_by_id(self, stream_id):
        """
        The caller is required to enter a transaction.
        """

        with self._metadata.cursor() as cursor:
            cursor.execute('SELECT * FROM datastream.streams WHERE id = %s', (stream_id,))
            try:
                stream = StreamDescriptor(*cursor.fetchone())
                self._streams[stream.uuid] = stream
                return stream
            except TypeError:
                raise exceptions.StreamNotFound

    def _get_tags(self, stream):
        try:
            earliest_datapoint = self.get_data(stream.uuid, stream.highest_granularity)[0]['t']
        except IndexError:
            earliest_datapoint = None

        try:
            latest_datapoint = self.get_data(stream.uuid, stream.highest_granularity, reverse=True)[0]['t']
        except IndexError:
            latest_datapoint = None

        tags = copy.deepcopy(stream.tags)
        tags.update({
            'stream_id': stream.uuid,
            'value_downsamplers': stream.value_downsamplers,
            'time_downsamplers': ['mean'],
            'highest_granularity': stream.highest_granularity,
            'pending_backprocess': stream.pending_backprocess,
            'earliest_datapoint': earliest_datapoint,
            'latest_datapoint': latest_datapoint,
            'value_type': stream.value_type,
            'value_type_options': stream.value_type_options,
        })

        # Check for derived stream.
        if stream.derived_from:
            tags.update({'derived_from': copy.deepcopy(stream.derived_from)})

        # Check for stream dependencies.
        with self._metadata:
            with self._metadata.cursor() as cursor:
                cursor.execute('SELECT * FROM datastream.dependencies WHERE src_stream = %s', (stream.uuid,))
                contributes_to = {}
                for src_stream, dst_stream, options in cursor:
                    contributes_to[dst_stream] = {
                        'op': options['op'],
                        'args': options['args'],
                    }

                if contributes_to:
                    tags.update({'contributes_to': contributes_to})

        return tags

    def get_tags(self, stream_id):
        """
        Returns the tags for the specified stream.

        :param stream_id: Stream identifier
        :return: A dictionary of tags for the stream
        """

        return self._get_tags(self._get_stream(stream_id))

    def _update_metadata(self, stream):
        """
        The caller is required to enter a transaction.
        """

        with self._metadata.cursor() as cursor:
            cursor.execute('UPDATE datastream.streams SET tags = %(tags)s WHERE id = %(id)s', {
                'id': stream.uuid,
                'tags': PostgresJson(stream.get_tags()),
            })

    def update_tags(self, stream_id, tags):
        """
        Updates stream tags with new tags, overriding existing ones.

        :param stream_id: Stream identifier
        :param tags: A dictionary of new tags
        """

        def update_dict(a, b, only_existing=False):
            for k, v in b.items():
                if only_existing and k not in a:
                    continue

                if isinstance(v, collections.Mapping):
                    if k in a:
                        update_dict(a[k], v)
                    else:
                        a[k] = v
                else:
                    a[k] = v

        with self._metadata:
            stream = self._get_stream_by_id(stream_id)
            update_dict(stream.tags, tags)
            self._update_metadata(stream)

    def remove_tag(self, stream_id, tag):
        """
        Removes stream tag.

        :param stream_id: Stream identifier
        :param tag: Dictionary describing the tag(s) to remove (values are ignored)
        """

        def clean_dict(a, b):
            for k, v in b.items():
                if k not in a:
                    continue

                if isinstance(v, collections.Mapping):
                    clean_dict(a[k], v)
                else:
                    del a[k]

        with self._metadata:
            stream = self._get_stream_by_id(stream_id)
            clean_dict(stream.tags, tag)
            self._update_metadata(stream)

    def clear_tags(self, stream_id):
        """
        Removes (clears) all non-readonly stream tags.

        Care should be taken that some tags are set immediately afterwards which uniquely
        identify a stream to be able to query the stream, in for example, `ensure_stream`.

        :param stream_id: Stream identifier
        """

        with self._metadata:
            stream = self._get_stream_by_id(stream_id)
            stream.tags = {}
            self._update_metadata(stream)

    def delete_streams(self, query_tags=None):
        """
        Deletes datapoints for all streams matching the specified
        query tags. If no query tags are specified, all datastream-related
        data is deleted from the backend.

        :param query_tags: Tags that should be matched to streams
        """

        with self._metadata:
            with self._metadata.cursor() as cursor:
                if query_tags is None:
                    cursor.execute('TRUNCATE datastream.streams CASCADE')
                    self._streams = {}

                    for series in self._influxdb.get_list_series():
                        try:
                            self._influxdb.query('DROP MEASUREMENT "%s"' % series['name'])
                        except influxdb.exceptions.InfluxDBClientError:
                            pass
                else:
                    condition = ''
                    query_args = ()

                    if 'stream_id' in query_tags:
                        condition = 'id = %s'
                        query_args = (query_tags['stream_id'],)
                    else:
                        condition = 'tags @> %s'
                        query_args = (PostgresJson(query_tags),)

                    # First delete all dependencies where matched streams are the destination.
                    cursor.execute(
                        '''DELETE FROM datastream.dependencies WHERE dst_stream IN (
                            SELECT id FROM datastream.streams WHERE %s
                        )''' % condition,
                        query_args
                    )
                    # The stremas that we are removing should have no more dependencies, so we can remove them.
                    try:
                        cursor.execute('DELETE FROM datastream.streams WHERE %s RETURNING id' % condition, query_args)
                    except psycopg2.IntegrityError:
                        raise exceptions.OutstandingDependenciesError(
                            "Unable to remove stream as derived streams depend on it"
                        )

                    for stream in cursor:
                        stream_id = stream[0]

                        # Remove stream from cache.
                        self._streams.pop(stream_id, None)

                        # Remove stream from InfluxDB.
                        try:
                            self._influxdb.query('DROP MEASUREMENT "%s"' % stream_id)
                        except influxdb.exceptions.InfluxDBClientError:
                            pass

    def _find_streams(self, query_tags=None, raw=False):
        return Streams(self, query_tags, raw=raw)

    def find_streams(self, query_tags=None):
        """
        Finds all streams matching the specified query tags.

        :param query_tags: Tags that should be matched to streams
        :return: A `Streams` iterator over matched stream descriptors
        """

        return self._find_streams(query_tags)

    def _validate_type(self, stream, value):
        if value is None:
            return

        if stream.value_type == 'numeric':
            if isinstance(value, numbers.Number):
                pass
            elif isinstance(value, dict):
                # As we have no way to store already downsampled data, we simply take the mean.
                value = value[api.VALUE_DOWNSAMPLERS['mean']]
            else:
                raise TypeError("Streams of type 'numeric' may only accept numbers or downsampled datapoints!")
        elif stream.value_type == 'nominal':
            # We allow arbitrary values to be stored for nominal values. But do remember that they are stored
            # as-is in the database so repeating the same huge value multiple times will be stored multiple
            # times. If values will be repeating it is better to instead store only some small keys representing
            # them.
            value = json.dumps(value)
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

                # Validate that node identifiers are not duplicated.
                vertices_set = set()
                for vertex in vertices:
                    try:
                        vid = vertex['i']
                        if vid in vertices_set:
                            raise ValueError("Duplicate vertex identifier '%s'!" % vid)
                        vertices_set.add(vid)
                    except KeyError:
                        raise ValueError("Graph vertices must contain a unique id under key 'i'!")

                # Validates edges.
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

                # Serialize as Base64-encoded LZMA-compressed JSON.
                value = base64.b64encode(lzma.compress(json.dumps(value)))
            else:
                raise TypeError("Streams of type 'graph' may only accept dictionary datapoints!")
        else:
            raise TypeError("Unsupported stream value type: %s" % stream.value_type)

        return value

    def _append_multiple(self, datapoints, raw=False):
        """
        Appends multiple datapoints into the datastream. Each datapoint should be
        described by a dictionary with fields `stream_id`, `value` and `timestamp`,
        which are the same as in `append`.

        :param datapoints: A list of datapoints to append
        """

        stream_ids = []
        points = []
        grouped_datapoints = {}
        now = datetime.datetime.now(pytz.utc)
        for datapoint in datapoints:
            stream = self._get_stream(datapoint['stream_id'])
            stream_ids.append(stream.uuid)

            if not raw:
                # Appending is not allowed for derived streams.
                if stream.derived_from is not None:
                    raise exceptions.AppendToDerivedStreamNotAllowed

            fields = {'value': self._validate_type(stream, datapoint['value'])}
            if fields['value'] is None:
                del fields['value']
                fields = {'value_null': True}

            point = {
                'measurement': stream.uuid,
                'fields': fields,
            }

            # Remove subsecond precision.
            timestamp = datapoint.get('timestamp', None)
            if timestamp is None:
                timestamp = now

            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=pytz.utc)

            point['time'] = timestamp.replace(microsecond=0)

            grouped_datapoints.setdefault(stream.uuid, []).append({
                'v': datapoint['value'],
                't': point['time'],
            })

            points.append(point)

        if points:
            try:
                self._influxdb.write_points(points)
            except influxdb.exceptions.InfluxDBServerError:
                raise exceptions.StreamAppendFailed

            self._backprocess_streams(stream_ids, grouped_datapoints)

    def append_multiple(self, datapoints):
        """
        Appends multiple datapoints into the datastream. Each datapoint should be
        described by a dictionary with fields `stream_id`, `value` and `timestamp`,
        which are the same as in `append`.

        :param datapoints: A list of datapoints to append
        """

        self._append_multiple(datapoints)

    def append(self, stream_id, value, timestamp=None, check_timestamp=True):
        """
        Appends a datapoint into the datastream.

        :param stream_id: Stream identifier
        :param value: Datapoint value
        :param timestamp: Datapoint timestamp, must be equal or larger (newer) than the latest one, monotonically increasing (optional)
        :param check_timestamp: Ignored for this backend
        """

        self.append_multiple([{
            'stream_id': stream_id,
            'value': value,
            'timestamp': timestamp,
        }])

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

        return []

    def backprocess_streams(self, query_tags=None, max_depth=3):
        """
        Performs backprocessing of any derived streams that are marked as
        'pending backprocess'.

        :param query_tags: Tags that should be matched to streams
        """

        while max_depth > 0:
            max_depth -= 1

            # Check if there are any streams pending backprocess.
            with self._metadata:
                streams = self._metadata.cursor()
                streams.execute('UPDATE datastream.streams SET pending_backprocess = false WHERE pending_backprocess = true RETURNING *')

            with streams:
                for stream in streams:
                    try:
                        # TODO: Lock stream for update to prevent concurrent backprocessing?
                        stream = StreamDescriptor(*stream)
                        self._streams[stream.uuid] = stream
                        self._backprocess_stream(stream)
                    except:
                        # TODO: Warn about an unhandled exception.
                        raise

    def _backprocess_streams(self, stream_ids, grouped_datapoints):
        """
        Performs stream backprocessing, given some existing datapoints.

        :param stream_ids: Source stream identifiers
        :param grouped_datapoints: A dictionary containing lists of datapoints grouped by stream
        """

        # Find dependent streams.
        with self._metadata:
            streams = self._metadata.cursor()
            streams.execute('''SELECT * FROM datastream.streams WHERE id IN (
                SELECT dst_stream FROM datastream.dependencies WHERE src_stream = ANY(%s::uuid[])
            )''', (stream_ids,))

        emitted_points = []
        for stream in streams:
            stream = StreamDescriptor(*stream)
            self._streams[stream.uuid] = stream
            emitted_points += self._backprocess_stream(stream, grouped_datapoints, emit=False)

        # Append datapoints emitted by the operators.
        self._append_multiple(emitted_points, raw=True)

    def _backprocess_stream(self, stream, cache=None, emit=True):
        """
        Performs backprocessing for a single stream.
        """

        requires_refresh = False
        operator = DerivationOperators.get(stream.derived_from['op'])(self, stream, stream.derived_from['args'])

        # Iterate over source streams. Note that since source stream definitions are immutable,
        # we don't need an additional query over 'dependencies' to get a consistent view.
        stream_iterators = []
        for src_stream in stream.derived_from['streams']:
            src_stream_dsc = self._get_stream(src_stream['stream'])

            if cache is not None and src_stream['granularity'] == src_stream_dsc.highest_granularity:
                try:
                    stream_iterators.append((src_stream, iter(cache[src_stream['stream']])))
                except KeyError:
                    continue
            else:
                # If input is from a lower granularity, we need to first determine the start
                # timestamp if none is defined.
                start_exclusive = src_stream.get('last_backprocessed_timestamp', None)
                end = None

                if src_stream['granularity'] != src_stream_dsc.highest_granularity:
                    try:
                        # Get the timestamp of the first datapoint.
                        if start_exclusive is None:
                            start_exclusive = self.get_data(
                                src_stream['stream'],
                                src_stream_dsc.highest_granularity,
                            )[0]['t']

                        # Get the timestamp of the last datapoint as we need to bound the
                        # automatic derivation.
                        end = self.get_data(
                            src_stream['stream'],
                            src_stream_dsc.highest_granularity,
                            reverse=True,
                        )[0]['t']
                    except IndexError:
                        # If there are no datapoints, skip this stream.
                        continue

                data = self.get_data(
                    src_stream['stream'],
                    src_stream['granularity'],
                    start_exclusive=start_exclusive,
                    end=end,
                ).batch(1000)

                stream_iterators.append((src_stream, iter(data)))

        # Apply streams in order until they all run out of datapoints.
        def get_timestamp(datapoint):
            if isinstance(datapoint['t'], datetime.datetime):
                return datapoint['t']

            return datapoint['t'][api.TIME_DOWNSAMPLERS['mean']]

        lookahead = {}
        while True:
            # Determine the current point in time so we can properly apply datapoints.
            current_ts = None
            for src_stream, data in stream_iterators:
                datapoint = lookahead.get(src_stream['stream'])
                if datapoint is None:
                    try:
                        datapoint = data.next()
                    except StopIteration:
                        continue

                ts = get_timestamp(datapoint)
                if current_ts is None or ts < current_ts:
                    current_ts = ts
                lookahead[src_stream['stream']] = datapoint

            if current_ts is None:
                break

            # Apply datapoints in proper order.
            for src_stream, data in stream_iterators:
                datapoint = lookahead.get(src_stream['stream'])
                if datapoint is not None:
                    while get_timestamp(datapoint) <= current_ts:
                        operator.update(src_stream, datapoint)

                        src_stream['last_backprocessed_timestamp'] = get_timestamp(datapoint)
                        requires_refresh = True

                        try:
                            datapoint = data.next()
                        except StopIteration:
                            datapoint = None
                            break

                    lookahead[src_stream['stream']] = datapoint

        # Append datapoints emitted by the operator.
        if emit:
            self._append_multiple(operator.emitted_points, raw=True)

        # Update destination stream metadata when changed.
        if requires_refresh:
            with self._metadata:
                self._update_metadata(stream)

        return operator.emitted_points

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

        if value_downsamplers is None:
            value_downsamplers = self.value_downsamplers
        if time_downsamplers is None:
            time_downsamplers = self.time_downsamplers

        stream = self._get_stream(stream_id)

        select = []
        where = []
        group_by = []
        order_by = 'ORDER BY time'

        def add_condition(field, operator, value):
            if isinstance(value, datetime.datetime):
                # Ensure timestamps are not out of bounds as queries will behave incorrectly then.
                if value < INFLUXDB_MINIMUM_TIMESTAMP:
                    value = INFLUXDB_MINIMUM_TIMESTAMP
                elif value > INFLUXDB_MAXIMUM_TIMESTAMP:
                    value = INFLUXDB_MAXIMUM_TIMESTAMP

                value = value.replace(microsecond=0).isoformat()

            where.append('%s %s \'%s\'' % (field, operator, value))

        if granularity > stream.highest_granularity:
            granularity = stream.highest_granularity

        if granularity < stream.highest_granularity:
            try:
                if start is None and start_exclusive is None:
                    # Set to earliest datapoint.
                    start = self.get_data(stream_id, stream.highest_granularity)[0]['t']

                if end is None and end_exclusive is None:
                    # Set to latest datapoint.
                    end = self.get_data(stream_id, stream.highest_granularity, reverse=True)[0]['t']
            except IndexError:
                return Datapoints(self, stream, None, [], [])

            # If there are no value downsamplers for the given stream, just return an empty result set.
            if not stream.value_downsamplers:
                return Datapoints(self, stream, None, [], [])

            group_by.append('time(%s)' % GRANULARITY_MAP[granularity.name])
            for downsampler in stream.value_downsamplers:
                select.append((VALUE_DOWNSAMPLER_MAP[downsampler] % 'value') + ' AS %s' % downsampler)
        else:
            select.append('value')
            select.append('value_null')

        if start is not None:
            add_condition('time', '>=', start)
        elif start_exclusive is not None:
            # In case we are performing an aggregation, we need to fix the start interval.
            if group_by:
                start_exclusive = granularity.round_timestamp(start_exclusive)
                start_exclusive += datetime.timedelta(seconds=granularity.duration_in_seconds())
                add_condition('time', '>=', start_exclusive)
            else:
                add_condition('time', '>', start_exclusive)

        # If it is not an aggregate query and upper bound is not specified, use the maximum possible
        # timestamp explicitly, as otherwise the upper bound will be now().
        if end is None and end_exclusive is None and not group_by:
            end = INFLUXDB_MAXIMUM_TIMESTAMP

        if end is not None:
            add_condition('time', '<=', end)
        elif end_exclusive is not None:
            add_condition('time', '<', end_exclusive)

        if group_by:
            group_by = 'GROUP BY %s' % (', '.join(group_by))
        else:
            group_by = ''

        if reverse:
            order_by = 'ORDER BY time DESC'

        if where:
            where = 'WHERE %s' % (' AND '.join(where))
        else:
            where = ''

        query = 'SELECT %(select)s FROM "%(measurement)s" %(where)s %(group_by)s %(order_by)s' % {
            'select': ', '.join(select),
            'measurement': stream.uuid,
            'where': where,
            'group_by': group_by,
            'order_by': order_by,
        }

        return Datapoints(self, stream, query, value_downsamplers, time_downsamplers)
