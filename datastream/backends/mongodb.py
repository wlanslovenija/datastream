import calendar, datetime, inspect, os, struct, time, uuid

import pytz

import pymongo
from bson import objectid

import mongoengine

from .. import api, exceptions, utils

DATABASE_ALIAS = 'datastream'

# The largest integer that can be stored in MongoDB; larger values need to use floats
MAXIMUM_INTEGER = 2**63 - 1

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
                getattr(cls, name) for name in cls.__dict__ if
                    name != 'values' and inspect.isclass(getattr(cls, name)) and getattr(cls, name) is not cls._Base and issubclass(getattr(cls, name), cls._Base)
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
            self.sum += datum

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
            self.sum += datum * datum

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
                values[self.key] = (n * ss - s**2) / (n * (n - 1))

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

class GranularityField(mongoengine.StringField):
    def __init__(self, **kwargs):
        kwargs.update({
            'choices': api.Granularity.values,
        })
        super(GranularityField, self).__init__(**kwargs)

    def to_python(self, value):
        return getattr(api.Granularity, value)

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
        if self.cursor is not None:
            for datapoint in self.cursor:
                yield self.stream._format_datapoint(datapoint)

    def __getitem__(self, key):
        if self.cursor is None:
            raise IndexError

        for datapoint in self.cursor.__getitem__(key):
            yield self.stream._format_datapoint(datapoint)

class DownsampleState(mongoengine.EmbeddedDocument):
    timestamp = mongoengine.DateTimeField()

    meta = dict(
        allow_inheritance=False,
    )

class Stream(mongoengine.Document):
    id = mongoengine.SequenceField(primary_key=True, db_alias=DATABASE_ALIAS)
    # TODO: Use binary UUID format
    external_id = mongoengine.UUIDField()
    value_downsamplers = mongoengine.ListField(mongoengine.StringField(
        choices=[downsampler.name for downsampler in ValueDownsamplers.values],
    ))
    downsample_state = mongoengine.MapField(mongoengine.EmbeddedDocumentField(DownsampleState))
    highest_granularity = GranularityField()
    tags = mongoengine.ListField(mongoengine.DynamicField())

    meta = dict(
        db_alias=DATABASE_ALIAS,
        collection='streams',
        indexes=('tags', 'external_id'),
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
    _max_timestamp = datetime.datetime.fromtimestamp(2**31-1, tz=pytz.utc)

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
            # TODO: Do we really need the following two indexes?
            collection.ensure_index([
                ('_id', pymongo.ASCENDING),
            ])
            collection.ensure_index([
                ('_id', pymongo.DESCENDING),
            ])

        self.callback = None

        # Used only to artificially advance time when testing, don't use!
        self._time_offset = ZERO_TIMEDELTA

    def set_callback(self, callback):
        """
        Sets a datapoint notification callback that will be called every time a
        new datapoint is added to the datastream.
        """

        self.callback = callback

    def _callback(self, stream_id, granularity, datapoint):
        """
        A helper method that invokes the callback when one is registered.

        Should be run after all backend's code so that if raises an exception, backend's state is consistent.
        """

        if self.callback is not None:
            self.callback(str(stream_id), granularity, self._format_datapoint(datapoint))

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

    def ensure_stream(self, query_tags, tags, value_downsamplers, highest_granularity):
        """
        Ensures that a specified stream exists.

        :param query_tags: Tags which uniquely identify a stream
        :param tags: Tags that should be used (together with `query_tags`) to create a
                     stream when it doesn't yet exist
        :param value_downsamplers: A set of names of value downsampler functions for this stream
        :param highest_granularity: Predicted highest granularity of the data the stream
                                    will store, may be used to optimize data storage
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

            # Initialize downsample state
            if highest_granularity != api.Granularity.values[-1]:
                for granularity in api.Granularity.values[api.Granularity.values.index(highest_granularity) + 1:]:
                    state = DownsampleState()
                    # TODO: Or maybe: granularity.round_timestamp(datetime.datetime.now(pytz.utc) + self._time_offset)
                    state.timestamp = None
                    stream.downsample_state[granularity.name] = state

            stream.save()
        except Stream.MultipleObjectsReturned:
            raise exceptions.MultipleStreamsReturned

        return unicode(stream.external_id)

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
        ]
        return tags

    def get_tags(self, stream_id):
        """
        Returns the tags for the specified stream.

        :param stream_id: Stream identifier
        :return: A list of tags for the stream
        """

        try:
            stream = Stream.objects.get(external_id=stream_id)
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

        Stream.objects(external_id=stream_id).update(set__tags=list(self._process_tags(tags)))

    def remove_tag(self, stream_id, tag):
        """
        Removes stream tag.

        :param stream_id: Stream identifier
        :param tag: Tag value to remove
        """

        Stream.objects(external_id=stream_id).update(pull__tags=tag)

    def clear_tags(self, stream_id):
        """
        Removes (clears) all non-readonly stream tags.

        Care should be taken that some tags are set immediately afterwards which uniquely
        identify a stream to be able to use query the stream, in for example, `ensure_stream`.

        :param stream_id: Stream identifier
        """

        Stream.objects(external_id=stream_id).update(set__tags=[])

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
                    query_set = query_set.filter(external_id=tag['stream_id'])
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

    def append(self, stream_id, value, timestamp=None, check_timestamp=True):
        """
        Appends a datapoint into the datastream.

        :param stream_id: Stream identifier
        :param value: Datapoint value
        :param timestamp: Datapoint timestamp, must be equal or larger (newer) than the latest one, monotonically increasing (optional)
        :param check_timestamp: Check if timestamp is equal or larger (newer) than the latest one (default: true)
        """

        self._supported_timestamp_range(timestamp)

        try:
            stream = Stream.objects.get(external_id=stream_id)
        except Stream.DoesNotExist:
            raise exceptions.StreamNotFound

        # Append the datapoint into appropriate granularity
        db = mongoengine.connection.get_db(DATABASE_ALIAS)
        collection = getattr(db.datapoints, stream.highest_granularity.name)
        if timestamp is None and self._time_offset == ZERO_TIMEDELTA:
            datapoint = {'m' : stream.id, 'v' : value}
        else:
            object_id = self._generate_object_id(timestamp)

            if check_timestamp:
                # TODO: There is a race condition here, between check and insert
                latest_timestamp = self._last_timestamp(stream)
                if object_id.generation_time < latest_timestamp:
                    raise exceptions.InvalidTimestamp("Datapoint timestamp must be equal or larger (newer) than the latest one '%s': %s" % (latest_timestamp, object_id.generation_time))

            # We always check this because it does not require database access
            self._timestamp_after_downsampled(stream, object_id.generation_time)

            datapoint = {'_id' : object_id, 'm' : stream.id, 'v' : value}

        datapoint['_id'] = collection.insert(datapoint, safe=True)

        if timestamp is None and self._time_offset == ZERO_TIMEDELTA:
            # When timestamp is not specified, database generates one, so we check it here
            try:
                self._timestamp_after_downsampled(stream, datapoint['_id'].generation_time)
            except exceptions.InvalidTimestamp:
                # Cleanup
                collection.remove(datapoint['_id'], safe=True)

                raise

        # Call callback last
        self._callback(stream.external_id, stream.highest_granularity, datapoint)

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
            stream = Stream.objects.get(external_id=stream_id)
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
        """

        if until is None:
            # TODO: Hm, this is not completely correct, because client time could be different than server time, we should allow use where client does not have to specify any timestamp and everything is done on the server
            until = datetime.datetime.now(pytz.utc) + self._time_offset

        for stream in self._get_stream_queryset(query_tags):
            self._downsample_check(stream, until)

    def _downsample_check(self, stream, until_timestamp):
        """
        Checks if we need to perform any stream downsampling. In case it is needed,
        we perform downsampling.

        :param stream: Stream instance
        :param until_timestamp: Timestamp of the newly inserted datum
        """

        for granularity in api.Granularity.values[api.Granularity.values.index(stream.highest_granularity) + 1:]:
            state = stream.downsample_state.get(granularity.name, None)
            rounded_timestamp = granularity.round_timestamp(until_timestamp)
            # TODO: Why "can't compare offset-naive and offset-aware datetimes" is sometimes thrown here?
            if state is None or state.timestamp is None or rounded_timestamp > state.timestamp:
                self._downsample(stream, granularity, rounded_timestamp)

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

        datapoints_for_callback = []

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
            downsampled_points.insert(datapoint, safe=True)

            datapoints_for_callback.append((stream.external_id, granularity, datapoint))

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

        # And call callback for all new datapoints
        for args in datapoints_for_callback:
            self._callback(*args)
