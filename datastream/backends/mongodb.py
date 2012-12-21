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
                getattr(cls, name) for name in cls.__dict__ if \
                    name != 'values' and inspect.isclass(getattr(cls, name)) and \
                    getattr(cls, name) is not cls._Base and issubclass(getattr(cls, name), cls._Base)
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
            'choices' : api.Granularity.values,
        })
        super(GranularityField, self).__init__(**kwargs)

    def to_python(self, value):
        return getattr(api.Granularity, value)

    def to_mongo(self, value):
        return value.__name__

    def validate(self, value):
        # No need for any special validation and no need for StringField validation
        pass

class DownsampleState(mongoengine.EmbeddedDocument):
    timestamp = mongoengine.DateTimeField()

    meta = dict(
        allow_inheritance = False,
    )

class Metric(mongoengine.Document):
    id = mongoengine.SequenceField(primary_key = True, db_alias = DATABASE_ALIAS)
    external_id = mongoengine.UUIDField()
    value_downsamplers = mongoengine.ListField(mongoengine.StringField(
        choices = [downsampler.name for downsampler in ValueDownsamplers.values],
    ))
    downsample_state = mongoengine.MapField(mongoengine.EmbeddedDocumentField(DownsampleState))
    highest_granularity = GranularityField()
    tags = mongoengine.ListField(mongoengine.DynamicField())

    meta = dict(
        db_alias = DATABASE_ALIAS,
        collection = 'metrics',
        indexes = ('tags', 'external_id'),
        allow_inheritance = False,
    )

class Backend(object):
    value_downsamplers = set([downsampler.name for downsampler in ValueDownsamplers.values])
    time_downsamplers = set([downsampler.name for downsampler in TimeDownsamplers.values])

    def __init__(self, database_name, **connection_settings):
        """
        Initializes the MongoDB backend.

        :param database_name: MongoDB database name
        :param connection_settings: Extra connection settings as defined for `mongoengine.register_connection`
        """

        # Setup the database connection to MongoDB
        mongoengine.connect(database_name, DATABASE_ALIAS, **connection_settings)

        assert \
            set(
                sum([
                    [downsampler.name] + list(getattr(downsampler, 'dependencies', ())) \
                    for downsampler in ValueDownsamplers.values
                ], [])
            ) <= set(api.VALUE_DOWNSAMPLERS.keys())

        assert \
            set(
                sum([
                    [downsampler.name] + list(getattr(downsampler, 'dependencies', ())) \
                    for downsampler in TimeDownsamplers.values
                ], [])
            ) <= set(api.TIME_DOWNSAMPLERS.keys())

        # Ensure indices on datapoints collections
        db = mongoengine.connection.get_db(DATABASE_ALIAS)
        for granularity in api.Granularity.values:
            collection = getattr(db.datapoints, granularity.name)
            collection.ensure_index([
                ('_id', pymongo.ASCENDING),
                ('m', pymongo.ASCENDING),
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

    def _callback(self, metric_id, granularity, datapoint):
        """
        A helper method that invokes the callback when one is registered.

        Should be run after all backend's code so that if raises an exception, backend's state is consistent.
        """

        if self.callback is not None:
            self.callback(str(metric_id), granularity, self._format_datapoint(datapoint))

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

        try:
            metric = Metric.objects.get(tags__all = query_tags)
        except Metric.DoesNotExist:
            # Create a new metric
            metric = Metric()
            metric.external_id = uuid.uuid4()

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

            metric.value_downsamplers = list(value_downsamplers)
            metric.highest_granularity = highest_granularity
            metric.tags = list(self._process_tags(query_tags).union(self._process_tags(tags)))

            # Initialize downsample state
            if highest_granularity != api.Granularity.values[-1]:
                for granularity in api.Granularity.values[api.Granularity.values.index(highest_granularity) + 1:]:
                    state = DownsampleState()
                    # TODO: Or maybe: granularity.round_timestamp(datetime.datetime.now(pytz.utc) + self._time_offset)
                    state.timestamp = None
                    metric.downsample_state[granularity.name] = state

            metric.save()
        except Metric.MultipleObjectsReturned:
            raise exceptions.MultipleMetricsReturned

        return unicode(metric.external_id)

    def _get_metric_tags(self, metric):
        """
        Returns a metric descriptor in the form of tags.
        """

        tags = metric.tags
        tags += [
            { 'metric_id' : unicode(metric.external_id) },
            { 'value_downsamplers' : metric.value_downsamplers },
            { 'time_downsamplers' : self.time_downsamplers },
            { 'highest_granularity' : metric.highest_granularity },
        ]
        return tags

    def get_tags(self, metric_id):
        """
        Returns the tags for the specified metric.

        :param metric_id: Metric identifier
        :return: A list of tags for the metric
        """

        try:
            metric = Metric.objects.get(external_id = metric_id)
            tags = self._get_metric_tags(metric)
        except Metric.DoesNotExist:
            raise exceptions.MetricNotFound

        return tags

    def update_tags(self, metric_id, tags):
        """
        Updates metric tags with new tags, overriding existing ones.

        :param metric_id: Metric identifier
        :param tags: A list of new tags
        """

        Metric.objects(external_id = metric_id).update(set__tags = list(self._process_tags(tags)))

    def remove_tag(self, metric_id, tag):
        """
        Removes metric tag.

        :param metric_id: Metric identifier
        :param tag: Tag value to remove
        """

        Metric.objects(external_id = metric_id).update(pull__tags = tag)

    def clear_tags(self, metric_id):
        """
        Removes (clears) all non-readonly metric tags.

        Care should be taken that some tags are set immediately afterwards which uniquely
        identify a metric to be able to use query the metric, in for example, `ensure_metric`.

        :param metric_id: Metric identifier
        """

        Metric.objects(external_id = metric_id).update(set__tags = [])

    def _get_metric_queryset(self, query_tags):
        """
        Returns a queryset that matches the specified metric tags.

        :param query_tags: Tags that should be matched to metrics
        :return: A filtered queryset
        """

        if query_tags is None:
            query_tags = []

        query_set = Metric.objects.all()
        for tag in query_tags[:]:
            if isinstance(tag, dict):
                if 'metric_id' in tag:
                    query_set = query_set.filter(external_id = tag['metric_id'])
                    query_tags.remove(tag)

        if not query_tags:
            return query_set
        else:
            return query_set.filter(tags__all = query_tags)

    def find_metrics(self, query_tags=None):
        """
        Finds all metrics matching the specified query tags.

        :param query_tags: Tags that should be matched to metrics
        :return: A list of matched metric descriptors
        """

        query_set = self._get_metric_queryset(query_tags)
        return [self._get_metric_tags(m) for m in query_set]

    def insert(self, metric_id, value, timestamp=None):
        """
        Inserts a data point into the data stream.

        :param metric_id: Metric identifier
        :param value: Metric value
        :param timestamp: Timestamp of the value, must be equal or larger (newer) than the latest one, monotonically increasing (optional)
        """

        try:
            metric = Metric.objects.get(external_id = metric_id)
        except Metric.DoesNotExist:
            raise exceptions.MetricNotFound

        # Insert the datapoint into appropriate granularity
        db = mongoengine.connection.get_db(DATABASE_ALIAS)
        collection = getattr(db.datapoints, metric.highest_granularity.name)
        if timestamp is None and self._time_offset == ZERO_TIMEDELTA:
            datapoint = { 'm' : metric.id, 'v' : value }
        else:
            object_id = self._generate_object_id(timestamp)
            datapoint = { '_id' : object_id, 'm' : metric.id, 'v' : value }

        datapoint['_id'] = collection.insert(datapoint, safe = True)

        # Call callback last
        self._callback(metric.external_id, metric.highest_granularity, datapoint)

    def _format_datapoint(self, datapoint):
        """
        Formats a datapoint so it is suitable for user output.

        :param datapoint: Raw datapoint from MongoDB database
        :return: A properly formatted datapoint
        """

        return {
            't' : datapoint.get('t', datapoint['_id'].generation_time),
            'v' : datapoint['v']
        }

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

        try:
            metric = Metric.objects.get(external_id = metric_id)
        except Metric.DoesNotExist:
            raise exceptions.MetricNotFound

        # This should already be checked at the API level
        assert granularity in api.Granularity.values

        if granularity > metric.highest_granularity:
            granularity = metric.highest_granularity

        if granularity == metric.highest_granularity:
            # On highest granularity downsamplers are not used
            value_downsamplers = None
            time_downsamplers = None

        downsamplers = []

        if value_downsamplers is not None:
            value_downsamplers = set(value_downsamplers)
            if not value_downsamplers <= self.value_downsamplers:
                raise exceptions.UnsupportedDownsampler(
                    "Unsupported value downsampler(s): %s" % list(value_downsamplers - self.value_downsamplers),
                )

            downsamplers += ['v.%s' % api.VALUE_DOWNSAMPLERS[d] for d in value_downsamplers]

        if time_downsamplers is not None:
            time_downsamplers = set(time_downsamplers)
            if not time_downsamplers <= self.time_downsamplers:
                raise exceptions.UnsupportedDownsampler(
                    "Unsupported time downsampler(s): %s" % list(time_downsamplers - self.time_downsamplers),
                )

            downsamplers += ['t.%s' % api.TIME_DOWNSAMPLERS[d] for d in time_downsamplers]

        if downsamplers == []:
            downsamplers = None

        # Get the datapoints
        db = mongoengine.connection.get_db(DATABASE_ALIAS)
        collection = getattr(db.datapoints, granularity.name)
        # Round the start time to include the first granular value of the
        # corresponding interval (ie. values from 15:00 to 16:00 are stored at
        # the time 15:00 for hours granularity)
        time_query = {
            '$gte' : objectid.ObjectId.from_datetime(granularity.round_timestamp(start)),
        }

        if end is not None:
            # We add one second and use strict less-than to cover all
            # possible ObjectId values in a given "end" timestamp
            end += datetime.timedelta(seconds = 1)
            # No need to round the end time as the last granularity is
            # automatically included
            time_query.update({
                '$lt' : objectid.ObjectId.from_datetime(end),
            })

        pts = collection.find({
            '_id' : time_query,
            'm' : metric.id,
        }, downsamplers).sort('_id')

        return [self._format_datapoint(x) for x in pts]

    def delete_metrics(self, query_tags=None):
        """
        Deletes datapoints for all metrics matching the specified
        query tags. If no query tags are specified, all downstream-related
        data is deleted from the backend.

        :param query_tags: Tags that should be matched to metrics
        """

        if query_tags is None:
            db = mongoengine.connection.get_db(DATABASE_ALIAS)
            for granularity in api.Granularity.values:
                collection = getattr(db.datapoints, granularity.name)
                collection.drop()
            db.metrics.drop()
        else:
            # TODO: Implement
            raise NotImplementedError

    def downsample_metrics(self, query_tags=None):
        """
        Requests the backend to downsample all metrics matching the specified
        query tags.

        :param query_tags: Tags that should be matched to metrics
        """

        now = datetime.datetime.now(pytz.utc) + self._time_offset
        qs = self._get_metric_queryset(query_tags)

        for metric in qs:
            self._downsample_check(metric, now)

    def _downsample_check(self, metric, datum_timestamp):
        """
        Checks if we need to perform any metric downsampling. In case it is needed,
        we perform downsampling.

        :param metric: Metric instance
        :param datum_timestamp: Timestamp of the newly inserted datum
        """
        for granularity in api.Granularity.values[api.Granularity.values.index(metric.highest_granularity) + 1:]:
            state = metric.downsample_state.get(granularity.name, None)
            rounded_timestamp = granularity.round_timestamp(datum_timestamp)
            if state is None or state.timestamp is None or rounded_timestamp > state.timestamp:
                self._downsample(metric, granularity, rounded_timestamp)

    def _generate_object_id(self, timestamp=None):
        """
        Generates a new ObjectId.

        :param timestamp: Desired timestamp (optional)
        """

        oid = objectid.EMPTY

        # 4 bytes current time
        if timestamp is not None:
            oid += struct.pack('>i', int(calendar.timegm(timestamp.utctimetuple()) + self._time_offset.total_seconds()))
        else:
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

    def _generate_timed_metric_object_id(self, timestamp, metric_id):
        """
        Generates a unique ObjectID for a specific timestamp and metric identifier.

        :param timestamp: Desired timestamp
        :param metric_id: 8-byte packed metric identifier
        :return: A valid object identifier
        """

        oid = objectid.EMPTY
        # 4 bytes timestamp
        oid += struct.pack('>i', int(calendar.timegm(timestamp.utctimetuple())))
        # 8 bytes of packed metric identifier
        oid += metric_id
        return objectid.ObjectId(oid)

    def _downsample(self, metric, granularity, current_timestamp):
        """
        Performs downsampling on the given metric and granularity.

        :param metric: Metric instance
        :param granularity: Lower granularity to downsample into
        :param current_timestamp: Timestamp of the last inserted datapoint, rounded
                                  to specified granularity
        """

        db = mongoengine.connection.get_db(DATABASE_ALIAS)

        # Determine the interval that needs downsampling
        datapoints = getattr(db.datapoints, metric.highest_granularity.name)
        state = metric.downsample_state[granularity.name]
        if state.timestamp is not None:
            datapoints = datapoints.find({
                '_id' : {
                    '$gte' : objectid.ObjectId.from_datetime(state.timestamp),
                    '$lt'  : objectid.ObjectId.from_datetime(current_timestamp)
                },
                'm' : metric.id,
            })
        else:
            # All datapoints should be selected as we obviously haven't done any downsampling yet
            datapoints = datapoints.find({
                '_id' : {
                    '$lt' : objectid.ObjectId.from_datetime(current_timestamp)
                },
                'm' : metric.id
            })

        # Construct downsampler instances
        value_downsamplers = []
        for downsampler in ValueDownsamplers.values:
            if downsampler.name in metric.value_downsamplers:
                value_downsamplers.append(downsampler())

        time_downsamplers = []
        for downsampler in TimeDownsamplers.values:
            time_downsamplers.append(downsampler())

        # Pack metric identifier to be used for object id generation
        metric_id = struct.pack('>Q', metric.id)

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
            point_id = self._generate_timed_metric_object_id(timestamp, metric_id)
            datapoint = { '_id' : point_id, 'm' : metric.id, 'v' : value, 't' : time }
            downsampled_points.update(
                { '_id' : point_id, 'm' : metric.id },
                datapoint,
                upsert = True,
                safe = True,
            )

            datapoints_for_callback.append((metric.external_id, granularity, datapoint))

        downsampled_points = getattr(db.datapoints, granularity.name)
        last_timestamp = None
        for datapoint in datapoints.sort('_id'):
            ts = datapoint['_id'].generation_time
            rounded_timestamp = granularity.round_timestamp(ts)
            if last_timestamp is None:
                for x in value_downsamplers + time_downsamplers:
                    x.initialize()
            elif last_timestamp != rounded_timestamp:
                store_downsampled_datapoint(rounded_timestamp)

            last_timestamp = rounded_timestamp

            # Update all downsamplers for the current datapoint
            for x in value_downsamplers:
                x.update(datapoint['v'])
            for x in time_downsamplers:
                x.update(int(calendar.timegm(ts.utctimetuple())))

        # Don't forget to store the last downsampled datapoint
        if last_timestamp is not None:
            store_downsampled_datapoint(last_timestamp)

        # At the end, update the current timestamp in downsample_state
        metric.downsample_state[granularity.name].timestamp = last_timestamp

        # And call callback for all new datapoints
        for args in datapoints_for_callback:
            self._callback(*args)
