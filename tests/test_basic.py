import datetime, time, unittest

import pytz

import mongoengine

import datastream
from datastream import exceptions

from datastream.backends import mongodb

class MongoDBBasicTest(unittest.TestCase):
    database_name = 'test_database'

    def _test_callback(self, metric_id, granularity, datapoint):
        self._callback_points.append((metric_id, granularity, datapoint))

    def setUp(self):
        self.datastream = datastream.Datastream(mongodb.Backend(self.database_name), self._test_callback)
        self.value_downsamplers = self.datastream.backend.value_downsamplers
        self.time_downsamplers = self.datastream.backend.time_downsamplers
        self._callback_points = []

    def tearDown(self):
        db = mongoengine.connection.get_db(mongodb.DATABASE_ALIAS)
        for collection in db.collection_names():
            if collection == 'system.indexes':
                continue
            db.drop_collection(collection)

#@unittest.skip("performing stress test")
class BasicTest(MongoDBBasicTest):
    def test_basic(self):
        query_tags = [
            {'name': 'foobar'},
        ]
        tags = [
            'more',
            {'andmore': 'bar'},
        ]
        metric_id = self.datastream.ensure_metric(query_tags, tags, self.value_downsamplers, datastream.Granularity.Seconds)

        metric = datastream.Metric(self.datastream.get_tags(metric_id))
        self.assertEqual(metric.id, metric_id)
        self.assertItemsEqual(metric.value_downsamplers, self.value_downsamplers)
        self.assertItemsEqual(metric.time_downsamplers, self.time_downsamplers)
        self.assertEqual(metric.highest_granularity, datastream.Granularity.Seconds)
        self.assertItemsEqual(metric.tags, query_tags + tags)

        # Test metric tag manipulation
        rm_tags = self.datastream.get_tags(metric_id)
        self.datastream.remove_tag(metric_id, 'more')
        new_tags = self.datastream.get_tags(metric_id)
        rm_tags.remove('more')
        self.assertItemsEqual(new_tags, rm_tags)

        self.datastream.clear_tags(metric_id)
        metric = datastream.Metric(self.datastream.get_tags(metric_id))
        self.assertItemsEqual(metric.tags, [])

        self.datastream.update_tags(metric_id, query_tags + tags)
        metric = datastream.Metric(self.datastream.get_tags(metric_id))
        self.assertItemsEqual(metric.tags, query_tags + tags)

        # Should not do anything
        self.datastream.downsample_metrics()

        data = self.datastream.get_data(metric_id, datastream.Granularity.Seconds, datetime.datetime.utcfromtimestamp(0), datetime.datetime.utcfromtimestamp(time.time()))
        self.assertItemsEqual(data, [])

        data = self.datastream.get_data(metric_id, datastream.Granularity.Minutes, datetime.datetime.utcfromtimestamp(0), datetime.datetime.utcfromtimestamp(time.time()))
        self.assertItemsEqual(data, [])

        # Callback should not have been fired
        self.assertItemsEqual(self._callback_points, [])

        self.datastream.append(metric_id, 42)
        self.assertRaises(datastream.exceptions.InvalidTimestamp, lambda: self.datastream.append(metric_id, 42, datetime.datetime.min))

        data = self.datastream.get_data(metric_id, datastream.Granularity.Seconds, datetime.datetime.utcfromtimestamp(0), end_exclusive=datetime.datetime.utcfromtimestamp(time.time()))
        self.assertEqual(len(data), 0)

        data = self.datastream.get_data(metric_id, datastream.Granularity.Seconds, datetime.datetime.utcfromtimestamp(0), datetime.datetime.utcfromtimestamp(time.time()))
        self.assertEqual(len(data), 1)

        self.assertEqual(len(self._callback_points), 1)
        cb_metric_id, cb_granularity, cb_datapoint = self._callback_points[0]
        self.assertEqual(cb_metric_id, metric_id)
        self.assertEqual(cb_granularity, datastream.Granularity.Seconds)
        self.assertItemsEqual(cb_datapoint, data[0])

        data = self.datastream.get_data(metric_id, datastream.Granularity.Minutes, datetime.datetime.utcfromtimestamp(0))
        self.assertItemsEqual(data, [])

        # Artificially increase backend time for a minute so that downsample will do something for minute granularity
        self.datastream.backend._time_offset += datetime.timedelta(minutes=1)

        self.datastream.downsample_metrics()

        data = self.datastream.get_data(
            metric_id,
            datastream.Granularity.Seconds,
            datetime.datetime.utcfromtimestamp(0),
            datetime.datetime.utcfromtimestamp(time.time()) + self.datastream.backend._time_offset,
        )
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['v'], 42)

        data = self.datastream.get_data(metric_id, datastream.Granularity.Seconds, datetime.datetime.utcfromtimestamp(0))
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['v'], 42)

        # TODO: Sometimes test fail with 4 datapoints here, why?
        self.assertEqual(len(self._callback_points), 3)
        cb_metric_id, cb_granularity, cb_datapoint = self._callback_points[1]
        self.assertEqual(cb_metric_id, metric_id)
        self.assertEqual(cb_granularity, datastream.Granularity.Seconds10)
        cb_metric_id, cb_granularity, cb_datapoint = self._callback_points[2]
        self.assertEqual(cb_metric_id, metric_id)
        self.assertEqual(cb_granularity, datastream.Granularity.Minutes)

        value_downsamplers_keys = [datastream.VALUE_DOWNSAMPLERS[d] for d in self.value_downsamplers]
        time_downsamplers_keys = [datastream.TIME_DOWNSAMPLERS[d] for d in self.time_downsamplers]

        data = self.datastream.get_data(
            metric_id,
            datastream.Granularity.Minutes,
            datetime.datetime.utcfromtimestamp(0),
            datetime.datetime.utcfromtimestamp(time.time()) + self.datastream.backend._time_offset,
        )
        self.assertEqual(len(data), 1)
        self.assertItemsEqual(data[0]['v'].keys(), value_downsamplers_keys)
        self.assertItemsEqual(data[0]['t'].keys(), time_downsamplers_keys)
        self.assertItemsEqual(data[0], cb_datapoint)

        data = self.datastream.get_data(metric_id, datastream.Granularity.Minutes, datetime.datetime.utcfromtimestamp(0))
        self.assertEqual(len(data), 1)
        self.assertItemsEqual(data[0]['v'].keys(), value_downsamplers_keys)
        self.assertItemsEqual(data[0]['t'].keys(), time_downsamplers_keys)
        self.assertTrue(datastream.VALUE_DOWNSAMPLERS['count'] in data[0]['v'].keys())

        data = self.datastream.get_data(metric_id, datastream.Granularity.Minutes, datetime.datetime.utcfromtimestamp(0), value_downsamplers=('count',))
        self.assertEqual(len(data), 1)
        self.assertItemsEqual(data[0]['v'].keys(), (datastream.VALUE_DOWNSAMPLERS['count'],))
        self.assertEqual(data[0]['v'][datastream.VALUE_DOWNSAMPLERS['count']], 1)

    def test_timestamp_ranges(self):
        metric_id = self.datastream.ensure_metric([{'name': 'foopub'}], [], self.value_downsamplers, datastream.Granularity.Seconds)
        with self.assertRaises(exceptions.InvalidTimestamp):
            self.datastream.append(metric_id, 1, mongodb.Backend._min_timestamp - datetime.timedelta(seconds=1))
        with self.assertRaises(exceptions.InvalidTimestamp):
            self.datastream.append(metric_id, 1, datetime.datetime.min)
        with self.assertRaises(exceptions.InvalidTimestamp):
            self.datastream.append(metric_id, 1, mongodb.Backend._max_timestamp + datetime.timedelta(seconds=1))
        with self.assertRaises(exceptions.InvalidTimestamp):
            self.datastream.append(metric_id, 1, datetime.datetime.max)

    def test_monotonicity_multiple(self):
        metric_id = self.datastream.ensure_metric([{'name': 'fooclub'}], [], self.value_downsamplers, datastream.Granularity.Seconds)

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)

        self.datastream.append(metric_id, 1, ts)
        self.datastream.append(metric_id, 2, ts)
        self.datastream.append(metric_id, 3, ts)
        self.datastream.append(metric_id, 4, ts)
        self.datastream.append(metric_id, 5, ts)

        self.datastream.downsample_metrics(until=ts + datetime.timedelta(hours=10))

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start=ts)
        self.assertEqual(len(data), 5)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start=ts, end=ts)
        self.assertEqual(len(data), 5)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start_exclusive=ts)
        self.assertEqual(len(data), 0)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start_exclusive=ts, end=ts)
        self.assertEqual(len(data), 0)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start_exclusive=ts, end_exclusive=ts)
        self.assertEqual(len(data), 0)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start=ts, end_exclusive=ts)
        self.assertEqual(len(data), 0)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds10, start=ts)
        self.assertEqual(len(data), 1)

        self.assertEqual(data[0]['t']['a'], ts) # first
        self.assertEqual(data[0]['t']['z'], ts) # last
        self.assertEqual(data[0]['t']['m'], ts) # mean
        self.assertEqual(data[0]['t']['a'], ts) # median
        self.assertEqual(data[0]['v']['c'], 5) # count
        self.assertEqual(data[0]['v']['d'], 2.5) # standard deviation
        self.assertEqual(data[0]['v']['m'], 3.0) # mean
        self.assertEqual(data[0]['v']['l'], 1) # minimum
        self.assertEqual(data[0]['v']['q'], 55) # sum of squares
        self.assertEqual(data[0]['v']['s'], 15) # sum
        self.assertEqual(data[0]['v']['u'], 5) # maximum

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds10, start=ts)
        self.assertEqual(len(data), 1)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Minutes, start=ts)
        self.assertEqual(len(data), 1)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Minutes10, start=ts)
        self.assertEqual(len(data), 1)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Hours, start=ts)
        self.assertEqual(len(data), 1)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Hours6, start=ts)
        self.assertEqual(len(data), 1)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Days, start=ts)
        self.assertEqual(len(data), 0)

    def test_monotonicity_timestamp(self):
        metric_id = self.datastream.ensure_metric([{'name': 'fooclub'}], [], self.value_downsamplers, datastream.Granularity.Seconds)
        self.datastream.append(metric_id, 1, datetime.datetime(2000, 1, 1, 12, 0, 0))
        self.datastream.append(metric_id, 1, datetime.datetime(2000, 1, 3, 12, 0, 0))
        with self.assertRaises(exceptions.InvalidTimestamp):
            self.datastream.append(metric_id, 1, datetime.datetime(2000, 1, 2, 12, 0, 0))
        self.datastream.append(metric_id, 1, datetime.datetime(2000, 1, 2, 12, 0, 0), False)

    def test_monotonicity_realtime(self):
        metric_id = self.datastream.ensure_metric([{'name': 'fooclub'}], [], self.value_downsamplers, datastream.Granularity.Seconds)
        self.datastream.append(metric_id, 1)

        # Artificially increase backend time for a minute
        self.datastream.backend._time_offset += datetime.timedelta(minutes=1)

        self.datastream.append(metric_id, 1)

        # Artificially decrease backend time for 30 seconds (cannot be 1 minute because this disabled testing code-path)
        self.datastream.backend._time_offset -= datetime.timedelta(seconds=30)

        with self.assertRaises(exceptions.InvalidTimestamp):
            self.datastream.append(metric_id, 1)

        self.datastream.append(metric_id, 1, check_timestamp=False)

    def test_downsample_freeze(self):
        metric_id = self.datastream.ensure_metric([{'name': 'fooclub'}], [], self.value_downsamplers, datastream.Granularity.Seconds)

        self.datastream.append(metric_id, 1, datetime.datetime(2000, 1, 1, 12, 0, 0))
        self.datastream.append(metric_id, 1, datetime.datetime(2000, 1, 3, 12, 0, 0))

        self.datastream.downsample_metrics(until=datetime.datetime(2000, 1, 3, 12, 0, 10))

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds10, start=datetime.datetime.min)
        self.assertEqual(len(data), 2)

        with self.assertRaises(exceptions.InvalidTimestamp):
            self.datastream.append(metric_id, 1, datetime.datetime(2000, 1, 3, 12, 0, 0))
        with self.assertRaises(exceptions.InvalidTimestamp):
            self.datastream.append(metric_id, 1, datetime.datetime(2000, 1, 3, 12, 0, 5))

        self.datastream.append(metric_id, 1, datetime.datetime(2000, 1, 3, 12, 0, 10))
        self.datastream.append(metric_id, 1, datetime.datetime(2000, 1, 4, 12, 0, 0))

        self.datastream.downsample_metrics(until=datetime.datetime(2000, 1, 10, 12, 0, 0))

        with self.assertRaises(exceptions.InvalidTimestamp):
            self.datastream.append(metric_id, 1, datetime.datetime(2000, 1, 4, 12, 0, 0))
        with self.assertRaises(exceptions.InvalidTimestamp):
            self.datastream.append(metric_id, 1, datetime.datetime(2000, 1, 5, 12, 0, 0))
        with self.assertRaises(exceptions.InvalidTimestamp):
            self.datastream.append(metric_id, 1, datetime.datetime(2000, 1, 9, 12, 0, 0))

        self.datastream.append(metric_id, 1, datetime.datetime(2000, 1, 10, 12, 0, 0))
        self.datastream.append(metric_id, 1, datetime.datetime(2000, 1, 10, 12, 0, 1))

    def test_granularities(self):
        query_tags = [
            {'name': 'foodata'},
        ]
        tags = []

        metric_id = self.datastream.ensure_metric(query_tags, tags, self.value_downsamplers, datastream.Granularity.Seconds)

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0)
        for i in range(1200):
            self.datastream.append(metric_id, i, ts)
            ts += datetime.timedelta(seconds=1)

        self.datastream.downsample_metrics(until=ts)

        s = datetime.datetime(2000, 1, 1, 12, 0, 0)
        e = datetime.datetime(2000, 1, 1, 12, 1, 0)

        # SECONDS
        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start=s, end=e)
        self.assertEqual(len(data), 61)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start_exclusive=s, end=e)
        self.assertEqual(len(data), 60)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start=s, end_exclusive=e)
        self.assertEqual(len(data), 60)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start_exclusive=s, end_exclusive=e)
        self.assertEqual(len(data), 59)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start=s)
        self.assertEqual(len(data), 1200)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start_exclusive=s)
        self.assertEqual(len(data), 1199)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start=s, end=datetime.datetime.max)
        self.assertEqual(len(data), 1200)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start_exclusive=s, end=datetime.datetime.max)
        self.assertEqual(len(data), 1199)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start=s, end_exclusive=datetime.datetime.max)
        self.assertEqual(len(data), 1200)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start_exclusive=s, end_exclusive=datetime.datetime.max)
        self.assertEqual(len(data), 1199)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start=datetime.datetime.min, end=e)
        self.assertEqual(len(data), 61)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start_exclusive=datetime.datetime.min, end=e)
        self.assertEqual(len(data), 61)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start=datetime.datetime.min, end_exclusive=e)
        self.assertEqual(len(data), 60)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start_exclusive=datetime.datetime.min, end_exclusive=e)
        self.assertEqual(len(data), 60)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start=datetime.datetime.min)
        self.assertEqual(len(data), 1200)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start_exclusive=datetime.datetime.min)
        self.assertEqual(len(data), 1200)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start=datetime.datetime.min, end=datetime.datetime.max)
        self.assertEqual(len(data), 1200)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start_exclusive=datetime.datetime.min, end=datetime.datetime.max)
        self.assertEqual(len(data), 1200)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start=datetime.datetime.min, end_exclusive=datetime.datetime.max)
        self.assertEqual(len(data), 1200)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start_exclusive=datetime.datetime.min, end_exclusive=datetime.datetime.max)
        self.assertEqual(len(data), 1200)

        #10 SECONDS
        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds10, start=s, end=e)
        self.assertEqual(len(data), 7)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds10, start_exclusive=s, end=e)
        self.assertEqual(len(data), 6)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds10, start=s, end_exclusive=e)
        self.assertEqual(len(data), 6)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds10, start_exclusive=s, end_exclusive=e)
        self.assertEqual(len(data), 5)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds10, start=datetime.datetime.min, end=datetime.datetime.max)
        self.assertEqual(len(data), 120)

        # MINUTES
        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Minutes, start=s, end=e)
        self.assertEqual(len(data), 2)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Minutes, start_exclusive=s, end=e)
        self.assertEqual(len(data), 1)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Minutes, start=s, end_exclusive=e)
        self.assertEqual(len(data), 1)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Minutes, start_exclusive=s, end_exclusive=e)
        self.assertEqual(len(data), 0)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Minutes, start=datetime.datetime.min, end=datetime.datetime.max)
        self.assertEqual(len(data), 20)

        # 10 MINUTES
        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Minutes10, start=datetime.datetime.min)
        self.assertEqual(len(data), 2)

        # HOURS
        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Hours, start=datetime.datetime.min)
        self.assertEqual(len(data), 0)

        # 6 HOURS
        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Hours6, start=datetime.datetime.min)
        self.assertEqual(len(data), 0)

        # DAYS
        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Days, start=datetime.datetime.min)
        self.assertEqual(len(data), 0)

    def test_granularities_multiple(self):
        query_tags = [
            {'name': 'foodata'},
        ]
        tags = []

        metric_id = self.datastream.ensure_metric(query_tags, tags, self.value_downsamplers, datastream.Granularity.Seconds)

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0)
        for i in range(1200):
            for j in range(3):
                self.datastream.append(metric_id, j * i, ts)
            ts += datetime.timedelta(seconds=1)

        self.datastream.downsample_metrics(until=ts)

        s = datetime.datetime(2000, 1, 1, 12, 0, 0)
        e = datetime.datetime(2000, 1, 1, 12, 1, 0)

        # SECONDS
        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start=s, end=e)
        self.assertEqual(len(data), 3 * 61)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start_exclusive=s, end=e)
        self.assertEqual(len(data), 3 * 60)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start=s, end_exclusive=e)
        self.assertEqual(len(data), 3 * 60)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start_exclusive=s, end_exclusive=e)
        self.assertEqual(len(data), 3 * 59)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start=s)
        self.assertEqual(len(data), 3 * 1200)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start_exclusive=s)
        self.assertEqual(len(data), 3 * 1199)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start=s, end=datetime.datetime.max)
        self.assertEqual(len(data), 3 * 1200)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start_exclusive=s, end=datetime.datetime.max)
        self.assertEqual(len(data), 3 * 1199)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start=s, end_exclusive=datetime.datetime.max)
        self.assertEqual(len(data), 3 * 1200)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start_exclusive=s, end_exclusive=datetime.datetime.max)
        self.assertEqual(len(data), 3 * 1199)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start=datetime.datetime.min, end=e)
        self.assertEqual(len(data), 3 * 61)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start_exclusive=datetime.datetime.min, end=e)
        self.assertEqual(len(data), 3 * 61)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start=datetime.datetime.min, end_exclusive=e)
        self.assertEqual(len(data), 3 * 60)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start_exclusive=datetime.datetime.min, end_exclusive=e)
        self.assertEqual(len(data), 3 * 60)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start=datetime.datetime.min)
        self.assertEqual(len(data), 3 * 1200)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start_exclusive=datetime.datetime.min)
        self.assertEqual(len(data), 3 * 1200)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start=datetime.datetime.min, end=datetime.datetime.max)
        self.assertEqual(len(data), 3 * 1200)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start_exclusive=datetime.datetime.min, end=datetime.datetime.max)
        self.assertEqual(len(data), 3 * 1200)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start=datetime.datetime.min, end_exclusive=datetime.datetime.max)
        self.assertEqual(len(data), 3 * 1200)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds, start_exclusive=datetime.datetime.min, end_exclusive=datetime.datetime.max)
        self.assertEqual(len(data), 3 * 1200)

        #10 SECONDS
        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds10, start=s, end=e)
        self.assertEqual(len(data), 7)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds10, start_exclusive=s, end=e)
        self.assertEqual(len(data), 6)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds10, start=s, end_exclusive=e)
        self.assertEqual(len(data), 6)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds10, start_exclusive=s, end_exclusive=e)
        self.assertEqual(len(data), 5)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Seconds10, start=datetime.datetime.min, end=datetime.datetime.max)
        self.assertEqual(len(data), 120)

        # MINUTES
        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Minutes, start=s, end=e)
        self.assertEqual(len(data), 2)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Minutes, start_exclusive=s, end=e)
        self.assertEqual(len(data), 1)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Minutes, start=s, end_exclusive=e)
        self.assertEqual(len(data), 1)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Minutes, start_exclusive=s, end_exclusive=e)
        self.assertEqual(len(data), 0)

        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Minutes, start=datetime.datetime.min, end=datetime.datetime.max)
        self.assertEqual(len(data), 20)

        # 10 MINUTES
        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Minutes10, start=datetime.datetime.min)
        self.assertEqual(len(data), 2)

        # HOURS
        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Hours, start=datetime.datetime.min)
        self.assertEqual(len(data), 0)

        # 6 HOURS
        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Hours6, start=datetime.datetime.min)
        self.assertEqual(len(data), 0)

        # DAYS
        data = self.datastream.get_data(metric_id, self.datastream.Granularity.Days, start=datetime.datetime.min)
        self.assertEqual(len(data), 0)

@unittest.skip("stress test")
class StressTest(MongoDBBasicTest):
    def test_stress(self):
        metric_id = self.datastream.ensure_metric(
            [{'name': 'stressme'}],
            [],
            self.value_downsamplers,
            datastream.Granularity.Seconds,
        )

        # 1 year, append each second, downsample after each hour
        ts = datetime.datetime(2000, 1, 1, 12, 0, 0)
        start_time = time.time()
        for i in range(1, 356 * 24 * 60 * 60):
            self.datastream.append(metric_id, i, ts)
            ts += datetime.timedelta(seconds=1)

            if i % 3600 == 0:
                t1 = time.time()
                self.datastream.downsample_metrics(until=ts)
                t2 = time.time()
                print "%08d insert: %d:%02d    downsample: %d:%02d" % (
                    i,
                    (t1 - start_time) / 60,
                    (t1 - start_time) % 60,
                    (t2 - t1) / 60,
                    (t2 - t1) % 60,
                )

                start_time = t2
