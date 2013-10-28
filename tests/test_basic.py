import datetime, time, unittest

import pytz

import mongoengine

import datastream
from datastream import exceptions

from datastream.backends import mongodb

class MongoDBBasicTest(unittest.TestCase):
    database_name = 'test_database'

    def _test_callback(self, stream_id, granularity, datapoint):
        self._callback_points.append((stream_id, granularity, datapoint))

    def setUp(self):
        backend = mongodb.Backend(self.database_name)
        backend._test_callback = self._test_callback
        self.datastream = datastream.Datastream(backend)
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
        stream_id = self.datastream.ensure_stream(query_tags, tags, self.value_downsamplers, datastream.Granularity.Seconds)

        stream = datastream.Stream(self.datastream.get_tags(stream_id))
        self.assertEqual(stream.id, stream_id)
        self.assertItemsEqual(stream.value_downsamplers, self.value_downsamplers)
        self.assertItemsEqual(stream.time_downsamplers, self.time_downsamplers)
        self.assertEqual(stream.highest_granularity, datastream.Granularity.Seconds)
        self.assertItemsEqual(stream.tags, query_tags + tags)

        # Test stream tag manipulation
        rm_tags = self.datastream.get_tags(stream_id)
        self.datastream.remove_tag(stream_id, 'more')
        new_tags = self.datastream.get_tags(stream_id)
        rm_tags.remove('more')
        self.assertItemsEqual(new_tags, rm_tags)

        self.datastream.clear_tags(stream_id)
        stream = datastream.Stream(self.datastream.get_tags(stream_id))
        self.assertItemsEqual(stream.tags, [])

        self.datastream.update_tags(stream_id, query_tags + tags)
        stream = datastream.Stream(self.datastream.get_tags(stream_id))
        self.assertItemsEqual(stream.tags, query_tags + tags)

        # Should not do anything
        self.assertItemsEqual(self.datastream.downsample_streams(), [])

        data = self.datastream.get_data(stream_id, datastream.Granularity.Seconds, datetime.datetime.utcfromtimestamp(0), datetime.datetime.utcfromtimestamp(time.time()))
        self.assertEqual(len(data), 0)

        data = self.datastream.get_data(stream_id, datastream.Granularity.Minutes, datetime.datetime.utcfromtimestamp(0), datetime.datetime.utcfromtimestamp(time.time()))
        self.assertEqual(len(data), 0)

        # Callback should not have been fired
        self.assertItemsEqual(self._callback_points, [])

        self.assertEqual(self.datastream.append(stream_id, 42)['datapoint']['v'], 42)
        self.assertRaises(datastream.exceptions.InvalidTimestamp, lambda: self.datastream.append(stream_id, 42, datetime.datetime.min))

        data = self.datastream.get_data(stream_id, datastream.Granularity.Seconds, datetime.datetime.utcfromtimestamp(0), end_exclusive=datetime.datetime.utcfromtimestamp(time.time()))
        self.assertEqual(len(data), 0)

        data = self.datastream.get_data(stream_id, datastream.Granularity.Seconds, datetime.datetime.utcfromtimestamp(0), datetime.datetime.utcfromtimestamp(time.time()))
        self.assertEqual(len(data), 1)

        self.assertEqual(len(self._callback_points), 1)
        cb_stream_id, cb_granularity, cb_datapoint = self._callback_points[0]
        self.assertEqual(cb_stream_id, stream_id)
        self.assertEqual(cb_granularity, datastream.Granularity.Seconds)
        self.assertItemsEqual(cb_datapoint, data[0])

        data = self.datastream.get_data(stream_id, datastream.Granularity.Minutes, datetime.datetime.utcfromtimestamp(0))
        self.assertEqual(len(data), 0)

        # Artificially increase backend time for a minute so that downsample will do something for minute granularity
        self.datastream.backend._time_offset += datetime.timedelta(minutes=1)

        new_datapoints = self.datastream.downsample_streams()
        self.assertEquals(len(new_datapoints), 2)
        self.assertEquals(new_datapoints[0]['datapoint']['v'], {'c': 1, 'd': 0, 'm': 42.0, 'l': 42, 'q': 1764, 's': 42, 'u': 42})
        self.assertEquals(new_datapoints[1]['datapoint']['v'], {'c': 1, 'd': 0, 'm': 42.0, 'l': 42, 'q': 1764, 's': 42, 'u': 42})

        data = self.datastream.get_data(
            stream_id,
            datastream.Granularity.Seconds,
            datetime.datetime.utcfromtimestamp(0),
            datetime.datetime.utcfromtimestamp(time.time()) + self.datastream.backend._time_offset,
        )
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['v'], 42)

        data = self.datastream.get_data(stream_id, datastream.Granularity.Seconds, datetime.datetime.utcfromtimestamp(0))
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['v'], 42)

        # TODO: Sometimes test fail with 4 datapoints here, why?
        self.assertEqual(len(self._callback_points), 3)
        cb_stream_id, cb_granularity, cb_datapoint = self._callback_points[1]
        self.assertEqual(cb_stream_id, stream_id)
        self.assertEqual(cb_granularity, datastream.Granularity.Seconds10)
        cb_stream_id, cb_granularity, cb_datapoint = self._callback_points[2]
        self.assertEqual(cb_stream_id, stream_id)
        self.assertEqual(cb_granularity, datastream.Granularity.Minutes)

        value_downsamplers_keys = [datastream.VALUE_DOWNSAMPLERS[d] for d in self.value_downsamplers]
        time_downsamplers_keys = [datastream.TIME_DOWNSAMPLERS[d] for d in self.time_downsamplers]

        data = self.datastream.get_data(
            stream_id,
            datastream.Granularity.Minutes,
            datetime.datetime.utcfromtimestamp(0),
            datetime.datetime.utcfromtimestamp(time.time()) + self.datastream.backend._time_offset,
        )
        self.assertEqual(len(data), 1)
        self.assertItemsEqual(data[0]['v'].keys(), value_downsamplers_keys)
        self.assertItemsEqual(data[0]['t'].keys(), time_downsamplers_keys)
        self.assertItemsEqual(data[0], cb_datapoint)

        data = self.datastream.get_data(stream_id, datastream.Granularity.Minutes, datetime.datetime.utcfromtimestamp(0))
        self.assertEqual(len(data), 1)
        self.assertItemsEqual(data[0]['v'].keys(), value_downsamplers_keys)
        self.assertItemsEqual(data[0]['t'].keys(), time_downsamplers_keys)
        self.assertTrue(datastream.VALUE_DOWNSAMPLERS['count'] in data[0]['v'].keys())

        data = self.datastream.get_data(stream_id, datastream.Granularity.Minutes, datetime.datetime.utcfromtimestamp(0), value_downsamplers=('count',))
        self.assertEqual(len(data), 1)
        self.assertItemsEqual(data[0]['v'].keys(), (datastream.VALUE_DOWNSAMPLERS['count'],))
        self.assertEqual(data[0]['v'][datastream.VALUE_DOWNSAMPLERS['count']], 1)

    def test_derived_streams(self):
        streamA_id = self.datastream.ensure_stream([{'name': 'srcA'}], [], self.value_downsamplers, datastream.Granularity.Seconds)
        streamB_id = self.datastream.ensure_stream([{'name': 'srcB'}], [], self.value_downsamplers, datastream.Granularity.Minutes)

        with self.assertRaises(exceptions.IncompatibleGranularities):
            self.datastream.ensure_stream([{'name': 'derived'}], [], self.value_downsamplers, datastream.Granularity.Seconds,
                derive_from=[streamA_id, streamB_id], derive_op='sum')
        with self.assertRaises(exceptions.IncompatibleGranularities):
            self.datastream.ensure_stream([{'name': 'derived'}], [], self.value_downsamplers, datastream.Granularity.Minutes,
                derive_from=[streamA_id, streamB_id], derive_op='sum')
        with self.assertRaises(exceptions.UnsupportedDeriveOperator):
            self.datastream.ensure_stream([{'name': 'derived'}], [], self.value_downsamplers, datastream.Granularity.Minutes,
                derive_from=[streamA_id, streamB_id], derive_op='foobar')
        with self.assertRaises(exceptions.StreamNotFound):
            self.datastream.ensure_stream([{'name': 'derived'}], [], self.value_downsamplers, datastream.Granularity.Seconds,
                derive_from=[streamA_id, '00000000-0000-0000-0000-000000000000'], derive_op='sum')

        streamA_id = self.datastream.ensure_stream([{'name': 'srcX'}], [], self.value_downsamplers, datastream.Granularity.Seconds)
        streamB_id = self.datastream.ensure_stream([{'name': 'srcY'}], [], self.value_downsamplers, datastream.Granularity.Seconds)

        with self.assertRaises(exceptions.InvalidOperatorArguments):
            self.datastream.ensure_stream([{'name': 'derived'}], [], self.value_downsamplers, datastream.Granularity.Seconds,
                derive_from=[streamA_id, streamB_id], derive_op='derivative')
        
        streamA = datastream.Stream(self.datastream.get_tags(streamA_id))
        streamB = datastream.Stream(self.datastream.get_tags(streamB_id))
        self.assertEqual(hasattr(streamA, 'contributes_to'), False)
        self.assertEqual(hasattr(streamB, 'contributes_to'), False)

        stream_id = self.datastream.ensure_stream([{'name': 'derived'}], [], self.value_downsamplers, datastream.Granularity.Seconds,
            derive_from=[streamA_id, streamB_id], derive_op='sum')

        streamA = datastream.Stream(self.datastream.get_tags(streamA_id))
        streamB = datastream.Stream(self.datastream.get_tags(streamB_id))
        stream = datastream.Stream(self.datastream.get_tags(stream_id))

        self.assertEqual(len(streamA.contributes_to), 1)
        self.assertEqual(len(streamB.contributes_to), 1)
        self.assertEqual(hasattr(stream, 'contributes_to'), False)

        self.assertEqual(hasattr(streamA, 'derived_from'), False)
        self.assertEqual(hasattr(streamB, 'derived_from'), False)
        self.assertIsNotNone(stream.derived_from)

        another_stream_id = self.datastream.ensure_stream([{'name': 'derived2'}], [], self.value_downsamplers, datastream.Granularity.Seconds,
            derive_from=streamA_id, derive_op='derivative')

        streamA = datastream.Stream(self.datastream.get_tags(streamA_id))
        streamB = datastream.Stream(self.datastream.get_tags(streamB_id))
        self.assertEqual(len(streamA.contributes_to), 2)
        self.assertEqual(len(streamB.contributes_to), 1)

        with self.assertRaises(exceptions.AppendToDerivedStreamNotAllowed):
            self.datastream.append(stream_id, 42)

        # Test derived stream chaining
        chained_stream_id = self.datastream.ensure_stream([{'name': 'chained'}], [], self.value_downsamplers, datastream.Granularity.Seconds,
            derive_from=[streamA_id, another_stream_id], derive_op='sum')

        ts1 = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        self.assertEqual(self.datastream.append(streamA_id, 21, ts1)['datapoint']['t'], ts1)
        self.assertEqual(self.datastream.append(streamB_id, 21, ts1)['datapoint']['v'], 21)

        ts2 = datetime.datetime(2000, 1, 1, 12, 0, 1, tzinfo=pytz.utc)
        self.datastream.append(streamA_id, 25, ts2)
        self.datastream.append(streamB_id, 25, ts2)

        ts2 = datetime.datetime(2000, 1, 1, 12, 0, 2, tzinfo=pytz.utc)
        self.datastream.append(streamA_id, 28, ts2)
        self.datastream.append(streamB_id, 28, ts2)

        ts2 = datetime.datetime(2000, 1, 1, 12, 0, 3, tzinfo=pytz.utc)
        self.datastream.append(streamA_id, 32, ts2)
        self.datastream.append(streamB_id, 32, ts2)

        ts2 = datetime.datetime(2000, 1, 1, 12, 0, 4, tzinfo=pytz.utc)
        self.datastream.append(streamB_id, 25, ts2)
        self.datastream.append(streamA_id, 25, ts2)

        ts2 = datetime.datetime(2000, 1, 1, 12, 0, 10, tzinfo=pytz.utc)
        self.datastream.append(streamA_id, 70, ts2)
        self.datastream.append(streamB_id, 42, ts2)

        ts2 = datetime.datetime(2000, 1, 1, 12, 0, 11, tzinfo=pytz.utc)
        self.datastream.append(streamA_id, 74, ts2)
        self.datastream.append(streamB_id, 42, ts2)

        # Test sum operator
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=ts1)
        self.assertEqual([x['v'] for x in data], [42, 50, 56, 64, 50, 112, 116])

        # Test derivative operator
        data = self.datastream.get_data(another_stream_id, self.datastream.Granularity.Seconds, start=ts1)
        self.assertEqual([x['v'] for x in data], [4.0, 3.0, 4.0, -7.0, 7.5, 4.0])

        # Test results of chained streams
        data = self.datastream.get_data(chained_stream_id, self.datastream.Granularity.Seconds, start=ts1)
        self.assertEqual([x['v'] for x in data], [29.0, 31.0, 36.0, 18.0, 77.5, 78.0])

        # Test named source streams
        streamA_id = self.datastream.ensure_stream([{'name': 'fooA'}], [], self.value_downsamplers, datastream.Granularity.Seconds)
        streamB_id = self.datastream.ensure_stream([{'name': 'fooB'}], [], self.value_downsamplers, datastream.Granularity.Seconds)
        stream_id = self.datastream.ensure_stream([{'name': 'derived3'}], [], self.value_downsamplers, datastream.Granularity.Seconds,
            derive_from=[
                {'name': 'Stream A', 'stream': streamA_id},
                {'name': 'Stream B', 'stream': streamB_id},
            ],
            derive_op='sum'
        )

        ts1 = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        self.datastream.append(streamA_id, 21, ts1)
        self.datastream.append(streamB_id, 21, ts1)

        ts2 = datetime.datetime(2000, 1, 1, 12, 0, 1, tzinfo=pytz.utc)
        self.datastream.append(streamA_id, 25, ts2)
        self.datastream.append(streamB_id, 25, ts2)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=ts1)
        self.assertEqual([x['v'] for x in data], [42, 50])

        # Test invalid granularity specification
        streamC_id = self.datastream.ensure_stream([{'name': 'srcZ'}], [], self.value_downsamplers, datastream.Granularity.Minutes)
        with self.assertRaises(exceptions.IncompatibleGranularities):
            self.datastream.ensure_stream([{'name': 'derived4'}], [], self.value_downsamplers, datastream.Granularity.Seconds,
                derive_from=[
                    {'name': 'Stream A', 'stream': streamA_id},
                    {'name': 'Stream C', 'stream': streamC_id, 'granularity': self.datastream.Granularity.Seconds},
                ],
                derive_op='sum'
            )

        # Test sum for different granularities
        streamA_id = self.datastream.ensure_stream([{'name': 'gA'}], [], self.value_downsamplers, datastream.Granularity.Seconds)
        streamB_id = self.datastream.ensure_stream([{'name': 'gB'}], [], self.value_downsamplers, datastream.Granularity.Seconds)
        stream_id = self.datastream.ensure_stream([{'name': 'sumdifg'}], [], self.value_downsamplers, datastream.Granularity.Seconds10,
            derive_from=[
                {'name': 'Stream A', 'stream': streamA_id, 'granularity': datastream.Granularity.Seconds10},
                {'name': 'Stream B', 'stream': streamB_id, 'granularity': datastream.Granularity.Seconds10},
            ],
            derive_op='sum'
        )

        for i in xrange(32):
            ts = datetime.datetime(2000, 1, 1, 12, 0, i, tzinfo=pytz.utc)
            self.datastream.append(streamA_id, 5, ts)
            self.datastream.append(streamB_id, 5, ts)

        # Before downsampling, nothing should be present in derived stream
        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start=ts)
        self.assertEqual(len(data), 0)

        x = self.datastream.downsample_streams(until=ts + datetime.timedelta(hours=10))

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start=ts)
        self.assertEqual([x['v'] for x in data], [10.0, 10.0, 10.0, 10.0])

        # Test counter reset operator
        streamA_id = self.datastream.ensure_stream([{'name': 'crA'}], [], self.value_downsamplers, datastream.Granularity.Seconds)
        reset_stream_id = self.datastream.ensure_stream([{'name': 'reset'}], [], self.value_downsamplers, datastream.Granularity.Seconds,
            derive_from=streamA_id, derive_op='counter_reset')

        for i, v in enumerate([10, 23, 28, 44, 2, 17, 90, 30, 2]):
            ts = datetime.datetime(2000, 1, 1, 12, 0, i, tzinfo=pytz.utc)
            self.datastream.append(streamA_id, v, ts)

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        data = self.datastream.get_data(reset_stream_id, self.datastream.Granularity.Seconds, start=ts)
        self.assertEqual([x['v'] for x in data], [1, 1, 1])

        # Test counter derivative operator
        uptime_stream_id = self.datastream.ensure_stream([{'name': 'up'}], [], self.value_downsamplers, datastream.Granularity.Seconds)
        reset_stream_id = self.datastream.ensure_stream([{'name': 'rsup'}], [], self.value_downsamplers, datastream.Granularity.Seconds,
            derive_from=uptime_stream_id, derive_op='counter_reset')
        data_stream_id = self.datastream.ensure_stream([{'name': 'data'}], [], self.value_downsamplers, datastream.Granularity.Seconds)
        stream_id = self.datastream.ensure_stream([{'name': 'rate'}], [], self.value_downsamplers, datastream.Granularity.Seconds,
            derive_from=[
                {'name': 'reset', 'stream': reset_stream_id},
                {'stream': data_stream_id},
            ],
            derive_op='counter_derivative',
            derive_args={'max_value': 256}
        )

        uptime = [10, 23, 28, 44, 2, 17, 90, 30, 2]
        data   = [ 5, 12,  7, 25, 7, 18, 33, 40, 5]
        for i, (u, v) in enumerate(zip(uptime, data)):
            ts = datetime.datetime(2000, 1, 1, 12, 0, i, tzinfo=pytz.utc)
            self.datastream.append(uptime_stream_id, u, ts)
            self.datastream.append(data_stream_id, v, ts)

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=ts)
        self.assertEqual([x['v'] for x in data], [7.0, 251.0, 18.0, 11.0, 15.0])

        # Test stream backprocessing with the above uptime and data streams
        reset_stream_id = self.datastream.ensure_stream([{'name': 'rsup2'}], [], self.value_downsamplers, datastream.Granularity.Seconds,
            derive_from=uptime_stream_id, derive_op='counter_reset')
        stream_id = self.datastream.ensure_stream([{'name': 'rate2'}], [], self.value_downsamplers, datastream.Granularity.Seconds,
            derive_from=[
                {'name': 'reset', 'stream': reset_stream_id},
                {'stream': data_stream_id},
            ],
            derive_op='counter_derivative',
            derive_args={'max_value': 256}
        )

        stream = datastream.Stream(self.datastream.get_tags(stream_id))
        self.assertEqual(stream.pending_backprocess, True)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=ts)
        self.assertEqual(len(data), 0)

        self.datastream.backprocess_streams()
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=ts)
        self.assertEqual([x['v'] for x in data], [7.0, 251.0, 18.0, 11.0, 15.0])

        stream = datastream.Stream(self.datastream.get_tags(stream_id))
        self.assertEqual(stream.pending_backprocess, False)

    def test_timestamp_ranges(self):
        stream_id = self.datastream.ensure_stream([{'name': 'foopub'}], [], self.value_downsamplers, datastream.Granularity.Seconds)
        with self.assertRaises(exceptions.InvalidTimestamp):
            self.datastream.append(stream_id, 1, mongodb.Backend._min_timestamp - datetime.timedelta(seconds=1))
        with self.assertRaises(exceptions.InvalidTimestamp):
            self.datastream.append(stream_id, 1, datetime.datetime.min)
        with self.assertRaises(exceptions.InvalidTimestamp):
            self.datastream.append(stream_id, 1, mongodb.Backend._max_timestamp + datetime.timedelta(seconds=1))
        with self.assertRaises(exceptions.InvalidTimestamp):
            self.datastream.append(stream_id, 1, datetime.datetime.max)

    def test_monotonicity_multiple(self):
        stream_id = self.datastream.ensure_stream([{'name': 'fooclub'}], [], self.value_downsamplers, datastream.Granularity.Seconds)

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)

        self.datastream.append(stream_id, 1, ts)
        self.datastream.append(stream_id, 2, ts)
        self.datastream.append(stream_id, 3, ts)
        self.datastream.append(stream_id, 4, ts)
        self.datastream.append(stream_id, 5, ts)

        self.datastream.downsample_streams(until=ts + datetime.timedelta(hours=10))

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=ts)
        self.assertEqual(len(data), 5)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=ts, end=ts)
        self.assertEqual(len(data), 5)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=ts)
        self.assertEqual(len(data), 0)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=ts, end=ts)
        self.assertEqual(len(data), 0)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=ts, end_exclusive=ts)
        self.assertEqual(len(data), 0)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=ts, end_exclusive=ts)
        self.assertEqual(len(data), 0)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start=ts)
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

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start=ts)
        self.assertEqual(len(data), 1)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Minutes, start=ts)
        self.assertEqual(len(data), 1)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Minutes10, start=ts)
        self.assertEqual(len(data), 1)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Hours, start=ts)
        self.assertEqual(len(data), 1)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Hours6, start=ts)
        self.assertEqual(len(data), 1)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Days, start=ts)
        self.assertEqual(len(data), 0)

    def test_monotonicity_timestamp(self):
        stream_id = self.datastream.ensure_stream([{'name': 'fooclub'}], [], self.value_downsamplers, datastream.Granularity.Seconds)
        self.datastream.append(stream_id, 1, datetime.datetime(2000, 1, 1, 12, 0, 0))
        self.datastream.append(stream_id, 1, datetime.datetime(2000, 1, 3, 12, 0, 0))
        with self.assertRaises(exceptions.InvalidTimestamp):
            self.datastream.append(stream_id, 1, datetime.datetime(2000, 1, 2, 12, 0, 0))
        self.datastream.append(stream_id, 1, datetime.datetime(2000, 1, 2, 12, 0, 0), False)

    def test_monotonicity_realtime(self):
        stream_id = self.datastream.ensure_stream([{'name': 'fooclub'}], [], self.value_downsamplers, datastream.Granularity.Seconds)
        self.datastream.append(stream_id, 1)

        # Artificially increase backend time for a minute
        self.datastream.backend._time_offset += datetime.timedelta(minutes=1)

        self.datastream.append(stream_id, 1)

        # Artificially decrease backend time for 30 seconds (cannot be 1 minute because this disabled testing code-path)
        self.datastream.backend._time_offset -= datetime.timedelta(seconds=30)

        with self.assertRaises(exceptions.InvalidTimestamp):
            self.datastream.append(stream_id, 1)

        self.datastream.append(stream_id, 1, check_timestamp=False)

    def test_downsample_freeze(self):
        stream_id = self.datastream.ensure_stream([{'name': 'fooclub'}], [], self.value_downsamplers, datastream.Granularity.Seconds)

        self.datastream.append(stream_id, 1, datetime.datetime(2000, 1, 1, 12, 0, 0))
        self.datastream.append(stream_id, 1, datetime.datetime(2000, 1, 3, 12, 0, 0))

        self.datastream.downsample_streams(until=datetime.datetime(2000, 1, 3, 12, 0, 10))

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start=datetime.datetime.min)
        self.assertEqual(len(data), 2)

        with self.assertRaises(exceptions.InvalidTimestamp):
            self.datastream.append(stream_id, 1, datetime.datetime(2000, 1, 3, 12, 0, 0))
        with self.assertRaises(exceptions.InvalidTimestamp):
            self.datastream.append(stream_id, 1, datetime.datetime(2000, 1, 3, 12, 0, 5))

        self.datastream.append(stream_id, 1, datetime.datetime(2000, 1, 3, 12, 0, 10))
        self.datastream.append(stream_id, 1, datetime.datetime(2000, 1, 4, 12, 0, 0))

        self.datastream.downsample_streams(until=datetime.datetime(2000, 1, 10, 12, 0, 0))

        with self.assertRaises(exceptions.InvalidTimestamp):
            self.datastream.append(stream_id, 1, datetime.datetime(2000, 1, 4, 12, 0, 0))
        with self.assertRaises(exceptions.InvalidTimestamp):
            self.datastream.append(stream_id, 1, datetime.datetime(2000, 1, 5, 12, 0, 0))
        with self.assertRaises(exceptions.InvalidTimestamp):
            self.datastream.append(stream_id, 1, datetime.datetime(2000, 1, 9, 12, 0, 0))

        self.datastream.append(stream_id, 1, datetime.datetime(2000, 1, 10, 12, 0, 0))
        self.datastream.append(stream_id, 1, datetime.datetime(2000, 1, 10, 12, 0, 1))

    def test_granularities(self):
        query_tags = [
            {'name': 'foodata'},
        ]
        tags = []

        stream_id = self.datastream.ensure_stream(query_tags, tags, self.value_downsamplers, datastream.Granularity.Seconds)

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0)
        for i in range(1200):
            self.datastream.append(stream_id, i, ts)
            ts += datetime.timedelta(seconds=1)

        self.datastream.downsample_streams(until=ts)

        s = datetime.datetime(2000, 1, 1, 12, 0, 0)
        e = datetime.datetime(2000, 1, 1, 12, 1, 0)

        # SECONDS
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=s, end=e)
        self.assertEqual(len(data), 61)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=s, end=e)
        self.assertEqual(len(data), 60)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=s, end_exclusive=e)
        self.assertEqual(len(data), 60)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=s, end_exclusive=e)
        self.assertEqual(len(data), 59)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=s)
        self.assertEqual(len(data), 1200)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=s)
        self.assertEqual(len(data), 1199)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=s, end=datetime.datetime.max)
        self.assertEqual(len(data), 1200)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=s, end=datetime.datetime.max)
        self.assertEqual(len(data), 1199)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=s, end_exclusive=datetime.datetime.max)
        self.assertEqual(len(data), 1200)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=s, end_exclusive=datetime.datetime.max)
        self.assertEqual(len(data), 1199)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=datetime.datetime.min, end=e)
        self.assertEqual(len(data), 61)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=datetime.datetime.min, end=e)
        self.assertEqual(len(data), 61)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=datetime.datetime.min, end_exclusive=e)
        self.assertEqual(len(data), 60)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=datetime.datetime.min, end_exclusive=e)
        self.assertEqual(len(data), 60)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=datetime.datetime.min)
        self.assertEqual(len(data), 1200)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=datetime.datetime.min)
        self.assertEqual(len(data), 1200)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=datetime.datetime.min, end=datetime.datetime.max)
        self.assertEqual(len(data), 1200)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=datetime.datetime.min, end=datetime.datetime.max)
        self.assertEqual(len(data), 1200)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=datetime.datetime.min, end_exclusive=datetime.datetime.max)
        self.assertEqual(len(data), 1200)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=datetime.datetime.min, end_exclusive=datetime.datetime.max)
        self.assertEqual(len(data), 1200)

        #10 SECONDS
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start=s, end=e)
        self.assertEqual(len(data), 7)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start_exclusive=s, end=e)
        self.assertEqual(len(data), 6)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start=s, end_exclusive=e)
        self.assertEqual(len(data), 6)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start_exclusive=s, end_exclusive=e)
        self.assertEqual(len(data), 5)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start=datetime.datetime.min, end=datetime.datetime.max)
        self.assertEqual(len(data), 120)

        # MINUTES
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Minutes, start=s, end=e)
        self.assertEqual(len(data), 2)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Minutes, start_exclusive=s, end=e)
        self.assertEqual(len(data), 1)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Minutes, start=s, end_exclusive=e)
        self.assertEqual(len(data), 1)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Minutes, start_exclusive=s, end_exclusive=e)
        self.assertEqual(len(data), 0)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Minutes, start=datetime.datetime.min, end=datetime.datetime.max)
        self.assertEqual(len(data), 20)

        # 10 MINUTES
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Minutes10, start=datetime.datetime.min)
        self.assertEqual(len(data), 2)

        # HOURS
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Hours, start=datetime.datetime.min)
        self.assertEqual(len(data), 0)

        # 6 HOURS
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Hours6, start=datetime.datetime.min)
        self.assertEqual(len(data), 0)

        # DAYS
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Days, start=datetime.datetime.min)
        self.assertEqual(len(data), 0)

    def test_granularities_multiple(self):
        query_tags = [
            {'name': 'foodata'},
        ]
        tags = []

        stream_id = self.datastream.ensure_stream(query_tags, tags, self.value_downsamplers, datastream.Granularity.Seconds)

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0)
        for i in range(1200):
            for j in range(3):
                self.datastream.append(stream_id, j * i, ts)
            ts += datetime.timedelta(seconds=1)

        self.datastream.downsample_streams(until=ts)

        s = datetime.datetime(2000, 1, 1, 12, 0, 0)
        e = datetime.datetime(2000, 1, 1, 12, 1, 0)

        # SECONDS
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=s, end=e)
        self.assertEqual(len(data), 3 * 61)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=s, end=e)
        self.assertEqual(len(data), 3 * 60)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=s, end_exclusive=e)
        self.assertEqual(len(data), 3 * 60)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=s, end_exclusive=e)
        self.assertEqual(len(data), 3 * 59)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=s)
        self.assertEqual(len(data), 3 * 1200)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=s)
        self.assertEqual(len(data), 3 * 1199)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=s, end=datetime.datetime.max)
        self.assertEqual(len(data), 3 * 1200)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=s, end=datetime.datetime.max)
        self.assertEqual(len(data), 3 * 1199)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=s, end_exclusive=datetime.datetime.max)
        self.assertEqual(len(data), 3 * 1200)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=s, end_exclusive=datetime.datetime.max)
        self.assertEqual(len(data), 3 * 1199)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=datetime.datetime.min, end=e)
        self.assertEqual(len(data), 3 * 61)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=datetime.datetime.min, end=e)
        self.assertEqual(len(data), 3 * 61)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=datetime.datetime.min, end_exclusive=e)
        self.assertEqual(len(data), 3 * 60)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=datetime.datetime.min, end_exclusive=e)
        self.assertEqual(len(data), 3 * 60)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=datetime.datetime.min)
        self.assertEqual(len(data), 3 * 1200)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=datetime.datetime.min)
        self.assertEqual(len(data), 3 * 1200)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=datetime.datetime.min, end=datetime.datetime.max)
        self.assertEqual(len(data), 3 * 1200)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=datetime.datetime.min, end=datetime.datetime.max)
        self.assertEqual(len(data), 3 * 1200)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=datetime.datetime.min, end_exclusive=datetime.datetime.max)
        self.assertEqual(len(data), 3 * 1200)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=datetime.datetime.min, end_exclusive=datetime.datetime.max)
        self.assertEqual(len(data), 3 * 1200)

        #10 SECONDS
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start=s, end=e)
        self.assertEqual(len(data), 7)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start_exclusive=s, end=e)
        self.assertEqual(len(data), 6)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start=s, end_exclusive=e)
        self.assertEqual(len(data), 6)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start_exclusive=s, end_exclusive=e)
        self.assertEqual(len(data), 5)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start=datetime.datetime.min, end=datetime.datetime.max)
        self.assertEqual(len(data), 120)

        # MINUTES
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Minutes, start=s, end=e)
        self.assertEqual(len(data), 2)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Minutes, start_exclusive=s, end=e)
        self.assertEqual(len(data), 1)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Minutes, start=s, end_exclusive=e)
        self.assertEqual(len(data), 1)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Minutes, start_exclusive=s, end_exclusive=e)
        self.assertEqual(len(data), 0)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Minutes, start=datetime.datetime.min, end=datetime.datetime.max)
        self.assertEqual(len(data), 20)

        # 10 MINUTES
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Minutes10, start=datetime.datetime.min)
        self.assertEqual(len(data), 2)

        # HOURS
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Hours, start=datetime.datetime.min)
        self.assertEqual(len(data), 0)

        # 6 HOURS
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Hours6, start=datetime.datetime.min)
        self.assertEqual(len(data), 0)

        # DAYS
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Days, start=datetime.datetime.min)
        self.assertEqual(len(data), 0)

@unittest.skip("stress test")
class StressTest(MongoDBBasicTest):
    def test_stress(self):
        stream_id = self.datastream.ensure_stream(
            [{'name': 'stressme'}],
            [],
            self.value_downsamplers,
            datastream.Granularity.Seconds,
        )

        # 1 year, append each second, downsample after each hour
        ts = datetime.datetime(2000, 1, 1, 12, 0, 0)
        start_time = time.time()
        for i in range(1, 356 * 24 * 60 * 60):
            self.datastream.append(stream_id, i, ts)
            ts += datetime.timedelta(seconds=1)

            if i % 3600 == 0:
                t1 = time.time()
                self.datastream.downsample_streams(until=ts)
                t2 = time.time()
                print "%08d insert: %d:%02d    downsample: %d:%02d" % (
                    i,
                    (t1 - start_time) / 60,
                    (t1 - start_time) % 60,
                    (t2 - t1) / 60,
                    (t2 - t1) % 60,
                )

                start_time = t2
