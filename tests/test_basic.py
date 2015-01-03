import collections
import contextlib
import datetime
import decimal
import random
import time
import warnings
import sys
import threading
import unittest

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

    def _tearDown(self):
        db = mongoengine.connection.get_db(mongodb.DATABASE_ALIAS)
        for collection in db.collection_names():
            if collection in ('system.indexes', 'system.profile'):
                continue
            db.drop_collection(collection)

    def tearDown(self):
        self._tearDown()
        with self.switch_database():
            self._tearDown()

    @contextlib.contextmanager
    def time_offset(self, offset=datetime.timedelta(minutes=1)):
        prev = self.datastream.backend._time_offset
        self.datastream.backend._time_offset = offset
        yield
        self.datastream.backend._time_offset = prev

    @contextlib.contextmanager
    def switch_database(self):
        self.datastream._switch_database('%s_switch' % self.database_name)
        yield
        self.datastream._switch_database(self.database_name)


#@unittest.skip("performing stress test")
class BasicTest(MongoDBBasicTest):
    def _test_data_types(self, data):
        for d in data:
            self.assertIsInstance(d['v'], (int, long, float, decimal.Decimal, dict, None.__class__))
            if isinstance(d['v'], dict):
                for v in d['v'].values():
                    self.assertIsInstance(v, (int, long, float, decimal.Decimal, None.__class__))

    def test_basic(self):
        query_tags = {
            'name': 'foobar',
        }
        tags = {
            'andmore': 'bar',
            'andcomplex': {'foo': ['bar', 'foo']},
        }
        stream_id = self.datastream.ensure_stream(query_tags, tags, self.value_downsamplers, datastream.Granularity.Seconds)

        stream = datastream.Stream(self.datastream.get_tags(stream_id))
        self.assertEqual(stream.id, stream_id)
        self.assertItemsEqual(stream.value_downsamplers, self.value_downsamplers)
        self.assertItemsEqual(stream.time_downsamplers, self.time_downsamplers)
        self.assertEqual(stream.highest_granularity, datastream.Granularity.Seconds)
        self.assertEqual(stream.earliest_datapoint, None)
        self.assertEqual(stream.latest_datapoint, None)

        combined_tags = query_tags.copy()
        combined_tags.update(tags)
        self.assertItemsEqual(stream.tags, combined_tags)

        # Test multi tag query
        other_stream_id = self.datastream.ensure_stream({'name': 'foobar', 'andmore': 'bar'}, tags, self.value_downsamplers, datastream.Granularity.Seconds)
        self.assertEqual(stream_id, other_stream_id)
        other_stream_id = self.datastream.ensure_stream({'name': 'foobar', 'andmore': 'wrong'}, tags, self.value_downsamplers, datastream.Granularity.Seconds)
        self.assertNotEqual(stream_id, other_stream_id)
        self.datastream.delete_streams({'name': 'foobar', 'andmore': 'wrong'})

        # Test stream tag manipulation
        rm_tags = self.datastream.get_tags(stream_id)
        self.datastream.remove_tag(stream_id, {'andmore': ''})
        new_tags = self.datastream.get_tags(stream_id)
        del rm_tags['andmore']
        self.assertItemsEqual(new_tags, rm_tags)

        self.datastream.clear_tags(stream_id)
        stream = datastream.Stream(self.datastream.get_tags(stream_id))
        self.assertItemsEqual(stream.tags, {})

        self.datastream.update_tags(stream_id, combined_tags)
        stream = datastream.Stream(self.datastream.get_tags(stream_id))
        self.assertItemsEqual(stream.tags, combined_tags)

        another_stream_id = self.datastream.ensure_stream({'name': 'xyz'}, {'x': 1}, self.value_downsamplers, datastream.Granularity.Seconds)
        as_tags = self.datastream.get_tags(another_stream_id)
        self.assertEqual(as_tags['x'], 1)
        another_stream_id = self.datastream.ensure_stream({'name': 'xyz'}, {'x': 2}, self.value_downsamplers, datastream.Granularity.Seconds)
        as_tags = self.datastream.get_tags(another_stream_id)
        self.assertEqual(as_tags['x'], 2)

        # Should not do anything.
        with self.time_offset():
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
        data = list(data)
        self._test_data_types(data)

        self.assertEqual(len(self._callback_points), 1)
        cb_stream_id, cb_granularity, cb_datapoint = self._callback_points[0]
        self.assertEqual(cb_stream_id, stream_id)
        self.assertEqual(cb_granularity, datastream.Granularity.Seconds)
        self.assertItemsEqual(cb_datapoint, data[0])

        data = self.datastream.get_data(stream_id, datastream.Granularity.Minutes, datetime.datetime.utcfromtimestamp(0))
        self.assertEqual(len(data), 0)

        offset = datetime.timedelta(minutes=1)

        # Temporary increase backend time for a minute so that downsample will do something for minute granularity.
        with self.time_offset(offset):
            # Add a datapoint one minute into the future.
            self.assertEqual(self.datastream.append(stream_id, 42)['datapoint']['v'], 42)

            # Downsample.
            new_datapoints = self.datastream.downsample_streams(return_datapoints=True)

        # At least Seconds10 and Minutes granularities should be available because we artificially increased backend time
        # See https://github.com/wlanslovenija/datastream/issues/12
        self.assertTrue(len(new_datapoints) >= 2)
        self.assertEquals(new_datapoints[0]['datapoint']['v'], {'c': 1, 'd': 0, 'm': 42.0, 'l': 42, 'q': 1764, 's': 42, 'u': 42})
        self.assertEquals(new_datapoints[6]['datapoint']['v'], {'c': 1, 'd': 0, 'm': 42.0, 'l': 42, 'q': 1764, 's': 42, 'u': 42})

        data = self.datastream.get_data(
            stream_id,
            datastream.Granularity.Seconds,
            datetime.datetime.utcfromtimestamp(0),
            datetime.datetime.utcfromtimestamp(time.time()) + offset,
        )
        self.assertEqual(len(data), 2)
        data = list(data)
        self._test_data_types(data)
        self.assertEqual(data[0]['v'], 42)

        data = self.datastream.get_data(
            stream_id,
            datastream.Granularity.Seconds,
            datetime.datetime.utcfromtimestamp(0),
        )
        self.assertEqual(len(data), 2)
        data = list(data)
        self._test_data_types(data)
        self.assertEqual(data[0]['v'], 42)

        stream = datastream.Stream(self.datastream.get_tags(stream_id))
        self.assertEqual(stream.earliest_datapoint, data[0]['t'])
        self.assertEqual(stream.latest_datapoint, data[1]['t'])
        self.assertTrue(datastream.Granularity.Seconds10.name in stream.downsampled_until)
        self.assertTrue(datastream.Granularity.Minutes.name in stream.downsampled_until)
        self.assertTrue(datastream.Granularity.Minutes10.name in stream.downsampled_until)
        self.assertTrue(datastream.Granularity.Hours.name in stream.downsampled_until)
        self.assertTrue(datastream.Granularity.Hours6.name in stream.downsampled_until)
        self.assertTrue(datastream.Granularity.Days.name in stream.downsampled_until)

        # At least Seconds10 and Minutes granularities should be available because we artificially increased backend time
        # See https://github.com/wlanslovenija/datastream/issues/12
        self.assertTrue(len(self._callback_points) >= 3, len(self._callback_points))
        cb_stream_id, cb_granularity, cb_datapoint = self._callback_points[2]
        self.assertEqual(cb_stream_id, stream_id)
        self.assertEqual(cb_granularity, datastream.Granularity.Seconds10)
        cb_stream_id, cb_granularity, cb_datapoint = self._callback_points[8]
        self.assertEqual(cb_stream_id, stream_id)
        self.assertEqual(cb_granularity, datastream.Granularity.Minutes)

        value_downsamplers_keys = [datastream.VALUE_DOWNSAMPLERS[d] for d in self.value_downsamplers]
        time_downsamplers_keys = [datastream.TIME_DOWNSAMPLERS[d] for d in self.time_downsamplers]

        data = self.datastream.get_data(
            stream_id,
            datastream.Granularity.Minutes,
            datetime.datetime.utcfromtimestamp(0),
            datetime.datetime.utcfromtimestamp(time.time()) + offset,
        )
        self.assertEqual(len(data), 1)
        data = list(data)
        self._test_data_types(data)
        self.assertItemsEqual(data[0]['v'].keys(), value_downsamplers_keys)
        self.assertItemsEqual(data[0]['t'].keys(), time_downsamplers_keys)
        self.assertItemsEqual(data[0], cb_datapoint)
        self.assertTrue(datastream.VALUE_DOWNSAMPLERS['count'] in data[0]['v'].keys())

        data = self.datastream.get_data(
            stream_id,
            datastream.Granularity.Minutes,
            datetime.datetime.utcfromtimestamp(0),
        )
        self.assertEqual(len(data), 1)
        data = list(data)
        self._test_data_types(data)
        self.assertItemsEqual(data[0]['v'].keys(), value_downsamplers_keys)
        self.assertItemsEqual(data[0]['t'].keys(), time_downsamplers_keys)
        self.assertItemsEqual(data[0], cb_datapoint)
        self.assertTrue(datastream.VALUE_DOWNSAMPLERS['count'] in data[0]['v'].keys())

        datapoint = data[0]

        data = self.datastream.get_data(
            stream_id,
            datastream.Granularity.Minutes,
            datetime.datetime.utcfromtimestamp(0),
            value_downsamplers=('count',),
        )
        self.assertEqual(len(data), 1)
        data = list(data)
        self._test_data_types(data)
        self.assertItemsEqual(data[0]['v'].keys(), (datastream.VALUE_DOWNSAMPLERS['count'],))
        self.assertEqual(data[0]['v'][datastream.VALUE_DOWNSAMPLERS['count']], datapoint['v'][datastream.VALUE_DOWNSAMPLERS['count']])
        self.assertEqual(data[0]['t'], datapoint['t'])

        data = self.datastream.get_data(
            stream_id,
            datastream.Granularity.Minutes,
            datetime.datetime.utcfromtimestamp(0),
            time_downsamplers=('first',),
        )
        self.assertEqual(len(data), 1)
        data = list(data)
        self._test_data_types(data)
        self.assertItemsEqual(data[0]['v'], datapoint['v'])
        self.assertItemsEqual(data[0]['t'].keys(), (datastream.TIME_DOWNSAMPLERS['first'],))
        self.assertEqual(data[0]['t'][datastream.TIME_DOWNSAMPLERS['first']], datapoint['t'][datastream.TIME_DOWNSAMPLERS['first']])

        data = self.datastream.get_data(
            stream_id,
            datastream.Granularity.Minutes,
            datetime.datetime.utcfromtimestamp(0),
            value_downsamplers=('count',),
            time_downsamplers=('first',),
        )
        self.assertEqual(len(data), 1)
        data = list(data)
        self._test_data_types(data)
        self.assertItemsEqual(data[0]['v'].keys(), (datastream.VALUE_DOWNSAMPLERS['count'],))
        self.assertEqual(data[0]['v'][datastream.VALUE_DOWNSAMPLERS['count']], datapoint['v'][datastream.VALUE_DOWNSAMPLERS['count']])
        self.assertItemsEqual(data[0]['t'].keys(), (datastream.TIME_DOWNSAMPLERS['first'],))
        self.assertEqual(data[0]['t'][datastream.TIME_DOWNSAMPLERS['first']], datapoint['t'][datastream.TIME_DOWNSAMPLERS['first']])

        data = self.datastream.get_data(
            stream_id,
            datastream.Granularity.Minutes,
            datetime.datetime.utcfromtimestamp(0),
            value_downsamplers=(),
        )
        self.assertEqual(len(data), 1)
        data = list(data)
        # Not testing of data types because there are no values
        self.assertFalse('v' in data[0], data[0].get('v', None))
        self.assertEqual(data[0]['t'], datapoint['t'])

        data = self.datastream.get_data(
            stream_id,
            datastream.Granularity.Minutes,
            datetime.datetime.utcfromtimestamp(0),
            time_downsamplers=(),
        )
        self.assertEqual(len(data), 1)
        data = list(data)
        self._test_data_types(data)
        self.assertItemsEqual(data[0]['v'], datapoint['v'])
        self.assertFalse('t' in data[0], data[0].get('t', None))

        data = self.datastream.get_data(
            stream_id,
            datastream.Granularity.Minutes,
            datetime.datetime.utcfromtimestamp(0),
            value_downsamplers=(),
            time_downsamplers=(),
        )
        self.assertEqual(len(data), 1)
        data = list(data)
        # Not testing of data types because there are no values
        self.assertFalse('v' in data[0], data[0].get('v', None))
        self.assertFalse('t' in data[0], data[0].get('t', None))

        # Test stream removal
        self.datastream.delete_streams(query_tags)
        with self.assertRaises(exceptions.StreamNotFound):
            self.datastream.get_data(stream_id, datastream.Granularity.Minutes, datetime.datetime.utcfromtimestamp(0))

    def test_derived_streams(self):
        streamA_id = self.datastream.ensure_stream({'name': 'srcA'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)
        streamB_id = self.datastream.ensure_stream({'name': 'srcB'}, {}, self.value_downsamplers, datastream.Granularity.Minutes)

        with self.assertRaises(exceptions.IncompatibleGranularities):
            self.datastream.ensure_stream(
                {'name': 'derived'},
                {},
                self.value_downsamplers,
                datastream.Granularity.Seconds,
                derive_from=[streamA_id, streamB_id],
                derive_op='sum',
            )
        with self.assertRaises(exceptions.IncompatibleGranularities):
            self.datastream.ensure_stream(
                {'name': 'derived'},
                {},
                self.value_downsamplers,
                datastream.Granularity.Minutes,
                derive_from=[streamA_id, streamB_id],
                derive_op='sum',
            )
        with self.assertRaises(exceptions.UnsupportedDeriveOperator):
            self.datastream.ensure_stream(
                {'name': 'derived'},
                {},
                self.value_downsamplers,
                datastream.Granularity.Minutes,
                derive_from=[streamA_id, streamB_id],
                derive_op='foobar',
            )
        with self.assertRaises(exceptions.StreamNotFound):
            self.datastream.ensure_stream(
                {'name': 'derived'},
                {},
                self.value_downsamplers,
                datastream.Granularity.Seconds,
                derive_from=[streamA_id, '00000000-0000-0000-0000-000000000000'],
                derive_op='sum',
            )

        streamA_id = self.datastream.ensure_stream({'name': 'srcX'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)
        streamB_id = self.datastream.ensure_stream({'name': 'srcY'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)

        with self.assertRaises(exceptions.InvalidOperatorArguments):
            self.datastream.ensure_stream(
                {'name': 'derived'},
                {},
                self.value_downsamplers,
                datastream.Granularity.Seconds,
                derive_from=[streamA_id, streamB_id],
                derive_op='derivative',
            )

        streamA = datastream.Stream(self.datastream.get_tags(streamA_id))
        streamB = datastream.Stream(self.datastream.get_tags(streamB_id))
        self.assertEqual(hasattr(streamA, 'contributes_to'), False)
        self.assertEqual(hasattr(streamB, 'contributes_to'), False)

        stream_id = self.datastream.ensure_stream(
            {'name': 'derived'},
            {},
            self.value_downsamplers,
            datastream.Granularity.Seconds,
            derive_from=[streamA_id, streamB_id],
            derive_op='sum',
        )

        # Attempt to ensure the stream with incompatible configuration (operator)
        with self.assertRaises(exceptions.InconsistentStreamConfiguration):
            self.datastream.ensure_stream(
                {'name': 'derived'},
                {},
                self.value_downsamplers,
                datastream.Granularity.Seconds,
                derive_from=[streamA_id, streamB_id],
                derive_op='derivative',
            )

        # Attempt to ensure the stream with incompatible configuration (input streams)
        with self.assertRaises(exceptions.InconsistentStreamConfiguration):
            self.datastream.ensure_stream(
                {'name': 'derived'},
                {},
                self.value_downsamplers,
                datastream.Granularity.Seconds,
                derive_from=[streamB_id],
                derive_op='sum',
            )

        streamA = datastream.Stream(self.datastream.get_tags(streamA_id))
        streamB = datastream.Stream(self.datastream.get_tags(streamB_id))
        stream = datastream.Stream(self.datastream.get_tags(stream_id))

        self.assertEqual(len(streamA.contributes_to), 1)
        self.assertEqual(len(streamB.contributes_to), 1)
        self.assertEqual(hasattr(stream, 'contributes_to'), False)

        self.assertEqual(hasattr(streamA, 'derived_from'), False)
        self.assertEqual(hasattr(streamB, 'derived_from'), False)
        self.assertIsNotNone(stream.derived_from)

        another_stream_id = self.datastream.ensure_stream(
            {'name': 'derived2'},
            {},
            self.value_downsamplers,
            datastream.Granularity.Seconds,
            derive_from=streamA_id,
            derive_op='derivative',
        )

        streamA = datastream.Stream(self.datastream.get_tags(streamA_id))
        streamB = datastream.Stream(self.datastream.get_tags(streamB_id))
        self.assertEqual(len(streamA.contributes_to), 2)
        self.assertEqual(len(streamB.contributes_to), 1)

        with self.assertRaises(exceptions.AppendToDerivedStreamNotAllowed):
            self.datastream.append(stream_id, 42)

        # Test derived stream chaining
        chained_stream_id = self.datastream.ensure_stream(
            {'name': 'chained'},
            {},
            self.value_downsamplers,
            datastream.Granularity.Seconds,
            derive_from=[streamA_id, another_stream_id],
            derive_op='sum',
        )

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
        data = list(data)
        self._test_data_types(data)
        self.assertEqual([x['v'] for x in data], [42, 50, 56, 64, 50, 112, 116])

        # Test derivative operator
        data = self.datastream.get_data(another_stream_id, self.datastream.Granularity.Seconds, start=ts1)
        data = list(data)
        self._test_data_types(data)
        self.assertEqual([x['v'] for x in data], [4.0, 3.0, 4.0, -7.0, decimal.Decimal('7.5'), 4.0])

        # Test results of chained streams
        data = self.datastream.get_data(chained_stream_id, self.datastream.Granularity.Seconds, start=ts1)
        data = list(data)
        self._test_data_types(data)
        self.assertEqual([x['v'] for x in data], [29.0, 31.0, 36.0, 18.0, decimal.Decimal('77.5'), 78.0])

        # Test named source streams
        streamA_id = self.datastream.ensure_stream({'name': 'fooA'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)
        streamB_id = self.datastream.ensure_stream({'name': 'fooB'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)
        stream_id = self.datastream.ensure_stream(
            {'name': 'derived3'},
            {},
            self.value_downsamplers,
            datastream.Granularity.Seconds,
            derive_from=[
                {'name': 'Stream A', 'stream': streamA_id},
                {'name': 'Stream B', 'stream': streamB_id},
            ],
            derive_op='sum',
        )

        ts1 = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        self.datastream.append(streamA_id, 21, ts1)
        self.datastream.append(streamB_id, 21, ts1)

        ts2 = datetime.datetime(2000, 1, 1, 12, 0, 1, tzinfo=pytz.utc)
        self.datastream.append(streamA_id, 25, ts2)
        self.datastream.append(streamB_id, 25, ts2)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=ts1)
        data = list(data)
        self._test_data_types(data)
        self.assertEqual([x['v'] for x in data], [42, 50])

        # Test invalid granularity specification
        streamC_id = self.datastream.ensure_stream({'name': 'srcZ'}, {}, self.value_downsamplers, datastream.Granularity.Minutes)
        with self.assertRaises(exceptions.IncompatibleGranularities):
            self.datastream.ensure_stream(
                {'name': 'derived4'},
                {},
                self.value_downsamplers,
                datastream.Granularity.Seconds,
                derive_from=[
                    {'name': 'Stream A', 'stream': streamA_id},
                    {'name': 'Stream C', 'stream': streamC_id, 'granularity': self.datastream.Granularity.Seconds},
                ],
                derive_op='sum',
            )

        # Test sum for different granularities
        streamA_id = self.datastream.ensure_stream({'name': 'gA'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)
        streamB_id = self.datastream.ensure_stream({'name': 'gB'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)
        stream_id = self.datastream.ensure_stream(
            {'name': 'sumdifg'},
            {},
            self.value_downsamplers,
            datastream.Granularity.Seconds10,
            derive_from=[
                {'name': 'Stream A', 'stream': streamA_id, 'granularity': datastream.Granularity.Seconds10},
                {'name': 'Stream B', 'stream': streamB_id, 'granularity': datastream.Granularity.Seconds10},
            ],
            derive_op='sum',
        )

        for i in xrange(32):
            ts = datetime.datetime(2000, 1, 1, 12, 0, i, tzinfo=pytz.utc)
            self.datastream.append(streamA_id, 5, ts)
            self.datastream.append(streamB_id, 5, ts)

        # Before downsampling, nothing should be present in derived stream
        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start=ts)
        self.assertEqual(len(data), 0)

        with self.time_offset():
            x = self.datastream.downsample_streams(until=ts + datetime.timedelta(hours=10))

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start=ts)
        data = list(data)
        self._test_data_types(data)
        self.assertEqual([x['v'] for x in data], [10.0, 10.0, 10.0])

        # Test counter reset operator
        streamA_id = self.datastream.ensure_stream({'name': 'crA'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)
        reset_stream_id = self.datastream.ensure_stream(
            {'name': 'reset'},
            {},
            self.value_downsamplers,
            datastream.Granularity.Seconds,
            derive_from=streamA_id,
            derive_op='counter_reset',
        )

        for i, v in enumerate([10, 23, 28, 44, 2, 17, 90, 30, 2]):
            ts = datetime.datetime(2000, 1, 1, 12, 0, i, tzinfo=pytz.utc)
            self.datastream.append(streamA_id, v, ts)

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        data = self.datastream.get_data(reset_stream_id, self.datastream.Granularity.Seconds, start=ts)
        data = list(data)
        self._test_data_types(data)
        self.assertEqual([x['v'] for x in data], [1, 1, 1])

        # Test counter derivative operator
        uptime_stream_id = self.datastream.ensure_stream({'name': 'up'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)
        reset_stream_id = self.datastream.ensure_stream(
            {'name': 'rsup'},
            {},
            self.value_downsamplers, datastream.Granularity.Seconds,
            derive_from=uptime_stream_id,
            derive_op='counter_reset',
        )
        data_stream_id = self.datastream.ensure_stream({'name': 'data'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)
        stream_id = self.datastream.ensure_stream(
            {'name': 'rate'},
            {},
            self.value_downsamplers,
            datastream.Granularity.Seconds,
            derive_from=[
                {'name': 'reset', 'stream': reset_stream_id},
                {'stream': data_stream_id},
            ],
            derive_op='counter_derivative',
            derive_args={'max_value': 256},
        )

        uptime = [10, 23, 28, 44, 2, 17, 90, 30, 2]
        data = [5, 12, 7, 25, 7, 18, 33, 40, 5]
        for i, (u, v) in enumerate(zip(uptime, data)):
            ts = datetime.datetime(2000, 1, 1, 12, 0, i, tzinfo=pytz.utc)
            self.datastream.append(uptime_stream_id, u, ts)
            self.datastream.append(data_stream_id, v, ts)

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=ts)
        data = list(data)
        self._test_data_types(data)
        self.assertEqual([x['v'] for x in data], [7.0, 251.0, 18.0, 11.0, 15.0])

        # Test stream backprocessing with the above uptime and data streams
        reset_stream_id = self.datastream.ensure_stream(
            {'name': 'rsup2'},
            {},
            self.value_downsamplers,
            datastream.Granularity.Seconds,
            derive_from=uptime_stream_id,
            derive_op='counter_reset',
        )
        stream_id = self.datastream.ensure_stream(
            {'name': 'rate2'},
            {},
            self.value_downsamplers,
            datastream.Granularity.Seconds,
            derive_from=[
                {'name': 'reset', 'stream': reset_stream_id},
                {'stream': data_stream_id},
            ],
            derive_op='counter_derivative',
            derive_args={'max_value': 256},
        )

        stream = datastream.Stream(self.datastream.get_tags(stream_id))
        self.assertEqual(stream.pending_backprocess, True)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=ts)
        self.assertEqual(len(data), 0)

        self.datastream.backprocess_streams()
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=ts)
        data = list(data)
        self._test_data_types(data)
        self.assertEqual([x['v'] for x in data], [7.0, 251.0, 18.0, 11.0, 15.0])

        stream = datastream.Stream(self.datastream.get_tags(stream_id))
        self.assertEqual(stream.pending_backprocess, False)

        # Test derived stream removal
        with self.assertRaises(exceptions.OutstandingDependenciesError):
            self.datastream.delete_streams({'name': 'up'})
        with self.assertRaises(exceptions.OutstandingDependenciesError):
            self.datastream.delete_streams({'name': 'data'})
        with self.assertRaises(exceptions.OutstandingDependenciesError):
            self.datastream.delete_streams({'name': 'rsup'})

        self.datastream.delete_streams({'name': 'rate2'})
        self.datastream.delete_streams({'name': 'rsup2'})
        self.datastream.delete_streams({'name': 'rate'})
        self.datastream.delete_streams({'name': 'rsup'})
        self.datastream.delete_streams({'name': 'data'})
        self.datastream.delete_streams({'name': 'up'})

    def test_null_values(self):
        stream_id = self.datastream.ensure_stream({'name': 'foo'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)

        # Basic test with one stream
        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        self.datastream.append(stream_id, None, ts)

        ts = datetime.datetime(2000, 1, 1, 12, 0, 1, tzinfo=pytz.utc)
        self.datastream.append(stream_id, 10, ts)

        ts = datetime.datetime(2000, 1, 1, 12, 1, 0, tzinfo=pytz.utc)
        self.datastream.append(stream_id, None, ts)

        ts = datetime.datetime(2000, 1, 1, 12, 1, 1, tzinfo=pytz.utc)
        self.datastream.append(stream_id, None, ts)

        ts = datetime.datetime(2000, 1, 1, 12, 2, 0, tzinfo=pytz.utc)
        self.datastream.append(stream_id, 2, ts)

        with self.time_offset():
            self.datastream.downsample_streams()

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start=ts)
        data = list(data)

        self._test_data_types(data)

        self.assertEqual(data[0]['v']['c'], 1)     # count
        self.assertEqual(data[0]['v']['d'], 0.0)   # standard deviation
        self.assertEqual(data[0]['v']['m'], 10.0)  # mean
        self.assertEqual(data[0]['v']['l'], 10.0)  # minimum
        self.assertEqual(data[0]['v']['q'], 100.0) # sum of squares
        self.assertEqual(data[0]['v']['s'], 10.0)  # sum
        self.assertEqual(data[0]['v']['u'], 10.0)  # maximum

        self.assertEqual(data[1]['v']['c'], 0)    # count
        self.assertEqual(data[1]['v']['d'], None) # standard deviation
        self.assertEqual(data[1]['v']['m'], None) # mean
        self.assertEqual(data[1]['v']['l'], None) # minimum
        self.assertEqual(data[1]['v']['q'], None) # sum of squares
        self.assertEqual(data[1]['v']['s'], None) # sum
        self.assertEqual(data[1]['v']['u'], None) # maximum

        # Test with different derivation operators

        # Sum: two streams, only one has a null value for a datapoint
        other_stream_id = self.datastream.ensure_stream({'name': 'bar1'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        self.datastream.append(other_stream_id, 1, ts)

        ts = datetime.datetime(2000, 1, 1, 12, 0, 1, tzinfo=pytz.utc)
        self.datastream.append(other_stream_id, 1, ts)

        ts = datetime.datetime(2000, 1, 1, 12, 1, 0, tzinfo=pytz.utc)
        self.datastream.append(other_stream_id, 1, ts)

        ts = datetime.datetime(2000, 1, 1, 12, 1, 1, tzinfo=pytz.utc)
        self.datastream.append(other_stream_id, 1, ts)

        ts = datetime.datetime(2000, 1, 1, 12, 2, 0, tzinfo=pytz.utc)
        self.datastream.append(other_stream_id, 1, ts)

        sum_stream_id = self.datastream.ensure_stream(
            {'name': 'null_sum1'},
            {},
            self.value_downsamplers,
            datastream.Granularity.Seconds,
            derive_from=[stream_id, other_stream_id],
            derive_op='sum',
        )
        self.datastream.backprocess_streams()

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        data = self.datastream.get_data(sum_stream_id, self.datastream.Granularity.Seconds, start=ts)
        data = list(data)
        self._test_data_types(data)

        self.assertEqual([x['v'] for x in data], [1, 11, 1, 1, 3])

        # Sum: two streams, both have null values for a datapoint
        other_stream_id = self.datastream.ensure_stream({'name': 'bar2'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        self.datastream.append(other_stream_id, None, ts)

        ts = datetime.datetime(2000, 1, 1, 12, 0, 1, tzinfo=pytz.utc)
        self.datastream.append(other_stream_id, None, ts)

        ts = datetime.datetime(2000, 1, 1, 12, 1, 0, tzinfo=pytz.utc)
        self.datastream.append(other_stream_id, None, ts)

        ts = datetime.datetime(2000, 1, 1, 12, 1, 1, tzinfo=pytz.utc)
        self.datastream.append(other_stream_id, None, ts)

        ts = datetime.datetime(2000, 1, 1, 12, 2, 0, tzinfo=pytz.utc)
        self.datastream.append(other_stream_id, None, ts)

        sum_stream_id = self.datastream.ensure_stream(
            {'name': 'null_sum2'},
            {},
            self.value_downsamplers,
            datastream.Granularity.Seconds,
            derive_from=[stream_id, other_stream_id],
            derive_op='sum',
        )
        self.datastream.backprocess_streams()

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        data = self.datastream.get_data(sum_stream_id, self.datastream.Granularity.Seconds, start=ts)
        data = list(data)
        self._test_data_types(data)

        self.assertEqual([x['v'] for x in data], [None, 10, None, None, 2])

        # Derivative
        derivative_stream_id = self.datastream.ensure_stream(
            {'name': 'null_derivative'},
            {},
            self.value_downsamplers,
            datastream.Granularity.Seconds,
            derive_from=[stream_id],
            derive_op='derivative',
        )
        self.datastream.backprocess_streams()

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        data = self.datastream.get_data(derivative_stream_id, self.datastream.Granularity.Seconds, start=ts)
        data = list(data)
        self._test_data_types(data)

        self.assertEqual([x['v'] for x in data], [None, None, None])

        # Counter derivative
        reset_stream_id = self.datastream.ensure_stream({'name': 'reset'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)
        counter_derivative_stream_id = self.datastream.ensure_stream(
            {'name': 'null_counter_derivative'},
            {},
            self.value_downsamplers,
            datastream.Granularity.Seconds,
            derive_from=[
                {'name': 'reset', 'stream': reset_stream_id},
                {'stream': stream_id},
            ],
            derive_op='counter_derivative',
        )
        self.datastream.backprocess_streams()

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        data = self.datastream.get_data(counter_derivative_stream_id, self.datastream.Granularity.Seconds, start=ts)
        data = list(data)
        self._test_data_types(data)

        self.assertEqual([x['v'] for x in data], [None, None, None])

        # Counter reset
        reset_stream_id = self.datastream.ensure_stream(
            {'name': 'null_reset'},
            {},
            self.value_downsamplers,
            datastream.Granularity.Seconds,
            derive_from=stream_id,
            derive_op='counter_reset',
        )

        self.datastream.backprocess_streams()

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        data = self.datastream.get_data(reset_stream_id, self.datastream.Granularity.Seconds, start=ts)
        data = list(data)
        self._test_data_types(data)

        self.assertEqual([x['v'] for x in data], [1])

        # Test null value insertion on downsampling
        stream_id = self.datastream.ensure_stream({'name': 'bar'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        self.datastream.append(stream_id, 10, ts)

        ts = datetime.datetime(2000, 1, 1, 12, 10, 0, tzinfo=pytz.utc)
        self.datastream.append(stream_id, 20, ts)

        ts = datetime.datetime(2000, 1, 1, 12, 13, 45, tzinfo=pytz.utc)
        with self.time_offset():
            self.datastream.downsample_streams(until=ts)

        ts = datetime.datetime(2000, 1, 1, 12, 20, 0, tzinfo=pytz.utc)
        self.datastream.append(stream_id, 30, ts)

        ts = datetime.datetime(2000, 1, 1, 12, 21, 0, tzinfo=pytz.utc)
        self.datastream.append(stream_id, 30, ts)

        with self.time_offset():
            self.datastream.downsample_streams()

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Minutes, start=ts)
        data = list(data)
        self._test_data_types(data)

        self.assertEqual([x['v']['m'] for x in data], [10.] + [None] * 9 + [20.] + [None] * 9 + [30.])

    def test_big_integers(self):
        stream_id = self.datastream.ensure_stream({'name': 'foo'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)

        for i in xrange(40):
            ts = datetime.datetime(2000, 1, 1, 12, 0, i, tzinfo=pytz.utc)
            if i < 20:
                self.datastream.append(stream_id, 340282366920938463463374607431768211456, ts)
            else:
                self.datastream.append(stream_id, decimal.Decimal(340282366920938463463374607431768211456), ts)

        with self.time_offset():
            self.datastream.downsample_streams(until=ts + datetime.timedelta(hours=10))

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=ts)
        data = list(data)
        self._test_data_types(data)

        self.assertEqual(len(data), 40)
        self.assertEqual([x['v'] for x in data], [340282366920938463463374607431768211456] * 40)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start=ts)
        data = list(data)
        self._test_data_types(data)

        self.assertEqual(data[0]['v']['c'], 10)    # count
        self.assertEqual(data[0]['v']['d'], 0.0)   # standard deviation
        self.assertAlmostEqual(data[0]['v']['m'], 340282366920938463463374607431768211456)  # mean
        self.assertEqual(data[0]['v']['l'], 340282366920938463463374607431768211456)  # minimum
        self.assertEqual(data[0]['v']['q'], 10 * (340282366920938463463374607431768211456 ** 2)) # sum of squares
        self.assertEqual(data[0]['v']['s'], 340282366920938463463374607431768211456 * 10)  # sum
        self.assertEqual(data[0]['v']['u'], 340282366920938463463374607431768211456)  # maximum

        # Test derived streams
        other_stream_id = self.datastream.ensure_stream({'name': 'bar'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)

        for i in xrange(40):
            ts = datetime.datetime(2000, 1, 1, 12, 0, i, tzinfo=pytz.utc)
            self.datastream.append(other_stream_id, 340282366920938463463374607431768211456, ts)

        sum_stream_id = self.datastream.ensure_stream(
            {'name': 'big_sum'},
            {},
            self.value_downsamplers,
            datastream.Granularity.Seconds,
            derive_from=[stream_id, other_stream_id],
            derive_op='sum',
        )
        self.datastream.backprocess_streams()

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        data = self.datastream.get_data(sum_stream_id, self.datastream.Granularity.Seconds, start=ts)
        data = list(data)
        self.assertEqual([x['v'] for x in data], [340282366920938463463374607431768211456 * 2] * 40)
        self._test_data_types(data)

        # Derivative
        derivative_stream_id = self.datastream.ensure_stream(
            {'name': 'big_derivative'},
            {},
            self.value_downsamplers,
            datastream.Granularity.Seconds,
            derive_from=[stream_id],
            derive_op='derivative',
        )
        self.datastream.backprocess_streams()

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        data = self.datastream.get_data(derivative_stream_id, self.datastream.Granularity.Seconds, start=ts)
        data = list(data)
        self._test_data_types(data)

        self.assertEqual([x['v'] for x in data], [0.0] * 39)

        # Counter derivative
        reset_stream_id = self.datastream.ensure_stream({'name': 'reset'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)
        counter_derivative_stream_id = self.datastream.ensure_stream(
            {'name': 'big_counter_derivative'},
            {},
            self.value_downsamplers,
            datastream.Granularity.Seconds,
            derive_from=[
                {'name': 'reset', 'stream': reset_stream_id},
                {'stream': stream_id},
            ],
            derive_op='counter_derivative',
        )
        self.datastream.backprocess_streams()

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        data = self.datastream.get_data(counter_derivative_stream_id, self.datastream.Granularity.Seconds, start=ts)
        data = list(data)
        self._test_data_types(data)

        self.assertEqual([x['v'] for x in data], [0.0] * 39)

    def test_timestamp_ranges(self):
        stream_id = self.datastream.ensure_stream({'name': 'foopub'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)
        with self.assertRaises(exceptions.InvalidTimestamp):
            self.datastream.append(stream_id, 1, mongodb.Backend._min_timestamp - datetime.timedelta(seconds=1))
        with self.assertRaises(exceptions.InvalidTimestamp):
            self.datastream.append(stream_id, 1, datetime.datetime.min)
        with self.assertRaises(exceptions.InvalidTimestamp):
            self.datastream.append(stream_id, 1, mongodb.Backend._max_timestamp + datetime.timedelta(seconds=1))
        with self.assertRaises(exceptions.InvalidTimestamp):
            self.datastream.append(stream_id, 1, datetime.datetime.max)

    def test_monotonicity_multiple(self):
        stream_id = self.datastream.ensure_stream({'name': 'fooclub'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)

        self.datastream.append(stream_id, 1, ts)
        self.datastream.append(stream_id, 2, ts)
        self.datastream.append(stream_id, 3, ts)
        self.datastream.append(stream_id, 4, ts)
        self.datastream.append(stream_id, 5, ts)

        self.datastream.append(stream_id, None, ts + datetime.timedelta(hours=10))

        with self.time_offset():
            self.datastream.downsample_streams(until=ts + datetime.timedelta(hours=11))

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=ts)
        self.assertEqual(len(data), 6)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=ts, end=ts)
        self.assertEqual(len(data), 5)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=ts)
        self.assertEqual(len(data), 1)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=ts, end=ts)
        self.assertEqual(len(data), 0)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=ts, end_exclusive=ts)
        self.assertEqual(len(data), 0)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=ts, end_exclusive=ts)
        self.assertEqual(len(data), 0)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start=ts)
        # Because of inserted NULL values
        self.assertEqual(len(data), 3600)
        data = list(data)
        self._test_data_types(data)

        self.assertEqual(data[0]['t']['a'], ts) # first
        self.assertEqual(data[0]['t']['z'], ts) # last
        self.assertEqual(data[0]['t']['m'], ts) # mean
        self.assertEqual(data[0]['v']['c'], 5) # count
        self.assertAlmostEqual(data[0]['v']['d'], decimal.Decimal('1.5811388300841898')) # standard deviation
        self.assertEqual(data[0]['v']['m'], 3.0) # mean
        self.assertEqual(data[0]['v']['l'], 1) # minimum
        self.assertEqual(data[0]['v']['q'], 55) # sum of squares
        self.assertEqual(data[0]['v']['s'], 15) # sum
        self.assertEqual(data[0]['v']['u'], 5) # maximum

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Minutes, start=ts)
        self.assertEqual(len(data), 600)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Minutes10, start=ts)
        self.assertEqual(len(data), 60)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Hours, start=ts)
        self.assertEqual(len(data), 10)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Hours6, start=ts)
        self.assertEqual(len(data), 1)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Days, start=ts)
        self.assertEqual(len(data), 0)

    def test_monotonicity_timestamp(self):
        stream_id = self.datastream.ensure_stream({'name': 'fooclub'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)
        self.datastream.append(stream_id, 1, datetime.datetime(2000, 1, 1, 12, 0, 0))
        self.datastream.append(stream_id, 1, datetime.datetime(2000, 1, 3, 12, 0, 0))
        with self.assertRaises(exceptions.InvalidTimestamp):
            self.datastream.append(stream_id, 1, datetime.datetime(2000, 1, 2, 12, 0, 0))
        self.datastream.append(stream_id, 1, datetime.datetime(2000, 1, 2, 12, 0, 0), False)

    def test_monotonicity_realtime(self):
        stream_id = self.datastream.ensure_stream({'name': 'fooclub'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)
        self.datastream.append(stream_id, 1)

        # Temporary increase backend time for a minute.
        with self.time_offset(datetime.timedelta(minutes=1)):
            self.datastream.append(stream_id, 1)

        # Temporary increase backend time for 30 seconds (cannot be 1 minute because this disabled testing code-path)
        with self.time_offset(datetime.timedelta(seconds=30)):
            # Now time is before the last inserted datapoint, we cannot add another datapoint.
            with self.assertRaises(exceptions.InvalidTimestamp):
                self.datastream.append(stream_id, 1)

            # But we can disable the check.
            self.datastream.append(stream_id, 1, check_timestamp=False)

    def test_downsample_freeze(self):
        stream_id = self.datastream.ensure_stream({'name': 'fooclub'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)

        self.datastream.append(stream_id, 1, datetime.datetime(2000, 1, 1, 12, 0, 0))
        self.datastream.append(stream_id, 1, datetime.datetime(2000, 1, 3, 12, 0, 0))

        with self.time_offset():
            self.datastream.downsample_streams(until=datetime.datetime(2000, 1, 3, 12, 0, 10))

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start=datetime.datetime.min)
        self.assertEqual(len(data), 17280)
        self._test_data_types(data)

        self.datastream.append(stream_id, 1, datetime.datetime(2000, 1, 3, 12, 0, 0))
        self.datastream.append(stream_id, 1, datetime.datetime(2000, 1, 3, 12, 0, 5))

        self.datastream.append(stream_id, 1, datetime.datetime(2000, 1, 3, 12, 0, 10))
        self.datastream.append(stream_id, 1, datetime.datetime(2000, 1, 4, 12, 0, 0))

        with self.time_offset():
            self.datastream.downsample_streams(until=datetime.datetime(2000, 1, 10, 12, 0, 0))

        self.datastream.append(stream_id, 1, datetime.datetime(2000, 1, 5, 12, 0, 0))
        self.datastream.append(stream_id, 1, datetime.datetime(2000, 1, 9, 12, 0, 0))

        self.datastream.append(stream_id, 1, datetime.datetime(2000, 1, 10, 12, 0, 0))
        self.datastream.append(stream_id, 1, datetime.datetime(2000, 1, 10, 12, 0, 1))

    def test_stream_types(self):
        with self.assertRaises(exceptions.UnsupportedValueType):
            self.datastream.ensure_stream({'name': 'foo'}, {}, self.value_downsamplers, datastream.Granularity.Seconds, value_type='wtfvaluewtf')

        stream_id = self.datastream.ensure_stream({'name': 'foo'}, {}, ['count'], datastream.Granularity.Seconds, value_type='graph')
        with self.assertRaises(exceptions.InconsistentStreamConfiguration):
            self.datastream.ensure_stream({'name': 'foo'}, {}, ['count'], datastream.Granularity.Seconds)
        with self.assertRaises(exceptions.UnsupportedDownsampler):
            self.datastream.ensure_stream({'name': 'bar'}, {}, self.value_downsamplers, datastream.Granularity.Seconds, value_type='graph')
        with self.assertRaises(exceptions.UnsupportedDeriveOperator):
            stream_id = self.datastream.ensure_stream(
                {'name': 'derived'},
                {},
                ['count'],
                datastream.Granularity.Seconds,
                derive_from=[stream_id, stream_id],
                derive_op='sum',
                value_type='graph',
            )

        stream_id = self.datastream.ensure_stream({'name': 'goo'}, {}, ['count'], datastream.Granularity.Seconds, value_type='graph')
        with self.assertRaises(TypeError):
            self.datastream.append(stream_id, 42, datetime.datetime(2000, 1, 1, 0, 0, 0))
        with self.assertRaises(ValueError):
            self.datastream.append(stream_id, {}, datetime.datetime(2000, 1, 1, 0, 0, 0))

        # Test invalid graph
        with self.assertRaises(ValueError):
            self.datastream.append(
                stream_id,
                {
                    'v': [{'foo': 'bar'}],
                },
                datetime.datetime(2000, 1, 1, 0, 0, 0)
            )

        with self.assertRaises(ValueError):
            self.datastream.append(
                stream_id,
                {
                    'v': [{'foo': 'bar'}],
                    'e': []
                },
                datetime.datetime(2000, 1, 1, 0, 0, 0)
            )

        with self.assertRaises(ValueError):
            self.datastream.append(
                stream_id,
                {
                    'v': [
                        {'i': 'bar'},
                        {'i': 'bar'},
                    ],
                    'e': []
                },
                datetime.datetime(2000, 1, 1, 0, 0, 0)
            )

        with self.assertRaises(ValueError):
            self.datastream.append(
                stream_id,
                {
                    'v': [
                        {'i': 'foo'},
                        {'i': 'bar'},
                    ],
                    'e': [{}]
                },
                datetime.datetime(2000, 1, 1, 0, 0, 0)
            )

        with self.assertRaises(ValueError):
            self.datastream.append(
                stream_id,
                {
                    'v': [
                        {'i': 'foo'},
                        {'i': 'bar'},
                    ],
                    'e': [{'f': 'foo'}]
                },
                datetime.datetime(2000, 1, 1, 0, 0, 0)
            )

        with self.assertRaises(ValueError):
            self.datastream.append(
                stream_id,
                {
                    'v': [
                        {'i': 'foo'},
                        {'i': 'bar'},
                    ],
                    'e': [{'f': 'foo', 't': 'wtf'}]
                },
                datetime.datetime(2000, 1, 1, 0, 0, 0)
            )

        # Test graph insertion
        self.datastream.append(
            stream_id,
            {
                'v': [
                    {'i': 'foo'},
                    {'i': 'bar'},
                ],
                'e': [
                    {'f': 'foo', 't': 'bar'},
                ]
            },
            datetime.datetime(2000, 1, 1, 0, 0, 0)
        )

    def test_concurrent_append(self):
        stream_id = self.datastream.ensure_stream({'name': 'foo'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)

        def worker_append(minute, sleep):
            ts = datetime.datetime(2000, 1, 1, 0, minute, 0, tzinfo=pytz.utc)
            self.datastream.backend._test_concurrency.sleep = sleep
            try:
                self.datastream.append(stream_id, 1, ts)
            finally:
                self.datastream.backend._test_concurrency.sleep = False

        # CONCURRENCY SCENARIO
        # We simulate the following scenario:
        #   - At time t0, thread A starts inserting a datapoint with timestamp T0
        #   - Thread A modifies the latest_datapoint in stream metadata
        #   - At time t1, thread B starts inserting a datapoint with timestamp T1
        #     and immediately succeeds at time t2
        #   - At time t3, stream downsampling is initiated
        #   - At time t4, thread A succeeds inserting a new datapoint with timestamp T0
        #
        # It holds:
        #   t0 < t1 < t2 < t3 < t4
        #   T0 << T1
        #
        # We check that downsampling results are consistent.

        # Create some datapoints so there is something to downsample
        for i in xrange(5):
            ts = datetime.datetime(2000, 1, 1, 0, 0, i, tzinfo=pytz.utc)
            self.datastream.append(stream_id, 1, ts)
        # Sleep so there will be some datapoints to downsample outside the margin
        time.sleep(mongodb.DOWNSAMPLE_SAFETY_MARGIN + 5)
        # First thread should sleep half of safety margin
        t1 = threading.Thread(target=worker_append, args=(1, True))
        t1.start()
        # Wait for the first thread to start
        time.sleep(2)
        # Second thread should not sleep
        worker_append(5, False)
        # Start downsampling before the first thread inserts
        datapoints = self.datastream.downsample_streams(return_datapoints=True)
        # Wait for the first thread to finish
        t1.join()

        self.assertEqual(len(datapoints), 7)

        with self.time_offset():
            datapoints = self.datastream.downsample_streams(return_datapoints=True)

        self.assertEqual(len(datapoints), 28)

    def test_concurrent(self):
        for i in xrange(10):
            stream_id = self.datastream.ensure_stream({'name': i}, {}, self.value_downsamplers, datastream.Granularity.Seconds)
            ts = datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=pytz.utc)
            for j in xrange(1000):
                self.datastream.append(stream_id, 1, ts)
                ts += datetime.timedelta(seconds=4)

        def worker(results):
            try:
                datapoints = self.datastream.downsample_streams(return_datapoints=True)
                results.append(len(datapoints))
            except:
                results.append(sys.exc_info())

        threads = []
        results = collections.deque()
        with self.time_offset():
            for i in xrange(5):
                t = threading.Thread(target=worker, args=(results,))
                threads.append(t)
                t.start()

            for t in threads:
                t.join()

        if results:
            all_datapoints = 0
            for result in results:
                if isinstance(result, int):
                    all_datapoints += result
                else:
                    raise result[1], None, result[2]

            self.assertEqual(all_datapoints, 4720)

    def test_downsamplers(self):
        # Test with floats that have issues with exact representation
        stream_id = self.datastream.ensure_stream({'name': 'small'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)
        ts0 = datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=pytz.utc)
        ts = ts0

        for i in xrange(100):
            self.datastream.append(stream_id, 0.05, ts)
            ts += datetime.timedelta(seconds=1)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            with self.time_offset():
                self.datastream.downsample_streams(until=ts)

            self.assertEqual(any([x.category == exceptions.InvalidValueWarning for x in w]), False)

        # Test with random numbers
        random.seed(42)
        points = 43205
        interval = 2
        downsample_every = 5000
        src_data = [random.randint(-1000, 1000) for _ in xrange(points)]
        ts0 = datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=pytz.utc)
        ts = ts0

        stream_id = self.datastream.ensure_stream({'name': 'test'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)
        for i, v in enumerate(src_data):
            self.datastream.append(stream_id, v, ts)
            ts += datetime.timedelta(seconds=interval)

            if (i + 1) % downsample_every == 0:
                with self.time_offset():
                    self.datastream.downsample_streams(until=ts)

        with self.time_offset():
            self.datastream.downsample_streams(until=ts)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=ts0, end=ts)
        self.assertEqual(len(data), points)
        self._test_data_types(data)
        self.assertEqual(mongodb.total_seconds(ts - ts0), points * interval)

        def check_values(data, n):
            self.assertEqual(data[0]['v']['c'], n) # count
            self.assertAlmostEqual(data[0]['v']['m'], decimal.Decimal(sum(src_data[:n])) / n) # mean
            self.assertEqual(data[0]['v']['l'], min(src_data[:n])) # minimum
            self.assertEqual(data[0]['v']['u'], max(src_data[:n])) # maximum
            self.assertEqual(data[0]['v']['s'], sum(src_data[:n])) # sum
            self.assertEqual(data[0]['v']['q'], sum([x ** 2 for x in src_data[:n]])) # sum of squares

            self.assertEqual(data[0]['t']['a'], ts0) # first
            self.assertEqual(data[0]['t']['z'], ts0 + datetime.timedelta(seconds=(n - 1) * interval)) # last
            self.assertEqual(data[0]['t']['m'], ts0 + datetime.timedelta(seconds=(n - 1) * interval / 2)) # mean

        for granularity in self.datastream.Granularity.values[1:]:
            if granularity.duration_in_seconds() >= points * interval:
                break

            data = self.datastream.get_data(stream_id, granularity, start=ts0, end=ts)
            data = list(data)
            self._test_data_types(data)
            check_values(data, granularity.duration_in_seconds() / interval)

        # Test a stream with empty value downsamplers
        stream_id = self.datastream.ensure_stream({'name': 'up'}, {}, [], datastream.Granularity.Seconds)
        ts0 = datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=pytz.utc)
        ts = ts0

        for i in xrange(100):
            self.datastream.append(stream_id, 0.05, ts)
            ts += datetime.timedelta(seconds=1)

        with self.time_offset():
            self.datastream.downsample_streams(until=ts)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=ts0, end=ts)
        self.assertEqual(len(data), 100)
        # Check that all lower granularities are empty
        for granularity in self.datastream.Granularity.values[1:]:
            data = self.datastream.get_data(stream_id, granularity, start=ts0, end=ts)
            self.assertEqual(len(data), 0)

    def test_already_downsampled(self):
        stream_id = self.datastream.ensure_stream({'name': 'test'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)

        ts0 = datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=pytz.utc)
        ts = ts0

        with self.assertRaises(ValueError):
            self.datastream.append(stream_id, {'m': 3}, ts)
        with self.assertRaises(ValueError):
            self.datastream.append(stream_id, {'m': 3, 's': 9, 'l': 3, 'u': 3, 'q': 27, 'd': 0, 'c': 3, 'extra': 77}, ts)

        for i in xrange(30):
            self.datastream.append(stream_id, {'m': 3, 's': 9, 'l': 3, 'u': 3, 'q': 27, 'd': 0, 'c': 3}, ts)
            self.datastream.append(stream_id, 3, ts)
            self.datastream.append(stream_id, 3, ts)
            self.datastream.append(stream_id, 3, ts)
            ts += datetime.timedelta(seconds=1)

        with self.time_offset():
            self.datastream.downsample_streams()

        data = self.datastream.get_data(stream_id, datastream.Granularity.Seconds10, start=ts0, end=ts)
        data = list(data)
        self._test_data_types(data)

        self.assertEqual(data[0]['v']['c'], 60) # count
        self.assertEqual(data[0]['v']['m'], 3.0) # mean
        self.assertEqual(data[0]['v']['l'], 3) # minimum
        self.assertEqual(data[0]['v']['u'], 3) # maximum
        self.assertEqual(data[0]['v']['s'], 180) # sum
        self.assertEqual(data[0]['v']['q'], 540) # sum of squares

    def test_granularities(self):
        query_tags = {
            'name': 'foodata',
        }
        tags = {}

        stream_id = self.datastream.ensure_stream(query_tags, tags, self.value_downsamplers, datastream.Granularity.Seconds)

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0)
        for i in range(1200):
            self.datastream.append(stream_id, i, ts)
            ts += datetime.timedelta(seconds=1)

        with self.time_offset():
            self.datastream.downsample_streams(until=ts)

        s = datetime.datetime(2000, 1, 1, 12, 0, 0)
        e = datetime.datetime(2000, 1, 1, 12, 1, 0)

        # SECONDS
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=s, end=e)
        self.assertEqual(len(data), 61)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=s, end=e)
        self.assertEqual(len(data), 60)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=s, end_exclusive=e)
        self.assertEqual(len(data), 60)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=s, end_exclusive=e)
        self.assertEqual(len(data), 59)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=s)
        self.assertEqual(len(data), 1200)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=s)
        self.assertEqual(len(data), 1199)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=s, end=datetime.datetime.max)
        self.assertEqual(len(data), 1200)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=s, end=datetime.datetime.max)
        self.assertEqual(len(data), 1199)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=s, end_exclusive=datetime.datetime.max)
        self.assertEqual(len(data), 1200)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=s, end_exclusive=datetime.datetime.max)
        self.assertEqual(len(data), 1199)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=datetime.datetime.min, end=e)
        self.assertEqual(len(data), 61)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=datetime.datetime.min, end=e)
        self.assertEqual(len(data), 61)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=datetime.datetime.min, end_exclusive=e)
        self.assertEqual(len(data), 60)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=datetime.datetime.min, end_exclusive=e)
        self.assertEqual(len(data), 60)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=datetime.datetime.min)
        self.assertEqual(len(data), 1200)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=datetime.datetime.min)
        self.assertEqual(len(data), 1200)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=datetime.datetime.min, end=datetime.datetime.max)
        self.assertEqual(len(data), 1200)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=datetime.datetime.min, end=datetime.datetime.max)
        self.assertEqual(len(data), 1200)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=datetime.datetime.min, end_exclusive=datetime.datetime.max)
        self.assertEqual(len(data), 1200)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=datetime.datetime.min, end_exclusive=datetime.datetime.max)
        self.assertEqual(len(data), 1200)
        self._test_data_types(data)

        #10 SECONDS
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start=s, end=e)
        self.assertEqual(len(data), 7)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start_exclusive=s, end=e)
        self.assertEqual(len(data), 6)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start=s, end_exclusive=e)
        self.assertEqual(len(data), 6)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start_exclusive=s, end_exclusive=e)
        self.assertEqual(len(data), 5)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start=datetime.datetime.min, end=datetime.datetime.max)
        self.assertEqual(len(data), 119)
        self._test_data_types(data)

        # MINUTES
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Minutes, start=s, end=e)
        self.assertEqual(len(data), 2)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Minutes, start_exclusive=s, end=e)
        self.assertEqual(len(data), 1)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Minutes, start=s, end_exclusive=e)
        self.assertEqual(len(data), 1)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Minutes, start_exclusive=s, end_exclusive=e)
        self.assertEqual(len(data), 0)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Minutes, start=datetime.datetime.min, end=datetime.datetime.max)
        self.assertEqual(len(data), 19)
        self._test_data_types(data)

        # 10 MINUTES
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Minutes10, start=datetime.datetime.min)
        self.assertEqual(len(data), 1)
        self._test_data_types(data)

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
        query_tags = {
            'name': 'foodata',
        }
        tags = {}

        stream_id = self.datastream.ensure_stream(query_tags, tags, self.value_downsamplers, datastream.Granularity.Seconds)

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0)
        for i in range(1200):
            for j in range(3):
                self.datastream.append(stream_id, j * i, ts)
            ts += datetime.timedelta(seconds=1)

        with self.time_offset():
            self.datastream.downsample_streams(until=ts)

        s = datetime.datetime(2000, 1, 1, 12, 0, 0)
        e = datetime.datetime(2000, 1, 1, 12, 1, 0)

        # SECONDS
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=s, end=e)
        self.assertEqual(len(data), 3 * 61)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=s, end=e)
        self.assertEqual(len(data), 3 * 60)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=s, end_exclusive=e)
        self.assertEqual(len(data), 3 * 60)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=s, end_exclusive=e)
        self.assertEqual(len(data), 3 * 59)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=s)
        self.assertEqual(len(data), 3 * 1200)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=s)
        self.assertEqual(len(data), 3 * 1199)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=s, end=datetime.datetime.max)
        self.assertEqual(len(data), 3 * 1200)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=s, end=datetime.datetime.max)
        self.assertEqual(len(data), 3 * 1199)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=s, end_exclusive=datetime.datetime.max)
        self.assertEqual(len(data), 3 * 1200)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=s, end_exclusive=datetime.datetime.max)
        self.assertEqual(len(data), 3 * 1199)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=datetime.datetime.min, end=e)
        self.assertEqual(len(data), 3 * 61)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=datetime.datetime.min, end=e)
        self.assertEqual(len(data), 3 * 61)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=datetime.datetime.min, end_exclusive=e)
        self.assertEqual(len(data), 3 * 60)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=datetime.datetime.min, end_exclusive=e)
        self.assertEqual(len(data), 3 * 60)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=datetime.datetime.min)
        self.assertEqual(len(data), 3 * 1200)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=datetime.datetime.min)
        self.assertEqual(len(data), 3 * 1200)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=datetime.datetime.min, end=datetime.datetime.max)
        self.assertEqual(len(data), 3 * 1200)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=datetime.datetime.min, end=datetime.datetime.max)
        self.assertEqual(len(data), 3 * 1200)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=datetime.datetime.min, end_exclusive=datetime.datetime.max)
        self.assertEqual(len(data), 3 * 1200)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start_exclusive=datetime.datetime.min, end_exclusive=datetime.datetime.max)
        self.assertEqual(len(data), 3 * 1200)
        self._test_data_types(data)

        #10 SECONDS
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start=s, end=e)
        self.assertEqual(len(data), 7)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start_exclusive=s, end=e)
        self.assertEqual(len(data), 6)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start=s, end_exclusive=e)
        self.assertEqual(len(data), 6)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start_exclusive=s, end_exclusive=e)
        self.assertEqual(len(data), 5)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start=datetime.datetime.min, end=datetime.datetime.max)
        self.assertEqual(len(data), 119)
        self._test_data_types(data)

        # MINUTES
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Minutes, start=s, end=e)
        self.assertEqual(len(data), 2)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Minutes, start_exclusive=s, end=e)
        self.assertEqual(len(data), 1)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Minutes, start=s, end_exclusive=e)
        self.assertEqual(len(data), 1)
        self._test_data_types(data)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Minutes, start_exclusive=s, end_exclusive=e)
        self.assertEqual(len(data), 0)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Minutes, start=datetime.datetime.min, end=datetime.datetime.max)
        self.assertEqual(len(data), 19)
        self._test_data_types(data)

        # 10 MINUTES
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Minutes10, start=datetime.datetime.min)
        self.assertEqual(len(data), 1)
        self._test_data_types(data)

        # HOURS
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Hours, start=datetime.datetime.min)
        self.assertEqual(len(data), 0)

        # 6 HOURS
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Hours6, start=datetime.datetime.min)
        self.assertEqual(len(data), 0)

        # DAYS
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Days, start=datetime.datetime.min)
        self.assertEqual(len(data), 0)

    def test_batch_size_and_slice(self):
        # A MongoDB specific test. Using internal __retrieved from a MongoDB cursor.

        query_tags = {
            'name': 'foodata',
        }
        tags = {}

        stream_id = self.datastream.ensure_stream(query_tags, tags, self.value_downsamplers, datastream.Granularity.Seconds)

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0)
        for i in range(1200):
            self.datastream.append(stream_id, i, ts)
            ts += datetime.timedelta(seconds=1)

        s = datetime.datetime(2000, 1, 1, 12, 0, 0)

        def get_retrieved(cursor):
            return getattr(cursor, '_%s__retrieved' % cursor.__class__.__name__)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=s)
        cursor = data._get_backend_cursor()

        # Testing batch size. We want that a batch size of documents is transferred even if only
        # a subset of documents is really used.
        data.batch_size(200)

        # 0 to begin with.
        self.assertEqual(get_retrieved(cursor), 0)

        # Let's read 100 documents.
        for i, d in enumerate(data):
            if i >= 100:
                break

        # But 200 documents should be transferred.
        self.assertEqual(get_retrieved(cursor), 200)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=s)
        cursor = data._get_backend_cursor()

        data.batch_size(1000)
        self.assertEqual(get_retrieved(cursor), 0)
        for i, d in enumerate(data):
            if i >= 100:
                break
        self.assertEqual(get_retrieved(cursor), 1000)

        # Testing if slicing transfers only a subset of documents.
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=s)
        cursor = data._get_backend_cursor()

        self.assertEqual(get_retrieved(cursor), 0)
        self.assertEqual(len(list(data)), 1200)
        self.assertEqual(get_retrieved(cursor), 1200)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=s)
        cursor = data._get_backend_cursor()

        self.assertEqual(get_retrieved(cursor), 0)
        self.assertEqual(len(list(data[0:100])), 100)
        self.assertEqual(get_retrieved(cursor), 100)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=s)
        cursor = data._get_backend_cursor()

        self.assertEqual(get_retrieved(cursor), 0)
        self.assertEqual(len(list(data[100:200])), 100)
        self.assertEqual(get_retrieved(cursor), 100)

        # A bad approach. First doing the list and then slicing. This transfers everything.

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=s)
        cursor = data._get_backend_cursor()

        self.assertEqual(get_retrieved(cursor), 0)
        self.assertEqual(len(list(data)[0:100]), 100)
        self.assertEqual(get_retrieved(cursor), 1200)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=s)
        cursor = data._get_backend_cursor()

        self.assertEqual(get_retrieved(cursor), 0)
        self.assertEqual(len(list(data)[100:200]), 100)
        self.assertEqual(get_retrieved(cursor), 1200)

    def test_database_switch(self):
        # We want to test if we can switch the database after connection so that we are sure
        # we can run tests in a special database in django-datastream without destroying any
        # real data in the process.

        # Make sure we are starting with an empty database.
        self.assertEqual(len(self.datastream.find_streams()), 0)

        # Run some stuff to get it non-empty.
        self.test_basic()

        # One stream at the end.
        self.assertEqual(len(self.datastream.find_streams()), 1)

        stream_id = self.datastream.find_streams()[0]['stream_id']

        self.assertEqual(self.datastream.append(stream_id, 42)['datapoint']['v'], 42)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, datetime.datetime.utcfromtimestamp(0))

        self.assertEqual(len(list(data)), 1)

        with self.switch_database():
            # No streams in the new database.
            self.assertEqual(len(self.datastream.find_streams()), 0)

            # Run some stuff again.
            self._callback_points = []
            self.test_basic()

            self.assertEqual(len(self.datastream.find_streams()), 1)

            # Delete everything.
            self.datastream.delete_streams()

            self.assertEqual(len(self.datastream.find_streams()), 0)

        # But when we get back, the stream should still be here.
        self.assertEqual(len(self.datastream.find_streams()), 1)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, datetime.datetime.utcfromtimestamp(0))

        self.assertEqual(len(list(data)), 1)

    def test_find_streams(self):
        # Make sure we are starting with an empty database.
        self.assertEqual(len(self.datastream.find_streams()), 0)

        # Ensure a stream.
        query_tags = {
            'title': 'Stream 1',
        }
        tags = {
            'visualization': {
                'time_downsamplers': [
                    'mean',
                ],
                'hidden': False,
                'minimum': 0,
                'type': 'line',
                'value_downsamplers': [
                    'mean',
                    'min',
                    'max',
                ],
            },
            'stream_number': 1,
        }
        stream_id = self.datastream.ensure_stream(query_tags, tags, self.value_downsamplers, datastream.Granularity.Seconds)

        # One stream at the end.
        self.assertEqual(len(self.datastream.find_streams()), 1)

        # We do not allow finding by stream_id.
        self.assertEqual(len(self.datastream.find_streams({'stream_id': stream_id})), 0)

        # Internal tags should not be exposed through the API.
        self.assertEqual(len(self.datastream.find_streams({'tags': {'value_type': 'numeric'}})), 0)
        # Seconds, because this is class name in the database.
        self.assertEqual(len(self.datastream.find_streams({'tags': {'highest_granularity': 'Seconds'}})), 0)

        # But do by other tags.
        self.assertEqual(len(self.datastream.find_streams({'title': 'Stream 1'})), 1)
        self.assertEqual(len(self.datastream.find_streams({'title': {'iexact': 'stream 1'}})), 1)
        self.assertEqual(len(self.datastream.find_streams({'title': {'iexact': 'strEAm 1'}})), 1)
        self.assertEqual(len(self.datastream.find_streams({'title': {'icontains': 'strEAm'}})), 1)
        self.assertEqual(len(self.datastream.find_streams({'stream_number': 1})), 1)
        self.assertEqual(len(self.datastream.find_streams({'stream_number': {'gte': 1}})), 1)
        self.assertEqual(len(self.datastream.find_streams({'stream_number': {'gt': 1}})), 0)
        self.assertEqual(len(self.datastream.find_streams({'visualization': {'value_downsamplers': 'mean'}})), 1)
        self.assertEqual(len(self.datastream.find_streams({'visualization': {'value_downsamplers': {'in': ['mean']}}})), 1)
        self.assertEqual(len(self.datastream.find_streams({'visualization': {'value_downsamplers': {'all': ['mean', 'min']}}})), 1)
        self.assertEqual(len(self.datastream.find_streams({'visualization': {'value_downsamplers': {'all': ['mean', 'foobar']}}})), 0)

        with self.assertRaises(exceptions.ReservedTagNameError):
            self.assertEqual(len(self.datastream.find_streams({'andmore__bar': 'value'})), 1)

        # But other queries can use stream_id.
        self.datastream.delete_streams({'stream_id': stream_id})
        self.assertEqual(len(self.datastream.find_streams()), 0)


@unittest.skip('stress test')
class StressTest(MongoDBBasicTest):
    def test_stress(self):
        stream_id = self.datastream.ensure_stream(
            {'name': 'stressme'},
            {},
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
                with self.time_offset():
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
