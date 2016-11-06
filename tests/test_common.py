import datetime
import pytz
import random
import time
import warnings

import datastream
from datastream import exceptions


class CommonTestsMixin(object):
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
        self.assertEqual(stream.value_type, 'numeric')
        self.assertEqual(stream.value_type_options, {'high_accuracy': False})

        combined_tags = query_tags.copy()
        combined_tags.update(tags)
        self.assertItemsEqual(stream.tags, combined_tags)

        other_stream_id = self.datastream.ensure_stream({'name': 'foobar'}, tags, self.value_downsamplers, datastream.Granularity.Seconds)
        self.assertEqual(stream_id, other_stream_id)

        # Test multi tag query.
        other_stream_id = self.datastream.ensure_stream({'name': 'foobar', 'andmore': 'wrong'}, tags, self.value_downsamplers, datastream.Granularity.Seconds)
        self.assertNotEqual(stream_id, other_stream_id)
        with self.assertRaises(exceptions.MultipleStreamsReturned):
            self.datastream.ensure_stream(query_tags, tags, self.value_downsamplers, datastream.Granularity.Seconds)
        self.datastream.delete_streams({'name': 'foobar', 'andmore': 'wrong'})

        # Test stream tag manipulation.
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

        if self.datastream.backend.requires_downsampling:
            # Should not do anything.
            with self.time_offset():
                self.assertItemsEqual(self.datastream.downsample_streams(), [])

        now = datetime.datetime.utcnow()

        data = self.datastream.get_data(stream_id, datastream.Granularity.Seconds, datetime.datetime.utcfromtimestamp(0), now)
        self.assertEqual(len(data), 0)

        if not self.datastream.backend.downsampled_always_exist:
            data = self.datastream.get_data(stream_id, datastream.Granularity.Minutes, datetime.datetime.utcfromtimestamp(0), now)
            self.assertEqual(len(data), 0)

        if hasattr(self, '_callback_points'):
            # Callback should not have been fired.
            self.assertItemsEqual(self._callback_points, [])

        self.datastream.append(stream_id, 42, now)
        data = self.datastream.get_data(stream_id, datastream.Granularity.Seconds, datetime.datetime.utcfromtimestamp(0), end_exclusive=now)
        self.assertEqual(len(data), 0)

        data = self.datastream.get_data(stream_id, datastream.Granularity.Seconds, datetime.datetime.utcfromtimestamp(0), now)
        self.assertEqual(len(data), 1)
        data = list(data)
        self._test_data_types(data)

        if hasattr(self, '_callback_points'):
            self.assertEqual(len(self._callback_points), 1)
            cb_stream_id, cb_granularity, cb_datapoint = self._callback_points[0]
            self.assertEqual(cb_stream_id, stream_id)
            self.assertEqual(cb_granularity, datastream.Granularity.Seconds)
            self.assertItemsEqual(cb_datapoint, data[0])

        offset = datetime.timedelta(minutes=1)

        if self.datastream.backend.requires_downsampling:
            if not self.datastream.backend.downsampled_always_exist:
                data = self.datastream.get_data(stream_id, datastream.Granularity.Minutes, datetime.datetime.utcfromtimestamp(0))
                self.assertEqual(len(data), 0)

            # Temporary increase backend time for a minute so that downsample will do something for minute granularity.
            with self.time_offset(offset):
                # Add a datapoint one minute into the future.
                self.datastream.append(stream_id, 42)

                # Downsample.
                new_datapoints = self.datastream.downsample_streams(return_datapoints=True)

            # At least Seconds10 and Minutes granularities should be available because we artificially increased backend time
            # See https://github.com/wlanslovenija/datastream/issues/12
            self.assertTrue(len(new_datapoints) >= 2)
            self.assertEquals(new_datapoints[0]['datapoint']['v'], {'c': 1, 'd': 0, 'm': 42.0, 'l': 42, 'q': 1764, 's': 42, 'u': 42})
            self.assertEquals(new_datapoints[6]['datapoint']['v'], {'c': 1, 'd': 0, 'm': 42.0, 'l': 42, 'q': 1764, 's': 42, 'u': 42})
        else:
            # Add a datapoint one minute into the future.
            self.datastream.append(stream_id, 42, now + offset)

        data = self.datastream.get_data(
            stream_id,
            datastream.Granularity.Seconds,
            datetime.datetime.utcfromtimestamp(0),
            # We increase the tested boundary by an additional second as the test may sometimes
            # fail when it started near the end of a second and the datapoint would then be
            # appended in the next second.
            now + offset + datetime.timedelta(seconds=1),
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
        if hasattr(stream, 'downsampled_until'):
            self.assertTrue(datastream.Granularity.Seconds10.name in stream.downsampled_until)
            self.assertTrue(datastream.Granularity.Minutes.name in stream.downsampled_until)
            self.assertTrue(datastream.Granularity.Minutes10.name in stream.downsampled_until)
            self.assertTrue(datastream.Granularity.Hours.name in stream.downsampled_until)
            self.assertTrue(datastream.Granularity.Hours6.name in stream.downsampled_until)
            self.assertTrue(datastream.Granularity.Days.name in stream.downsampled_until)

        # At least Seconds10 and Minutes granularities should be available because we artificially increased backend time
        # See https://github.com/wlanslovenija/datastream/issues/12
        if hasattr(self, '_callback_points'):
            self.assertTrue(len(self._callback_points) >= 3, len(self._callback_points))
            cb_stream_id, cb_granularity, cb_datapoint = self._callback_points[2]
            self.assertEqual(cb_stream_id, stream_id)
            self.assertEqual(cb_granularity, datastream.Granularity.Seconds10)
            cb_stream_id, cb_granularity, cb_datapoint = self._callback_points[8]
            self.assertEqual(cb_stream_id, stream_id)
            self.assertEqual(cb_granularity, datastream.Granularity.Minutes)

        value_downsamplers_keys = [datastream.VALUE_DOWNSAMPLERS[d] for d in self.value_downsamplers]
        time_downsamplers_keys = [datastream.TIME_DOWNSAMPLERS[d] for d in self.time_downsamplers]

        if self.datastream.backend.downsampled_always_exist:
            start_timestamp = now
            end_timestamp = now + offset
        else:
            start_timestamp = datetime.datetime.utcfromtimestamp(0)
            end_timestamp = datetime.datetime.utcfromtimestamp(time.time()) + offset

        data = self.datastream.get_data(
            stream_id,
            datastream.Granularity.Minutes,
            start_timestamp,
            end_timestamp,
        )
        if self.datastream.backend.requires_downsampling:
            self.assertEqual(len(data), 1)
        else:
            self.assertEqual(len(data), 2)
        data = list(data)
        self._test_data_types(data)
        self.assertItemsEqual(data[0]['v'].keys(), value_downsamplers_keys)
        self.assertItemsEqual(data[0]['t'].keys(), time_downsamplers_keys)
        if hasattr(self, '_callback_points'):
            self.assertItemsEqual(data[0], cb_datapoint)
        self.assertTrue(datastream.VALUE_DOWNSAMPLERS['count'] in data[0]['v'].keys())

        if not self.datastream.backend.downsampled_always_exist:
            data = self.datastream.get_data(
                stream_id,
                datastream.Granularity.Minutes,
                start_timestamp,
            )
            if self.datastream.backend.requires_downsampling:
                self.assertEqual(len(data), 1)
            else:
                self.assertEqual(len(data), 2)
            data = list(data)
            self._test_data_types(data)
            self.assertItemsEqual(data[0]['v'].keys(), value_downsamplers_keys)
            self.assertItemsEqual(data[0]['t'].keys(), time_downsamplers_keys)
            if hasattr(self, '_callback_points'):
                self.assertItemsEqual(data[0], cb_datapoint)
            self.assertTrue(datastream.VALUE_DOWNSAMPLERS['count'] in data[0]['v'].keys())

        datapoint = data[0]

        data = self.datastream.get_data(
            stream_id,
            datastream.Granularity.Minutes,
            start_timestamp,
            end_timestamp,
            value_downsamplers=('count',),
        )
        if self.datastream.backend.requires_downsampling:
            self.assertEqual(len(data), 1)
        else:
            self.assertEqual(len(data), 2)
        data = list(data)
        self._test_data_types(data)
        self.assertItemsEqual(data[0]['v'].keys(), (datastream.VALUE_DOWNSAMPLERS['count'],))
        self.assertEqual(data[0]['v'][datastream.VALUE_DOWNSAMPLERS['count']], datapoint['v'][datastream.VALUE_DOWNSAMPLERS['count']])
        self.assertEqual(data[0]['t'], datapoint['t'])

        # Test time downsampler subset selection.
        if 'first' in self.time_downsamplers:
            data = self.datastream.get_data(
                stream_id,
                datastream.Granularity.Minutes,
                start_timestamp,
                end_timestamp,
                time_downsamplers=('first',),
            )
            if self.datastream.backend.requires_downsampling:
                self.assertEqual(len(data), 1)
            else:
                self.assertEqual(len(data), 2)
            data = list(data)
            self._test_data_types(data)
            self.assertItemsEqual(data[0]['v'], datapoint['v'])
            self.assertItemsEqual(data[0]['t'].keys(), (datastream.TIME_DOWNSAMPLERS['first'],))
            self.assertEqual(data[0]['t'][datastream.TIME_DOWNSAMPLERS['first']], datapoint['t'][datastream.TIME_DOWNSAMPLERS['first']])

            data = self.datastream.get_data(
                stream_id,
                datastream.Granularity.Minutes,
                start_timestamp,
                end_timestamp,
                value_downsamplers=('count',),
                time_downsamplers=('first',),
            )
            if self.datastream.backend.requires_downsampling:
                self.assertEqual(len(data), 1)
            else:
                self.assertEqual(len(data), 2)
            data = list(data)
            self._test_data_types(data)
            self.assertItemsEqual(data[0]['v'].keys(), (datastream.VALUE_DOWNSAMPLERS['count'],))
            self.assertEqual(data[0]['v'][datastream.VALUE_DOWNSAMPLERS['count']], datapoint['v'][datastream.VALUE_DOWNSAMPLERS['count']])
            self.assertItemsEqual(data[0]['t'].keys(), (datastream.TIME_DOWNSAMPLERS['first'],))
            self.assertEqual(data[0]['t'][datastream.TIME_DOWNSAMPLERS['first']], datapoint['t'][datastream.TIME_DOWNSAMPLERS['first']])

        data = self.datastream.get_data(
            stream_id,
            datastream.Granularity.Minutes,
            start_timestamp,
            end_timestamp,
            value_downsamplers=(),
        )
        if self.datastream.backend.requires_downsampling:
            self.assertEqual(len(data), 1)
        else:
            self.assertEqual(len(data), 2)
        data = list(data)
        # Not testing of data types because there are no values.
        self.assertFalse('v' in data[0], data[0].get('v', None))
        self.assertEqual(data[0]['t'], datapoint['t'])

        data = self.datastream.get_data(
            stream_id,
            datastream.Granularity.Minutes,
            start_timestamp,
            end_timestamp,
            time_downsamplers=(),
        )
        if self.datastream.backend.requires_downsampling:
            self.assertEqual(len(data), 1)
        else:
            self.assertEqual(len(data), 2)
        data = list(data)
        self._test_data_types(data)
        self.assertItemsEqual(data[0]['v'], datapoint['v'])
        self.assertFalse('t' in data[0], data[0].get('t', None))

        data = self.datastream.get_data(
            stream_id,
            datastream.Granularity.Minutes,
            start_timestamp,
            end_timestamp,
            value_downsamplers=(),
            time_downsamplers=(),
        )
        if self.datastream.backend.requires_downsampling:
            self.assertEqual(len(data), 1)
        else:
            self.assertEqual(len(data), 2)
        data = list(data)
        # Not testing of data types because there are no values.
        self.assertFalse('v' in data[0], data[0].get('v', None))
        self.assertFalse('t' in data[0], data[0].get('t', None))

        # Test stream removal.
        self.datastream.delete_streams(query_tags)
        with self.assertRaises(exceptions.StreamNotFound):
            self.datastream.get_data(stream_id, datastream.Granularity.Minutes, datetime.datetime.utcfromtimestamp(0))

    def test_granularities(self):
        query_tags = {
            'name': 'foodata',
        }
        tags = {}

        stream_id = self.datastream.ensure_stream(query_tags, tags, self.value_downsamplers, datastream.Granularity.Seconds)

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0)
        datapoints = []
        for i in range(1200):
            datapoints.append({'stream_id': stream_id, 'value': i, 'timestamp': ts})
            ts += datetime.timedelta(seconds=1)

        self.datastream.append_multiple(datapoints)

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

        if self.datastream.backend.downsampled_always_exist:
            start_timestamp = s
            end_timestamp = e
        else:
            start_timestamp = datetime.datetime.min
            end_timestamp = None

        # 10 MINUTES
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Minutes10, start=start_timestamp, end=end_timestamp)
        self.assertEqual(len(data), 1)
        self._test_data_types(data)

        # HOURS
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Hours, start=start_timestamp, end=end_timestamp)
        if self.datastream.backend.downsampled_always_exist:
            self.assertEqual(len(data), 1)
        else:
            self.assertEqual(len(data), 0)

        # 6 HOURS
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Hours6, start=start_timestamp, end=end_timestamp)
        if self.datastream.backend.downsampled_always_exist:
            self.assertEqual(len(data), 1)
        else:
            self.assertEqual(len(data), 0)

        # DAYS
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Days, start=start_timestamp, end=end_timestamp)
        if self.datastream.backend.downsampled_always_exist:
            self.assertEqual(len(data), 1)
        else:
            self.assertEqual(len(data), 0)

    def test_null_values(self):
        stream_id = self.datastream.ensure_stream({'name': 'foo'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)

        # Basic test with one stream.
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
        end_ts = ts

        # Test highest granularity.
        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=ts, end=end_ts)
        data = list(data)

        self.assertEqual([x['v'] for x in data], [None, 10, None, None, 2])

        # Test downsampled.
        with self.time_offset():
            self.datastream.downsample_streams()

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start=ts, end=end_ts)
        data = list(data)

        self._test_data_types(data)

        self.assertEqual(data[0]['v']['c'], 1)     # count
        self.assertEqual(data[0]['v']['d'], 0.0)   # standard deviation
        self.assertEqual(data[0]['v']['m'], 10.0)  # mean
        self.assertEqual(data[0]['v']['l'], 10.0)  # minimum
        if 'sum_squares' in self.value_downsamplers:
            self.assertEqual(data[0]['v']['q'], 100.0) # sum of squares
        self.assertEqual(data[0]['v']['s'], 10.0)  # sum
        self.assertEqual(data[0]['v']['u'], 10.0)  # maximum

        self.assertEqual(data[1]['v']['c'], 0)    # count
        self.assertEqual(data[1]['v']['d'], None) # standard deviation
        self.assertEqual(data[1]['v']['m'], None) # mean
        self.assertEqual(data[1]['v']['l'], None) # minimum
        if 'sum_squares' in self.value_downsamplers:
            self.assertEqual(data[1]['v']['q'], None) # sum of squares
        self.assertEqual(data[1]['v']['s'], None) # sum
        self.assertEqual(data[1]['v']['u'], None) # maximum

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
            ['count'],
            datastream.Granularity.Seconds,
            value_type='nominal',
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
        end_ts = ts

        with self.time_offset():
            self.datastream.downsample_streams()

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Minutes, start=ts, end_exclusive=end_ts)
        data = list(data)
        self._test_data_types(data)

        self.assertEqual([x['v']['m'] for x in data], [10.] + [None] * 9 + [20.] + [None] * 9 + [30.])

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

        # Ensure some more times with the same query tags, but different tags.
        for i in xrange(10):
            tags['stream_number'] = i
            stream_id = self.datastream.ensure_stream(query_tags, tags, self.value_downsamplers, datastream.Granularity.Seconds)

        # One stream at the end.
        streams = self.datastream.find_streams()
        self.assertEqual(len(streams), 1)
        self.assertEqual(streams[0]['title'], query_tags['title'])
        self.assertEqual(streams[0]['stream_number'], 9)

        # We do not allow finding by stream_id.
        self.assertEqual(len(self.datastream.find_streams({'stream_id': stream_id})), 0)

        # Internal tags should not be exposed through the API.
        self.assertEqual(len(self.datastream.find_streams({'tags': {'value_type': 'numeric'}})), 0)
        # Seconds, because this is class name in the database.
        self.assertEqual(len(self.datastream.find_streams({'tags': {'highest_granularity': 'Seconds'}})), 0)

        self.assertEqual(len(self.datastream.find_streams({'title': 'Stream 1'})), 1)
        self.assertEqual(len(self.datastream.find_streams({'stream_number': 1})), 0)
        self.assertEqual(len(self.datastream.find_streams({'stream_number': 9})), 1)

        # But delete streams can use stream_id.
        self.datastream.delete_streams({'stream_id': stream_id})
        self.assertEqual(len(self.datastream.find_streams()), 0)

    def test_stream_types(self):
        with self.assertRaises(exceptions.UnsupportedValueType):
            self.datastream.ensure_stream({'name': 'foo'}, {}, self.value_downsamplers, datastream.Granularity.Seconds, value_type='wtfvaluewtf')
        with self.assertRaises(TypeError):
            self.datastream.ensure_stream({'name': 'foo'}, {}, self.value_downsamplers, datastream.Granularity.Seconds, value_type='numeric', value_type_options='bar')

        stream_id = self.datastream.ensure_stream({'name': 'foo'}, {}, ['count'], datastream.Granularity.Seconds, value_type='graph')
        with self.assertRaises(exceptions.InconsistentStreamConfiguration):
            self.datastream.ensure_stream({'name': 'foo'}, {}, ['count'], datastream.Granularity.Seconds)
        with self.assertRaises(exceptions.UnsupportedDownsampler):
            self.datastream.ensure_stream({'name': 'bar'}, {}, self.value_downsamplers, datastream.Granularity.Seconds, value_type='graph')

        stream_id = self.datastream.ensure_stream({'name': 'zoo'}, {}, ['count'], datastream.Granularity.Seconds, value_type='nominal')
        with self.assertRaises(exceptions.UnsupportedDownsampler):
            self.datastream.ensure_stream({'name': 'bar'}, {}, self.value_downsamplers, datastream.Granularity.Seconds, value_type='nominal')

        stream_id = self.datastream.ensure_stream({'name': 'goo'}, {}, ['count'], datastream.Granularity.Seconds, value_type='graph')
        with self.assertRaises(TypeError):
            self.datastream.append(stream_id, 42, datetime.datetime(2000, 1, 1, 0, 0, 0))
        with self.assertRaises(ValueError):
            self.datastream.append(stream_id, {}, datetime.datetime(2000, 1, 1, 0, 0, 0))

        # Test invalid graphs.
        with self.assertRaises(ValueError):
            # Missing ID field "i".
            self.datastream.append(
                stream_id,
                {
                    'v': [{'foo': 'bar'}],
                },
                datetime.datetime(2000, 1, 1, 0, 0, 0),
            )

        with self.assertRaises(ValueError):
            # Missing ID field "i".
            self.datastream.append(
                stream_id,
                {
                    'v': [{'foo': 'bar'}],
                    'e': [],
                },
                datetime.datetime(2000, 1, 1, 0, 0, 0),
            )

        with self.assertRaises(ValueError):
            # Two nodes with the same ID.
            self.datastream.append(
                stream_id,
                {
                    'v': [
                        {'i': 'bar'},
                        {'i': 'bar'},
                    ],
                    'e': [],
                },
                datetime.datetime(2000, 1, 1, 0, 0, 0),
            )

        with self.assertRaises(ValueError):
            # Edge without values.
            self.datastream.append(
                stream_id,
                {
                    'v': [
                        {'i': 'foo'},
                        {'i': 'bar'},
                    ],
                    'e': [{}],
                },
                datetime.datetime(2000, 1, 1, 0, 0, 0),
            )

        with self.assertRaises(ValueError):
            # Edge only with from value.
            self.datastream.append(
                stream_id,
                {
                    'v': [
                        {'i': 'foo'},
                        {'i': 'bar'},
                    ],
                    'e': [{'f': 'foo'}],
                },
                datetime.datetime(2000, 1, 1, 0, 0, 0),
            )

        with self.assertRaises(ValueError):
            # Edge only with unknown to value.
            self.datastream.append(
                stream_id,
                {
                    'v': [
                        {'i': 'foo'},
                        {'i': 'bar'},
                    ],
                    'e': [{'f': 'foo', 't': 'wtf'}],
                },
                datetime.datetime(2000, 1, 1, 0, 0, 0),
            )

        # Test graph insertion.
        self.datastream.append(
            stream_id,
            {
                'v': [
                    {'i': 'foo'},
                    {'i': 'bar'},
                ],
                'e': [
                    {'f': 'foo', 't': 'bar'},
                ],
            },
            datetime.datetime(2000, 1, 1, 0, 0, 0),
        )

        self.datastream.append(
            stream_id,
            {
                'v': [
                    {'i': 'foo'},
                    {'i': 'bar'},
                ],
                'e': [],
            },
            datetime.datetime(2000, 1, 1, 0, 0, 1),
        )

        self.datastream.append(
            stream_id,
            {
                'v': [
                    {'i': 'foo'},
                    {'i': 'bar'},
                ],
                'e': [],
            },
            datetime.datetime(2000, 1, 1, 0, 1, 1),
        )

        # Ensure that downsampling and querying of graph-typed streams works correctly.
        if self.datastream.backend.requires_downsampling:
            with self.time_offset():
                self.datastream.downsample_streams()

        s = datetime.datetime(2000, 1, 1, 0, 0, 0)
        e = datetime.datetime(2000, 1, 1, 0, 1, 0)
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Minutes, start=s, end_exclusive=e)
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['v']['c'], 2)

        stream_id = self.datastream.ensure_stream({'name': 'loo'}, {}, ['count'], datastream.Granularity.Seconds, value_type='nominal')

        # We allow any value. There is no type checking between values of the same stream either.
        values = [10, {}, 'label', {'foo': 'bar', 'z': 42}]
        ts = datetime.datetime(2000, 1, 1, 0, 1, 0)
        for value in values:
            self.datastream.append(stream_id, value, ts)
            ts += datetime.timedelta(seconds=1)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=datetime.datetime.min)

        self.assertEqual(values, [datapoint['v'] for datapoint in data])

    def test_stream_types_derive_operators(self):
        stream_id = self.datastream.ensure_stream({'name': 'foo'}, {}, ['count'], datastream.Granularity.Seconds, value_type='graph')
        with self.assertRaises(exceptions.UnsupportedDeriveOperator):
            self.datastream.ensure_stream(
                {'name': 'derived'},
                {},
                ['count'],
                datastream.Granularity.Seconds,
                derive_from=[stream_id, stream_id],
                derive_op='sum',
                value_type='graph',
            )

        stream_id = self.datastream.ensure_stream({'name': 'zoo'}, {}, ['count'], datastream.Granularity.Seconds, value_type='nominal')
        with self.assertRaises(exceptions.UnsupportedDeriveOperator):
            self.datastream.ensure_stream(
                {'name': 'derived'},
                {},
                ['count'],
                datastream.Granularity.Seconds,
                derive_from=[stream_id, stream_id],
                derive_op='sum',
                value_type='nominal',
            )

    def test_append_multiple(self):
        stream_id = self.datastream.ensure_stream({'name': 'foo'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)
        stream2_id = self.datastream.ensure_stream({'name': 'foo2'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)

        points = []
        ts = datetime.datetime(2000, 1, 1, 12, 0, 0)
        for i in xrange(200):
            points.append({
                'stream_id': (stream_id, stream2_id)[i % 2],
                'value': i,
                'timestamp': ts,
            })
            ts += datetime.timedelta(seconds=1)

        self.datastream.append_multiple(points)

        self.assertEqual(len(self.datastream.get_data(stream_id, datastream.Granularity.Seconds, start=datetime.datetime.min)), 100)
        self.assertEqual(len(self.datastream.get_data(stream2_id, datastream.Granularity.Seconds, start=datetime.datetime.min)), 100)

    def test_downsample_freeze(self):
        stream_id = self.datastream.ensure_stream({'name': 'fooclub'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)

        self.datastream.append(stream_id, 1, datetime.datetime(2000, 1, 1, 12, 0, 0))
        self.datastream.append(stream_id, 1, datetime.datetime(2000, 1, 3, 12, 0, 0))

        with self.time_offset():
            self.datastream.downsample_streams(until=datetime.datetime(2000, 1, 3, 12, 0, 10))

        if self.datastream.backend.downsampled_always_exist:
            start_timestamp = datetime.datetime(2000, 1, 1, 12, 0, 0)
            end_timestamp = datetime.datetime(2000, 1, 3, 12, 0, 0)
        else:
            start_timestamp = datetime.datetime.min
            end_timestamp = None

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start=start_timestamp, end_exclusive=end_timestamp)
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

    def test_downsamplers(self):
        # Test with floats that have issues with exact representation.
        stream_id = self.datastream.ensure_stream({'name': 'small'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)
        ts0 = datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=pytz.utc)
        ts = ts0

        datapoints = []
        for i in xrange(100):
            datapoints.append({'stream_id': stream_id, 'value': 0.05, 'timestamp': ts})
            ts += datetime.timedelta(seconds=1)

        self.datastream.append_multiple(datapoints)

        if self.datastream.backend.requires_downsampling:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                with self.time_offset():
                    self.datastream.downsample_streams(until=ts)

                self.assertEqual(any([x.category == exceptions.InvalidValueWarning for x in w]), False)

        # Test with random numbers.
        random.seed(42)
        points = 43205
        interval = 2
        downsample_every = 5000
        src_data = [random.randint(-1000, 1000) for _ in xrange(points)]
        ts0 = datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=pytz.utc)
        ts = ts0

        stream_id = self.datastream.ensure_stream({'name': 'test'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)
        datapoints = []
        for i, v in enumerate(src_data):
            datapoints.append({'stream_id': stream_id, 'value': v, 'timestamp': ts})
            ts += datetime.timedelta(seconds=interval)

            if len(datapoints) >= 200:
                self.datastream.append_multiple(datapoints)
                datapoints = []

            if self.datastream.backend.requires_downsampling:
                if (i + 1) % downsample_every == 0:
                    with self.time_offset():
                        self.datastream.downsample_streams(until=ts)

        self.datastream.append_multiple(datapoints)
        datapoints = []

        if self.datastream.backend.requires_downsampling:
            with self.time_offset():
                self.datastream.downsample_streams(until=ts)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=ts0, end=ts)
        self.assertEqual(len(data), points)
        self._test_data_types(data)
        #self.assertEqual(mongodb.total_seconds(ts - ts0), points * interval)

        def check_values(granularity, data, n):
            self.assertEqual(data[0]['v']['c'], n) # count
            self.assertAlmostEqual(data[0]['v']['m'], float(sum(src_data[:n])) / n) # mean
            self.assertEqual(data[0]['v']['l'], min(src_data[:n])) # minimum
            self.assertEqual(data[0]['v']['u'], max(src_data[:n])) # maximum
            self.assertEqual(data[0]['v']['s'], sum(src_data[:n])) # sum
            if 'sum_squares' in self.value_downsamplers:
                self.assertEqual(data[0]['v']['q'], sum([x ** 2 for x in src_data[:n]])) # sum of squares

            if 'first' in self.time_downsamplers:
                self.assertEqual(data[0]['t']['a'], ts0) # first
            if 'last' in self.time_downsamplers:
                self.assertEqual(data[0]['t']['z'], ts0 + datetime.timedelta(seconds=(n - 1) * interval)) # last

            if self.datastream.backend.downsampled_timestamps_start_bucket:
                self.assertEqual(data[0]['t']['m'], granularity.round_timestamp(ts0))
            else:
                self.assertEqual(data[0]['t']['m'], ts0 + datetime.timedelta(seconds=(n - 1) * interval / 2)) # mean

        for granularity in self.datastream.Granularity.values[1:]:
            if granularity.duration_in_seconds() >= points * interval:
                break

            data = self.datastream.get_data(stream_id, granularity, start=ts0, end=ts)
            data = list(data)
            self._test_data_types(data)
            check_values(granularity, data, granularity.duration_in_seconds() / interval)

        # Test a stream with empty value downsamplers.
        stream_id = self.datastream.ensure_stream({'name': 'up'}, {}, [], datastream.Granularity.Seconds)
        ts0 = datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=pytz.utc)
        ts = ts0

        datapoints = []
        for i in xrange(100):
            datapoints.append({'stream_id': stream_id, 'value': 0.05, 'timestamp': ts})
            ts += datetime.timedelta(seconds=1)

        self.datastream.append_multiple(datapoints)

        if self.datastream.backend.requires_downsampling:
            with self.time_offset():
                self.datastream.downsample_streams(until=ts)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=ts0, end=ts)
        self.assertEqual(len(data), 100)
        # Check that all lower granularities are empty.
        for granularity in self.datastream.Granularity.values[1:]:
            data = self.datastream.get_data(stream_id, granularity, start=ts0, end=ts)
            self.assertEqual(len(data), 0)

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

        # Attempt to ensure the stream with incompatible configuration (operator).
        with self.assertRaises(exceptions.InconsistentStreamConfiguration):
            self.datastream.ensure_stream(
                {'name': 'derived'},
                {},
                self.value_downsamplers,
                datastream.Granularity.Seconds,
                derive_from=[streamA_id, streamB_id],
                derive_op='derivative',
            )

        # Attempt to ensure the stream with incompatible configuration (input streams).
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

        # Test derived stream chaining.
        chained_stream_id = self.datastream.ensure_stream(
            {'name': 'chained'},
            {},
            self.value_downsamplers,
            datastream.Granularity.Seconds,
            derive_from=[streamA_id, another_stream_id],
            derive_op='sum',
        )

        ts1 = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        self.datastream.append(streamA_id, 21, ts1)
        self.datastream.append(streamB_id, 21, ts1)

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

        if self.datastream.backend.requires_derived_stream_backprocess:
            self.datastream.backprocess_streams()

        # Check sum operator results.
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=ts1)
        data = list(data)
        self._test_data_types(data)
        self.assertEqual([x['v'] for x in data], [42, 50, 56, 64, 50, 112, 116])

        # Check derivative operator results.
        data = self.datastream.get_data(another_stream_id, self.datastream.Granularity.Seconds, start=ts1)
        data = list(data)
        self._test_data_types(data)
        self.assertEqual([x['v'] for x in data], [4.0, 3.0, 4.0, -7.0, 7.5, 4.0])

        # Check results of chained streams.
        data = self.datastream.get_data(chained_stream_id, self.datastream.Granularity.Seconds, start=ts1)
        data = list(data)
        self._test_data_types(data)
        self.assertEqual([x['v'] for x in data], [29.0, 31.0, 36.0, 18.0, 77.5, 78.0])

        # Test named source streams.
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

        if self.datastream.backend.requires_derived_stream_backprocess:
            self.datastream.backprocess_streams()

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=ts1)
        data = list(data)
        self._test_data_types(data)
        self.assertEqual([x['v'] for x in data], [42, 50])

        # Test invalid granularity specification.
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

        # Test sum for different granularities.
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

        if self.datastream.backend.requires_derived_stream_backprocess:
            self.datastream.backprocess_streams()

        if self.datastream.backend.requires_downsampling:
            # Before downsampling, nothing should be present in derived stream.
            ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
            data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start=ts)
            self.assertEqual(len(data), 0)

            with self.time_offset():
                self.datastream.downsample_streams(until=ts + datetime.timedelta(hours=10))

        if self.datastream.backend.requires_derived_stream_backprocess:
            self.datastream.backprocess_streams()

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start=ts)
        data = list(data)
        self._test_data_types(data)
        self.assertEqual([x['v'] for x in data], [10.0, 10.0, 10.0])

        # Test counter reset operator.
        streamA_id = self.datastream.ensure_stream({'name': 'crA'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)
        reset_stream_id = self.datastream.ensure_stream(
            {'name': 'reset'},
            {},
            ['count'],
            datastream.Granularity.Seconds,
            value_type='nominal',
            derive_from=streamA_id,
            derive_op='counter_reset',
        )

        for i, v in enumerate([10, 23, 28, 44, 2, 17, 90, 30, 2, 5, 10, 15, 23, 1, 7, 12, 19, 25, 31, 45, 51]):
            ts = datetime.datetime(2000, 1, 1, 12, 0, i, tzinfo=pytz.utc)
            self.datastream.append(streamA_id, v, ts)

        if self.datastream.backend.requires_derived_stream_backprocess:
            self.datastream.backprocess_streams()

        ts1 = ts
        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        data = self.datastream.get_data(reset_stream_id, self.datastream.Granularity.Seconds, start=ts)
        data = list(data)
        self._test_data_types(data)
        self.assertEqual([x['v'] for x in data], [1, 1, 1, 1])

        if self.datastream.backend.requires_downsampling:
            with self.time_offset():
                self.datastream.downsample_streams(until=ts + datetime.timedelta(hours=10))

        data = self.datastream.get_data(reset_stream_id, self.datastream.Granularity.Seconds10, start=ts, end_exclusive=ts1)
        data = list(data)
        self._test_data_types(data)
        self.assertEqual([x['v']['c'] for x in data], [3, 1])

        # Test counter derivative operator.
        uptime_stream_id = self.datastream.ensure_stream({'name': 'up'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)
        reset_stream_id = self.datastream.ensure_stream(
            {'name': 'rsup'},
            {},
            ['count'],
            datastream.Granularity.Seconds,
            value_type='nominal',
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

        if self.datastream.backend.requires_derived_stream_backprocess:
            self.datastream.backprocess_streams()

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=ts)
        data = list(data)
        self._test_data_types(data)
        self.assertEqual([x['v'] for x in data], [7.0, 251.0, 18.0, 11.0, 15.0])

        # Test stream backprocessing with the above uptime and data streams.
        reset_stream_id = self.datastream.ensure_stream(
            {'name': 'rsup2'},
            {},
            ['count'],
            datastream.Granularity.Seconds,
            value_type='nominal',
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

        # Test disable of initial backprocessing.
        reset_stream_id = self.datastream.ensure_stream(
            {'name': 'rsup3'},
            {},
            ['count'],
            datastream.Granularity.Seconds,
            value_type='nominal',
            derive_from=uptime_stream_id,
            derive_op='counter_reset',
            derive_backprocess=False,
        )
        stream_id = self.datastream.ensure_stream(
            {'name': 'rate3'},
            {},
            self.value_downsamplers,
            datastream.Granularity.Seconds,
            derive_from=[
                {'name': 'reset', 'stream': reset_stream_id},
                {'stream': data_stream_id},
            ],
            derive_op='counter_derivative',
            derive_args={'max_value': 256},
            derive_backprocess=False,
        )

        stream = datastream.Stream(self.datastream.get_tags(stream_id))
        self.assertEqual(stream.pending_backprocess, False)

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=ts)
        self.assertEqual(len(data), 0)

        self.datastream.backprocess_streams()

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=ts)
        self.assertEqual(len(data), 0)

        # Test backprocessing of a lot of data, to trigger multiple batches.
        streamA_id = self.datastream.ensure_stream({'name': 'batchA'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)
        streamB_id = self.datastream.ensure_stream({'name': 'batchB'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)
        stream_id = self.datastream.ensure_stream(
            {'name': 'derived5'},
            {},
            self.value_downsamplers,
            datastream.Granularity.Seconds,
            derive_from=[
                {'name': 'Stream A', 'stream': streamA_id},
                {'name': 'Stream B', 'stream': streamB_id},
            ],
            derive_op='sum',
        )

        # TODO: Currently this test assumes batches are 1000 points. Make batch size detectable.
        ts0 = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        for i in xrange(2000):
            ts = ts0 + datetime.timedelta(seconds=i)
            self.datastream.append(streamA_id, 1, ts)
            self.datastream.append(streamB_id, 1, ts)

        self.datastream.backprocess_streams()

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=ts0)
        self.assertEqual(len(data), 2000)
        self.assertEqual(sum((x['v'] for x in data)), 4000)

        # Test derived stream removal.
        with self.assertRaises(exceptions.OutstandingDependenciesError):
            self.datastream.delete_streams({'name': 'up'})
        with self.assertRaises(exceptions.OutstandingDependenciesError):
            self.datastream.delete_streams({'name': 'data'})
        with self.assertRaises(exceptions.OutstandingDependenciesError):
            self.datastream.delete_streams({'name': 'rsup'})

        self.datastream.delete_streams({'name': 'rate2'})
        self.datastream.delete_streams({'name': 'rsup2'})

        # Test multiple streams removal in correct order.
        self.datastream.delete_streams({})
