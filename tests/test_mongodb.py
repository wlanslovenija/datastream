import collections
import datetime
import decimal
import pytz
import sys
import threading
import time
import unittest

import datastream
from datastream import exceptions
from datastream.backends import mongodb

from . import base, test_common


class MongoDBTestsMixin(object):
    def test_null_values_derivation_operators(self):
        # Run the null values test to get the stream.
        self.test_null_values()
        stream_id = self.datastream.ensure_stream({'name': 'foo'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)

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

        with self.time_offset():
            self.datastream.downsample_streams()

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Minutes, start=ts)
        data = list(data)
        self._test_data_types(data)

        self.assertEqual([x['v']['m'] for x in data], [10.] + [None] * 9 + [20.] + [None] * 9 + [30.])

    def test_high_accuracy(self):
        stream_id = self.datastream.ensure_stream(
            {'name': 'foo'}, {}, self.value_downsamplers, datastream.Granularity.Seconds,
            value_type='numeric',
            value_type_options={'high_accuracy': True}
        )

        for i in xrange(40):
            ts = datetime.datetime(2000, 1, 1, 12, 0, i, tzinfo=pytz.utc)
            self.datastream.append(stream_id, decimal.Decimal('2.71828182845904523536028747135266249775724709369995957496696762'), ts)

        with self.time_offset():
            self.datastream.downsample_streams(until=ts + datetime.timedelta(hours=10))

        ts = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds, start=ts)
        data = list(data)
        self._test_data_types(data)

        for datapoint in data:
            self.assertEqual(datapoint['v'], decimal.Decimal('2.71828182845904523536028747135266249775724709369995957496696762'))

        data = self.datastream.get_data(stream_id, self.datastream.Granularity.Seconds10, start=ts)
        data = list(data)
        self._test_data_types(data)

        self.assertEqual(data[0]['v']['m'], decimal.Decimal('2.71828182845904523536028747135266249775724709369995957496696762'))

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
        self.assertAlmostEqual(data[0]['v']['d'], 1.4142135623730951) # standard deviation
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

        # Try with various null values
        stream_id = self.datastream.ensure_stream({'name': 'test2'}, {}, self.value_downsamplers, datastream.Granularity.Seconds)

        ts0 = datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=pytz.utc)
        ts = ts0

        for i in xrange(30):
            self.datastream.append(stream_id, {'m': None, 's': None, 'l': None, 'u': None, 'q': None, 'd': None, 'c': None}, ts)
            self.datastream.append(stream_id, {'m': 42, 's': 42, 'l': 42, 'u': 42, 'q': None, 'd': 0, 'c': 1}, ts)
            ts += datetime.timedelta(seconds=1)

        with self.time_offset():
            self.datastream.downsample_streams()

        data = self.datastream.get_data(stream_id, datastream.Granularity.Seconds10, start=ts0, end=ts)
        data = list(data)
        self._test_data_types(data)

        self.assertEqual(data[0]['v']['c'], 10) # count
        self.assertEqual(data[0]['v']['m'], 42) # mean
        self.assertEqual(data[0]['v']['l'], 42) # minimum
        self.assertEqual(data[0]['v']['u'], 42) # maximum
        self.assertEqual(data[0]['v']['s'], 420) # sum
        self.assertEqual(data[0]['v']['q'], None) # sum of squares

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

    def test_find_streams_mongodb(self):
        # Note that the MongoDB backend supports more filtering options.

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
        self.datastream.ensure_stream(query_tags, tags, self.value_downsamplers, datastream.Granularity.Seconds)

        self.assertEqual(len(self.datastream.find_streams({'title': {'iexact': 'stream 1'}})), 1)
        self.assertEqual(len(self.datastream.find_streams({'title': {'iexact': 'strEAm 1'}})), 1)
        self.assertEqual(len(self.datastream.find_streams({'title': {'icontains': 'strEAm'}})), 1)
        self.assertEqual(len(self.datastream.find_streams({'stream_number': {'gte': 1}})), 1)
        self.assertEqual(len(self.datastream.find_streams({'stream_number': {'gt': 1}})), 0)
        self.assertEqual(len(self.datastream.find_streams({'visualization': {'value_downsamplers': 'mean'}})), 1)
        self.assertEqual(len(self.datastream.find_streams({'visualization': {'value_downsamplers': {'in': ['mean']}}})), 1)
        self.assertEqual(len(self.datastream.find_streams({'visualization': {'value_downsamplers': {'all': ['mean', 'min']}}})), 1)
        self.assertEqual(len(self.datastream.find_streams({'visualization': {'value_downsamplers': {'all': ['mean', 'foobar']}}})), 0)

        with self.assertRaises(exceptions.ReservedTagNameError):
            self.assertEqual(len(self.datastream.find_streams({'andmore__bar': 'value'})), 1)


# Test MongoDB backend.
class TestMongoDB(test_common.CommonTestsMixin, MongoDBTestsMixin, base.DatastreamMongoDBTestCase):
    pass


@unittest.skip('stress test')
class StressTest(base.DatastreamMongoDBTestCase):
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
