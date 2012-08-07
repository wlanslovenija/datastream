import datetime, time, unittest

import mongoengine

import datastream
from datastream.backends import mongodb

class BasicTest(object):
    def test_basic(self):
        query_tags = [
            {'name': 'foobar'},
        ]
        tags = [
            'more',
            {'andmore': 'bar'},
        ]
        metric_id = self.datastream.ensure_metric(query_tags, tags, self.downsamplers, datastream.Granularity.Seconds)

        metric = datastream.Metric(self.datastream.get_tags(metric_id))
        self.assertEqual(metric.id, metric_id)
        self.assertItemsEqual(metric.downsamplers, self.downsamplers)
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

        self.datastream.insert(metric_id, 42)

        data = self.datastream.get_data(metric_id, datastream.Granularity.Seconds, datetime.datetime.utcfromtimestamp(0), datetime.datetime.utcfromtimestamp(time.time()))
        self.assertEqual(len(data), 1)

        data = self.datastream.get_data(metric_id, datastream.Granularity.Minutes, datetime.datetime.utcfromtimestamp(0))
        self.assertItemsEqual(data, [])

        # Artificially increase backend time for a minute so that downsample will do something for minute granularity
        self.datastream.backend._time_offset += datetime.timedelta(minutes=1)

        self.datastream.downsample_metrics()

        data = self.datastream.get_data(metric_id, datastream.Granularity.Seconds, datetime.datetime.utcfromtimestamp(0), datetime.datetime.utcfromtimestamp(time.time()) + self.datastream.backend._time_offset)
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['v'], 42)

        data = self.datastream.get_data(metric_id, datastream.Granularity.Seconds, datetime.datetime.utcfromtimestamp(0))
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['v'], 42)

        downsamplers_keys = [datastream.DOWNSAMPLERS[d] for d in self.downsamplers]

        data = self.datastream.get_data(metric_id, datastream.Granularity.Minutes, datetime.datetime.utcfromtimestamp(0), datetime.datetime.utcfromtimestamp(time.time()) + self.datastream.backend._time_offset)
        self.assertEqual(len(data), 1)
        self.assertItemsEqual(data[0]['v'].keys(), downsamplers_keys)

        data = self.datastream.get_data(metric_id, datastream.Granularity.Minutes, datetime.datetime.utcfromtimestamp(0))
        self.assertEqual(len(data), 1)
        self.assertItemsEqual(data[0]['v'].keys(), downsamplers_keys)
        self.assertTrue(datastream.DOWNSAMPLERS['count'] in data[0]['v'].keys())

        data = self.datastream.get_data(metric_id, datastream.Granularity.Minutes, datetime.datetime.utcfromtimestamp(0), downsamplers=('count',))
        self.assertEqual(len(data), 1)
        self.assertItemsEqual(data[0]['v'].keys(), (datastream.DOWNSAMPLERS['count'],))
        self.assertEqual(data[0]['v'][datastream.DOWNSAMPLERS['count']], 1)

class MongoDBBasicTest(BasicTest, unittest.TestCase):
    database_name = 'test_database'

    def setUp(self):
        self.datastream = datastream.Datastream(mongodb.Backend(self.database_name))
        self.downsamplers = self.datastream.backend.downsamplers

    def tearDown(self):
        db = mongoengine.connection.get_db(mongodb.DATABASE_ALIAS)
        for collection in db.collection_names():
            if collection == 'system.indexes':
                continue
            db.drop_collection(collection)
