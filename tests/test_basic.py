import datetime, unittest

import mongoengine

import datastream
from datastream.backends import mongodb

class BasicTest(object):
    def test_basic(self):
        query_tags = (
            {'name': 'foobar'},
        )
        tags = (
            'more',
            {'andmore': 'bar'},
        )
        metric_id = self.datastream.ensure_metric(query_tags, tags, self.downsamplers, datastream.GRANULARITIES[0])

        metric = datastream.Metric(self.datastream.get_tags(metric_id))
        self.assertEqual(metric.id, metric_id)
        self.assertItemsEqual(metric.downsamplers, self.downsamplers)
        self.assertEqual(metric.highest_granularity, datastream.GRANULARITIES[0])
        self.assertItemsEqual(metric.tags, query_tags + tags)

        # Should not do anything
        self.datastream.downsample_metrics()

        data = self.datastream.get_data(metric_id, datastream.GRANULARITIES[0], datetime.datetime.utcfromtimestamp(0), datetime.datetime.now())
        self.assertItemsEqual(data, [])

        self.datastream.insert(metric_id, 42)
        self.datastream.downsample_metrics()

        data = self.datastream.get_data(metric_id, datastream.GRANULARITIES[0], datetime.datetime.utcfromtimestamp(0), datetime.datetime.now())
        self.assertEqual(len(data), 1)

class MongoDBBasicTest(BasicTest, unittest.TestCase):
    database_name = 'test_database'

    def setUp(self):
        self.downsamplers = mongodb.DOWNSAMPLERS
        self.datastream = datastream.Datastream(mongodb.Backend(self.database_name))

    def tearDown(self):
        db = mongoengine.connection.get_db(mongodb.DATABASE_ALIAS)
        for collection in db.collection_names():
            if collection == 'system.indexes':
                continue
            db.drop_collection(collection)
