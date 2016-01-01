import contextlib
import datetime
import decimal
import unittest

import mongoengine

import datastream
from datastream.backends import mongodb, influxdb


class DatastreamBackendTestCase(unittest.TestCase):
    def _test_data_types(self, data):
        for d in data:
            self.assertIsInstance(d['v'], (int, long, float, decimal.Decimal, dict, None.__class__))
            if isinstance(d['v'], dict):
                for v in d['v'].values():
                    self.assertIsInstance(v, (int, long, float, decimal.Decimal, None.__class__))


class DatastreamMongoDBTestCase(DatastreamBackendTestCase):
    database_name = 'test_database'

    def _test_callback(self, stream_id, granularity, datapoint):
        self._callback_points.append((stream_id, granularity, datapoint))

    def setUp(self):
        backend = mongodb.Backend(self.database_name, host='127.0.0.1', port=27017)
        backend._test_callback = self._test_callback
        self.datastream = datastream.Datastream(backend)
        self.value_downsamplers = self.datastream.backend.value_downsamplers
        self.time_downsamplers = self.datastream.backend.time_downsamplers
        self._callback_points = []

    def _initializeDatabase(self):
        db = mongoengine.connection.get_db(mongodb.DATABASE_ALIAS)
        for collection in db.collection_names():
            if collection in ('system.indexes', 'system.profile'):
                continue
            db.drop_collection(collection)

    def tearDown(self):
        self._initializeDatabase()
        with self.switch_database():
            self._initializeDatabase()

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


class DatastreamInfluxDBTestCase(DatastreamBackendTestCase):
    database_name = 'test_database'

    def setUp(self):
        backend = influxdb.Backend(
            connection_influxdb={'host': '127.0.0.1', 'port': 8086, 'database': self.database_name},
            connection_metadata={
                'host': '127.0.0.1', 'port': 5432, 'database': self.database_name,
                'user': 'test',
                'password': 'test',
            },
        )
        self.datastream = datastream.Datastream(backend)
        self.value_downsamplers = self.datastream.backend.value_downsamplers
        self.time_downsamplers = self.datastream.backend.time_downsamplers

        with self.datastream.backend._metadata:
            with self.datastream.backend._metadata.cursor() as cursor:
                cursor.execute('DROP SCHEMA IF EXISTS datastream CASCADE')
                self.datastream.backend._create_schema()

        self.datastream.backend._influxdb.query('DROP DATABASE %s' % self.database_name)
        self.datastream.backend._influxdb.create_database(self.database_name)

    def tearDown(self):
        #with self.datastream.backend._metadata:
        #    with self.datastream.backend._metadata.cursor() as cursor:
        #        cursor.execute('DROP SCHEMA IF EXISTS datastream CASCADE')

        self.datastream.backend._influxdb.drop_database(self.database_name)

    @contextlib.contextmanager
    def time_offset(self, offset=datetime.timedelta(minutes=1)):
        yield
