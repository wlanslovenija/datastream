.. _reference:

Reference
=========

API
---

.. autoclass:: datastream.api.Datastream
   :members:

Backends
--------

.. autoclass:: datastream.backends.mongodb.Backend

Implementation Details
......................

The basic API operations are implemented in backends, which are responsible for storing the datapoints,
performing downsampling and executing queries. Supported backend is MongoDB.

Streams are stored in the streams collection, datapoints are stored in the
datapoints.<granularity> collections, where <granularity> is seconds, minutes,
hours, and days.

TODO: Describe the schema.

This backend supports all defined downsampling functions. There are two ways in which updates can be handled:

* At the end of each measurement interval, downsampling is performed for all values in the previous interval.

* Incrementally, after inserting each point.

When performing downsampling, we have to differentiate between two timestamps:

* Datapoint raw timestamp is the timestamp of the raw datapoint that has been inserted for a given stream. It always has second granularity.

* Datapoint downsampled timestamp is generated from the raw timestamp by rounding it to the given granularity. For example if raw timestamp is 31-07-2012 12:23:52, then the downsampled timestamp for hour granularity would be 31-07-2012 12:00:00 and for month granularity would be 01-07-2012 00:00:00. TODO: I am not sure if this is the best. You are discarding information about time-spread of datapoints on higher granularity here. Like missing datapoints on higher granularity, having only one datapoint there, having datapoints not in in regular intervals and so on. I would take the average (or median?) of timestamps of datapoints in the interval.

Based on highest_granularity value, raw datapoints are stored in the collection
configured by highest_granularity and only lower granularity values are
downsampled. Requests for granularity higher than highest_granularity simply
return values from highest_granularity collection.

TODO: Describe how downsampled metadata is stored and updated by downsampling functions.

Value Downsamplers
------------------

.. method:: mean(key: m)

    Average of all datapoints.

.. method:: median(key: e)

    Median of all datapoints.

.. method:: sum(key: s)

    Sum of all datapoints.

.. method:: min(key: l, for lower)

    Minimum value of all dataponts.

.. method:: max(key: u, for upper)

    Maximum value of all datapoints.

.. method:: sum_squares(key: q)

    Sum of squares of all datapoints.

.. method:: std_dev(key: d)

    Standard deviation of all datapoints.

.. method:: count(key: c)

    Number of all datapoints.

.. method:: most_often(key: o, for often)

    The most often occurring value of all datapoints.

.. method:: least_often(key: r, for rare)

    The least often occurring value of all datapoints.

.. method:: frequencies(key: f)

    For each value number of occurrences in all datapoints.


Time Downsamplers
-----------------

.. method:: mean(key: m)

    Average of all timestamps.

.. method:: median(key: e)

    Median of all timestamps.

.. method:: first(key: a, is the first in the alphabet)

    The first timestamp of all datapoints.

.. method:: last(key: z, is the last in the alphabet)

    The last timestamp of all datapoints.

Derive Operators
----------------

.. method:: sum(src_streams, dst_stream)

    Sum of multiple streams.

.. method:: derivative(src_stream, dst_stream)

    Derivative of a stream.

.. method:: counter_reset(src_stream, dst_stream)

    Generates a counter reset stream.

.. method:: counter_derivative([{'name': 'reset', 'stream': reset_stream_id}, {'stream': data_stream_id}], dst_stream, max_value=None)

    Derivative of a monotonically increasing counter stream.

Exceptions
----------

.. py:module:: datastream.exceptions

.. autoclass:: DatastreamException

.. autoclass:: StreamNotFound

.. autoclass:: MultipleStreamsReturned

.. autoclass:: InconsistentStreamConfiguration

.. autoclass:: OutstandingDependenciesError

.. autoclass:: UnsupportedDownsampler

.. autoclass:: UnsupportedGranularity

.. autoclass:: UnsupportedDeriveOperator

.. autoclass:: ReservedTagNameError

.. autoclass:: InvalidTimestamp

.. autoclass:: IncompatibleGranularities

.. autoclass:: AppendToDerivedStreamNotAllowed

.. autoclass:: InvalidOperatorArguments

.. autoclass:: DatastreamWarning

.. autoclass:: InvalidValueWarning

.. autoclass:: InternalInconsistencyWarning
