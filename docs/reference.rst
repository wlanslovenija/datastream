.. _reference:

Reference
=========

API
---

.. autoclass:: datastream.api.Datastream
   :members:

Backends
--------

API operations are implemented in backends, which are responsible for storing datapoints,
performing downsampling, deriving streams, and executing queries.

.. autoclass:: datastream.backends.mongodb.Backend

Implementation Details
......................

Streams are stored in the ``streams`` collection, datapoints are stored in the ``datapoints.<granularity>`` collections,
where ``<granularity>`` is one of the possible granularity levels.

.. todo::

    Describe the schema.
    Describe how _id value is constructed for downsampled values.

When performing downsampling, we have to differentiate between two timestamps:

* Datapoint timestamp is the timestamp of the datapoint that has been inserted for a given granularity level.
  On the highest granularity level it is always second precision. On lower granularity levels it is a dictionary
  of multiple values, depending on time downsamplers settings for a given stream.

* Internal datapoint timestamp (stored in datapoint's ``_id``) is based on a timespan for the given granularity
  level. For example, if a datapoint was inserted at 31-07-2012 12:23:52, then the downsampled internal timestamp for
  the timespan this datapoint is in for hour granularity would be 31-07-2012 12:00:00 and for month granularity would
  be 01-07-2012 00:00:00.

Based on ``highest_granularity`` value, appended datapoints are stored in the collection configured by
``highest_granularity`` and only lower granularity values are downsampled. Requests for granularity
higher than ``highest_granularity`` simply return values from ``highest_granularity`` collection.
``highest_granularity`` is just an optimization to not store unnecessary datapoints for granularity levels
which would have at most one datapoint for their granularity timespans.

.. todo::

    Describe how downsampled metadata is stored and updated by downsampling functions.
    Describe how derivation metadata is stored and used (and about derivation state).

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
