.. _reference:

Reference
=========

API
---

The basic API operations are implemented in backends, which are
responsible for storing the datapoints, performing downsampling and executing
time span queries. Supported backends are MongoDB and Tempo.

.. autoclass:: datastream.api.Datastream
   :members:

Tags are a set of arbitrary JSON-serializable values that can be assigned to
each stream. Although tags can be complex values, simple values like strings or
dicts of strings are preferred.

Backends
--------

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


.. autoclass:: datastream.backends.mongodb.Backend


Value Downsamplers
------------------

.. method:: mean(key: m)

    average of all datapoints

.. method:: median(key: e)

    median of all datapoints

.. method:: sum(key: s)

    sum of all datapoints

.. method:: min(key: l, for lower)

    minimum value of all dataponts

.. method:: max(key: u, for upper)

    maximum value of all datapoints

.. method:: sum_squares(key: q)

    sum of squares of all datapoints

.. method:: std_dev(key: d)

    standard deviation of all datapoints

.. method:: count(key: c)

    number of all datapoints

.. method:: most_often(key: o, for often)

    the most often occurring value of all datapoints

.. method:: least_often(key: r, for rare)

    the least often occurring value of all datapoints

.. method:: frequencies(key: f)

    for each value number of occurrences in all datapoints


Time Downsamplers
-----------------

.. method:: mean(key: m)

    average of all timestamps

.. method:: median(key: e)

    median of all timestamps

.. method:: first(key: f)

    first timestamp in the interval

.. method:: last(key: l)

    last timestamp in the interval


Exceptions
----------

.. py:module:: datastream.exceptions

.. autoclass:: DatastreamException

.. autoclass:: StreamNotFound

.. autoclass:: MultipleStreamsReturned

.. autoclass:: UnsupportedDownsampler

.. autoclass:: UnsupportedGranularity

.. autoclass:: ReservedTagNameError

.. autoclass:: InvalidTimestamp
