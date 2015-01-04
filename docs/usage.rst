Usage
=====

Datastream API provides a Python interface which you can initialize with MongoDB backend by::

    import datastream
    from datastream.backends import mongodb

    stream = datastream.Datastream(mongodb.Backend('database_name'))

MongoDB backend accepts some additional connection settings, if this is needed.

After that you can create new streams, insert datapoints into them and query streams. See :ref:`API reference <reference>`
for more information.

Tags
----

Each stream can have arbitrary JSON-serializable metadata associated to it through arbitrary tags. You can then query
streams by using those tags. Some tags are reserved to not conflict with stream settings and some tags are used by
higher-level packages like django-datastream_. Although tags can be complex values, simple values like strings or
simple dicts are preferred.

Types
-----

Datastream API supports various types for values stored as datapoints. Types influence how downsampling is done.
Currently supported types are:

* ``numeric`` – each datapoint value is a number
* ``nominal`` – each datapoint value is a an arbitrary value, but most often a simple label
* ``graph`` – each datapoint value is a graph

Numeric values can be integers, floats, :py:class:`decimal.Decimal`, or any other instance of :py:class:`numbers.Number`.
Alternatively, one can append an already downsampled value in the same format and with all values downsampled values for
a given stream have. This is useful when the source of their values already provides information from multiple
samples. For example, pinging over the Internet sends multiple packets and then returns min, max, mean times.
By storing directly min, max, and mean values, no information is lost and can be reused by Datastream API.

`Nominal values`_ (also known as qualitative) can be any JSON-serializable arbitrary value, but most often they
are a simple label. Values are stored as-is in the database so repeating the same huge value multiple times will
be stored multiple times. If values will be repeating it is better to instead store only some small keys representing
them. Nominal values do not have a defined order between them.

`Graph values`_ are stored as dicts in the format::

    {
        "v": [
            {"i": "foo"},
            {"i": "bar"}
        ],
        "e": [
            {"f": "foo", "t": "bar"}
        ]
    }

It contains a list of vertices ``v`` where each vertex element contains its ID ``i``. IDs can be of arbitrary type.
Vertices can contain additional fields which are ignored, but might be used by downsamplers. List of edges ``e``
contains edges from vertex with ID equal to ``f``, to vertex with ID equal to ``t``. Additional fields are ignored,
but might be used by downsamplers as well.

.. _Nominal values: https://en.wikipedia.org/wiki/Level_of_measurement#Nominal_scale
.. _Graph values: https://en.wikipedia.org/wiki/Graph_%28mathematics%29

Downsampling
------------

Datastream API automatically downsample datapoints to lower granularity levels. Highest supported resolution for
datapoints is a second, and then Datastream API will downsample them. If you know that you will insert datapoints
at lower granularity levels (for example, only every 5 minutes), you can specify that so that Datastream API can
optimize.

Downsampling happens both for the datapoint value and the datapoint timestamp. It takes a list of datapoints for a
timespan at a higher granularity level and creates a downsampled value and downsampled timestamp for a datapoint
at a lower granularity level. You can configure what exactly this downsampled datapoint contains. You can for
example configure that it contains a mean, minimum and maximum of all values from a timespan. Same for the timestamp,
for example, you can configure that timestamp for the datapoint contains first, last and mean timestamps of all
datapoints from a timespan.

All downsampling timespans for all streams are equal and rounded at reasonable boundaries (for example, hour granularity
starts and ends at full hour).

Derived Streams
---------------

Datastream API supports derived streams. Streams which are automatically generated from other streams as new datapoints
are appended to those streams. For example, you can create a stream which computes derivative of another stream. Or sums
multiple streams together.

Django HTTP Interface
---------------------

We provide a Django HTTP RESTful interface through django-datastream_ package. You can use it
directly in your Django application, or check its source code to learn more how to integrate
Datastream API into your application.

.. _django-datastream: https://github.com/wlanslovenija/django-datastream
