Datastream API
==============

Datastream API is one of the projects of `wlan slovenija`_ open wireless network.
It is a Python API for time-series data which abstracts the database which is used to store the data,
providing a powerful and unified API.
It provides an easy way to insert time-series datapoints and automatically downsample them into multiple
levels of granularity for efficient querying time-series data at various time scales.

.. _wlan slovenija: https://wlan-si.net

Documentation is found at:

http://datastream.readthedocs.org/

We provide a Django HTTP RESTful interface through django-datastream_ package. You can use it
directly in your Django application, or check its source code to learn more how to integrate
Datastream API into your application.

.. _django-datastream: https://github.com/wlanslovenija/django-datastream

For questions and development discussions use `development mailing list`_.

.. _development mailing list: https://wlan-si.net/lists/info/development
