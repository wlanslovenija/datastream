GRANULARITIES = (
    'seconds',
    'minutes',
    'hours',
    'days',
)

RESERVED_TAGS = (
    'metric_id',
    'downsamplers',
    'highest_granularity',
)

class Datastream(object):
    def __init__(self, backend):
        """
        Class constructor.

        :param backend: Backend instance
        """

        self.backend = backend

    def ensure_metric(self, query_tags, tags, downsamplers, highest_granularity):
        """
        Ensures that a specified metric exists.

        :param query_tags: Tags which uniquely identify a metric
        :param tags: Tags that should be used (together with `query_tags`) to create a
            metric when it doesn't yet exist
        :param downsamplers: A set of names of downsampler functions for this metric
        :param highest_granularity: Predicted highest granularity of the data the metric
            will store, may be used to optimize data storage
        :return: A metric identifier
        """

        return self.backend.ensure_metric(query_tags, tags, downsamplers, highest_granularity)

    def get_tags(self, metric_id):
        """
        Returns the tags for the specified metric.

        :param metric_id: Metric identifier
        :return: A list of tags for the metric
        """

        return self.backend.get_tags(metric_id)

    def update_tags(self, metric_id, tags):
        """
        Updates metric tags with new tags, overriding existing ones.

        :param metric_id: Metric identifier
        :param tags: A list of new tags
        """

        return self.backend.update_tags(metric_id, tags)

    def find_metrics(self, query_tags):
        """
        Finds all metrics matching the specified query tags.

        :param query_tags: Tags that should be matched to metrics
        :return: A list of matched metric descriptors
        """

        return self.backend.find_metrics(query_tags)

    def insert(self, metric_id, value):
        """
        Inserts a data point into the data stream.

        :param metric_id: Metric identifier
        :param value: Metric value
        """

        return self.backend.insert(metric_id, value)

    def get_data(self, metric_id, granularity, start, end):
        """
        Retrieves data from a certain time range and of a certain granularity.

        :param metric_id: Metric identifier
        :param granularity: Wanted granularity
        :param start: Time range start
        :param end: Time range end
        :return: A list of datapoints
        """

        return self.backend.get_data(metric_id, granularity, start, end)

    def downsample_metrics(self, query_tags):
        """
        Requests the backend to downsample all metrics matching the specified
        query tags.

        :param query_tags: Tags that should be matched to metrics
        """

        return self.backend.downsample_metrics(query_tags)
