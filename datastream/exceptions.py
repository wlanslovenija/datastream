class DatastreamException(Exception):
    pass

class MetricNotFound(DatastreamException):
    pass

class MultipleMetricsReturned(DatastreamException):
    pass

class UnsupportedDownsampler(DatastreamException, ValueError):
    pass

class UnsupportedGranularity(DatastreamException, ValueError):
    pass

class ReservedTagNameError(DatastreamException, ValueError):
    pass
