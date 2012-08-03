class DatastreamException(Exception):
    pass

class MetricNotFound(DatastreamException):
    pass

class MultipleMetricsReturned(DatastreamException):
    pass

class UnsupportedDownsampler(DatastreamException):
    pass

class UnsupportedGranularity(DatastreamException):
    pass

class ReservedTagNameError(DatastreamException):
    pass
