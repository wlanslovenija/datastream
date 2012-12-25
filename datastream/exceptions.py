class DatastreamException(Exception):
    pass

class StreamNotFound(DatastreamException):
    pass

class MultipleStreamsReturned(DatastreamException):
    pass

class UnsupportedDownsampler(DatastreamException, ValueError):
    pass

class UnsupportedGranularity(DatastreamException, ValueError):
    pass

class ReservedTagNameError(DatastreamException, ValueError):
    pass

class InvalidTimestamp(DatastreamException, ValueError):
    pass
