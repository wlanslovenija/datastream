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


class UnsupportedDeriveOperator(DatastreamException, ValueError):
    pass


class ReservedTagNameError(DatastreamException, ValueError):
    pass


class InvalidTimestamp(DatastreamException, ValueError):
    pass


class IncompatibleGranularities(DatastreamException, ValueError):
    pass


class AppendToDerivedStreamNotAllowed(DatastreamException, ValueError):
    pass


class InvalidOperatorArguments(DatastreamException, ValueError):
    pass


class DatastreamWarning(RuntimeWarning):
    pass


class InvalidValueWarning(DatastreamWarning):
    pass


class InternalInconsistencyWarning(DatastreamWarning):
    pass
