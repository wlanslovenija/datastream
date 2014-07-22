class DatastreamException(Exception):
    """
    The base class for all datastream API exceptions.
    """

    # Just to remove useless docstring on __init__ in documentation
    def __init__(self, *args, **kwargs):
        """
        """

        super(DatastreamException, self).__init__(*args, **kwargs)


class StreamNotFound(DatastreamException):
    """
    Raised when stream queried for is not found.
    """

    pass


class MultipleStreamsReturned(DatastreamException):
    """
    Raised when multiple streams found when queried for operations which operate on only one stream, like
    :py:meth:`~datastream.api.Datastream.ensure_stream`. Specify more specific query tags.
    """

    pass


class InconsistentStreamConfiguration(DatastreamException):
    """
    Raised when stream configuration passed to :py:meth:`~datastream.api.Datastream.ensure_stream` is
    inconsistent and/or conflicting.
    """

    pass


class OutstandingDependenciesError(DatastreamException):
    """
    Raised when stream cannot be deleted because it is a dependency for another stream.
    """

    pass


class UnsupportedDownsampler(DatastreamException, ValueError):
    """
    Raised when downsampler requested is unsupported.
    """

    pass


class UnsupportedGranularity(DatastreamException, ValueError):
    """
    Raised when granularity level requested is unsupported.
    """

    pass


class UnsupportedDeriveOperator(DatastreamException, ValueError):
    """
    Raised when derive operator requested is unsupported.
    """

    pass


class UnsupportedValueType(DatastreamException, TypeError):
    """
    Raised when value type requested is unsupported.
    """

    pass


class ReservedTagNameError(DatastreamException, ValueError):
    """
    Raised when updating tags with a reserved tag name.
    """
    pass


class InvalidTimestamp(DatastreamException, ValueError):
    """
    Raised when an invalid timestamp was provided.
    """
    pass


class IncompatibleGranularities(DatastreamException, ValueError):
    """
    Raised when derived stream's granularity is incompatible with source stream's granularity.
    """

    pass


class IncompatibleTypes(DatastreamException, TypeError):
    """
    Raised when derived stream's value type is incompatible with source stream's value type.
    """

    pass


class AppendToDerivedStreamNotAllowed(DatastreamException, ValueError):
    """
    Raised when attempting to append to a derived stream.
    """

    pass


class InvalidOperatorArguments(DatastreamException, ValueError):
    """
    Raised when derive operators received invalid arguments.
    """
    pass


class LockExpiredMidMaintenance(DatastreamException):
    """
    Raised when a maintenance lock expires inside a maintenance operation.
    """
    pass


class StreamAppendContended(DatastreamException):
    """
    Raised when too many processes are trying to append to the same stream.
    """
    pass


class DatastreamWarning(RuntimeWarning):
    """
    The base class for all datastream API runtime warnings.
    """

    # Just to remove useless docstring on __init__ in documentation
    def __init__(self, *args, **kwargs):
        """
        """

        super(DatastreamWarning, self).__init__(*args, **kwargs)


class InvalidValueWarning(DatastreamWarning):
    """
    Warning used when an invalid value is encountered.
    """

    pass


class InternalInconsistencyWarning(DatastreamWarning):
    """
    Warning used when an internal inconsistency is detected.
    """

    pass


class DownsampleConsistencyNotGuaranteed(DatastreamWarning):
    """
    Warning used when consistency of downsampled values with original datapoints
    is no longer guaranteed due to some condition. Reseting downsample state and
    redoing downsampling could be necessary.
    """

    pass
