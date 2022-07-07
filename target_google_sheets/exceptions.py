class TargetException(Exception):
    """Base for all TargetGoogleSheets exceptions"""


class MessageNotRecognized(TargetException):
    """Raised when encountering an unknown Singer message type"""


class SchemaNotFound(TargetException):
    """Raised when RECORD message are received before a SCHEMA messages"""


class OverflowedSink(TargetException):
    """Exception which occurs when Sink reaches MAX_SINK_LIMIT number of rows"""
