"""The restaurant floor -- one waiter serving every table.

The event loop is the waiter: it runs ready callbacks, watches the
kitchen timers, and sleeps when there is nothing to do.
"""

from pynet.loop.reactor import ErrorContext, EventLoop, Handle, TimerHandle

__all__ = ["ErrorContext", "EventLoop", "Handle", "TimerHandle"]
