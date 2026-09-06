"""Software Factory backend public surface.

The package replaces the former monolithic ``swfactory.backend`` module while preserving its public
imports.  Service semantics live in ``service`` and HTTP transport in ``server``.
"""

from .service import Factory, Refused
from .server import make_server, serve

__all__ = ["Factory", "Refused", "make_server", "serve"]
