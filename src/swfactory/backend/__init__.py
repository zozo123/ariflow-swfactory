"""Software Factory backend public surface.

The package replaces the former monolithic ``swfactory.backend`` module while preserving its public
imports.  Service semantics live in ``service`` and HTTP transport in ``server``.
"""

from .server import make_server, serve
from .service import Factory, Refused

__all__ = ["Factory", "Refused", "make_server", "serve"]
