import warnings
warnings.warn("Using luigi_extension is deprecated. It is now called 'pipeline'", DeprecationWarning,
              stacklevel=2)

from pycarol.pipeline import *