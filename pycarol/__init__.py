import os
import tempfile

__version__ = '2.3'


__BUCKET_NAME__= 'carol-internal'
__TEMP_STORAGE__ = os.path.join(tempfile.gettempdir(),'carolina/cache')