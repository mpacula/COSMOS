import warnings
import exceptions

warnings.filterwarnings("ignore", category=exceptions.RuntimeWarning, module='django.db.backends.sqlite3.base', lineno=50)


__version__="0.4.3"