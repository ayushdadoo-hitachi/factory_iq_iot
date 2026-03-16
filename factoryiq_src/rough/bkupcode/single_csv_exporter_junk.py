import sys, importlib
import core.logging_config as logcfg

print("sys.path[0:5] =>", sys.path[:5])
print("logging_config file =>", logcfg.__file__)
print("has get_module_logger =>", hasattr(logcfg, "get_module_logger"))
print("module dict sample =>", [n for n in dir(logcfg) if "logger" in n or "setup" in n])

# hard reload (clears the *module*, not just its attributes)
if "core.logging_config" in sys.modules:
    del sys.modules["core.logging_config"]

import core.logging_config as logcfg
importlib.reload(logcfg)

print("RELOADED logging_config file =>", logcfg.__file__)
print("has get_module_logger (after reload) =>", hasattr(logcfg, "get_module_logger"))