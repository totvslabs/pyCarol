import importlib.util

# Dynamically load modules

# This part was created to handle different mappings loading. Depending on the specified mapping, the correct module
# will be dynamically loaded.

loaded_modules = {}

app_mappings = dict(
)

def _load_module(module_name, modules_path):
    spec = importlib.import_module('.'.join([modules_path, module_name]))
    loaded_modules[module_name] = spec


def _load_mapping_module(module_name, modules_path):
    global loaded_modules
    global app_mappings
    if module_name not in loaded_modules:
        _load_module(module_name, modules_path)
    module = loaded_modules[module_name]
    app_mappings[module_name] = module.get_mapping_task


def get_ingestion_task(mapping_id='mdm', dm_name='', modules_path='app.datamodel.mappings'):
    global app_mappings

    if mapping_id not in app_mappings:
        _load_mapping_module(mapping_id, modules_path)

    return app_mappings[mapping_id](dm_name)
