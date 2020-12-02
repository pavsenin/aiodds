from utils import Log
from config_provider import config

log = Log(config, 'publish')
for python_file in ['config', 'utils', 'db', 'constants', 'telegram', 'preprocessing', 'scraping', 'updating', 'update']:
    output = config.OS.copy_file(f'{python_file}.py', config.to_abs_path('prod', f'{python_file}.py'))
    if output != b'':
        log.debug(output)