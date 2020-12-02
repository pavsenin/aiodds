from utils import Log
from config_provider import config

log = Log(config, 'publish')
for python_file in ['config', 'utils', 'db', 'constants', 'telegram', 'preprocessing', 'scraping', 'updating', 'update']:
    result = config.OS.copy_file(config.to_abs_path('src/aiodds', f'{python_file}.py'), config.to_abs_path('prod', f'{python_file}.py'))
    log.debug(f'Copy {python_file} with {result}')