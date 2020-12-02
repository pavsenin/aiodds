from config_provider import config

for python_file in ['config', 'utils', 'db', 'constants', 'telegram', 'preprocessing', 'scraping', 'updating', 'update']:
    config.OS.copy_file(config.to_abs_path('src/aiodds', f'{python_file}.py'), config.to_abs_path('prod', f'{python_file}.py'))