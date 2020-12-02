import os
from telegram import Telegram
from config_provider import config

success = True
for python_file in ['config', 'utils', 'db', 'constants', 'telegram', 'preprocessing', 'scraping', 'updating', 'update']:
    result = config.OS.copy_file(f'{python_file}.py', config.to_abs_path('prod', f'{python_file}.py'))
    success = success and result == b''
    if not success:
        break

url = f"https://gitlab.com/pavsenin/aiodds/-/pipelines/{os.environ['CI_PIPELINE_ID']}"
bot = Telegram()
if success:
    bot.send_message(f'AIOdds deployment was {bot.link('successful', url)}')
else:
    bot.send_message(f'AIOdds deployment {bot.link('failed', url)}: {python_file}')