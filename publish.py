from telegram import Telegram
from config_provider import config

success = True
for python_file in ['config', 'utils', 'db', 'constants', 'telegram', 'preprocessing', 'scraping', 'updating', 'update']:
    result = config.OS.copy_file(f'{python_file}.py', config.to_abs_path('prod', f'{python_file}.py'))
    success = success and result == b''
    if not success:
        break
bot = Telegram()
if success:
    bot.send_message(bot.green('AIOdds deployment was successful'))
else:
    bot.send_message(bot.red(f'AIOdds deployment failed: {python_file}'))