import os
from telegram import Telegram

publish_results = input()
print(publish_results)
url = f"https://gitlab.com/pavsenin/aiodds/-/pipelines/{os.environ['CI_PIPELINE_ID']}"
bot = Telegram()
if not publish_results:
    bot.send_message(f"AIOdds deployment was {bot.link('successful', url)}")
else:
    bot.send_message(f"AIOdds deployment {bot.link('failed', url)}: {python_file}")