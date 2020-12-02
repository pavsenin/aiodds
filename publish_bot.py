import os, sys

from telegram import Telegram

bot = Telegram()
link = bot.link('deployment', f"https://gitlab.com/pavsenin/aiodds/-/pipelines/{os.environ['CI_PIPELINE_ID']}")
if not sys.stdin:
    bot.send_message(f"AIOdds {link} was successful")
else:
    bot.send_message(f"AIOdds {link} failed: {sys.stdin}")