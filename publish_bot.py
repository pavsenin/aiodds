import os, sys

from telegram import Telegram
from publish import log

bot = Telegram()
std_input = None
if not sys.stdin.isatty():
    std_input = sys.stdin.readlines()
link = bot.link('deployment', f"https://gitlab.com/pavsenin/aiodds/-/pipelines/{os.environ['CI_PIPELINE_ID']}")
if not std_input:
    message = f"AIOdds {link} was successful"
else:
    message = f"AIOdds {link} failed: {std_input}"

log.debug(message)
bot.send_message(message)