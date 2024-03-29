from requests import get

class Telegram:
    def __init__(self):
        self.bot_token = '1411465992:AAGM2PsdxJadPFwDNvxdCLShVgeocIhBDs8'
        self.bot_chat_id = '712721440'
        self.url = 'https://api.telegram.org/'

    def link(self, text, url): return f'[{text}]({url})'

    def send_message(self, message):
        request = f'{self.url}bot{self.bot_token}/sendMessage?chat_id={self.bot_chat_id}&parse_mode=Markdown&text={message}'
        return get(request).json()

    def get_updates(self):
        request = f'{self.url}bot{self.bot_token}/getUpdates'
        return get(request).json()