from utils import Ubuntu, Windows
from abc import ABC, abstractmethod

class BaseConfig(ABC):
    def __init__(self, os):
        self.os = os
    @property
    def OS(self): return self.os
    @property
    @abstractmethod
    def Database(self): pass
    @property
    def User(self): return 'aspcapper'
    @property
    def Password(self): return 'Asp11291109'
    @property
    def Host(self): return '109.68.215.54'

class LocalConfig(BaseConfig):
    def __init__(self):
        super().__init__(Windows())
    @property
    def Database(self): return 'aioddsdb_test'

class RemoteConfig(BaseConfig):
    def __init__(self):
        super().__init__(Ubuntu())
class TestConfig(RemoteConfig):
    @property
    def Database(self): return 'aioddsdb_test'

class ProductionConfig(RemoteConfig):
    @property
    def Database(self): return 'aioddsdb'

config = TestConfig()