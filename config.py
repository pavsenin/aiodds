import os
from abc import ABC, abstractmethod

class BaseConfig(ABC):
    @property    
    @abstractmethod
    def TimestampMultiplier(self):
        pass
    @property
    @abstractmethod
    def Database(self):
        pass
    @property
    def User(self):
        return 'aspcapper'
    @property
    def Password(self):
        return 'Asp11291109'
    @property
    def Host(self):
        return '109.68.215.54'

class LocalConfig(BaseConfig):
    @property
    def TimestampMultiplier(self):
        return 1
    @property
    def Database(self):
        return 'aioddsdb_test'

class RemoteConfig(BaseConfig):
    @property
    def TimestampMultiplier(self):
        return 1000

class TestConfig(RemoteConfig):
    @property
    def Database(self):
        return 'aioddsdb_test'

class ProductionConfig(RemoteConfig):
    @property
    def Database(self):
        return 'aioddsdb'

config = LocalConfig()