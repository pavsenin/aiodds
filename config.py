import os
from abc import ABC, abstractmethod

class BaseConfig(ABC):
    @property    
    @abstractmethod
    def TimestampMultiplier(self):
        pass
    @property
    @abstractmethod
    def DatabaseName(self):
        pass

class LocalConfig(BaseConfig):
    @property
    def TimestampMultiplier(self):
        return 1
    @property
    def DatabaseName(self):
        return 'aioddsdb_test'

class RemoteConfig(BaseConfig):
    @property
    def TimestampMultiplier(self):
        return 1000

class TestConfig(RemoteConfig):
    @property
    def DatabaseName(self):
        return 'aioddsdb_test'

class ProductionConfig(RemoteConfig):
    @property
    def DatabaseName(self):
        return 'aioddsdb'

config = LocalConfig()