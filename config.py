import os, subprocess
from abc import ABC, abstractmethod

class OS(ABC):
    @abstractmethod
    def clear_ram(self): pass
    @abstractmethod
    def copy_file(self, source, target): pass
class Ubuntu(OS):
    def clear_ram(self):
        return subprocess.check_output('sync; echo 1 > /proc/sys/vm/drop_caches', shell=True)
    def copy_file(self, source, target):
        return subprocess.check_output(f'yes | cp -rf {source} {target}', shell=True)
class Windows(OS):
    def clear_ram(self):
        return None
    def copy_file(self, source, target):
        return None

class BaseConfig(ABC):
    def __init__(self, os):
        self.os = os
    @property
    def OS(self): return self.os
    def to_abs_path(self, *paths): return os.path.join(self.Root, *paths)
    @property
    @abstractmethod
    def Root(self): pass
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
    def Root(self): return 'c:/git/pavsenin/aiodds/'
    @property
    def Database(self): return 'aioddsdb_test'

class RemoteConfig(BaseConfig):
    def __init__(self):
        super().__init__(Ubuntu())
    @property
    def Root(self): return '/usr/aiodds/'
class TestConfig(RemoteConfig):
    @property
    def Database(self): return 'aioddsdb_test'

class ProductionConfig(RemoteConfig):
    @property
    def Database(self): return 'aioddsdb'