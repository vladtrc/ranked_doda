import datetime
from abc import ABC, abstractmethod


class UserRepository(ABC):
    @abstractmethod
    def get_or_create_user_by_name(self, name):
        pass


class HeroRepository(ABC):
    @abstractmethod
    def get_or_create_hero_id_by_name(self, name):
        pass


class UserResultRepository(ABC):
    @abstractmethod
    def create_user_result(self, result):
        pass


class MatchRepository(ABC):
    @abstractmethod
    def get_or_create_match_by_date(self, date: datetime.datetime, defaults: dict) -> int:
        pass
