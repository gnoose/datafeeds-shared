from abc import ABC, abstractmethod

from ..model import UrjanetData, GridiumBillingPeriodCollection


class UrjanetGridiumTransformer(ABC):
    @abstractmethod
    def urja_to_gridium(self, urja_data: UrjanetData) -> GridiumBillingPeriodCollection:
        pass
