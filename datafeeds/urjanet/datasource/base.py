"""Base classes for objects that fetch Urjanet data

An "UrjanetDataSource" loads data into the data model defined in
../model. Expressing this concept as an abstract class is maybe
a little excessive, from the python perspective, but mainly serves
a documentation purpose
"""
from abc import ABC, abstractmethod
from enum import Enum
from ..model import UrjanetData


class CommodityType(Enum):
    """Use this to convert between the meter table commodity and Urjanet's ServiceType."""
    ccf = ["water", "irrigation"]
    kw = ["electric"]


class UrjanetDataSource(ABC):
    """Base class for classes that load Urjanet data into a model"""

    def __init__(self):
        pass

    @abstractmethod
    def load(self) -> UrjanetData:
        """Load Urjanet data into an UrjanetData model object

        Any parameters required to inform this process should be defined as
        fields in the implementing class.

        Returns:
           An UrjanetData object
        """
        pass
