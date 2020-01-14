from typing import Dict
from datafeeds.urjanet.model import UrjanetData


# These functions are currently a little silly, and only serve to stop
# one from calling serialization routines directly on model objects


def urja_to_json(urja_data: UrjanetData) -> Dict:
    return urja_data.to_json()


def json_to_urja(json_dict: Dict) -> UrjanetData:
    return UrjanetData(json_dict)
