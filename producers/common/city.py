from pathlib import Path

import yaml

from producers.common.models import City

CITIES_DIR = Path(__file__).resolve().parents[2] / "config" / "cities"


def load_city(name: str) -> City:
    path = CITIES_DIR / f"{name}.yaml"
    city = City.model_validate(yaml.safe_load(path.read_text()))
    ids = [c.id for c in city.corridors]
    if len(ids) != len(set(ids)):
        raise ValueError(f"duplicate corridor ids in {path}")
    return city
