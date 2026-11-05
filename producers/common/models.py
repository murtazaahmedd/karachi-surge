from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field


class Point(BaseModel):
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)


class Corridor(BaseModel):
    id: str = Field(pattern=r"^[a-z0-9_]+$")
    name: str
    start: Point
    end: Point
    sample_point: Point


class City(BaseModel):
    city: str
    timezone: str
    weather_point: Point
    corridors: list[Corridor] = []


class TrafficFlowEvent(BaseModel):
    event_id: UUID
    city: str
    corridor_id: str
    h3_cell: str
    current_speed_kmh: float = Field(ge=0)
    free_flow_speed_kmh: float = Field(gt=0)
    congestion_ratio: float = Field(ge=0, le=1)
    confidence: float = Field(ge=0, le=1)
    event_time: datetime
    ingest_time: datetime
    source: str


class WeatherEvent(BaseModel):
    event_id: UUID
    city: str
    temp_c: float
    precip_mm: float = Field(ge=0)
    humidity: float = Field(ge=0, le=100)
    wind_kmh: float = Field(ge=0)
    weather_code: int
    event_time: datetime
    ingest_time: datetime
    source: str


class RideRequestEvent(BaseModel):
    request_id: UUID
    city: str
    pickup_h3: str
    dropoff_h3: str
    est_distance_km: float = Field(ge=0)
    event_time: datetime
    ingest_time: datetime
    source: str
