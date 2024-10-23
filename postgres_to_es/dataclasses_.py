from dataclasses import dataclass
from datetime import date, datetime
from uuid import UUID


@dataclass
class Genre:
    id: UUID
    name: str
    description: str
    created: datetime
    modified: datetime


@dataclass
class Person:
    id: UUID
    full_name: str
    created: datetime
    modified: datetime


@dataclass
class Filmwork:
    id: UUID
    title: str
    description: str
    creation_date: date
    file_path: str
    rating: float
    type: str
    created: datetime
    modified: datetime


@dataclass
class GenreFilmwork:
    id: UUID
    film_work_id: UUID
    genre_id: UUID
    created: datetime


@dataclass
class PersonFilmwork:
    id: UUID
    film_work_id: UUID
    person_id: UUID
    role: str
    created: datetime
