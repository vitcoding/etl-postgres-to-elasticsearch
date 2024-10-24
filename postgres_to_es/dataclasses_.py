from dataclasses import dataclass
from datetime import date, datetime
from uuid import UUID


class IterMixin:
    def __iter__(self):
        yield from [
            (attribute, getattr(self, attribute))
            for attribute in self.__match_args__
        ]


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
class Filmwork(IterMixin):
    id: UUID
    title: str
    description: str
    creation_date: date
    rating: float
    type: str
    created: datetime
    modified: datetime

    # def __iter__(self):
    #     yield from [
    #         (attribute, getattr(self, attribute))
    #         for attribute in self.__match_args__
    #     ]


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


### for debug
if __name__ == "__main__":
    import datetime

    fw1 = Filmwork(
        id=UUID("3d825f60-9fff-4dfe-b294-1a45fa1e115d"),
        title="Star Wars: Episode IV - A New Hope",
        description="The Imperial Forces, under orders from cruel Darth Vader, hold Princess Leia hostage in their efforts to quell the rebellion against the Galactic Empire. Luke Skywalker and Han Solo, captain of the Millennium Falcon, work together with the companionable droid duo R2-D2 and C-3PO to rescue the beautiful princess, help the Rebel Alliance and restore freedom and justice to the Galaxy.",
        creation_date=None,
        rating=8.6,
        type="movie",
        created=datetime.datetime(2021, 6, 16, 20, 14, 9, 221838),
        modified=datetime.datetime(2021, 6, 16, 20, 14, 9, 221855),
    )
    # print(fw1.__match_args__)
    # print(fw1.id)
    print(dict(fw1))
