from dataclasses import dataclass
from datetime import date, datetime
from uuid import UUID

from validate import validate_data


class IterMixin:
    def __iter__(self):
        yield from [
            (attribute, getattr(self, attribute))
            for attribute in self.__match_args__
        ]


class PostInitMixin:
    def __post_init__(self):
        for attribute in self.__match_args__:
            attribute, value = list(
                validate_data({attribute: getattr(self, attribute)}).items()
            )[0]

            setattr(self, attribute, value)


@dataclass
class FilmworkExtract(PostInitMixin, IterMixin):
    id: UUID
    imdb_rating: float
    g_genre: str
    title: str
    description: str
    p_id: UUID
    p_name: str
    p_role: str


@dataclass
class FilmworkTransform(IterMixin):
    id: UUID
    imdb_rating: float
    genres: list
    title: str
    description: str
    directors_names: list
    actors_names: list
    writers_names: list
    directors: list  # id, name
    actors: list  # id, name
    writers: list  # id, name

    ### other_roles: dict = None


### for debug
if __name__ == "__main__":
    import datetime

    fw1 = FilmworkExtract(
        **{
            # "id": UUID("c9e1f6f0-4f1e-4a76-92ee-76c1942faa97"),
            "id": "c9e1f6f0-4f1e-4a76-92ee-76c1942faa97",
            "imdb_rating": 7.3,
            "g_genre": "Action",
            "title": "Star Trek: Discovery",
            "description": "Ten years before Kirk, Spock, and the Enterprise, the USS Discovery discovers new worlds and lifeforms as one Starfleet officer learns to understand all things alien.",
            "p_id": UUID("bd2a8ab8-a7bc-45cf-852b-d23cb1cf4b5d"),
            "p_name": "Sonequa Martin-Green",
            "p_role": "actor",
        }
    )

    # print(fw1.__match_args__)
    print(fw1)
    print(fw1.id)
    print(type(fw1.id))
    print(dict(fw1))
    print("\n")
    for key, value in dict(fw1).items():
        # print(f"{repr(key)} :   {value}")
        print(f"{repr(key)}")


# @dataclass
# class Genre:
#     id: UUID
#     name: str
#     description: str
#     created: datetime
#     modified: datetime


# @dataclass
# class Person:
#     id: UUID
#     full_name: str
#     created: datetime
#     modified: datetime


# @dataclass
# class Filmwork_0(IterMixin):
#     id: UUID
#     title: str
#     description: str
#     creation_date: date
#     rating: float
#     type: str
#     created: datetime
#     modified: datetime

#     # def __iter__(self):
#     #     yield from [
#     #         (attribute, getattr(self, attribute))
#     #         for attribute in self.__match_args__
#     #     ]


# @dataclass
# class GenreFilmwork:
#     id: UUID
#     film_work_id: UUID
#     genre_id: UUID
#     created: datetime


# @dataclass
# class PersonFilmwork:
#     id: UUID
#     film_work_id: UUID
#     person_id: UUID
#     role: str
#     created: datetime

# fw0_1 = Filmwork_0(
#     id=UUID("3d825f60-9fff-4dfe-b294-1a45fa1e115d"),
#     title="Star Wars: Episode IV - A New Hope",
#     description="The Imperial Forces, under orders from cruel Darth Vader, hold Princess Leia hostage in their efforts to quell the rebellion against the Galactic Empire. Luke Skywalker and Han Solo, captain of the Millennium Falcon, work together with the companionable droid duo R2-D2 and C-3PO to rescue the beautiful princess, help the Rebel Alliance and restore freedom and justice to the Galaxy.",
#     creation_date=None,
#     rating=8.6,
#     type="movie",
#     created=datetime.datetime(2021, 6, 16, 20, 14, 9, 221838),
#     modified=datetime.datetime(2021, 6, 16, 20, 14, 9, 221855),
# )
# print(dict(fw0_1))
