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
class Filmwork(PostInitMixin, IterMixin):
    fw_id: UUID
    title: str
    description: str
    creation_date: date
    rating: float
    type: str
    created: datetime
    modified: datetime
    g_id: UUID
    genre: str
    p_id: UUID
    person_name: str
    person_role: str

    # genres: list = None
    # actors: list = None
    # directors: list = None
    # writers: list = None
    # other_roles: list = None


### for debug
if __name__ == "__main__":
    import datetime

    fw1 = Filmwork(
        **{
            # "fw_id": UUID("0d79e7f3-842f-4006-aa1c-f18e4e76abbe"),
            "fw_id": "0d79e7f3-842f-4006-aa1c-f18e4e76abbe",
            "title": "A Star for Two",
            "description": "Teenage lovers in the 1940s are separated by World War II and then later reunited in the 1980s, but encounter new complications when they try to rekindle their relationship..",
            "rating": 7.2,
            "creation_date": None,
            "type": "movie",
            "created": datetime.datetime(2021, 6, 16, 20, 14, 9, 240214),
            "modified": datetime.datetime(2021, 6, 16, 20, 14, 9, 240230),
            # "p_id": UUID("11b36fce-4afb-430c-8f51-4ced0704007a"),
            "p_id": "11b36fce-4afb-430c-8f51-4ced0704007a",
            "person_name": "Anthony Quinn",
            "person_role": "actor",
            "g_id": UUID("1cacff68-643e-4ddd-8f57-84b62538081a"),
            "genre": "Drama",
        }
    )

    # print(fw1.__match_args__)
    print(fw1)
    print(fw1.fw_id)
    print(type(fw1.fw_id))
    print(dict(fw1))


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
