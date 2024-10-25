from config import logger
from dataclasses_ import FilmworkExtract, FilmworkTransform


class TransformData:
    def __init__(self):
        pass

    @staticmethod
    def add_values(values_base: list | str, value_add: list) -> dict:
        if isinstance(values_base, list):
            if len(value_add) > 0 and value_add[0] not in values_base:
                values_base.extend(value_add)
                new_values = sorted(values_base, key=lambda v: v["name"])
            else:
                new_values = values_base
        if isinstance(values_base, str):
            temp_list = [
                element for element in values_base.split(", ") if element
            ]
            temp_list.extend(value_add)
            new_values = ", ".join(sorted(list(set(temp_list))))
        return new_values

    @staticmethod
    def aggregte_movie_dict(movie, transformed_row):
        logger.debug(
            "\nmovie: \n%s \n\ntransformed_row: \n%s\n", movie, transformed_row
        )

        for key in (
            "genres",
            "directors_names",
            "actors_names",
            "writers_names",
        ):
            if transformed_row[key]:
                movie[key] = __class__.add_values(
                    movie[key], transformed_row[key]
                )
            if key != "genres":
                another_key = key.replace("_names", "")
                movie[another_key] = __class__.add_values(
                    movie[another_key], transformed_row[another_key]
                )
        # logger.info("\nmovie: \n%s\n", movie)

        # if transformed_row["genres"]:
        #     movie["genres"] = __class__.add_values(
        #         movie["genres"], transformed_row["genres"]
        #     )
        # if transformed_row["directors_names"]:
        #     movie["directors_names"] = __class__.add_values(
        #         movie["directors_names"], transformed_row["directors_names"]
        #     )
        # if transformed_row["actors_names"]:
        #     movie["actors_names"] = __class__.add_values(
        #         movie["actors_names"], transformed_row["actors_names"]
        #     )
        # if transformed_row["writers_names"]:
        #     movie["writers_names"] = __class__.add_values(
        #         movie["writers_names"], transformed_row["writers_names"]
        #     )
        return movie

    def transform_batch(self, batch):
        transformed_batch = {}

        for data in batch:
            data_example = data[0] if len(data) > 0 else None
            logger.debug(
                "\nПример данных для трансформации: \n%s\n", data_example
            )
            for row in data:
                transformed_row = self.transform_row(row)
                row_id = transformed_row["id"]

                if row_id not in transformed_batch:
                    temp_dict = {}
                    logger.debug("\ntransformed_row: \n%s\n", transformed_row)
                    for key, value in transformed_row.items():
                        match key:
                            case (
                                "genres"
                                | "directors_names"
                                | "actors_names"
                                | "writers_names"
                            ):
                                temp_dict[key] = ",".join(value)
                            case _:
                                temp_dict[key] = value
                    transformed_batch[row_id] = temp_dict
                    continue

                transformed_batch[row_id] = self.aggregte_movie_dict(
                    transformed_batch[row_id], transformed_row
                )

        transformed_data = list(transformed_batch.values())
        yield transformed_data

    def transform_row(self, row):
        row_dict = dict(FilmworkExtract(**row))
        logger.debug("\nrow_dict: \n%s\n", row_dict)

        movie, role_dict = {}, {}
        for key, value in row_dict.items():
            if key.startswith("g_"):
                movie["genres"] = []
                movie["genres"].append(value)
            elif key.startswith("p_"):
                role_dict[key] = value
            else:
                movie[key] = value
        for key in (
            "directors_names",
            "actors_names",
            "writers_names",
            "directors",
            "actors",
            "writers",
        ):
            movie[key] = []

        role = role_dict["p_role"]
        if role not in ("director", "actor", "writer"):
            logger.info("\nOther role!")
        for person_role in ("director", "actor", "writer"):
            if role == person_role:
                movie[f"{role}s"].append(self.transform_role_dict(role_dict))
                movie[f"{role}s_names"].append(role_dict["p_name"])

        logger.debug("\nmovie: \n%s\n", movie)
        movie = dict(FilmworkTransform(**movie))

        # print(row_dict)
        # print(movie, genre_dict, role_dict, sep="\n", end="\n\n")
        # for key, value in movie.items():
        #     print(f"{key} :    {value}")

        return movie

    @staticmethod
    def transform_role_dict(role_dict):
        new_role_dict = {}
        new_role_dict["id"] = role_dict["p_id"]
        new_role_dict["name"] = role_dict["p_name"]
        return new_role_dict
