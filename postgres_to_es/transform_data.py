from config import logger
from dataclasses_ import FilmworkExtract, FilmworkTransform


class TransformData:
    def __init__(self):
        pass

    def transform_batch(self, batch):
        transformed_batch = {}

        for row in batch:
            logger.debug("\nДанные для трансформации: \n%s\n", row)
            transformed_row = self.transform_row(row)
            row_id = transformed_row["id"]

            if row_id not in transformed_batch:

                logger.debug(
                    "\nДанные после трансформации: \n%s\n", transformed_row
                )

                transformed_batch[row_id] = transformed_row
                continue

            transformed_batch[row_id] = self.aggregte_movie_dict(
                transformed_batch[row_id], transformed_row
            )

        transformed_data = list(transformed_batch.values())
        return transformed_data

    def transform_row(self, row):
        row_dict = dict(FilmworkExtract(**row))
        logger.debug("\nrow_dict: \n%s\n", row_dict)

        movie, role_dict = {}, {}
        for key, value in row_dict.items():
            if key.startswith("g_"):
                movie["genres"] = []
                if value is not None:
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
        if role not in ("director", "actor", "writer", None):
            logger.info("\nOther role '%s'!", role)
        for person_role in ("director", "actor", "writer"):
            if role == person_role:
                movie[f"{role}s"].append(self.transform_role_dict(role_dict))
                movie[f"{role}s_names"].append(role_dict["p_name"])

        logger.debug("\nmovie: \n%s\n", movie)
        movie = dict(FilmworkTransform(**movie))
        return movie

    @staticmethod
    def transform_role_dict(role_dict):
        new_role_dict = {}
        new_role_dict["id"] = role_dict["p_id"]
        new_role_dict["name"] = role_dict["p_name"]
        return new_role_dict

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
        return movie

    @staticmethod
    def add_values(values_base: list | str, value_add: list) -> dict:
        if len(value_add) > 0 and value_add[0] not in values_base:
            values_base.extend(value_add)

            if isinstance(values_base[0], dict):
                new_values = sorted(values_base, key=lambda v: v["name"])
            else:
                new_values = sorted(values_base)

        else:
            new_values = values_base

        return new_values
