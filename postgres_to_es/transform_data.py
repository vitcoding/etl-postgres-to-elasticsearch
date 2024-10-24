from dataclasses_ import Filmwork


class TransformData:
    def __init__(self):
        pass

    def transform_batch(self, batch):
        transformed_batch = {}

        for data in batch:
            for row in data:
                transformed_row = self.transform_row(row)
                row_id = transformed_row["id"]

                if row_id not in transformed_batch:
                    transformed_batch[row_id] = transformed_row
                    continue

                movie = transformed_batch[row_id]
                # print(movie)
                if transformed_row["genres"]:
                    # print(list(transformed_row["genres"].keys())[0])
                    genre_name = list(transformed_row["genres"].keys())[0]
                    movie["genres"][genre_name] = transformed_row["genres"]
                if transformed_row["actors"]:
                    actor_name = list(transformed_row["actors"].keys())[0]
                    movie["actors"][actor_name] = transformed_row["actors"]
                if transformed_row["writers"]:
                    writer_name = list(transformed_row["writers"].keys())[0]
                    movie["writers"][writer_name] = transformed_row["writers"]
                if transformed_row["directors"]:
                    director_name = list(transformed_row["directors"].keys())[
                        0
                    ]
                    movie["directors"][director_name] = transformed_row[
                        "directors"
                    ]
                if transformed_row["other_roles"]:
                    other_role_name = list(
                        transformed_row["other_roles"].keys()
                    )[0]
                    movie["other_roles"][other_role_name] = transformed_row[
                        "other_roles"
                    ]

                transformed_batch[row_id] = movie

            transformed_data = list(transformed_batch.values())
            yield transformed_data

        # print(transformed_batch)

        # transformed_data = list(transformed_batch.values())
        # return transformed_data

    def transform_row(self, row):
        row_dict = dict(Filmwork(**row))

        movie, genre_dict, role_dict = {}, {}, {}
        for key, value in row_dict.items():
            if key.startswith("g_"):
                genre_dict[key] = value
            elif key.startswith("p_"):
                role_dict[key] = value
            else:
                movie[key] = value
        movie["genres"] = {genre_dict["g_genre"]: genre_dict}
        # movie["genre_names"]
        movie["actors"] = {}
        movie["writers"] = {}
        movie["directors"] = {}
        movie["other_roles"] = {}
        match role_dict["p_person_role"]:
            case "actor":
                movie["actors"] = {role_dict["p_person_name"]: role_dict}
            case "writer":
                movie["writers"] = {role_dict["p_person_name"]: role_dict}
            case "direcor":
                movie["directors"] = {role_dict["p_person_name"]: role_dict}
            case _:
                movie["other_roles"] = {role_dict["p_person_name"]: role_dict}

        # print(row_dict)
        # print(movie, genre_dict, role_dict, sep="\n", end="\n\n")
        # for key, value in movie.items():
        #     print(f"{key} :    {value}")

        return movie


################

# def transform_row(self, row):
#     row_dict = dict(Filmwork(**row))

#     movie, genre_dict, role_dict = {}, {}, {}
#     for key, value in row_dict.items():
#         if key.startswith("g_"):
#             genre_dict[key] = value
#         elif key.startswith("p_"):
#             role_dict[key] = value
#         else:
#             movie[key] = value
#     movie["genres"] = {genre_dict["g_genre"]: genre_dict}
#     # movie["genre_names"]
#     movie["actors"] = {}
#     movie["writers"] = {}
#     movie["directors"] = {}
#     movie["other_roles"] = {}
#     match role_dict["p_person_role"]:
#         case "actor":
#             movie["actors"] = {role_dict["p_person_name"]: role_dict}
#         case "writer":
#             movie["writers"] = {role_dict["p_person_name"]: role_dict}
#         case "direcor":
#             movie["directors"] = {role_dict["p_person_name"]: role_dict}
#         case _:
#             movie["other_roles"] = {role_dict["p_person_name"]: role_dict}

#     # print(row_dict)
#     # print(movie, genre_dict, role_dict, sep="\n", end="\n\n")
#     # for key, value in movie.items():
#     #     print(f"{key} :    {value}")

#     return movie
