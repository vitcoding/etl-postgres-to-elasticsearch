from typing import Any
from uuid import UUID

from dateutil.parser import parse


def validate_data(row: dict[str, Any]) -> dict[str, Any]:
    """функция валидации данных БД"""

    transformed_row = {}
    for key, value in dict(row).items():
        match key:
            case "id" | "fw_id" | "g_id" | "p_id":
                if isinstance(value, str):
                    value = UUID(value)
            case "creation_date":
                if isinstance(value, str):
                    value = parse(value)
            case (
                "created"
                | "modified"
                | "g_created"
                | "g_modified"
                | "p_created"
                | "p_modified"
            ):
                if isinstance(value, str):
                    value = parse(value)
            case _:
                key = key
                value = value

        transformed_row[key] = value
    return transformed_row


# def validate_data(row: dict[str, Any]) -> dict[str, Any]:
#     """функция валидации данных таблиц БД"""

#     transformed_row = {}
#     for key, value in dict(row).items():
#         match key:
#             case "id", "film_work_id", "g_id", "p_id":
#                 if isinstance(value, str):
#                     value = UUID(value)
#             case "creation_date":
#                 if isinstance(value, str):
#                     value = parse(value)
#             case "created_at":
#                 key = "created"
#                 if isinstance(value, str):
#                     value = parse(value)
#             case "updated_at":
#                 key = "modified"
#                 if isinstance(value, str):
#                     value = parse(value)
#             case _:
#                 key = key
#                 value = value

#         transformed_row[key] = value
#     return transformed_row
