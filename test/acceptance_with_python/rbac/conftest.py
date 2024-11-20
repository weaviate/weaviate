def _sanitize_role_name(name: str) -> str:
    return (
        name.replace("[", "")
        .replace("]", "")
        .replace("-", "")
        .replace(" ", "")
        .replace(".", "")
        .replace("{", "")
        .replace("}", "")
    )


def generate_missing_lists(permissions: list):
    result = []
    for i in range(len(permissions)):
        result.append(permissions[:i] + permissions[i + 1 :])
    return result
