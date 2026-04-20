import json
from pathlib import Path


_DATA_FILE = Path(__file__).with_name("User_Data.json")


def _read_all_users() -> dict:
    if not _DATA_FILE.exists():
        return {}
    try:
        data = json.loads(_DATA_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _write_all_users(data: dict) -> None:
    try:
        _DATA_FILE.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except OSError:
        # Ошибки записи не пробрасываем, чтобы не ломать основной поток бота.
        return


def load_user(user_id: int) -> dict:
    users = _read_all_users()
    raw = users.get(str(user_id))
    return raw if isinstance(raw, dict) else {}


def load_all_users() -> dict[str, dict]:
    users = _read_all_users()
    out: dict[str, dict] = {}
    for key, value in users.items():
        if isinstance(key, str) and isinstance(value, dict):
            out[key] = value
    return out


def save_user(user_id: int, data: dict) -> None:
    if not isinstance(data, dict):
        return
    users = _read_all_users()
    users[str(user_id)] = data
    _write_all_users(users)


def delete_user(user_id: int) -> None:
    users = _read_all_users()
    key = str(user_id)
    if key not in users:
        return
    users.pop(key, None)
    _write_all_users(users)
