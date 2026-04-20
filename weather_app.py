import json
import os
import sys
import requests
from dotenv import load_dotenv


load_dotenv()

_LAST_ERRORS: dict[str, str | None] = {
    "coordinates": None,
    "current_weather": None,
    "forecast": None,
    "air_pollution": None,
}


def _set_last_error(source: str, message: str | None) -> None:
    _LAST_ERRORS[source] = message


def get_last_error(source: str) -> str | None:
    return _LAST_ERRORS.get(source)


def _safe_request_json(url: str, params: dict, source: str) -> object | None:
    try:
        response = requests.get(url, params=params, timeout=10)
    except requests.RequestException:
        _set_last_error(source, "Сетевая ошибка при обращении к погодному сервису.")
        return None

    if response.status_code == 429:
        _set_last_error(source, "429: превышен лимит запросов к погодному сервису.")
        return None
    if 400 <= response.status_code < 500:
        _set_last_error(source, f"Ошибка запроса к погодному сервису ({response.status_code}).")
        return None
    if response.status_code >= 500:
        _set_last_error(source, f"Сервис погоды временно недоступен ({response.status_code}).")
        return None

    try:
        data = response.json()
    except ValueError:
        _set_last_error(source, "Пустой или некорректный ответ от погодного сервиса.")
        return None

    _set_last_error(source, None)
    return data


def get_coordinates(city: str, limit: int = 1) -> tuple[float, float] | None:
    api_key = os.getenv("API_KEY")
    if not api_key or not city.strip():
        _set_last_error("coordinates", "Не задан API-ключ или пустой город.")
        return None

    url = "http://api.openweathermap.org/geo/1.0/direct"
    params = {
        "q": city,
        "limit": limit,
        "appid": api_key,
        "lang": "ru",
    }

    data = _safe_request_json(url, params, "coordinates")
    if not isinstance(data, list):
        if get_last_error("coordinates") is None:
            _set_last_error("coordinates", "Пустой ответ геокодинга.")
        return None

    if not data:
        _set_last_error("coordinates", "Город не найден.")
        return None

    try:
        lat = float(data[0]["lat"])
        lon = float(data[0]["lon"])
    except (KeyError, TypeError, ValueError, IndexError):
        _set_last_error("coordinates", "В ответе геокодинга отсутствуют координаты.")
        return None

    _set_last_error("coordinates", None)
    return lat, lon


def get_current_weather(lat: float, lon: float) -> dict:
    api_key = os.getenv("API_KEY")
    if not api_key:
        _set_last_error("current_weather", "Не задан API-ключ.")
        return {}

    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "lat": lat,
        "lon": lon,
        "appid": api_key,
        "units": "metric",
        "lang": "ru",
    }

    data = _safe_request_json(url, params, "current_weather")
    if not isinstance(data, dict):
        if get_last_error("current_weather") is None:
            _set_last_error("current_weather", "Пустой ответ по текущей погоде.")
        return {}

    _set_last_error("current_weather", None)
    return data


def get_forecast_5d3h(lat: float, lon: float) -> list[dict]:
    api_key = os.getenv("API_KEY")
    if not api_key:
        _set_last_error("forecast", "Не задан API-ключ.")
        return []

    url = "https://api.openweathermap.org/data/2.5/forecast"
    params = {
        "lat": lat,
        "lon": lon,
        "appid": api_key,
        "units": "metric",
        "lang": "ru",
    }

    data = _safe_request_json(url, params, "forecast")
    if not isinstance(data, dict):
        if get_last_error("forecast") is None:
            _set_last_error("forecast", "Пустой ответ по прогнозу.")
        return []

    raw_list = data.get("list")
    if not isinstance(raw_list, list):
        _set_last_error("forecast", "В ответе прогноза нет списка.")
        return []

    _set_last_error("forecast", None)
    return [item for item in raw_list if isinstance(item, dict)]


def get_air_pollution(lat: float, lon: float) -> dict:
    api_key = os.getenv("API_KEY")
    if not api_key:
        _set_last_error("air_pollution", "Не задан API-ключ.")
        return {}

    url = "https://api.openweathermap.org/data/2.5/air_pollution"
    params = {
        "lat": lat,
        "lon": lon,
        "appid": api_key,
    }

    data = _safe_request_json(url, params, "air_pollution")
    if not isinstance(data, dict):
        if get_last_error("air_pollution") is None:
            _set_last_error("air_pollution", "Пустой ответ по качеству воздуха.")
        return {}

    raw_list = data.get("list")
    if not isinstance(raw_list, list) or not raw_list:
        _set_last_error("air_pollution", "В ответе качества воздуха нет данных.")
        return {}

    first = raw_list[0]
    if not isinstance(first, dict):
        _set_last_error("air_pollution", "Некорректный формат данных качества воздуха.")
        return {}

    components = first.get("components")
    if not isinstance(components, dict):
        _set_last_error("air_pollution", "В ответе отсутствуют компоненты загрязнений.")
        return {}

    _set_last_error("air_pollution", None)
    return components


_POLLUTION_THRESHOLDS: dict[str, tuple[float, float, float, float]] = {
    "so2": (20, 80, 250, 350),
    "no2": (40, 70, 150, 200),
    "pm10": (20, 50, 100, 200),
    "pm2_5": (10, 25, 50, 75),
    "o3": (60, 100, 140, 180),
    "co": (4400, 9400, 12400, 15400),
}

_AIR_QUALITY_LEVELS: dict[int, str] = {
    1: "В норме",
    2: "Умеренное загрязнение",
    3: "Сильное загрязнение",
    4: "Очень сильное загрязнение",
    5: "Критическое загрязнение",
}


def _safe_float(value: object) -> float | None:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    if v < 0:
        return 0.0
    return v


def _component_level(pollutant: str, value: float) -> int:
    u1, u2, u3, u4 = _POLLUTION_THRESHOLDS[pollutant]
    if value < u1:
        return 1
    if value < u2:
        return 2
    if value < u3:
        return 3
    if value < u4:
        return 4
    return 5


def analyze_air_pollution(components: dict, extended: bool = False) -> dict:
    """Сводка: худший уровень по шкале; каждый ключ — [концентрация, метка]; «3 worst_rated» — три худших по шкале."""
    if not isinstance(components, dict):
        return {}

    rated: list[tuple[str, float, int]] = []
    for key in _POLLUTION_THRESHOLDS:
        if key not in components:
            continue
        val = _safe_float(components[key])
        if val is None:
            continue
        rated.append((key, val, _component_level(key, val)))

    si = max((t[2] for t in rated), default=1)
    pollutant_order = {k: i for i, k in enumerate(_POLLUTION_THRESHOLDS)}
    top3 = sorted(rated, key=lambda t: (-t[2], pollutant_order[t[0]]))[:3]

    out: dict[str, object] = {"summary": _AIR_QUALITY_LEVELS[si]}

    for key in components:
        raw = components[key]
        val = _safe_float(raw)
        if val is None:
            continue
        if key in _POLLUTION_THRESHOLDS:
            li = _component_level(key, val)
            out[key] = [val, _AIR_QUALITY_LEVELS[li]]
        else:
            out[key] = [val, "не оценивается"]

    out["3 worst_rated"] = {
        k: [v, _AIR_QUALITY_LEVELS[i]] for k, v, i in top3
    }

    return out


def _print_analyze_air_pollution(result: dict) -> None:
    """Текстовый вывод в формате, ожидаемом для analyze_air_pollution."""
    out = sys.stdout
    if hasattr(out, "reconfigure"):
        try:
            out.reconfigure(encoding="utf-8", errors="replace")
        except (OSError, ValueError):
            pass

    def _j_str(s: str) -> str:
        return json.dumps(s, ensure_ascii=False)

    print("analyze_air_pollution:")
    summary = result.get("summary", "")
    print(f'  "summary": {_j_str(summary)}')
    print()
    worst = result.get("3 worst_rated")
    for key, val in result.items():
        if key in ("summary", "3 worst_rated"):
            continue
        if isinstance(val, list) and len(val) == 2:
            num, lab = val[0], val[1]
            lab_s = lab if isinstance(lab, str) else str(lab)
            print(f'  "{key}": {num}, {_j_str(lab_s)}')
    print()
    if isinstance(worst, dict):
        print('"3 worst_rated":')
        for key, val in worst.items():
            if isinstance(val, list) and len(val) == 2:
                num, lab = val[0], val[1]
                lab_s = lab if isinstance(lab, str) else str(lab)
                print(f'   "{key}": {num}, {_j_str(lab_s)}')


if __name__ == "__main__":
    test_city = "Moscow"
    coords = get_coordinates(test_city)
    print("get_coordinates:", coords)
    if coords:
        weather = get_current_weather(coords[0], coords[1])
        print(
            "get_current_weather:",
            json.dumps(weather, ensure_ascii=True, indent=2),
        )
        forecast = get_forecast_5d3h(coords[0], coords[1])
        print(
            "get_forecast_5d3h:",
            json.dumps(
                {"count": len(forecast), "sample": forecast[:1]},
                ensure_ascii=True,
                indent=2,
            ),
        )
        air = get_air_pollution(coords[0], coords[1])
        print(
            "get_air_pollution (components):",
            json.dumps(air, ensure_ascii=True, indent=2),
        )
        _print_analyze_air_pollution(analyze_air_pollution(air))
    else:
        print("get_current_weather:", {})
