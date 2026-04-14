import json
import os
import sys
import requests
from dotenv import load_dotenv


load_dotenv()


def get_coordinates(city: str, limit: int = 1) -> tuple[float, float] | None:
    api_key = os.getenv("API_KEY")
    if not api_key or not city.strip():
        return None

    url = "http://api.openweathermap.org/geo/1.0/direct"
    params = {
        "q": city,
        "limit": limit,
        "appid": api_key,
        "lang": "ru",
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
    except (requests.RequestException, ValueError):
        return None

    if not data:
        return None

    try:
        lat = float(data[0]["lat"])
        lon = float(data[0]["lon"])
    except (KeyError, TypeError, ValueError, IndexError):
        return None

    return lat, lon


def get_current_weather(lat: float, lon: float) -> dict:
    api_key = os.getenv("API_KEY")
    if not api_key:
        return {}

    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "lat": lat,
        "lon": lon,
        "appid": api_key,
        "units": "metric",
        "lang": "ru",
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
    except (requests.RequestException, ValueError):
        return {}

    if not isinstance(data, dict):
        return {}

    return data


def get_forecast_5d3h(lat: float, lon: float) -> list[dict]:
    api_key = os.getenv("API_KEY")
    if not api_key:
        return []

    url = "https://api.openweathermap.org/data/2.5/forecast"
    params = {
        "lat": lat,
        "lon": lon,
        "appid": api_key,
        "units": "metric",
        "lang": "ru",
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
    except (requests.RequestException, ValueError):
        return []

    if not isinstance(data, dict):
        return []

    raw_list = data.get("list")
    if not isinstance(raw_list, list):
        return []

    return [item for item in raw_list if isinstance(item, dict)]


def get_air_pollution(lat: float, lon: float) -> dict:
    api_key = os.getenv("API_KEY")
    if not api_key:
        return {}

    url = "https://api.openweathermap.org/data/2.5/air_pollution"
    params = {
        "lat": lat,
        "lon": lon,
        "appid": api_key,
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
    except (requests.RequestException, ValueError):
        return {}

    if not isinstance(data, dict):
        return {}

    raw_list = data.get("list")
    if not isinstance(raw_list, list) or not raw_list:
        return {}

    first = raw_list[0]
    if not isinstance(first, dict):
        return {}

    components = first.get("components")
    if not isinstance(components, dict):
        return {}

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
