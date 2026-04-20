import datetime as dt
import html
import os
import threading
import time
from collections import defaultdict

import telebot
from dotenv import load_dotenv
from storage import delete_user, load_all_users, load_user, save_user
from telebot import types

from weather_app import (
    analyze_air_pollution,
    get_air_pollution,
    get_coordinates,
    get_current_weather,
    get_forecast_5d3h,
    get_last_error,
)

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("Не найден BOT_TOKEN в .env")

bot = telebot.TeleBot(BOT_TOKEN)

# user_id -> {"mode": str, ...}
user_states: dict[int, dict] = {}

# user_id -> {"lat": float, "lon": float, "label": str}
saved_locations: dict[int, dict] = {}

# user_id -> {"chat_id": int, "enabled": bool, "lat": float, "lon": float, ...}
subscriptions: dict[int, dict] = {}

RATE_LIMIT_RETRY_DELAYS = (1, 2, 4)
NOTIFICATION_INTERVAL_HOURS = 2
TEST_NOTIFICATION_INTERVAL_SECONDS = 5 * 60
is_test_interval_mode = False
notifications_wakeup_event = threading.Event()


def _is_rate_limit_error(error: str | None) -> bool:
    return bool(error and "429" in error)


def _retry_on_rate_limit(fetch_fn, error_source: str):
    result = fetch_fn()
    error = get_last_error(error_source)
    if not _is_rate_limit_error(error):
        return result

    for delay in RATE_LIMIT_RETRY_DELAYS:
        time.sleep(delay)
        result = fetch_fn()
        error = get_last_error(error_source)
        if not _is_rate_limit_error(error):
            break
    return result


def _save_notification_user_data(user_id: int, city: str, lat: float, lon: float, enabled: bool) -> None:
    interval_h = TEST_NOTIFICATION_INTERVAL_SECONDS / 3600 if is_test_interval_mode else NOTIFICATION_INTERVAL_HOURS
    save_user(
        user_id,
        {
            "city": city,
            "lat": lat,
            "lon": lon,
            "notifications": {
                "enabled": enabled,
                "interval_h": round(interval_h, 4),
            },
        },
    )


def _load_notification_user_data(user_id: int, chat_id: int) -> None:
    data = load_user(user_id)
    if not isinstance(data, dict):
        return

    lat = data.get("lat")
    lon = data.get("lon")
    city = data.get("city")
    notifications = data.get("notifications")
    if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
        return

    label = str(city) if isinstance(city, str) and city.strip() else "вашей геолокации"
    saved_locations[user_id] = {"lat": float(lat), "lon": float(lon), "label": label}

    enabled = False
    if isinstance(notifications, dict):
        enabled = bool(notifications.get("enabled", False))

    sub = subscriptions.setdefault(user_id, {"chat_id": chat_id})
    sub.update(
        {
            "chat_id": chat_id,
            "enabled": enabled,
            "lat": float(lat),
            "lon": float(lon),
            "label": label,
            "interval_h": NOTIFICATION_INTERVAL_HOURS,
        }
    )


def _current_notification_interval_seconds() -> int:
    return TEST_NOTIFICATION_INTERVAL_SECONDS if is_test_interval_mode else NOTIFICATION_INTERVAL_HOURS * 3600


def _notification_interval_label() -> str:
    return "каждые 5 минут" if is_test_interval_mode else "каждые 2 часа"


def _bootstrap_subscriptions_from_storage() -> None:
    all_users = load_all_users()
    for user_id_str, data in all_users.items():
        try:
            user_id = int(user_id_str)
        except ValueError:
            continue

        lat = data.get("lat")
        lon = data.get("lon")
        city = data.get("city")
        notifications = data.get("notifications")
        if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
            continue
        if not isinstance(notifications, dict) or not bool(notifications.get("enabled")):
            continue

        label = str(city) if isinstance(city, str) and city.strip() else "вашей геолокации"
        saved_locations[user_id] = {"lat": float(lat), "lon": float(lon), "label": label}
        subscriptions[user_id] = {
            "chat_id": user_id,
            "enabled": True,
            "lat": float(lat),
            "lon": float(lon),
            "label": label,
            "interval_h": _current_notification_interval_seconds() / 3600,
        }


def main_menu() -> types.ReplyKeyboardMarkup:
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.add(
        types.KeyboardButton("Погода по городу"),
        types.KeyboardButton("Прогноз 5 дней (моё местоположение)"),
    )
    kb.add(
        types.KeyboardButton("Погода по геолокации"),
        types.KeyboardButton("Погодные уведомления"),
    )
    kb.add(
        types.KeyboardButton("Сравнить города"),
        types.KeyboardButton("Расширенные данные"),
    )
    return kb


def location_keyboard(text: str = "Отправить местоположение") -> types.ReplyKeyboardMarkup:
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.add(types.KeyboardButton(text, request_location=True))
    kb.add(types.KeyboardButton("Отмена"))
    return kb


def notifications_keyboard() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("Да", callback_data="notify|on"),
        types.InlineKeyboardButton("Нет", callback_data="notify|off"),
    )
    return kb


def format_timestamp(ts: int | None, timezone_shift: int = 0) -> str:
    if not ts:
        return "н/д"
    return dt.datetime.fromtimestamp(ts + timezone_shift, dt.UTC).strftime("%H:%M")


def get_weather_main_desc(weather: dict) -> tuple[str, str]:
    weather_block = weather.get("weather", [])
    if isinstance(weather_block, list) and weather_block:
        first = weather_block[0]
        if isinstance(first, dict):
            return str(first.get("main", "—")), str(first.get("description", "—")).capitalize()
    return "—", "—"


def format_current_weather(weather: dict, title: str | None = None) -> str:
    if not weather:
        return "Не удалось получить данные о погоде."

    city = weather.get("name", "Неизвестная точка")
    main = weather.get("main", {}) if isinstance(weather.get("main"), dict) else {}
    wind = weather.get("wind", {}) if isinstance(weather.get("wind"), dict) else {}
    clouds = weather.get("clouds", {}) if isinstance(weather.get("clouds"), dict) else {}
    weather_main, weather_desc = get_weather_main_desc(weather)

    prefix = f"📍 {title}\n" if title else f"📍 {city}\n"
    return (
        f"{prefix}"
        f"Состояние: {weather_desc} ({weather_main})\n"
        f"🌡 Температура: {main.get('temp', 'н/д')}°C (ощущается как {main.get('feels_like', 'н/д')}°C)\n"
        f"💧 Влажность: {main.get('humidity', 'н/д')}%\n"
        f"🧭 Давление: {main.get('pressure', 'н/д')} гПа\n"
        f"🌬 Ветер: {wind.get('speed', 'н/д')} м/с\n"
        f"☁ Облачность: {clouds.get('all', 'н/д')}%\n"
        f"👁 Видимость: {weather.get('visibility', 'н/д')} м"
    )


def build_daily_forecast(forecast: list[dict]) -> list[dict]:
    by_day: dict[str, list[dict]] = defaultdict(list)
    for item in forecast:
        dt_txt = item.get("dt_txt")
        if not isinstance(dt_txt, str) or len(dt_txt) < 10:
            continue
        by_day[dt_txt[:10]].append(item)

    daily: list[dict] = []
    for day, entries in sorted(by_day.items())[:5]:
        temps = []
        humidities = []
        winds = []
        pops = []
        descriptions = []
        midday_entry = entries[len(entries) // 2]

        for e in entries:
            main = e.get("main", {}) if isinstance(e.get("main"), dict) else {}
            wind = e.get("wind", {}) if isinstance(e.get("wind"), dict) else {}
            w_list = e.get("weather", [])

            t = main.get("temp")
            if isinstance(t, (int, float)):
                temps.append(t)
            h = main.get("humidity")
            if isinstance(h, (int, float)):
                humidities.append(h)
            ws = wind.get("speed")
            if isinstance(ws, (int, float)):
                winds.append(ws)
            p = e.get("pop")
            if isinstance(p, (int, float)):
                pops.append(p)

            if isinstance(w_list, list) and w_list and isinstance(w_list[0], dict):
                descriptions.append(str(w_list[0].get("description", "—")).capitalize())

            dt_txt = e.get("dt_txt")
            if isinstance(dt_txt, str) and "12:00:00" in dt_txt:
                midday_entry = e

        daily.append(
            {
                "date": day,
                "entries": entries,
                "midday": midday_entry,
                "temp_min": round(min(temps), 1) if temps else "н/д",
                "temp_max": round(max(temps), 1) if temps else "н/д",
                "humidity_avg": round(sum(humidities) / len(humidities), 1) if humidities else "н/д",
                "wind_max": round(max(winds), 1) if winds else "н/д",
                "pop_max": round(max(pops) * 100) if pops else 0,
                "description": descriptions[0] if descriptions else "—",
            }
        )
    return daily


def forecast_keyboard(days: list[dict]) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=1)
    for idx, day in enumerate(days):
        date_text = day["date"]
        label = (
            f"{date_text} | {day['temp_min']}..{day['temp_max']}°C | "
            f"{day['description']}"
        )
        kb.add(types.InlineKeyboardButton(label[:64], callback_data=f"fday|{idx}"))
    kb.add(types.InlineKeyboardButton("Закрыть", callback_data="fclose"))
    return kb


def forecast_detail_keyboard() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(types.InlineKeyboardButton("⬅ Назад к 5 дням", callback_data="fback"))
    kb.add(types.InlineKeyboardButton("Закрыть", callback_data="fclose"))
    return kb


def notifications_worker() -> None:
    while True:
        for user_id, sub in list(subscriptions.items()):
            if not sub.get("enabled"):
                continue

            lat = sub.get("lat")
            lon = sub.get("lon")
            chat_id = sub.get("chat_id")
            if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)) or not chat_id:
                continue

            current = _retry_on_rate_limit(lambda: get_current_weather(lat, lon), "current_weather")
            if not current:
                continue

            weather_main, weather_desc = get_weather_main_desc(current)
            current_temp = None
            main = current.get("main", {})
            if isinstance(main, dict):
                t = main.get("temp")
                if isinstance(t, (int, float)):
                    current_temp = float(t)

            alerts: list[str] = []

            prev_cond = sub.get("last_condition")
            prev_temp = sub.get("last_temp")
            if prev_cond and weather_main != prev_cond:
                alerts.append(f"Смена погоды: было {prev_cond}, стало {weather_main} ({weather_desc}).")
            if isinstance(prev_temp, (int, float)) and isinstance(current_temp, (int, float)):
                if abs(current_temp - prev_temp) >= 3:
                    alerts.append(f"Температура изменилась: {prev_temp:.1f}°C → {current_temp:.1f}°C.")

            forecast = _retry_on_rate_limit(lambda: get_forecast_5d3h(lat, lon), "forecast")
            tomorrow = (dt.date.today() + dt.timedelta(days=1)).isoformat()
            tomorrow_rain = False
            for item in forecast:
                dt_txt = item.get("dt_txt")
                if not isinstance(dt_txt, str) or not dt_txt.startswith(tomorrow):
                    continue
                weather_list = item.get("weather", [])
                if (
                    isinstance(weather_list, list)
                    and weather_list
                    and isinstance(weather_list[0], dict)
                    and str(weather_list[0].get("main", "")).lower() in {"rain", "drizzle", "thunderstorm"}
                ):
                    tomorrow_rain = True
                    break

            if tomorrow_rain and sub.get("last_rain_alert_day") != tomorrow:
                alerts.append("Завтра ожидаются осадки. Возьмите зонт ☔")
                sub["last_rain_alert_day"] = tomorrow

            sub["last_condition"] = weather_main
            if isinstance(current_temp, float):
                sub["last_temp"] = current_temp

            if is_test_interval_mode and not alerts:
                alerts.append(
                    f"Тестовый интервал: проверка выполнена. Сейчас {weather_desc.lower()}."
                )

            if alerts:
                label = sub.get("label", "вашей точке")
                msg = f"🔔 Погодное уведомление для {label}:\n" + "\n".join(f"• {a}" for a in alerts)
                try:
                    bot.send_message(chat_id, msg)
                except Exception:
                    pass

        notifications_wakeup_event.wait(timeout=_current_notification_interval_seconds())
        notifications_wakeup_event.clear()


def get_saved_location_or_none(user_id: int) -> dict | None:
    value = saved_locations.get(user_id)
    if not value:
        return None
    lat = value.get("lat")
    lon = value.get("lon")
    if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
        return None
    return value


@bot.message_handler(commands=["start", "help"])
def start_handler(message: types.Message) -> None:
    _load_notification_user_data(message.from_user.id, message.chat.id)
    bot.send_message(
        message.chat.id,
        (
            "Привет! Я погодный бот.\n\n"
            "Доступные функции:\n"
            "1) Погода по городу\n"
            "2) Прогноз 5 дней по сохраненной геолокации\n"
            "3) Погода по геолокации\n"
            "4) Погодные уведомления (каждые 2 часа)\n"
            "5) Сравнение двух городов\n"
            "6) Расширенные данные (город или гео)\n\n"
            "Выберите действие кнопками ниже."
        ),
        reply_markup=main_menu(),
    )


@bot.message_handler(content_types=["location"])
def location_handler(message: types.Message) -> None:
    user_id = message.from_user.id
    state = user_states.get(user_id, {})
    if not message.location:
        bot.send_message(
            message.chat.id,
            "Пожалуйста, отправьте геолокацию через кнопку location.",
            reply_markup=location_keyboard(),
        )
        return

    lat = float(message.location.latitude)
    lon = float(message.location.longitude)

    saved_locations[user_id] = {"lat": lat, "lon": lon, "label": "вашей геолокации"}
    current = _retry_on_rate_limit(lambda: get_current_weather(lat, lon), "current_weather")

    mode = state.get("mode")
    if mode in {"await_location_weather", "await_location_save_for_forecast"}:
        bot.send_message(
            message.chat.id,
            "Локация сохранена. Текущая погода:\n\n" + format_current_weather(current, "вашей геолокации"),
            reply_markup=main_menu(),
        )
        if mode == "await_location_save_for_forecast":
            forecast = _retry_on_rate_limit(lambda: get_forecast_5d3h(lat, lon), "forecast")
            daily = build_daily_forecast(forecast)
            if not daily:
                bot.send_message(message.chat.id, "Не удалось загрузить прогноз на 5 дней.")
            else:
                user_states[user_id] = {"mode": "view_forecast", "forecast_days": daily, "label": "вашей геолокации"}
                bot.send_message(
                    message.chat.id,
                    "📅 Прогноз на 5 дней. Нажмите на день для деталей:",
                    reply_markup=forecast_keyboard(daily),
                )
        user_states.pop(user_id, None)
        return

    if mode == "await_notification_source":
        sub = subscriptions.setdefault(user_id, {"chat_id": message.chat.id})
        sub.update(
            {
                "chat_id": message.chat.id,
                "enabled": True,
                "lat": lat,
                "lon": lon,
                "label": "вашей геолокации",
                "interval_h": _current_notification_interval_seconds() / 3600,
            }
        )
        _save_notification_user_data(user_id, "вашей геолокации", lat, lon, True)
        bot.send_message(
            message.chat.id,
            f"Подписка на уведомления включена. Буду проверять погоду {_notification_interval_label()}.",
            reply_markup=main_menu(),
        )
        user_states.pop(user_id, None)
        return

    if mode == "await_extended_input":
        send_extended_weather(message.chat.id, user_id, lat, lon, "вашей геолокации")
        user_states.pop(user_id, None)
        return

    # Универсальная реакция на произвольную отправку геолокации.
    bot.send_message(
        message.chat.id,
        "Локация сохранена. Текущая погода:\n\n" + format_current_weather(current, "вашей геолокации"),
        reply_markup=main_menu(),
    )


def send_extended_weather(chat_id: int, user_id: int, lat: float, lon: float, label: str) -> None:
    current = _retry_on_rate_limit(lambda: get_current_weather(lat, lon), "current_weather")
    if not current:
        bot.send_message(chat_id, "Не удалось получить расширенные данные по погоде.", reply_markup=main_menu())
        return

    saved_locations[user_id] = {"lat": lat, "lon": lon, "label": label}

    main = current.get("main", {}) if isinstance(current.get("main"), dict) else {}
    sys_block = current.get("sys", {}) if isinstance(current.get("sys"), dict) else {}
    clouds = current.get("clouds", {}) if isinstance(current.get("clouds"), dict) else {}
    weather_main, weather_desc = get_weather_main_desc(current)
    tz_shift = int(current.get("timezone", 0)) if isinstance(current.get("timezone", 0), int) else 0

    air_components = _retry_on_rate_limit(lambda: get_air_pollution(lat, lon), "air_pollution")
    air = analyze_air_pollution(air_components)
    air_summary = air.get("summary", "н/д") if isinstance(air, dict) else "н/д"
    worst = air.get("3 worst_rated", {}) if isinstance(air, dict) and isinstance(air.get("3 worst_rated"), dict) else {}
    worst_txt = ", ".join(
        f"{k.upper()}: {v[0]} ({v[1]})"
        for k, v in worst.items()
        if isinstance(v, list) and len(v) == 2
    )
    if not worst_txt:
        worst_txt = "н/д"

    uv_index = current.get("uvi", "н/д")
    text = (
        f"🧩 Расширенные данные для {label}\n"
        f"Состояние: {weather_desc} ({weather_main})\n"
        f"🌡 Температура: {main.get('temp', 'н/д')}°C\n"
        f"🤒 Ощущается как: {main.get('feels_like', 'н/д')}°C\n"
        f"💧 Влажность: {main.get('humidity', 'н/д')}%\n"
        f"🧭 Давление: {main.get('pressure', 'н/д')} гПа\n"
        f"☁ Облачность: {clouds.get('all', 'н/д')}%\n"
        f"🌬 Ветер: {(current.get('wind') or {}).get('speed', 'н/д')} м/с\n"
        f"🌅 Восход: {format_timestamp(sys_block.get('sunrise'), tz_shift)}\n"
        f"🌇 Закат: {format_timestamp(sys_block.get('sunset'), tz_shift)}\n"
        f"🔆 УФ-индекс: {uv_index}\n"
        f"🌫 Качество воздуха: {air_summary}\n"
        f"⚗ Худшие загрязнители: {worst_txt}"
    )
    bot.send_message(chat_id, text, reply_markup=main_menu())


@bot.callback_query_handler(func=lambda call: True)
def callback_handler(call: types.CallbackQuery) -> None:
    user_id = call.from_user.id
    data = call.data or ""

    if data.startswith("notify|"):
        action = data.split("|", 1)[1]
        if action == "on":
            _load_notification_user_data(user_id, call.message.chat.id)
            loc = get_saved_location_or_none(user_id)
            if loc:
                sub = subscriptions.setdefault(user_id, {"chat_id": call.message.chat.id})
                sub.update(
                    {
                        "chat_id": call.message.chat.id,
                        "enabled": True,
                        "lat": loc["lat"],
                        "lon": loc["lon"],
                        "label": str(loc.get("label", "вашей точке")),
                        "interval_h": _current_notification_interval_seconds() / 3600,
                    }
                )
                _save_notification_user_data(
                    user_id,
                    str(loc.get("label", "вашей точке")),
                    float(loc["lat"]),
                    float(loc["lon"]),
                    True,
                )
                bot.edit_message_text(
                    f"✅ Уведомления включены. Проверка погоды {_notification_interval_label()}.",
                    call.message.chat.id,
                    call.message.message_id,
                )
            else:
                user_states[user_id] = {"mode": "await_notification_source"}
                bot.edit_message_text(
                    "Чтобы включить уведомления, отправьте геолокацию или напишите город следующим сообщением.",
                    call.message.chat.id,
                    call.message.message_id,
                )
        else:
            sub = subscriptions.setdefault(user_id, {"chat_id": call.message.chat.id})
            sub["enabled"] = False
            delete_user(user_id)
            bot.edit_message_text(
                "❎ Уведомления отключены. Данные подписки удалены.",
                call.message.chat.id,
                call.message.message_id,
            )
        bot.answer_callback_query(call.id)
        return

    state = user_states.get(user_id, {})
    if data.startswith("fday|"):
        idx_str = data.split("|", 1)[1]
        days = state.get("forecast_days")
        if not isinstance(days, list):
            bot.answer_callback_query(call.id, "Сессия прогноза устарела.")
            return
        try:
            idx = int(idx_str)
            day = days[idx]
        except (ValueError, IndexError, TypeError):
            bot.answer_callback_query(call.id, "День не найден.")
            return

        midday = day.get("midday", {})
        w_main, w_desc = get_weather_main_desc(midday if isinstance(midday, dict) else {})
        detail = (
            f"📅 {day.get('date')}\n"
            f"🌡 Температура: {day.get('temp_min')}..{day.get('temp_max')}°C\n"
            f"💧 Средняя влажность: {day.get('humidity_avg')}%\n"
            f"🌬 Макс. ветер: {day.get('wind_max')} м/с\n"
            f"☔ Вероятность осадков: {day.get('pop_max')}%\n"
            f"🌥 Состояние (днем): {w_desc} ({w_main})"
        )
        bot.edit_message_text(
            detail,
            call.message.chat.id,
            call.message.message_id,
            reply_markup=forecast_detail_keyboard(),
        )
        bot.answer_callback_query(call.id)
        return

    if data == "fback":
        days = state.get("forecast_days")
        if not isinstance(days, list):
            bot.answer_callback_query(call.id, "Сессия прогноза устарела.")
            return
        bot.edit_message_text(
            "📅 Прогноз на 5 дней. Нажмите на день для деталей:",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=forecast_keyboard(days),
        )
        bot.answer_callback_query(call.id)
        return

    if data == "fclose":
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception:
            pass
        bot.answer_callback_query(call.id)
        return


@bot.message_handler(content_types=["text"])
def text_handler(message: types.Message) -> None:
    global is_test_interval_mode
    text = (message.text or "").strip()
    user_id = message.from_user.id
    state = user_states.get(user_id, {})
    mode = state.get("mode")

    if text == "//test_int":
        is_test_interval_mode = not is_test_interval_mode
        notifications_wakeup_event.set()
        bot.send_message(
            message.chat.id,
            f"Сервисный режим интервала: проверка погоды {_notification_interval_label()}.",
            reply_markup=main_menu(),
        )
        return

    if text == "Отмена":
        user_states.pop(user_id, None)
        bot.send_message(message.chat.id, "Действие отменено.", reply_markup=main_menu())
        return

    if text == "Погода по городу":
        user_states[user_id] = {"mode": "await_city_weather"}
        bot.send_message(message.chat.id, "Введите название города:")
        return

    if text == "Прогноз 5 дней (моё местоположение)":
        loc = get_saved_location_or_none(user_id)
        if not loc:
            user_states[user_id] = {"mode": "await_location_save_for_forecast"}
            bot.send_message(
                message.chat.id,
                "Сначала отправьте вашу геолокацию, чтобы сохранить ее для прогноза.",
                reply_markup=location_keyboard(),
            )
            return

        forecast = get_forecast_5d3h(loc["lat"], loc["lon"])
        daily = build_daily_forecast(forecast)
        if not daily:
            bot.send_message(message.chat.id, "Не удалось получить прогноз на 5 дней.", reply_markup=main_menu())
            return
        user_states[user_id] = {"mode": "view_forecast", "forecast_days": daily, "label": str(loc.get("label", "вашей геолокации"))}
        bot.send_message(
            message.chat.id,
            f"📅 Прогноз на 5 дней для {loc.get('label', 'вашей геолокации')}. Нажмите на день:",
            reply_markup=forecast_keyboard(daily),
        )
        return

    if text == "Погода по геолокации":
        user_states[user_id] = {"mode": "await_location_weather"}
        bot.send_message(
            message.chat.id,
            "Отправьте геолокацию кнопкой ниже.",
            reply_markup=location_keyboard(),
        )
        return

    if text == "Погодные уведомления":
        user_states.pop(user_id, None)
        bot.send_message(
            message.chat.id,
            f"Включить уведомления о погоде? (проверка {_notification_interval_label()})",
            reply_markup=notifications_keyboard(),
        )
        return

    if text == "Сравнить города":
        user_states[user_id] = {"mode": "await_compare_city1"}
        bot.send_message(message.chat.id, "Введите первый город:")
        return

    if text == "Расширенные данные":
        user_states[user_id] = {"mode": "await_extended_input"}
        bot.send_message(
            message.chat.id,
            "Введите название города или отправьте геолокацию.",
            reply_markup=location_keyboard("Отправить геолокацию для расширенных данных"),
        )
        return

    if mode == "await_city_weather":
        coords = _retry_on_rate_limit(lambda: get_coordinates(text), "coordinates")
        if not coords:
            bot.send_message(message.chat.id, "Город не найден.")
            return
        lat, lon = coords
        weather = _retry_on_rate_limit(lambda: get_current_weather(lat, lon), "current_weather")
        saved_locations[user_id] = {"lat": lat, "lon": lon, "label": text}
        bot.send_message(message.chat.id, format_current_weather(weather, text), reply_markup=main_menu())
        user_states.pop(user_id, None)
        return

    if mode in {"await_location_weather", "await_location_save_for_forecast"}:
        bot.send_message(
            message.chat.id,
            "Пожалуйста, отправьте геолокацию через кнопку location.",
            reply_markup=location_keyboard(),
        )
        return

    if mode == "await_notification_source":
        coords = _retry_on_rate_limit(lambda: get_coordinates(text), "coordinates")
        if not coords:
            bot.send_message(
                message.chat.id,
                "Не удалось определить город. Отправьте другой город или геолокацию.",
            )
            return
        lat, lon = coords
        sub = subscriptions.setdefault(user_id, {"chat_id": message.chat.id})
        sub.update(
            {
                "chat_id": message.chat.id,
                "enabled": True,
                "lat": lat,
                "lon": lon,
                "label": text,
                "interval_h": _current_notification_interval_seconds() / 3600,
            }
        )
        _save_notification_user_data(user_id, text, lat, lon, True)
        saved_locations[user_id] = {"lat": lat, "lon": lon, "label": text}
        bot.send_message(
            message.chat.id,
            f"Подписка включена для {text}. Проверка погоды {_notification_interval_label()}.",
            reply_markup=main_menu(),
        )
        user_states.pop(user_id, None)
        return

    if mode == "await_compare_city1":
        user_states[user_id] = {"mode": "await_compare_city2", "city1": text}
        bot.send_message(message.chat.id, "Введите второй город:")
        return

    if mode == "await_compare_city2":
        city1 = state.get("city1")
        city2 = text
        if not isinstance(city1, str):
            user_states.pop(user_id, None)
            bot.send_message(message.chat.id, "Сессия сравнения истекла. Попробуйте снова.", reply_markup=main_menu())
            return

        coords1 = _retry_on_rate_limit(lambda: get_coordinates(city1), "coordinates")
        coords2 = _retry_on_rate_limit(lambda: get_coordinates(city2), "coordinates")
        if not coords1 or not coords2:
            bot.send_message(message.chat.id, "Один из городов не найден. Попробуйте заново.", reply_markup=main_menu())
            user_states.pop(user_id, None)
            return

        w1 = _retry_on_rate_limit(lambda: get_current_weather(coords1[0], coords1[1]), "current_weather")
        w2 = _retry_on_rate_limit(lambda: get_current_weather(coords2[0], coords2[1]), "current_weather")
        m1 = w1.get("main", {}) if isinstance(w1.get("main"), dict) else {}
        m2 = w2.get("main", {}) if isinstance(w2.get("main"), dict) else {}
        d1 = get_weather_main_desc(w1)[1]
        d2 = get_weather_main_desc(w2)[1]

        rows = [
            ("Параметр", city1, city2),
            ("Темп.", f"{m1.get('temp', 'н/д')}°C", f"{m2.get('temp', 'н/д')}°C"),
            ("Ощущ.", f"{m1.get('feels_like', 'н/д')}°C", f"{m2.get('feels_like', 'н/д')}°C"),
            ("Влажн.", f"{m1.get('humidity', 'н/д')}%", f"{m2.get('humidity', 'н/д')}%"),
            ("Ветер", f"{(w1.get('wind') or {}).get('speed', 'н/д')} м/с", f"{(w2.get('wind') or {}).get('speed', 'н/д')} м/с"),
            ("Сост.", d1, d2),
        ]
        col1 = max(len(str(r[0])) for r in rows)
        col2 = max(len(str(r[1])) for r in rows)
        col3 = max(len(str(r[2])) for r in rows)

        table_lines = [
            f"{str(a):<{col1}} | {str(b):<{col2}} | {str(c):<{col3}}"
            for a, b, c in rows
        ]
        table = "\n".join(table_lines)
        text_out = f"📊 Сравнение городов\n<pre>{html.escape(table)}</pre>"
        bot.send_message(message.chat.id, text_out, parse_mode="HTML", reply_markup=main_menu())
        user_states.pop(user_id, None)
        return

    if mode == "await_extended_input":
        coords = _retry_on_rate_limit(lambda: get_coordinates(text), "coordinates")
        if not coords:
            bot.send_message(
                message.chat.id,
                "Город не найден. Введите другой город или отправьте геолокацию.",
            )
            return
        send_extended_weather(message.chat.id, user_id, coords[0], coords[1], text)
        user_states.pop(user_id, None)
        return

    bot.send_message(
        message.chat.id,
        "Выберите действие из меню или введите /start.",
        reply_markup=main_menu(),
    )


if __name__ == "__main__":
    _bootstrap_subscriptions_from_storage()
    threading.Thread(target=notifications_worker, daemon=True).start()
    while True:
        try:
            bot.infinity_polling(skip_pending=True)
        except Exception:
            time.sleep(5)
