import asyncio
import logging
import os
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from aiogram.filters import CommandStart
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext

import gspread
from google.oauth2.service_account import Credentials

from collections import defaultdict
import time

last_processed = defaultdict(lambda: (0, 0))
DEDUPE_TIMEOUT = 3 

# =============================
# НАСТРОЙКИ
# =============================

BOT_TOKEN = os.getenv("BOT_TOKEN", "8419718446:AAF0H8mgNFqFaZBSkB8-1GocarvnJcwUN7A")
STEPS_SHEET_ID = os.getenv("STEPS_SHEET_ID", "1OmGMsGHTv_iKxDHGVFN8grO_8MVSQfpRxiog5IJRrNg")
DATA_SHEET_ID = os.getenv("DATA_SHEET_ID", "1yFKgM6YyGXFBk4EUheVLgGPmFd0wPH469T-BwPTcUI8")

IGNORE_COLUMNS = {
    "СФ Выставлена",
    "Кол-во рейсов",
    "Бонус",
    "Комментарий",
    "Месяц операции",
    "Год операции",
    "1-да",
    "",
}

logging.basicConfig(level=logging.INFO)


# =============================
# GOOGLE SHEETS
# =============================

class SheetsManager:
    def __init__(
        self,
        credentials_json: str,
        steps_sheet_id: str,
        data_sheet_id: str,
        ignore_columns: set[str] | None = None
    ):
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]

        from google.oauth2.service_account import Credentials
        import json

        creds_dict = json.loads(credentials_json)
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)

        self.client = gspread.authorize(creds)
        self.steps_sheet_id = steps_sheet_id
        self.data_sheet_id = data_sheet_id
        self.ignore_columns = ignore_columns or set()

    def get_reference_data(self):
        sheet = self.client.open_by_key(self.steps_sheet_id).sheet1
        all_values = sheet.get_all_values()

        if not all_values:
            return {}

        headers = all_values[0]
        ref_data = {}

        for col_index, key in enumerate(headers):
            if not key.strip():
                continue
            values = []
            for row in all_values[1:]:
                if col_index < len(row):
                    cell = row[col_index].strip()
                    if cell:
                        values.append(cell)
            ref_data[key.strip()] = values

        return ref_data

    def save_data(self, answers: dict):
        format_work = answers.get("Формат работы", "")
        
        if "продажа" in format_work.lower():
            self._save_pair_operation(answers)
        else:
            self._save_single_operation(answers)

    def _save_single_operation(self, answers: dict):
        sheet = self.client.open_by_key(self.data_sheet_id).sheet1
        headers = sheet.row_values(1)

        format_work = answers.get("Формат работы", "")
        is_hourly = "час" in format_work.lower()
        is_trip = "рейс" in format_work.lower()

        material_value = "Услуги перевозки (час)" if is_hourly else "Услуги перевозки"

        hours_value = answers.get("Часы", "") if is_hourly else ""
        weight_value = ""
        unit = answers.get("Единица измерения")

        if not is_hourly and not is_trip:
            if unit == "тонна":
                weight_value = answers.get("Вес (в тоннах)", "")
            elif unit == "м³":
                weight_value = answers.get("Вес (в кубах)", "")

        record = {
            "Дата операции": answers.get("Дата", ""),
            "Статья ДДС": "Услуги перевозки (час)" if is_hourly else "Услуги перевозки",
            "Авто": answers.get("Авто", ""),
            "Кол-во рейсов": "" if is_hourly else answers.get("Кол-во рейсов", ""),
            "Часы": hours_value,
            "Контрагент": answers.get("Контрагент", ""),
            "Материал": material_value,
            "Объект": answers.get("Объект", ""),
            "Комментарий": answers.get("Комментарий", ""),
            "Цена": answers.get("Цена", ""),
            "Вес (тонна)": weight_value if unit == "тонна" else "",
            "Вес (куб)": hours_value if is_hourly else (weight_value if unit == "м³" else ""),
            "Тип операции": "Поступления",
            "Месяц операции": "",
            "Год операции": ""
        }

        row = [record.get(h, "") for h in headers]
        self._append_row_safe(sheet, row)

    def _save_pair_operation(self, answers: dict):
        sheet = self.client.open_by_key(self.data_sheet_id).sheet1
        headers = sheet.row_values(1)

        # === Запись 1: Закуп материала ===
        purchase = {
            "Дата операции": answers.get("Дата", ""),
            "Статья ДДС": "Закуп материала",
            "Авто": answers.get("Авто", ""),
            "Кол-во рейсов": answers.get("Кол-во рейсов", ""),
            "Контрагент": answers.get("Контрагент закуп", ""),
            "Материал": answers.get("Материал", ""),
            "Комментарий": answers.get("Комментарий", ""),
            "Цена": answers.get("Цена", ""),
            "Часы": answers.get("Часы", ""),
            "Вес (тонна)": answers.get("Вес (закуп)", "") if answers.get("Единица измерения (закуп)") == "тонна" else "",
            "Вес (куб)": answers.get("Вес (закуп)", "") if answers.get("Единица измерения (закуп)") == "м³" else "",
            "Тип операции": "Расходы",
            "Месяц операции": "",
            "Год операции": ""
        }

        # === Запись 2: Продажа материала ===
        sale = {
            "Дата операции": answers.get("Дата", ""),
            "Статья ДДС": "Продажа материала",
            "Авто": answers.get("Авто", ""),
            "Кол-во рейсов": answers.get("Кол-во рейсов", ""),
            "Контрагент": answers.get("Контрагент продаж", ""),
            "Материал": answers.get("Материал", ""),
            "Объект": answers.get("Объект продаж", ""),
            "Комментарий": answers.get("Комментарий", ""),
            "Цена": answers.get("Цена", ""),
            "Часы": answers.get("Часы", ""),
            "Вес (тонна)": answers.get("Вес (продажа)", "") if answers.get("Единица измерения (продажа)") == "тонна" else "",
            "Вес (куб)": answers.get("Вес (продажа)", "") if answers.get("Единица измерения (продажа)") == "м³" else "",
            "Тип операции": "Поступления",
            "Месяц операции": "",
            "Год операции": ""
        }

        self._append_row_safe(sheet, [purchase.get(h, "") for h in headers])
        self._append_row_safe(sheet, [sale.get(h, "") for h in headers])

    def _get_value_for_header(self, header: str, answers: dict) -> str:
        if header == "Статья ДДС":
            fmt = answers.get("Формат работы", "")
            if "час" in fmt.lower():
                return "Услуги перевозки (час)"
            elif "перевозка" in fmt.lower():
                return "Услуги перевозки"
            else:
                return fmt

        mapping = {
            "Дата операции": "Дата",
            "Авто": "Авто",
            "Кол-во рейсов": "Кол-во рейсов",
            "Контрагент": "Контрагент",
            "Материал": "Материал",
            "Объект": "Объект",
            "Комментарий": "Комментарий",
            "Цена": "Цена",
            "Часы": "Часы",
            "Тип операции": "Формат работы"
        }
        
        value = answers.get(mapping.get(header, ""), "")
        
        if header == "Тип операции":
            fmt = answers.get("Формат работы", "")
            if "закуп" in fmt.lower():
                return "Расходы"
            elif "продажа" in fmt.lower() or "перевозка" in fmt.lower():
                return "Поступления"
        elif header == "Вес (тонна)":
            if answers.get("Единица измерения") == "тонна":
                return answers.get("Вес", "")
        elif header == "Вес (куб)":
            if answers.get("Единица измерения") == "м³":
                return answers.get("Вес", "")
                
        return str(value) if value is not None else ""

    def _append_row_safe(self, sheet, row):
        important_indices = [
            i for i, h in enumerate(sheet.row_values(1))
            if h not in self.ignore_columns
        ]

        all_values = sheet.get_all_values()
        last_filled_row = 1
        for i in range(1, len(all_values)):
            sheet_row = all_values[i]
            while len(sheet_row) < len(row):
                sheet_row.append("")
            if any(sheet_row[idx].strip() for idx in important_indices if idx < len(sheet_row)):
                last_filled_row = i + 1

        target_row = last_filled_row + 1
        print(f"Записываем в строку {target_row}: {row}")
        sheet.update(range_name=f"A{target_row}", values=[row])


# =============================
# ШАГИ
# =============================

def get_steps_sequence(format_work: str = None):
    base_steps = [
        {"name": "Дата", "type": "date"},
        {"name": "Формат работы", "type": "choice", "options": [
            "Продажа материала",
            "Перевозка в часах",
            "Перевозка в тоннах",
            "Перевозка в кубах",
            "Перевозка в рейсах"
        ]}
    ]
    
    if not format_work:
        return base_steps

    auto_step = {"name": "Авто", "type": "text"}
    comment_step = {"name": "Комментарий", "type": "text"}

    if "продажа" in format_work.lower():
        return base_steps + [
            {"name": "Авто", "type": "text"},
            {"name": "Кол-во рейсов", "type": "number"},
            {"name": "Контрагент закуп", "type": "reference", "ref_key": "Контрагент закуп"},
            {"name": "Материал", "type": "reference", "ref_key": "Материал"},
            {"name": "Единица измерения (закуп)", "type": "choice", "options": ["тонна", "м³"]},
            {"name": "Вес (закуп)", "type": "number"},
            {"name": "Контрагент продаж", "type": "reference", "ref_key": "Контрагент продаж"},
            {"name": "Объект продаж", "type": "reference", "ref_key": "Объект продаж"},
            {"name": "Единица измерения (продажа)", "type": "choice", "options": ["тонна", "м³"]},
            {"name": "Вес (продажа)", "type": "number"},
            {"name": "Комментарий", "type": "text"},
        ]
    elif "час" in format_work.lower():
        return base_steps + [
            auto_step,
            {"name": "Контрагент", "type": "reference", "ref_key": "Контрагент"},
            {"name": "Объект", "type": "reference", "ref_key": "Объект"},
            {"name": "Часы", "type": "number"},
            comment_step
        ]
    elif "рейс" in format_work.lower():
        return base_steps + [
            auto_step,
            {"name": "Кол-во рейсов", "type": "number"},
            {"name": "Контрагент", "type": "reference", "ref_key": "Контрагент"},
            {"name": "Объект", "type": "reference", "ref_key": "Объект"},
            comment_step
        ]
    else:
        if "тонн" in format_work.lower():
            weight_step_name = "Вес (в тоннах)"
        elif "куб" in format_work.lower():
            weight_step_name = "Вес (в кубах)"
        else:
            weight_step_name = "Вес"

        return base_steps + [
            auto_step,
            {"name": "Кол-во рейсов", "type": "number"},
            {"name": "Контрагент", "type": "reference", "ref_key": "Контрагент"},
            {"name": "Объект", "type": "reference", "ref_key": "Объект"},
            {"name": weight_step_name, "type": "number"},
            comment_step
        ]

def get_step_description(step_name: str, answers: dict) -> str:
    descriptions = {
        "Дата": "Выберите дату операции",
        "Формат работы": "Выберите тип операции",
        "Авто": "Введите госномер автомобиля (3 цифры)",
        "Кол-во рейсов": "Укажите количество рейсов",
        "Контрагент закуп": "Выберите поставщика (от кого закупили)",
        "Объект закуп": "Укажите объект закупки",
        "Материал": "Выберите материал",
        "Единица измерения (закуп)": "Выберите единицу измерения для закупки",
        "Вес (закуп)": "Введите вес закупки",
        "Контрагент продаж": "Выберите покупателя (кому продали)",
        "Объект продаж": "Укажите объект доставки",
        "Единица измерения (продажа)": "Выберите единицу измерения для продажи",
        "Вес (продажа)": "Введите вес продажи",
        "Комментарий": "Добавьте комментарий (необязательно)",
        "Вес (в тоннах)": "Введите вес в тоннах",
        "Вес (в кубах)": "Введите вес в кубах"
    }
    return descriptions.get(step_name, f"Заполните поле: {step_name}")


# =============================
# ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ: ОТЧЁТ
# =============================

def build_summary(answers: dict) -> str:
    parts = []
    for k, v in answers.items():
        if v:
            parts.append(f"• {k}: {v}")
    return "\n".join(parts) if parts else "Пока ничего не заполнено."


# =============================
# FSM
# =============================

class Form(StatesGroup):
    filling = State()
    searching_counterparty = State()


# =============================
# КЛАВИАТУРЫ
# =============================

def main_menu_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Добавить отчет")]],
        resize_keyboard=True,
        persistent=True
    )


# =============================
# ИНИЦИАЛИЗАЦИЯ
# =============================

GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
if not GOOGLE_CREDENTIALS_JSON:
    raise ValueError("❌ GOOGLE_CREDENTIALS_JSON environment variable is required! "
                     "Please set it in Railway Variables.")

sheets = SheetsManager(
    credentials_json=GOOGLE_CREDENTIALS_JSON,
    steps_sheet_id=STEPS_SHEET_ID,
    data_sheet_id=DATA_SHEET_ID,
    ignore_columns=IGNORE_COLUMNS
)

router = Router()


# =============================
# ОСНОВНАЯ ЛОГИКА
# =============================

async def ask_current_step(message: Message, state: FSMContext):
    data = await state.get_data()
    steps = data["steps"]
    index = data["current_step"]
    answers = data.get("answers", {})
    ref_data = data.get("ref_data", {})

    summary = build_summary(answers)
    report_block = f"📋 Текущий отчёт:\n{summary}\n\n"

    if index >= len(steps):
        await show_final_report(message, state)
        return

    step = steps[index]
    step_name = step["name"]
    step_type = step["type"]
    current_answer = answers.get(step_name, "")

    description = get_step_description(step_name, answers)
    msg_text = report_block
    msg_text += f"Шаг {index + 1} из {len(steps)}: {step_name}\n{description}"
    if current_answer:
        msg_text += f"\n\nТекущий ответ: `{current_answer}`"
    msg_text += "\n\n"

    skip_btn = "Пропустить"
    back_btn = "⬅ Назад"

    if step_type == "date":
        today = datetime.today()
        yesterday = today - timedelta(days=1)
        options = [yesterday.strftime("%d.%m.%Y"), today.strftime("%d.%m.%Y")]
        buttons = [[InlineKeyboardButton(text=opt, callback_data=f"step:{opt}")] for opt in options]
        action_row = []
        if index > 0:
            action_row.append(InlineKeyboardButton(text=back_btn, callback_data="nav:back"))
        action_row.append(InlineKeyboardButton(text=skip_btn, callback_data="step:__SKIP__"))
        buttons.append(action_row)
        kb = InlineKeyboardMarkup(inline_keyboard=buttons)
        msg_text += "Выберите дату:"
        await message.answer(msg_text, reply_markup=kb)

    elif step_type == "choice":
        options = step["options"]
        buttons = [[InlineKeyboardButton(text=opt, callback_data=f"step:{opt}")] for opt in options]
        
        if index > 0:
            buttons.append([InlineKeyboardButton(text=back_btn, callback_data="nav:back")])
        
        kb = InlineKeyboardMarkup(inline_keyboard=buttons)
        msg_text += "Выберите вариант:"
        await message.answer(msg_text, reply_markup=kb)

    elif step_type == "reference":
        ref_key = step["ref_key"]
        full_list = ref_data.get(ref_key, []) or ["(нет данных)"]
        
        ref_map = {str(i): name for i, name in enumerate(full_list)}
        await state.update_data(
            counterparty_full_list=full_list,
            current_ref_key=ref_key,
            current_ref_map=ref_map
        )
        
        items_to_show = full_list[:7]
        buttons = []
        for i, item in enumerate(items_to_show):
            buttons.append([InlineKeyboardButton(text=item, callback_data=f"cp:{i}")])
        
        nav_row = []
        if index > 0:
            nav_row.append(InlineKeyboardButton(text=back_btn, callback_data="nav:back"))
        nav_row.append(InlineKeyboardButton(text="🔍 Поиск", callback_data="cp:search"))
        nav_row.append(InlineKeyboardButton(text=skip_btn, callback_data="step:__SKIP__"))
        buttons.append(nav_row)
        kb = InlineKeyboardMarkup(inline_keyboard=buttons)
        await message.answer(f"{msg_text}Выберите:", reply_markup=kb)

    else:
        keyboard = []
        if index > 0:
            keyboard.append([KeyboardButton(text=back_btn), KeyboardButton(text=skip_btn)])
        else:
            keyboard.append([KeyboardButton(text=skip_btn)])
        kb = ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True, one_time_keyboard=False)
        msg_text += "Введите значение:"
        await message.answer(msg_text, reply_markup=kb)


async def show_final_report(message: Message, state: FSMContext):
    data = await state.get_data()
    answers = data["answers"]
    summary = build_summary(answers)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data="final:confirm")],
        [InlineKeyboardButton(text="✏️ Изменить", callback_data="final:edit")],
        [InlineKeyboardButton(text="❌ Отменить", callback_data="final:cancel")]
    ])

    await message.answer(f"📋 Полный отчёт:\n{summary}", reply_markup=kb)


# =============================
# HANDLERS
# =============================

@router.message(CommandStart())
async def start(message: Message):
    await message.answer("Привет! Хочешь добавить отчёт?", reply_markup=main_menu_keyboard())


@router.message(F.text == "Добавить отчет")
async def start_form(message: Message, state: FSMContext):
    ref_data = sheets.get_reference_data()
    steps = get_steps_sequence()
    await state.set_state(Form.filling)
    await state.update_data(steps=steps, current_step=0, answers={}, ref_data=ref_data)
    await ask_current_step(message, state)

@router.callback_query(Form.filling, F.data == "nav:back")
async def handle_back(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    steps = data["steps"]
    index = data["current_step"]

    if index >= len(steps):
        await callback.answer("Форма уже завершена.", show_alert=True)
        return

    if index > 0:
        await state.update_data(current_step=index - 1)
        await callback.message.edit_reply_markup(reply_markup=None)
        await ask_current_step(callback.message, state)
    else:
        await callback.answer("Нельзя вернуться дальше", show_alert=True)

@router.callback_query(Form.filling, F.data.startswith("step:"))
async def handle_step_choice(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    steps = data["steps"]
    index = data["current_step"]

    if index >= len(steps):
        await callback.answer("Форма уже завершена.", show_alert=True)
        return

    value = callback.data.replace("step:", "")
    answers = data["answers"]
    step_name = steps[index]["name"]

    answers[step_name] = "" if value == "__SKIP__" else value

    if step_name == "Формат работы":
        if "тонн" in value.lower():
            answers["Единица измерения"] = "тонна"
        elif "куб" in value.lower():
            answers["Единица измерения"] = "м³"
        else:
            answers["Единица измерения"] = None

        new_sequence = get_steps_sequence(value)
        if len(new_sequence) < 3:
            await callback.message.answer("Ошибка: недостаточно шагов для этого формата.")
            return

        await state.update_data(
            answers=answers,
            current_step=2,
            steps=new_sequence
        )
        await callback.message.edit_reply_markup(reply_markup=None)
        await ask_current_step(callback.message, state)
        return

    await state.update_data(answers=answers, current_step=index + 1)
    await callback.message.edit_reply_markup(reply_markup=None)
    await ask_current_step(callback.message, state)


@router.callback_query(Form.filling, F.data == "cp:search")
async def start_search(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.message.answer("Введите часть названия:")
    await state.set_state(Form.searching_counterparty)


@router.message(Form.searching_counterparty)
async def handle_search_input(message: Message, state: FSMContext):
    query = message.text.strip().lower()
    data = await state.get_data()
    full_list = data.get("counterparty_full_list", [])
    filtered = [item for item in full_list if query in item.lower()]
    if not filtered:
        await message.answer("Не найдено. Попробуйте ещё:")
        return
    buttons = [[InlineKeyboardButton(text=item, callback_data=f"cp:{item}")] for item in filtered[:7]]
    buttons.append([
        InlineKeyboardButton(text="↺ Новый поиск", callback_data="cp:search"),
        InlineKeyboardButton(text="⬅ Отмена", callback_data="cp:cancel")
    ])
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await message.answer("Выберите:", reply_markup=kb)


@router.callback_query(Form.searching_counterparty, F.data.startswith("cp:"))
async def handle_search_result(callback: CallbackQuery, state: FSMContext):
    action = callback.data.split(":", 1)[1]
    if action == "search":
        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.message.answer("Введите часть названия:")
        return
    if action == "cancel":
        await state.set_state(Form.filling)
        await ask_current_step(callback.message, state)
        return

    data = await state.get_data()
    index = data["current_step"]
    steps = data["steps"]
    answers = data["answers"]
    step_name = steps[index]["name"]
    
    ref_map = data.get("current_ref_map", {})
    value = ref_map.get(action, action)
    
    answers[step_name] = value
    await state.update_data(answers=answers, current_step=index + 1)
    await state.set_state(Form.filling)
    await callback.message.edit_reply_markup(reply_markup=None)
    await ask_current_step(callback.message, state)


@router.callback_query(Form.filling, F.data.startswith("cp:"))
async def handle_reference_choice(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    steps = data["steps"]
    index = data["current_step"]

    if index >= len(steps):
        await callback.answer("Форма уже завершена.", show_alert=True)
        return

    ref_id = callback.data.split(":", 1)[1]
    ref_map = data.get("current_ref_map", {})
    value = ref_map.get(ref_id, "")

    answers = data["answers"]
    step_name = steps[index]["name"]
    answers[step_name] = value
    await state.update_data(answers=answers, current_step=index + 1)
    await callback.message.edit_reply_markup(reply_markup=None)
    await ask_current_step(callback.message, state)


@router.callback_query(Form.filling, F.data.startswith("edit_step:"))
async def handle_edit_step(callback: CallbackQuery, state: FSMContext):
    try:
        step_index = int(callback.data.split(":")[1])
    except (ValueError, IndexError):
        await callback.answer("Неверный формат кнопки.", show_alert=True)
        return

    data = await state.get_data()
    steps = data.get("steps")
    if not steps:
        await callback.answer("Ошибка: шаги не загружены.", show_alert=True)
        return    
    if step_index >= len(steps):
        await callback.answer("Недопустимый шаг.", show_alert=True)
        return

    await state.update_data(editing_step=step_index)
    await callback.message.edit_reply_markup(reply_markup=None)

    step = steps[step_index]
    step_name = step["name"]
    step_type = step["type"]
    ref_data = data.get("ref_data", {})

    description = get_step_description(step_name, data.get("answers", {}))
    msg_text = f"Изменение шага: {step_name}\n{description}\n\n"

    if step_type == "date":
        today = datetime.today()
        yesterday = today - timedelta(days=1)
        options = [yesterday.strftime("%d.%m.%Y"), today.strftime("%d.%m.%Y")]
        buttons = [[InlineKeyboardButton(text=opt, callback_data=f"edit_save:{opt}")] for opt in options]
        kb = InlineKeyboardMarkup(inline_keyboard=buttons)
        msg_text += "Выберите дату:"
        await callback.message.answer(msg_text, reply_markup=kb)

    elif step_type == "choice":
        options = step["options"]
        buttons = [[InlineKeyboardButton(text=opt, callback_data=f"edit_save:{opt}")] for opt in options]
        kb = InlineKeyboardMarkup(inline_keyboard=buttons)
        msg_text += "Выберите вариант:"
        await callback.message.answer(msg_text, reply_markup=kb)

    elif step_type == "reference":
        ref_key = step["ref_key"]
        full_list = ref_data.get(ref_key, []) or ["(нет данных)"]
        items_to_show = full_list[:7]
        
        edit_ref_map = {str(i): name for i, name in enumerate(items_to_show)}
        await state.update_data(edit_ref_map=edit_ref_map)
        
        buttons = []
        for i, item in enumerate(items_to_show):
            buttons.append([InlineKeyboardButton(text=item, callback_data=f"edit_save:{i}")])
        
        buttons.append([InlineKeyboardButton(text="🔍 Поиск", callback_data="edit_search")])
        kb = InlineKeyboardMarkup(inline_keyboard=buttons)
        await callback.message.answer(f"{msg_text}Выберите:", reply_markup=kb)

    else:
        await callback.message.answer(f"{msg_text}Введите новое значение:")


@router.callback_query(Form.filling, F.data.startswith("edit_save:"))
async def handle_edit_save(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    ref_id = callback.data.split(":", 1)[1]
    
    edit_ref_map = data.get("edit_ref_map", {})
    new_value = edit_ref_map.get(ref_id, ref_id)
    
    await _save_edited_value_and_return(callback.message, state, new_value)


@router.callback_query(F.data.startswith("final:"))
async def handle_final_action(callback: CallbackQuery, state: FSMContext):
    action = callback.data.split(":")[1]

    if action == "confirm":
        data = await state.get_data()
        sheets.save_data(data["answers"])
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🆕 Новый отчёт", callback_data="post:new")],
            [InlineKeyboardButton(text="🔄 Копия", callback_data="post:duplicate")],
            [InlineKeyboardButton(text="🏠 Главная", callback_data="post:main")]
        ])
        await callback.message.edit_text("✅ Запись сохранена!", reply_markup=kb)

    elif action == "edit":
        data = await state.get_data()
        steps = data.get("steps", [])
        
        if not steps:
            await callback.message.answer("Ошибка: шаги не найдены.")
            return

        buttons = []
        for i, step in enumerate(steps):
            name = step["name"]
            val = data["answers"].get(name, "")
            label = f"{name}: {val}" if val else name
            buttons.append([InlineKeyboardButton(text=label, callback_data=f"edit_step:{i}")])
        
        if not buttons:
            await callback.message.answer("Нет шагов для редактирования.")
            return

        kb = InlineKeyboardMarkup(inline_keyboard=buttons)
        await state.update_data(editing_mode=True)
        await callback.message.edit_text("Выберите шаг для изменения:", reply_markup=kb)

    elif action == "cancel":
        await state.clear()
        await callback.message.edit_text("❌ Отчёт отменён.")
        await start(callback.message)


@router.message(Form.filling)
async def handle_manual_input(message: Message, state: FSMContext):
    data = await state.get_data()

    if data.get("editing_mode"):
        await _save_edited_value_and_return(message, state, message.text.strip())
        return

    steps = data["steps"]
    index = data["current_step"]

    if index >= len(steps):
        await message.answer("Пожалуйста, используйте кнопки под сообщением.")
        return

    step_name = steps[index]["name"]

    if step_name == "Формат работы":
        await message.answer("Пожалуйста, выберите формат из списка ниже.")
        return

    answers = data["answers"]
    text = message.text.strip()
    if text == "⬅ Назад":
        if index > 0:
            await state.update_data(current_step=index - 1)
            await ask_current_step(message, state)
        return
    if text == "Пропустить":
        text = ""

    answers[step_name] = text
    await state.update_data(answers=answers, current_step=index + 1)
    await ask_current_step(message, state)


async def _save_edited_value_and_return(message_or_callback, state: FSMContext, new_value: str):
    data = await state.get_data()
    step_index = data["editing_step"]
    steps = data["steps"]
    answers = data["answers"]
    step_name = steps[step_index]["name"]

    answers[step_name] = new_value
    await state.update_data(answers=answers, editing_step=None, editing_mode=False)

    message = message_or_callback if isinstance(message_or_callback, Message) else message_or_callback.message
    await show_final_report(message, state)


@router.callback_query(F.data == "edit_search")
async def start_edit_search(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("Введите часть названия:")


@router.message(Form.searching_counterparty)
async def handle_edit_search_input(message: Message, state: FSMContext):
    query = message.text.strip().lower()
    data = await state.get_data()
    full_list = data.get("counterparty_full_list", [])
    filtered = [item for item in full_list if query in item.lower()]
    if not filtered:
        await message.answer("Не найдено. Попробуйте ещё:")
        return
    buttons = [[InlineKeyboardButton(text=item, callback_data=f"edit_save:{item}")] for item in filtered[:7]]
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await message.answer("Выберите:", reply_markup=kb)


@router.callback_query(F.data.startswith("post:"))
async def handle_post_action(callback: CallbackQuery, state: FSMContext):
    action = callback.data.split(":")[1]
    if action == "new":
        await state.clear()
        await start_form(callback.message, state)
    elif action == "duplicate":
        data = await state.get_data()
        original = data.get("answers", {})
        format_work = original.get("Формат работы", "")
        print("Format work for duplicate:", repr(format_work))
        await state.clear()
        await state.set_state(Form.filling)
        await state.update_data(
            steps=get_steps_sequence(format_work),
            answers=original.copy(),
            ref_data=sheets.get_reference_data()
        )
        await show_final_report(callback.message, state)
    elif action == "main":
        await state.clear()
        await start(callback.message)


# =============================
# MAIN
# =============================

async def main():
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())