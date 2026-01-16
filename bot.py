import os
import subprocess
import sys
from pathlib import Path
import pandas as pd

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

BASE_DIR = Path(__file__).resolve().parent

def env_set(name: str) -> bool:
    v = os.getenv(name)
    return bool(v and v.strip())

def parse_allowed_chat_ids() -> set[int]:
    raw = (os.getenv("ALLOWED_CHAT_IDS") or "").strip()
    if not raw:
        return set()  # пусто = разрешены все (для демо)
    ids = set()
    for part in raw.split(","):
        part = part.strip()
        if part:
            ids.add(int(part))
    return ids

ALLOWED_CHAT_IDS = parse_allowed_chat_ids()

def is_allowed(chat_id: int) -> bool:
    return (not ALLOWED_CHAT_IDS) or (chat_id in ALLOWED_CHAT_IDS)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat is None:
        return
    if not is_allowed(update.effective_chat.id):
        await update.message.reply_text("Сорри, этот бот не для этого чата 🙂")
        return

    await update.message.reply_text(
        "Привет! Я могу собрать отчёт из monday.\n\n"
        "Команды:\n"
        "/report — собрать и прислать summary_by_geo.csv\n"
    )

def run_report() -> tuple[Path, Path]:
    # Запускаем report.py тем же Python, которым запущен бот
    proc = subprocess.run(
        [sys.executable, str(BASE_DIR / "report.py")],
        cwd=str(BASE_DIR),
        capture_output=True,
        text=True,
        timeout=180,
    )

    # Логи пригодятся, если что-то упадёт
    (BASE_DIR / "last_report_stdout.txt").write_text(proc.stdout or "", encoding="utf-8")
    (BASE_DIR / "last_report_stderr.txt").write_text(proc.stderr or "", encoding="utf-8")

    if proc.returncode != 0:
        raise RuntimeError(
            "report.py завершился с ошибкой.\n\n"
            f"STDERR:\n{proc.stderr[-1500:]}"
        )

    summary_path = BASE_DIR / "summary_by_geo.csv"
    raw_path = BASE_DIR / "raw_items.csv"
    if not summary_path.exists():
        raise FileNotFoundError("Не найден summary_by_geo.csv после запуска отчёта")
    if not raw_path.exists():
        raise FileNotFoundError("Не найден raw_items.csv после запуска отчёта")

    return summary_path, raw_path

def preview_summary(summary_csv: Path) -> str:
    try:
        df = pd.read_csv(summary_csv)
        if df.empty:
            return "Сводка пустая (0 строк). Возможно, всё отфильтровано или нет подходящих статусов."
        # оставим топ-20, чтобы не спамить
        df = df.sort_values("Итого", ascending=False).head(20)
        lines = ["Сводка (топ 20):"]
        for _, r in df.iterrows():
            lines.append(f"- {r['geo']}: обучаются {int(r.get('Обучаются',0))}, ожидают {int(r.get('Ожидают',0))}, итого {int(r.get('Итого',0))}")
        return "\n".join(lines)
    except Exception:
        return "Сводку прочитать не смог (но CSV файл сейчас пришлю)."

async def report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat is None:
        return
    if not is_allowed(update.effective_chat.id):
        await update.message.reply_text("Сорри, этот бот не для этого чата 🙂")
        return

    # быстрая проверка, что токены заданы
    missing = []
    for v in ["MONDAY_API_TOKEN", "TELEGRAM_BOT_TOKEN"]:
        if not env_set(v):
            missing.append(v)
    if missing:
        await update.message.reply_text(f"Не заданы переменные окружения: {', '.join(missing)}")
        return

    msg = await update.message.reply_text("Собираю отчёт…")

    try:
        summary_csv, raw_csv = run_report()

        # небольшой текст + файлы
        await msg.edit_text(preview_summary(summary_csv))

        await update.message.reply_document(
            document=summary_csv.read_bytes(),
            filename="summary_by_geo.csv",
            caption="Готово: summary_by_geo.csv"
        )

        await update.message.reply_document(
            document=raw_csv.read_bytes(),
            filename="raw_items.csv",
            caption="На всякий: raw_items.csv"
        )

    except Exception as e:
        # если что — кидаем ошибку и логи
        await msg.edit_text(f"Упало 😬\n\n{e}")

        stderr_path = BASE_DIR / "last_report_stderr.txt"
        stdout_path = BASE_DIR / "last_report_stdout.txt"
        if stderr_path.exists():
            await update.message.reply_document(stderr_path.read_bytes(), filename="last_report_stderr.txt")
        if stdout_path.exists():
            await update.message.reply_document(stdout_path.read_bytes(), filename="last_report_stdout.txt")

def main():
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError('Не найден TELEGRAM_BOT_TOKEN. Задай переменную окружения.')

    app = Application.builder().token(token).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("report", report))
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()
