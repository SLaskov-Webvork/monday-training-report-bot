import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

BASE_DIR = Path(__file__).resolve().parent


def env(name: str) -> str:
    return (os.getenv(name) or "").strip()


def parse_allowed_chat_ids() -> set[int]:
    raw = env("ALLOWED_CHAT_IDS")
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


def run_report() -> tuple[Path, Path]:
    proc = subprocess.run(
        [sys.executable, str(BASE_DIR / "report.py")],
        cwd=str(BASE_DIR),
        capture_output=True,
        text=True,
        timeout=180,
    )

    (BASE_DIR / "last_report_stdout.txt").write_text(proc.stdout or "", encoding="utf-8")
    (BASE_DIR / "last_report_stderr.txt").write_text(proc.stderr or "", encoding="utf-8")

    if proc.returncode != 0:
        raise RuntimeError(
            "report.py завершился с ошибкой.\n\n"
            f"STDERR:\n{(proc.stderr or '')[-1500:]}"
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
            return "Сводка пустая (0 строк)."
        # если есть Итого — сортируем по нему, иначе по Обучаются
        sort_col = "Итого" if "Итого" in df.columns else ("Обучаются" if "Обучаются" in df.columns else None)
        if sort_col:
            df = df.sort_values(sort_col, ascending=False)
        df = df.head(20)

        lines = ["Сводка (топ 20):"]
        for _, r in df.iterrows():
            geo = r.get("geo", "")
            обуч = int(r.get("Обучаются", 0))
            ожид = int(r.get("Ожидают", 0))
            итого = int(r.get("Итого", обуч + ожид))
            lines.append(f"- {geo}: обучаются {обуч}, ожидают {ожид}, итого {итого}")
        return "\n".join(lines)
    except Exception:
        return "CSV сформирован — сейчас пришлю файлом."


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat is None:
        return
    if not is_allowed(update.effective_chat.id):
        await update.message.reply_text("Сорри, этот бот не для этого чата 🙂")
        return

    await update.message.reply_text(
        "Привет! Я собираю отчёт из monday.\n\n"
        "Команды:\n"
        "/report — собрать и прислать summary_by_geo.csv\n"
    )


async def report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat is None:
        return
    if not is_allowed(update.effective_chat.id):
        await update.message.reply_text("Сорри, этот бот не для этого чата 🙂")
        return

    # Проверка токенов
    if not env("MONDAY_API_TOKEN"):
        await update.message.reply_text("Не задан MONDAY_API_TOKEN в переменных окружения.")
        return

    msg = await update.message.reply_text("Собираю отчёт…")

    try:
        summary_csv, raw_csv = run_report()

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
        await msg.edit_text(f"Упало 😬\n\n{e}")

        stderr_path = BASE_DIR / "last_report_stderr.txt"
        stdout_path = BASE_DIR / "last_report_stdout.txt"
        if stderr_path.exists():
            await update.message.reply_document(stderr_path.read_bytes(), filename="last_report_stderr.txt")
        if stdout_path.exists():
            await update.message.reply_document(stdout_path.read_bytes(), filename="last_report_stdout.txt")


def main():
    tg_token = env("TELEGRAM_BOT_TOKEN")
    if not tg_token:
        raise RuntimeError("Не задан TELEGRAM_BOT_TOKEN")

    app = Application.builder().token(tg_token).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("report", report))

    # --- Режим Koyeb / Webhook ---
    webhook_base_url = env("WEBHOOK_BASE_URL")  # например: https://your-app.koyeb.app
    webhook_secret = env("WEBHOOK_SECRET")      # например: supersecret123
    port = int(env("PORT") or "8000")

    if webhook_base_url and webhook_secret:
        # URL, куда Telegram будет стучаться
        webhook_url = f"{webhook_base_url.rstrip('/')}/{webhook_secret}"
        # Путь на нашем сервере
        url_path = webhook_secret

        print("Запуск в режиме WEBHOOK")
        print("Webhook URL:", webhook_url)

        app.run_webhook(
            listen="0.0.0.0",
            port=port,
            url_path=url_path,
            webhook_url=webhook_url,
            drop_pending_updates=True,
        )
        return

    # --- Локальный режим (на твоём Маке), если нужно ---
    # На Koyeb так делать не надо.
    print("WEBHOOK переменные не заданы — запускаю polling (только для локального теста).")
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
