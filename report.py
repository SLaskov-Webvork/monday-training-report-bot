import os
import json
import requests
import pandas as pd

MONDAY_API_URL = "https://api.monday.com/v2"


def monday_request(query: str, variables: dict):
    token = os.getenv("MONDAY_API_TOKEN")
    if not token:
        raise RuntimeError("Не найден MONDAY_API_TOKEN")

    headers = {
        "Authorization": token,
        "Content-Type": "application/json"
    }

    resp = requests.post(
        MONDAY_API_URL,
        headers=headers,
        json={"query": query, "variables": variables},
        timeout=60
    )
    resp.raise_for_status()
    return resp.json()


def main():
    with open("config.json", "r", encoding="utf-8") as f:
        cfg = json.load(f)

    board_cfg = cfg["boards"][0]
    board_id = board_cfg["id"]
    status_column_id = board_cfg["status_column_id"]

    geo_map = cfg["geo_group_mapping"]
    ignored_groups = set(cfg.get("ignored_groups", []))

    learning_statuses = set(cfg["status_mapping"]["learning"])
    waiting_statuses = set(cfg["status_mapping"]["waiting"])

    print(f"Читаю доску: id={board_id}")

    query = """
    query ($board_id: ID!) {
      boards(ids: [$board_id]) {
        name
        groups {
          id
          title
        }
        items_page(limit: 500) {
          items {
            name
            group {
              title
            }
            column_values {
              id
              text
            }
          }
        }
      }
    }
    """

    data = monday_request(query, {"board_id": board_id})
    board = data["data"]["boards"][0]
    print(f"Читаю доску: {board['name']}")

    rows = []

    for item in board["items_page"]["items"]:
        group_title = item["group"]["title"]

        # ⛔ Игнорируем служебные группы
        if group_title in ignored_groups:
            continue

        geo = geo_map.get(group_title)
        if not geo:
            continue

        status_text = None
        for col in item["column_values"]:
            if col["id"] == status_column_id:
                status_text = col["text"]
                break

        if not status_text:
            continue

        rows.append({
            "name": item["name"],
            "geo": geo,
            "status": status_text
        })

    df = pd.DataFrame(rows)

    if df.empty:
        print("Нет данных после фильтрации")
        return

    df["Обучаются"] = df["status"].isin(learning_statuses)
    df["Ожидают"] = df["status"].isin(waiting_statuses)

    summary = (
        df.groupby("geo")[["Обучаются", "Ожидают"]]
        .sum()
        .reset_index()
    )

    print("\n=== СВОДКА ПО GEO ===")
    print(summary.to_string(index=False))

    df.to_csv("raw_items.csv", index=False, encoding="utf-8-sig")
    summary.to_csv("summary_by_geo.csv", index=False, encoding="utf-8-sig")

    print("\nФайлы сохранены:")
    print(" - raw_items.csv")
    print(" - summary_by_geo.csv")


if __name__ == "__main__":
    main()
