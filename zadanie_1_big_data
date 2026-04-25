#!/usr/bin/env python3
"""Rozwiązanie zadania z zajęć 1 (Zarządzanie Big Data).

Pipeline:
1) API REST Countries -> lista rekordów ("DataFrame-like")
2) Zapis do SQLite (kraje_swiata.db, tabela: kraje)
3) Analiza SQL
4) Wizualizacja: wykres słupkowy zapisany do pliku SVG
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

API_URL = "https://restcountries.com/v3.1/all"
DB_PATH = Path("kraje_swiata.db")
SVG_PATH = Path("populacja_regiony.svg")
POLAND_AREA_KM2 = 312_679

SAMPLE_DATA: list[dict[str, Any]] = [
    {
        "name": {"common": "Poland"},
        "capital": ["Warsaw"],
        "region": "Europe",
        "subregion": "Central Europe",
        "population": 37950802,
        "area": 312679.0,
        "currencies": {"PLN": {"name": "Polish złoty", "symbol": "zł"}},
    },
    {
        "name": {"common": "Germany"},
        "capital": ["Berlin"],
        "region": "Europe",
        "subregion": "Western Europe",
        "population": 83240525,
        "area": 357114.0,
        "currencies": {"EUR": {"name": "Euro", "symbol": "€"}},
    },
    {
        "name": {"common": "China"},
        "capital": ["Beijing"],
        "region": "Asia",
        "subregion": "Eastern Asia",
        "population": 1402112000,
        "area": 9706961.0,
        "currencies": {"CNY": {"name": "Chinese yuan", "symbol": "¥"}},
    },
    {
        "name": {"common": "India"},
        "capital": ["New Delhi"],
        "region": "Asia",
        "subregion": "Southern Asia",
        "population": 1428627663,
        "area": 3287590.0,
        "currencies": {"INR": {"name": "Indian rupee", "symbol": "₹"}},
    },
    {
        "name": {"common": "Canada"},
        "capital": ["Ottawa"],
        "region": "Americas",
        "subregion": "North America",
        "population": 40252851,
        "area": 9984670.0,
        "currencies": {"CAD": {"name": "Canadian dollar", "symbol": "$"}},
    },
]

Row = dict[str, Any]


def get_currency(currencies: dict[str, Any] | None) -> str | None:
    if not currencies:
        return None
    return next(iter(currencies.keys()), None)


def fetch_countries_data(url: str = API_URL) -> list[dict[str, Any]]:
    """Próbuje pobrać dane z API; przy braku sieci używa danych przykładowych."""
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urlopen(req, timeout=30) as response:  # nosec B310 - trusted endpoint used in classes
            return json.loads(response.read().decode("utf-8"))
    except URLError as error:
        print(f"[OSTRZEŻENIE] Nie udało się pobrać danych z API ({error}).")
        print("[OSTRZEŻENIE] Używam wbudowanego zbioru przykładowego (offline fallback).")
        return SAMPLE_DATA


def build_rows(data: list[dict[str, Any]]) -> list[Row]:
    rows: list[Row] = []
    for country in data:
        capitals = country.get("capital") or [None]
        rows.append(
            {
                "nazwa": (country.get("name") or {}).get("common"),
                "stolica": capitals[0] if isinstance(capitals, list) and capitals else None,
                "region": country.get("region"),
                "subregion": country.get("subregion"),
                "populacja": country.get("population"),
                "powierzchnia": country.get("area"),
                "waluta": get_currency(country.get("currencies")),
            }
        )
    return rows


def print_head_shape_dtypes(rows: list[Row], columns: list[str]) -> None:
    print("=== HEAD (5 pierwszych rekordów) ===")
    for idx, row in enumerate(rows[:5], start=1):
        print(f"{idx}. {row}")

    print("\n=== SHAPE ===")
    print((len(rows), len(columns)))

    inferred_types: dict[str, str] = {}
    for col in columns:
        sample = next((r[col] for r in rows if r.get(col) is not None), None)
        inferred_types[col] = type(sample).__name__ if sample is not None else "NoneType"

    print("\n=== DTYPES (typy inferowane) ===")
    for col, dtype in inferred_types.items():
        print(f"{col}: {dtype}")


def save_to_sqlite(rows: list[Row], db_path: Path = DB_PATH) -> None:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS kraje")
    cur.execute(
        """
        CREATE TABLE kraje (
            nazwa TEXT,
            stolica TEXT,
            region TEXT,
            subregion TEXT,
            populacja INTEGER,
            powierzchnia REAL,
            waluta TEXT
        )
        """
    )

    cur.executemany(
        """
        INSERT INTO kraje (nazwa, stolica, region, subregion, populacja, powierzchnia, waluta)
        VALUES (:nazwa, :stolica, :region, :subregion, :populacja, :powierzchnia, :waluta)
        """,
        rows,
    )
    conn.commit()
    conn.close()


def query_all(conn: sqlite3.Connection, sql: str) -> list[tuple[Any, ...]]:
    return conn.execute(sql).fetchall()


def run_sql_analyses(db_path: Path = DB_PATH) -> dict[str, list[tuple[Any, ...]]]:
    conn = sqlite3.connect(db_path)
    try:
        results = {
            "laczna_populacja": query_all(
                conn,
                "SELECT SUM(populacja) AS laczna_populacja_swiata FROM kraje",
            ),
            "top10_populacja": query_all(
                conn,
                """
                SELECT nazwa, populacja
                FROM kraje
                ORDER BY populacja DESC
                LIMIT 10
                """,
            ),
            "regiony_statystyki": query_all(
                conn,
                """
                SELECT
                    region,
                    COUNT(*) AS liczba_krajow,
                    ROUND(AVG(populacja), 2) AS srednia_populacja
                FROM kraje
                GROUP BY region
                ORDER BY liczba_krajow DESC
                """,
            ),
            "wieksze_niz_polska": query_all(
                conn,
                f"""
                SELECT nazwa, powierzchnia
                FROM kraje
                WHERE powierzchnia > {POLAND_AREA_KM2}
                ORDER BY powierzchnia DESC
                """,
            ),
            "najwieksza_gestosc": query_all(
                conn,
                """
                SELECT
                    nazwa,
                    populacja,
                    powierzchnia,
                    ROUND(populacja * 1.0 / powierzchnia, 2) AS gestosc_zaludnienia
                FROM kraje
                WHERE powierzchnia IS NOT NULL
                  AND powierzchnia > 0
                  AND populacja IS NOT NULL
                ORDER BY gestosc_zaludnienia DESC
                LIMIT 1
                """,
            ),
            "populacja_regionow": query_all(
                conn,
                """
                SELECT region, SUM(populacja) AS laczna_populacja
                FROM kraje
                WHERE region IS NOT NULL
                GROUP BY region
                ORDER BY laczna_populacja DESC
                """,
            ),
        }
        return results
    finally:
        conn.close()


def print_results(title: str, rows: list[tuple[Any, ...]], limit: int | None = None) -> None:
    print(f"\n=== {title} ===")
    view = rows[:limit] if limit is not None else rows
    for row in view:
        print(row)
    if limit is not None and len(rows) > limit:
        print(f"... ({len(rows)} wierszy łącznie)")


def save_svg_bar_chart(region_rows: list[tuple[Any, int]], out_path: Path = SVG_PATH) -> None:
    # region_rows: [(region, suma_populacji), ...]
    if not region_rows:
        out_path.write_text("<svg xmlns='http://www.w3.org/2000/svg'></svg>", encoding="utf-8")
        return

    width, height = 1200, 700
    margin_left, margin_right, margin_top, margin_bottom = 90, 40, 50, 180
    chart_w = width - margin_left - margin_right
    chart_h = height - margin_top - margin_bottom

    max_val = max(int(row[1] or 0) for row in region_rows) or 1
    bar_count = len(region_rows)
    slot_w = chart_w / bar_count
    bar_w = slot_w * 0.7

    svg: list[str] = [
        f"<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}'>",
        "<style>text{font-family:Arial,sans-serif;font-size:12px} .title{font-size:20px;font-weight:bold}</style>",
        f"<text x='{width/2}' y='30' text-anchor='middle' class='title'>Łączna populacja regionów świata</text>",
        f"<line x1='{margin_left}' y1='{margin_top + chart_h}' x2='{margin_left + chart_w}' y2='{margin_top + chart_h}' stroke='black'/>",
        f"<line x1='{margin_left}' y1='{margin_top}' x2='{margin_left}' y2='{margin_top + chart_h}' stroke='black'/>",
    ]

    for idx, (region, val) in enumerate(region_rows):
        val_i = int(val or 0)
        h = (val_i / max_val) * (chart_h - 10)
        x = margin_left + idx * slot_w + (slot_w - bar_w) / 2
        y = margin_top + chart_h - h
        svg.append(
            f"<rect x='{x:.1f}' y='{y:.1f}' width='{bar_w:.1f}' height='{h:.1f}' fill='#4C78A8'/>"
        )
        svg.append(
            f"<text x='{x + bar_w / 2:.1f}' y='{margin_top + chart_h + 18}' text-anchor='middle'>{(region or 'Brak')}</text>"
        )
        svg.append(
            f"<text x='{x + bar_w / 2:.1f}' y='{y - 6:.1f}' text-anchor='middle'>{val_i:,}</text>".replace(",", " ")
        )

    svg.append("</svg>")
    out_path.write_text("\n".join(svg), encoding="utf-8")


def main() -> None:
    data = fetch_countries_data()
    rows = build_rows(data)
    columns = ["nazwa", "stolica", "region", "subregion", "populacja", "powierzchnia", "waluta"]

    print_head_shape_dtypes(rows, columns)
    save_to_sqlite(rows)
    results = run_sql_analyses()

    print_results("ŁĄCZNA POPULACJA ŚWIATA", results["laczna_populacja"])
    print_results("TOP 10 KRAJÓW WG POPULACJI", results["top10_populacja"])
    print_results("REGIONY: LICZBA KRAJÓW I ŚREDNIA POPULACJA", results["regiony_statystyki"])
    print_results("KRAJE WIĘKSZE POWIERZCHNIOWO OD POLSKI", results["wieksze_niz_polska"], limit=20)
    print_results("NAJWYŻSZA GĘSTOŚĆ ZALUDNIENIA", results["najwieksza_gestosc"])

    save_svg_bar_chart(results["populacja_regionow"])
    print(f"\nWykres słupkowy zapisano do: {SVG_PATH.resolve()}")
    print(f"Baza SQLite zapisana w: {DB_PATH.resolve()}")


if __name__ == "__main__":
    main()
