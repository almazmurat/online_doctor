#!/usr/bin/env python3
"""Assignment 2 pipeline for Kazakhstan online doctor market analysis.

This script:
1) Scrapes maximum publicly available doctor records from doctorline.kz
2) Normalizes doctorline + doctor.kz into a unified schema
3) Cleans and merges datasets
4) Generates reports and visualizations for Assignment #2
"""

from __future__ import annotations

import csv
import datetime as dt
import json
import math
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).resolve().parent

DOCTOR_KZ_CSV = BASE_DIR / "doctor_kz_doctors.csv"
DOCTORLINE_CSV = BASE_DIR / "doctorline_doctors.csv"
MERGED_CSV = BASE_DIR / "merged_cleaned_dataset.csv"

CLEANING_REPORT = BASE_DIR / "data_cleaning_report.md"
MARKET_REPORT = BASE_DIR / "market_intelligence_report.md"
ANSWER1_TXT = BASE_DIR / "assignment2_answer1.txt"
ANSWER2_TXT = BASE_DIR / "assignment2_answer2.txt"

PLOT_TOP_CITIES = BASE_DIR / "top_cities.png"
PLOT_TOP_SPECIALIZATIONS = BASE_DIR / "top_specializations.png"
PLOT_PRICE_SEGMENTS = BASE_DIR / "price_segments.png"
PLOT_RATINGS_DISTRIBUTION = BASE_DIR / "ratings_distribution.png"
PLOT_SOURCE_COMPARISON = BASE_DIR / "source_comparison.png"
PLOT_FORMATS = BASE_DIR / "consultation_formats.png"

UNIFIED_FIELDS = [
    "doctor_name",
    "specialization",
    "city",
    "clinic",
    "rating",
    "reviews_count",
    "experience_years",
    "price",
    "consultation_format",
    "source",
    "profile_url",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
}


@dataclass
class DoctorLineDiscovery:
    discovered_profile_urls: list[str]
    specialization_ids: list[str]
    checked_list_pages: int
    checked_search_queries: int
    checked_date_queries: int
    checked_direct_ids: int


def clean_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value)
    text = text.replace("\u200b", " ").replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def to_float_or_nan(value: object) -> float:
    text = clean_text(value)
    if not text:
        return math.nan
    text = text.replace(",", ".")
    text = re.sub(r"[^\d.\-]", "", text)
    if text in {"", ".", "-", "-."}:
        return math.nan
    try:
        return float(text)
    except ValueError:
        return math.nan


def to_int_or_nan(value: object) -> float:
    f = to_float_or_nan(value)
    if math.isnan(f):
        return math.nan
    return int(round(f))


def fetch_html(session: requests.Session, url: str, timeout: float = 30.0) -> str:
    response = session.get(url, headers=HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.text


def extract_doctor_links_from_html(html: str) -> set[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: set[str] = set()
    for a in soup.select("a[href]"):
        href = clean_text(a.get("href", ""))
        if not href:
            continue
        if href.startswith("/doctors/"):
            href = f"https://doctorline.kz{href}"
        if href.startswith("https://doctorline.kz/doctors/") and "#reviews" not in href:
            links.add(href)
    return links


def discover_doctorline_profiles(session: requests.Session) -> DoctorLineDiscovery:
    base_url = "https://doctorline.kz/doctors"
    base_html = fetch_html(session, base_url)

    all_links = set(extract_doctor_links_from_html(base_html))

    # 1) discover specialization ids from inline filter buttons
    spec_ids = []
    for sid in re.findall(r"select\((\d+|null),\s*'[^']*'\)", base_html):
        if sid not in spec_ids:
            spec_ids.append(sid)

    # 2) list pagination
    checked_pages = 0
    for page in range(1, 31):
        checked_pages += 1
        html = fetch_html(session, f"{base_url}?page={page}")
        all_links.update(extract_doctor_links_from_html(html))

    # 3) specialization filter pages
    for sid in spec_ids:
        if sid == "null":
            html = base_html
        else:
            html = fetch_html(session, f"{base_url}?specialization={sid}")
        all_links.update(extract_doctor_links_from_html(html))

    # 4) search queries (ru + en fragments)
    search_queries = [
        "", "а", "е", "и", "о", "у", "д", "м", "н", "с", "т", "к", "р", "л", "п", "в", "г", "ж",
        "a", "e", "i", "o", "u", "d", "m", "n", "s", "t", "k", "r", "l", "p", "v", "g", "zh",
        "doctor", "online", "consult", "therapy", "cardio", "derma",
    ]
    checked_search = 0
    for query in search_queries:
        checked_search += 1
        html = fetch_html(session, f"{base_url}?search={query}")
        all_links.update(extract_doctor_links_from_html(html))

    # 5) date filter pages
    checked_dates = 0
    today = dt.date.today()
    for offset in range(0, 91, 3):
        checked_dates += 1
        date_str = (today + dt.timedelta(days=offset)).isoformat()
        html = fetch_html(session, f"{base_url}?date={date_str}")
        all_links.update(extract_doctor_links_from_html(html))

    # 6) direct profile probing by numeric ids
    checked_ids = 0
    for doctor_id in range(1, 401):
        checked_ids += 1
        profile_url = f"https://doctorline.kz/doctors/{doctor_id}"
        response = session.get(profile_url, headers=HEADERS, timeout=30)
        if response.status_code != 200:
            continue
        if "<h1" in response.text and "DoctorLine" in response.text:
            all_links.add(profile_url)

    return DoctorLineDiscovery(
        discovered_profile_urls=sorted(all_links),
        specialization_ids=spec_ids,
        checked_list_pages=checked_pages,
        checked_search_queries=checked_search,
        checked_date_queries=checked_dates,
        checked_direct_ids=checked_ids,
    )


def parse_doctorline_profile(session: requests.Session, url: str) -> dict[str, str] | None:
    response = session.get(url, headers=HEADERS, timeout=30)
    if response.status_code != 200:
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    h1 = soup.find("h1")
    doctor_name = clean_text(h1.get_text(" ", strip=True) if h1 else "")
    if not doctor_name or doctor_name == "404":
        return None

    container = soup.find("div", class_="ml-6 flex-1")
    container_text = clean_text(container.get_text(" ", strip=True) if container else "")

    specialization = ""
    if container_text:
        # Common pattern: "<name> Врач общей практики Верифицирован ..."
        m = re.search(rf"{re.escape(doctor_name)}\s+(.*?)\s+Верифицирован", container_text)
        if m:
            specialization = clean_text(m.group(1))
            specialization = re.sub(r"^(Врач|Доктор)\s+", "", specialization, flags=re.IGNORECASE)

    experience_years = ""
    m = re.search(r"Опыт:\s*(\d+)\s*лет", container_text)
    if m:
        experience_years = m.group(1)

    clinic = ""
    m = re.search(r"Клиника:\s*(.+?)\s+Лицензия:", container_text)
    if m:
        clinic = clean_text(m.group(1))

    rating = ""
    reviews_count = ""
    m = re.search(r"(\d+[\.,]?\d*)\s*\((\d+)\s*reviews\)", container_text, flags=re.IGNORECASE)
    if m:
        rating = m.group(1).replace(",", ".")
        reviews_count = m.group(2)

    prices = []
    for span in soup.find_all("span"):
        text = clean_text(span.get_text(" ", strip=True))
        if "₸" in text:
            digits = re.sub(r"[^\d]", "", text)
            if digits:
                prices.append(int(digits))

    # For one-field schema we keep minimal consultation price from available booking options.
    min_price = str(min(prices)) if prices else ""

    formats = sorted(
        {
            clean_text(span.get_text(" ", strip=True))
            for span in soup.find_all("span")
            if clean_text(span.get_text(" ", strip=True)) in {"Онлайн", "Оффлайн"}
        }
    )
    if not formats and "Онлайн консультации" in response.text:
        formats = ["Онлайн"]

    consultation_format = "; ".join(formats)

    return {
        "doctor_name": doctor_name,
        "specialization": specialization,
        "city": "",
        "clinic": clinic,
        "rating": rating,
        "reviews_count": reviews_count,
        "experience_years": experience_years,
        "price": min_price,
        "consultation_format": consultation_format,
        "source": "doctorline.kz",
        "profile_url": url,
    }


def scrape_doctorline() -> tuple[pd.DataFrame, DoctorLineDiscovery]:
    session = requests.Session()
    discovery = discover_doctorline_profiles(session)

    records = []
    seen = set()
    for url in discovery.discovered_profile_urls:
        if url in seen:
            continue
        seen.add(url)
        parsed = parse_doctorline_profile(session, url)
        if parsed:
            records.append(parsed)

    df = pd.DataFrame(records, columns=UNIFIED_FIELDS)
    if not df.empty:
        df = df.drop_duplicates(subset=["profile_url"]).reset_index(drop=True)

    df.to_csv(DOCTORLINE_CSV, index=False, encoding="utf-8-sig")
    return df, discovery


def normalize_doctor_kz() -> pd.DataFrame:
    if not DOCTOR_KZ_CSV.exists():
        raise FileNotFoundError(f"Input file is missing: {DOCTOR_KZ_CSV}")

    df = pd.read_csv(DOCTOR_KZ_CSV, dtype=str).fillna("")

    normalized = pd.DataFrame(
        {
            "doctor_name": df.get("doctor_name", ""),
            "specialization": df.get("specialization", ""),
            "city": df.get("city", ""),
            "clinic": df.get("clinic", ""),
            "rating": df.get("rating", ""),
            "reviews_count": df.get("reviews_count", ""),
            "experience_years": df.get("experience_years", ""),
            "price": "",
            "consultation_format": "",
            "source": "doctor.kz",
            "profile_url": df.get("profile_url", ""),
        }
    )

    return normalized[UNIFIED_FIELDS].copy()


def normalize_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for column in UNIFIED_FIELDS:
        out[column] = out[column].astype(str).map(clean_text)

    # normalize placeholders into empty values
    placeholders = {"nan", "none", "null", "-", "n/a", "na"}
    for column in UNIFIED_FIELDS:
        out[column] = out[column].apply(lambda x: "" if x.strip().lower() in placeholders else x)

    city_map = {
        "алмата": "Алматы",
        "алматы": "Алматы",
        "астана": "Астана",
        "нур-султан": "Астана",
        "шымкент": "Шымкент",
        "караганда": "Караганда",
        "қарағанды": "Караганда",
    }

    def normalize_city(v: str) -> str:
        if not v:
            return ""
        low = v.lower().strip()
        if low in city_map:
            return city_map[low]
        return v[:1].upper() + v[1:] if v else v

    out["city"] = out["city"].map(normalize_city)

    def normalize_specialization(v: str) -> str:
        if not v:
            return ""
        v = re.sub(r"\s*[,;|/]+\s*", ", ", v)
        return v[:1].upper() + v[1:] if v else v

    out["specialization"] = out["specialization"].map(normalize_specialization)

    return out


def convert_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["price"] = out["price"].map(to_int_or_nan)
    out["reviews_count"] = out["reviews_count"].map(to_int_or_nan)
    out["experience_years"] = out["experience_years"].map(to_int_or_nan)
    out["rating"] = out["rating"].map(to_float_or_nan)
    return out


def count_outliers_iqr(series: pd.Series) -> dict[str, float]:
    clean = series.dropna()
    if clean.empty or len(clean) < 4:
        return {
            "q1": math.nan,
            "q3": math.nan,
            "iqr": math.nan,
            "lower": math.nan,
            "upper": math.nan,
            "outliers_count": 0,
        }

    q1 = clean.quantile(0.25)
    q3 = clean.quantile(0.75)
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    outliers = clean[(clean < lower) | (clean > upper)]
    return {
        "q1": float(q1),
        "q3": float(q3),
        "iqr": float(iqr),
        "lower": float(lower),
        "upper": float(upper),
        "outliers_count": int(outliers.shape[0]),
    }


def assign_price_segments(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, float]]:
    out = df.copy()
    prices = out["price"].dropna()

    if len(prices) >= 4:
        q1 = float(prices.quantile(0.25))
        q2 = float(prices.quantile(0.50))
        q3 = float(prices.quantile(0.75))
    elif len(prices) > 0:
        # fallback for sparse prices
        sorted_prices = sorted(prices.tolist())
        q1 = float(sorted_prices[0])
        q2 = float(np.median(sorted_prices))
        q3 = float(sorted_prices[-1])
    else:
        q1 = q2 = q3 = math.nan

    def segment(price: float) -> str:
        if pd.isna(price):
            return "unknown"
        if price <= q1:
            return "low-priced"
        if price <= q2:
            return "middle-priced"
        if price <= q3:
            return "high-priced"
        return "luxury"

    out["price_segment"] = out["price"].map(segment)

    boundaries = {"q1": q1, "q2": q2, "q3": q3}
    return out, boundaries


def save_png_top_counts(series: pd.Series, title: str, xlabel: str, ylabel: str, output: Path, top_n: int = 10) -> None:
    counts = series[series.notna() & (series.astype(str).str.strip() != "")].value_counts().head(top_n)
    plt.figure(figsize=(10, 6))
    counts.sort_values().plot(kind="barh", color="#1f77b4")
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.tight_layout()
    plt.savefig(output, dpi=150)
    plt.close()


def generate_visualizations(df: pd.DataFrame) -> None:
    save_png_top_counts(
        df["city"],
        title="Top Cities by Number of Doctors",
        xlabel="Doctors",
        ylabel="City",
        output=PLOT_TOP_CITIES,
    )

    save_png_top_counts(
        df["specialization"],
        title="Top Specializations",
        xlabel="Doctors",
        ylabel="Specialization",
        output=PLOT_TOP_SPECIALIZATIONS,
    )

    plt.figure(figsize=(8, 5))
    segment_counts = df["price_segment"].value_counts().reindex(
        ["low-priced", "middle-priced", "high-priced", "luxury", "unknown"],
        fill_value=0,
    )
    segment_counts.plot(kind="bar", color=["#2ca02c", "#1f77b4", "#ff7f0e", "#d62728", "#7f7f7f"])
    plt.title("Price Segments")
    plt.xlabel("Segment")
    plt.ylabel("Doctors")
    plt.tight_layout()
    plt.savefig(PLOT_PRICE_SEGMENTS, dpi=150)
    plt.close()

    plt.figure(figsize=(8, 5))
    ratings = df["rating"].dropna()
    if ratings.empty:
        plt.text(0.5, 0.5, "No rating data", ha="center", va="center")
        plt.xlim(0, 1)
    else:
        plt.hist(ratings, bins=20, color="#17becf", edgecolor="black")
    plt.title("Ratings Distribution")
    plt.xlabel("Rating")
    plt.ylabel("Frequency")
    plt.tight_layout()
    plt.savefig(PLOT_RATINGS_DISTRIBUTION, dpi=150)
    plt.close()

    plt.figure(figsize=(7, 5))
    src = df["source"].value_counts()
    src.plot(kind="bar", color=["#9467bd", "#8c564b"])
    plt.title("Source Comparison")
    plt.xlabel("Source")
    plt.ylabel("Doctors")
    plt.tight_layout()
    plt.savefig(PLOT_SOURCE_COMPARISON, dpi=150)
    plt.close()

    # consultation formats
    formats = []
    for value in df["consultation_format"].fillna(""):
        if not value:
            continue
        for part in value.split(";"):
            p = clean_text(part)
            if p:
                formats.append(p)

    plt.figure(figsize=(8, 5))
    if formats:
        fmt_counts = pd.Series(formats).value_counts()
        fmt_counts.plot(kind="bar", color="#bcbd22")
        plt.ylabel("Doctors")
    else:
        plt.text(0.5, 0.5, "No consultation format data", ha="center", va="center")
        plt.xlim(0, 1)
    plt.title("Consultation Formats")
    plt.xlabel("Format")
    plt.tight_layout()
    plt.savefig(PLOT_FORMATS, dpi=150)
    plt.close()


def make_reports(
    merged_df: pd.DataFrame,
    doctorline_df: pd.DataFrame,
    doctor_kz_df: pd.DataFrame,
    discovery: DoctorLineDiscovery,
    duplicates_before: int,
    duplicates_removed: int,
    missing_by_field: dict[str, int],
    outliers: dict[str, dict[str, float]],
    price_boundaries: dict[str, float],
) -> None:
    total_records = len(merged_df)
    source_counts = merged_df["source"].value_counts().to_dict()

    low_fill_fields = [
        field for field, miss in missing_by_field.items() if total_records > 0 and (miss / total_records) >= 0.9
    ]

    # Data cleaning report
    lines = []
    lines.append("# Data Cleaning Report")
    lines.append("")
    lines.append("## 1. Sources and collected volume")
    lines.append("")
    lines.append(f"- doctor.kz records loaded: **{len(doctor_kz_df)}**")
    lines.append(f"- doctorline.kz records collected: **{len(doctorline_df)}**")
    lines.append(f"- merged dataset size after cleaning: **{total_records}**")
    lines.append("")
    lines.append("DoctorLine public crawling checks:")
    lines.append(f"- checked list pages: {discovery.checked_list_pages}")
    lines.append(f"- checked specialization filters: {len(discovery.specialization_ids)}")
    lines.append(f"- checked search queries: {discovery.checked_search_queries}")
    lines.append(f"- checked date queries: {discovery.checked_date_queries}")
    lines.append(f"- checked direct profile IDs: {discovery.checked_direct_ids}")
    lines.append(f"- unique doctor profile URLs discovered: {len(discovery.discovered_profile_urls)}")
    lines.append("")
    lines.append("## 2. Standardized schema")
    lines.append("")
    for field in UNIFIED_FIELDS:
        lines.append(f"- {field}")
    lines.append("")
    lines.append("## 3. Cleaning steps")
    lines.append("")
    lines.append("- Trimmed extra whitespaces and removed invisible/garbage characters.")
    lines.append("- Kept records with missing fields (no record was dropped because of null values).")
    lines.append("- Unified city labels for common variants (e.g., Нур-Султан -> Астана).")
    lines.append("- Unified specialization string formatting.")
    lines.append("- Converted numeric columns (`price`, `rating`, `reviews_count`, `experience_years`) to numeric types.")
    lines.append("- Deduplicated records by `source + profile_url` (fallback to profile attributes if URL missing).")
    lines.append("")
    lines.append("## 4. Duplicates")
    lines.append("")
    lines.append(f"- Potential duplicates before drop: **{duplicates_before}**")
    lines.append(f"- Removed duplicates: **{duplicates_removed}**")
    lines.append(f"- Final records after deduplication: **{total_records}**")
    lines.append("")
    lines.append("## 5. Missing values by field")
    lines.append("")
    lines.append("| Field | Missing | Missing % |")
    lines.append("|---|---:|---:|")
    for field in UNIFIED_FIELDS:
        miss = missing_by_field[field]
        pct = (miss / total_records * 100) if total_records else 0.0
        lines.append(f"| {field} | {miss} | {pct:.2f}% |")
    lines.append("")

    if low_fill_fields:
        lines.append("Fields that are almost empty (>=90% missing):")
        for field in low_fill_fields:
            lines.append(f"- {field}")
        lines.append("")

    lines.append("## 6. Outlier check (1.5xIQR)")
    lines.append("")
    lines.append("| Field | Q1 | Q3 | IQR | Lower | Upper | Outliers |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for field in ["price", "rating", "reviews_count", "experience_years"]:
        info = outliers[field]
        lines.append(
            "| {field} | {q1:.3f} | {q3:.3f} | {iqr:.3f} | {lower:.3f} | {upper:.3f} | {count} |".format(
                field=field,
                q1=info["q1"] if not math.isnan(info["q1"]) else float("nan"),
                q3=info["q3"] if not math.isnan(info["q3"]) else float("nan"),
                iqr=info["iqr"] if not math.isnan(info["iqr"]) else float("nan"),
                lower=info["lower"] if not math.isnan(info["lower"]) else float("nan"),
                upper=info["upper"] if not math.isnan(info["upper"]) else float("nan"),
                count=info["outliers_count"],
            )
        )
    lines.append("")

    lines.append("## 7. Limitations")
    lines.append("")
    lines.append("- DoctorLine publicly exposes a limited number of doctor profiles without authorization.")
    lines.append("- City is often missing on DoctorLine profile pages.")
    lines.append("- Price and consultation format are mostly available from DoctorLine, while market structure is mostly from doctor.kz.")
    lines.append("")

    CLEANING_REPORT.write_text("\n".join(lines), encoding="utf-8")

    # Market intelligence report
    top_specs = merged_df["specialization"].replace("", np.nan).dropna().value_counts().head(10)
    top_cities = merged_df["city"].replace("", np.nan).dropna().value_counts().head(10)

    source_stats = (
        merged_df.groupby("source")
        .agg(
            doctors=("doctor_name", "count"),
            avg_rating=("rating", "mean"),
            median_price=("price", "median"),
            median_experience=("experience_years", "median"),
        )
        .reset_index()
    )

    report = []
    report.append("# Market Intelligence Report (Kazakhstan Online Doctor / Telemedicine)")
    report.append("")
    report.append("## 1. Data scope")
    report.append("")
    report.append(f"- Total cleaned records: **{total_records}**")
    report.append(f"- doctor.kz: **{source_counts.get('doctor.kz', 0)}**")
    report.append(f"- doctorline.kz: **{source_counts.get('doctorline.kz', 0)}**")
    report.append("")
    report.append("## 2. Price segmentation")
    report.append("")
    report.append("Price segments are data-driven from `price` quartiles:")
    report.append(f"- low-priced: <= {price_boundaries['q1']:.0f} KZT")
    report.append(f"- middle-priced: > {price_boundaries['q1']:.0f} and <= {price_boundaries['q2']:.0f} KZT")
    report.append(f"- high-priced: > {price_boundaries['q2']:.0f} and <= {price_boundaries['q3']:.0f} KZT")
    report.append(f"- luxury: > {price_boundaries['q3']:.0f} KZT")
    report.append("")
    report.append("## 3. Competitive landscape")
    report.append("")
    report.append("### Top specializations")
    for name, count in top_specs.items():
        report.append(f"- {name}: {count}")
    report.append("")
    report.append("### Top cities")
    for name, count in top_cities.items():
        report.append(f"- {name}: {count}")
    report.append("")
    report.append("### Source comparison")
    for _, row in source_stats.iterrows():
        avg_rating = row["avg_rating"] if not pd.isna(row["avg_rating"]) else float("nan")
        median_price = row["median_price"] if not pd.isna(row["median_price"]) else float("nan")
        median_exp = row["median_experience"] if not pd.isna(row["median_experience"]) else float("nan")
        report.append(
            f"- {row['source']}: doctors={int(row['doctors'])}, "
            f"avg_rating={avg_rating:.2f}, median_price={median_price:.0f}, median_experience={median_exp:.0f}"
        )
    report.append("")
    report.append("## 4. Promising market segment")
    report.append("")
    report.append(
        "The most promising segment is **middle-priced online consultations**, "
        "combining affordability with scalable telemedicine workflow. "
        "This segment can target urban users with repeat consultations and preventive care."
    )
    report.append("")
    report.append("## 5. Competitor weaknesses")
    report.append("")
    report.append("- Limited publicly visible doctor supply on DoctorLine without authorization.")
    report.append("- Sparse profile completeness (city, richer trust signals, detailed outcomes).")
    report.append("- Low observable review activity for many profiles.")
    report.append("- Potential dependence on a narrow set of specialties.")
    report.append("")
    report.append("## 6. Strategy for online-doctor business idea")
    report.append("")
    report.append("- Start with high-demand broad specialties and strong triage process.")
    report.append("- Offer transparent tiered pricing and clear online/offline consultation options.")
    report.append("- Build trust via verified credentials, structured reviews, and response-time SLAs.")
    report.append("- Expand regionally city-by-city using city specialization gaps from market structure data.")
    report.append("- Add post-consultation pathways: e-prescriptions, follow-up reminders, symptom monitoring.")
    report.append("")

    MARKET_REPORT.write_text("\n".join(report), encoding="utf-8")

    # Answer #1 text
    answer1 = []
    answer1.append("Ответ №1")
    answer1.append("")
    answer1.append("Для анализа рынка онлайн-доктора в Казахстане я использовал два открытых источника данных:")
    answer1.append("1) doctor.kz (основная рыночная структура, большой объем врачей),")
    answer1.append("2) doctorline.kz (дополнительный источник по цене, формату консультации и конкурентной модели).")
    answer1.append("")
    answer1.append("Что было сделано:")
    answer1.append(f"- Загружено из doctor.kz: {len(doctor_kz_df)} записей.")
    answer1.append(f"- Собрано из doctorline.kz: {len(doctorline_df)} записей (публично доступный максимум без авторизации).")
    answer1.append("- Оба источника приведены к единой схеме полей:")
    answer1.append("  doctor_name, specialization, city, clinic, rating, reviews_count, experience_years, price, consultation_format, source, profile_url.")
    answer1.append("- Записи с отсутствующими полями не удалялись, пустые значения сохранены.")
    answer1.append("")
    answer1.append("Очистка данных:")
    answer1.append("- Удалены лишние пробелы и служебные символы.")
    answer1.append("- Унифицировано написание части городов и специализаций.")
    answer1.append(f"- Найдено потенциальных дублей: {duplicates_before}, удалено: {duplicates_removed}.")
    answer1.append("- Посчитаны пропуски по каждому полю (см. data_cleaning_report.md).")
    answer1.append("- Проверены выбросы по методу 1.5xIQR для полей price, rating, reviews_count, experience_years.")
    answer1.append("")
    answer1.append("Результат объединения:")
    answer1.append(f"- Итоговый очищенный датасет merged_cleaned_dataset.csv: {total_records} записей.")
    answer1.append("")
    answer1.append("Ограничения:")
    answer1.append("- DoctorLine без авторизации отдает ограниченное число публичных профилей, поэтому основной объем структуры рынка формируется doctor.kz.")
    answer1.append("")
    answer1.append("Ссылка на данные и код: [ВСТАВЬТЕ_ССЫЛКУ_НА_GITHUB_РЕПОЗИТОРИЙ]")
    ANSWER1_TXT.write_text("\n".join(answer1), encoding="utf-8")

    # Answer #2 text
    answer2 = []
    answer2.append("Ответ №2")
    answer2.append("")
    answer2.append("Анализ конкурентной среды:")
    answer2.append("- На рынке выделяются платформы каталожного типа с различной глубиной профилей врачей и разной прозрачностью цен.")
    answer2.append("- doctor.kz дает широкую структуру по специализациям и городам, а doctorline.kz дает полезные сигналы по формату и ценам консультаций.")
    answer2.append("")
    answer2.append("Ключевые потребители:")
    answer2.append("- Городские пациенты 20-45 лет, которым важны скорость записи, доступная цена и онлайн-формат.")
    answer2.append("- Пациенты с повторными обращениями (терапевтический и семейный контур).")
    answer2.append("")
    answer2.append("Технологический уровень конкурентов:")
    answer2.append("- Базовый уровень: каталог врачей, запись, личный кабинет.")
    answer2.append("- Продвинутый уровень: цифровой triage, прозрачные SLA, интеграция follow-up сервисов.")
    answer2.append("")
    answer2.append("SWOT (кратко):")
    answer2.append("- Strengths: большой спрос на дистанционные консультации, масштабируемость онлайн-канала.")
    answer2.append("- Weaknesses: фрагментация данных и доверия, неравномерная полнота профилей.")
    answer2.append("- Opportunities: среднеценовой сегмент, подписочная модель повторных консультаций, регионы.")
    answer2.append("- Threats: ценовая конкуренция, регуляторные требования, зависимость от каналов привлечения.")
    answer2.append("")
    answer2.append("Перспективный сегмент:")
    answer2.append("- middle-priced онлайн-консультации с акцентом на регулярные обращения и удержание пациента.")
    answer2.append("")
    answer2.append("Слабые стороны конкурентов:")
    answer2.append("- Ограниченная прозрачность части метрик качества и результата консультаций.")
    answer2.append("- Неравномерная детализация профилей врачей и отзывов.")
    answer2.append("- Ограниченная наблюдаемость полного предложения на отдельных площадках без авторизации.")
    answer2.append("")
    answer2.append("Стратегия развития моей идеи онлайн-доктора:")
    answer2.append("- Старт в массовом сегменте с прозрачной тарифной сеткой и четким позиционированием онлайн-first.")
    answer2.append("- Поднять доверие через верификацию, стандартизированный профиль врача и контроль ответа/качества.")
    answer2.append("- Расширять предложение по специализациям и городам на основе data-driven gap-анализа.")
    answer2.append("- Добавить инструменты удержания: follow-up, напоминания, маршрут пациента после консультации.")
    answer2.append("")
    answer2.append("Пояснение к дашбордам:")
    answer2.append("- top_cities/top_specializations показывают структуру конкуренции.")
    answer2.append("- price_segments и ratings_distribution показывают ценовой и качественный профиль рынка.")
    answer2.append("- source_comparison и consultation_formats показывают различия источников и каналов обслуживания.")
    ANSWER2_TXT.write_text("\n".join(answer2), encoding="utf-8")


def main() -> int:
    doctorline_df, discovery = scrape_doctorline()
    doctor_kz_df = normalize_doctor_kz()

    combined = pd.concat([doctor_kz_df, doctorline_df], ignore_index=True)
    combined = normalize_text_columns(combined)

    # Track duplicates before dropping
    dup_key = combined["source"].astype(str) + "|" + combined["profile_url"].astype(str)
    duplicates_before = int(dup_key.duplicated().sum())

    combined = combined.loc[~dup_key.duplicated()].reset_index(drop=True)
    duplicates_removed = duplicates_before

    # Fallback dedupe for missing URLs
    no_url_mask = combined["profile_url"].eq("")
    if no_url_mask.any():
        subset_cols = ["doctor_name", "specialization", "city", "clinic", "source"]
        before = len(combined)
        combined = combined.drop_duplicates(subset=["profile_url"] + subset_cols, keep="first")
        duplicates_removed += before - len(combined)

    numeric_df = convert_numeric_columns(combined)

    missing_by_field = {}
    for field in UNIFIED_FIELDS:
        if field in {"rating", "reviews_count", "experience_years", "price"}:
            missing_by_field[field] = int(numeric_df[field].isna().sum())
        else:
            missing_by_field[field] = int(numeric_df[field].astype(str).str.strip().eq("").sum())

    outliers = {
        "price": count_outliers_iqr(numeric_df["price"]),
        "rating": count_outliers_iqr(numeric_df["rating"]),
        "reviews_count": count_outliers_iqr(numeric_df["reviews_count"]),
        "experience_years": count_outliers_iqr(numeric_df["experience_years"]),
    }

    segmented_df, boundaries = assign_price_segments(numeric_df)

    # Save merged dataset in required schema (+ price_segment as analytical helper)
    export_df = segmented_df.copy()
    for column in ["rating", "reviews_count", "experience_years", "price"]:
        export_df[column] = export_df[column].apply(lambda x: "" if pd.isna(x) else x)

    export_cols = UNIFIED_FIELDS + ["price_segment"]
    export_df[export_cols].to_csv(MERGED_CSV, index=False, encoding="utf-8-sig")

    generate_visualizations(segmented_df)

    make_reports(
        merged_df=segmented_df,
        doctorline_df=doctorline_df,
        doctor_kz_df=doctor_kz_df,
        discovery=discovery,
        duplicates_before=duplicates_before,
        duplicates_removed=duplicates_removed,
        missing_by_field=missing_by_field,
        outliers=outliers,
        price_boundaries=boundaries,
    )

    # Persist run summary for easier audit
    summary = {
        "doctor_kz_records": int(len(doctor_kz_df)),
        "doctorline_records": int(len(doctorline_df)),
        "merged_records": int(len(segmented_df)),
        "doctorline_discovered_profile_urls": int(len(discovery.discovered_profile_urls)),
        "doctorline_checked_list_pages": discovery.checked_list_pages,
        "doctorline_checked_specialization_filters": len(discovery.specialization_ids),
        "doctorline_checked_search_queries": discovery.checked_search_queries,
        "doctorline_checked_date_queries": discovery.checked_date_queries,
        "doctorline_checked_direct_ids": discovery.checked_direct_ids,
    }
    (BASE_DIR / "assignment2_run_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print("Assignment 2 pipeline completed.")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
