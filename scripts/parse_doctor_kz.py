#!/usr/bin/env python3

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import json
import re
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Iterable

import requests
from bs4 import BeautifulSoup


BASE_URL = "https://doctor.kz"
LIST_URL = f"{BASE_URL}/doctors"
API_URL = f"{BASE_URL}/api/w-front/ru/v1/doctors"
OUTPUT_CSV = "doctor_kz_doctors.csv"
OUTPUT_JSON = "doctor_kz_doctors.json"
OUTPUT_XML = "doctor_kz_doctors.xml"
FIELDS = [
    "doctor_name",
    "specialization",
    "experience_years",
    "clinic",
    "city",
    "rating",
    "reviews_count",
    "profile_url",
]
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Parse publicly available doctor data from doctor.kz/doctors."
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Limit the number of API batches for a shorter run.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.05,
        help="Delay in seconds between API requests.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="HTTP timeout in seconds.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Requested API page size. The public endpoint currently caps this at 20.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Number of parallel API requests for the full scrape.",
    )
    return parser.parse_args()


def fetch_json(skip: int, limit: int, timeout: float) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(1, 6):
        try:
            response = requests.get(
                API_URL,
                headers=HEADERS,
                params={"skip": skip, "limit": limit},
                timeout=timeout,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as error:
            last_error = error
            if attempt == 5:
                break
            time.sleep(1.5 * attempt)
    raise RuntimeError(f"Failed to fetch API batch skip={skip}, limit={limit}: {last_error}")


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def join_texts(values: Iterable[str], separator: str = " • ") -> str:
    cleaned = [clean_text(value) for value in values if clean_text(value)]
    return separator.join(cleaned)


def extract_city(address_text: str) -> str:
    if not address_text:
        return ""
    return clean_text(address_text.split(",", 1)[0])


def extract_experience_years(record: dict[str, Any]) -> str:
    description_html = record.get("DESCRIPTION") or record.get("SHORT_DESCRIPTION") or ""
    if not description_html:
        return ""

    description_text = clean_text(BeautifulSoup(description_html, "html.parser").get_text(" ", strip=True))
    patterns = [
        r"(\d+)\s*[-–]?летн(?:ий|его|яя)\s+опыт",
        r"опыт\s+работы\s*(?:более\s*)?(\d+)\s*лет",
        r"стаж\s*(?:работы)?\s*[:\-]?\s*(\d+)",
        r"(\d+)\s*лет\s+опыт",
    ]
    for pattern in patterns:
        match = re.search(pattern, description_text, re.IGNORECASE)
        if match:
            return match.group(1)
    return ""


def extract_specialization(record: dict[str, Any]) -> str:
    specialty_list = record.get("specialty_list") or []
    if specialty_list:
        return join_texts(item.get("NAME", "") for item in specialty_list)
    return clean_text(record.get("specialties", ""))


def extract_clinic(record: dict[str, Any]) -> str:
    added_companies = record.get("added_companies") or []
    if added_companies:
        return clean_text(added_companies[0].get("COMPANY_NAME", ""))
    return ""


def record_to_output(record: dict[str, Any]) -> dict[str, str]:
    city_name = clean_text(record.get("CITY_NAME", ""))
    if not city_name:
        city_name = extract_city(clean_text(record.get("FULL_ADDRESS", "")))

    rating_value = record.get("RATING")
    rating = ""
    if rating_value not in (None, ""):
        rating = clean_text(str(rating_value))

    reviews_count = record.get("NUMBER_OF_COMMENTS")

    return {
        "doctor_name": clean_text(record.get("COMPANY_NAME", "")),
        "specialization": extract_specialization(record),
        "experience_years": extract_experience_years(record),
        "clinic": extract_clinic(record),
        "city": city_name,
        "rating": rating,
        "reviews_count": "" if reviews_count in (None, "") else str(reviews_count),
        "profile_url": f"{BASE_URL}/doctors/{record.get('COMPANY_CODE', '')}" if record.get("COMPANY_CODE") else "",
    }


def save_csv(records: list[dict[str, str]], output_path: Path) -> None:
    with output_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(records)


def save_json(records: list[dict[str, str]], output_path: Path) -> None:
    with output_path.open("w", encoding="utf-8") as json_file:
        json.dump(records, json_file, ensure_ascii=False, indent=2)


def save_xml(records: list[dict[str, str]], output_path: Path) -> None:
    root = ET.Element("doctors")
    for record in records:
        doctor_element = ET.SubElement(root, "doctor")
        for field in FIELDS:
            child = ET.SubElement(doctor_element, field)
            child.text = record.get(field, "")

    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")
    tree.write(output_path, encoding="utf-8", xml_declaration=True)


def deduplicate(records: list[dict[str, str]]) -> list[dict[str, str]]:
    seen_urls: set[str] = set()
    unique_records: list[dict[str, str]] = []
    for record in records:
        profile_url = record.get("profile_url", "")
        dedupe_key = profile_url or "|".join(record.get(field, "") for field in FIELDS)
        if dedupe_key in seen_urls:
            continue
        seen_urls.add(dedupe_key)
        unique_records.append(record)
    return unique_records


def main() -> int:
    args = parse_args()
    output_dir = Path(__file__).resolve().parent

    first_batch = fetch_json(skip=0, limit=args.limit, timeout=args.timeout)
    total_value = first_batch.get("total")
    if isinstance(total_value, int) and not isinstance(total_value, bool):
        total_records = total_value
    else:
        total_detail = first_batch.get("total_detail") or {}
        total_records = sum(value for value in total_detail.values() if isinstance(value, int))

    batch_size = len(first_batch.get("search_results", [])) or max(1, args.limit)
    total_batches = max(1, (total_records + batch_size - 1) // batch_size)
    if args.max_pages is not None:
        total_batches = min(total_batches, args.max_pages)

    all_records = [record_to_output(item) for item in first_batch.get("search_results", [])]
    failed_batches: list[int] = []
    print(
        f"Fetched batch 1/{total_batches}: {len(all_records)} raw records",
        file=sys.stderr,
    )

    def fetch_batch(batch_number: int) -> tuple[int, dict[str, Any]]:
        if args.delay > 0:
            time.sleep(args.delay)
        return (
            batch_number,
            fetch_json(
                skip=(batch_number - 1) * batch_size,
                limit=args.limit,
                timeout=args.timeout,
            ),
        )

    if total_batches > 1:
        completed_batches = 1
        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
            futures = {
                executor.submit(fetch_batch, batch_number): batch_number
                for batch_number in range(2, total_batches + 1)
            }

            for future in as_completed(futures):
                batch_number = futures[future]
                try:
                    _, batch = future.result()
                except RuntimeError as error:
                    failed_batches.append(batch_number)
                    print(f"Skipped batch {batch_number}: {error}", file=sys.stderr)
                    completed_batches += 1
                    continue

                batch_records = [record_to_output(item) for item in batch.get("search_results", [])]
                all_records.extend(batch_records)
                completed_batches += 1
                if completed_batches % 50 == 0 or completed_batches == total_batches:
                    print(
                        f"Fetched batch {completed_batches}/{total_batches}: total raw records {len(all_records)}",
                        file=sys.stderr,
                    )

    unique_records = deduplicate(all_records)
    save_csv(unique_records, output_dir / OUTPUT_CSV)
    save_json(unique_records, output_dir / OUTPUT_JSON)
    save_xml(unique_records, output_dir / OUTPUT_XML)

    if failed_batches:
        print(
            f"Completed with {len(failed_batches)} skipped batches: {failed_batches[:20]}",
            file=sys.stderr,
        )
    print(f"Saved {len(unique_records)} unique doctor records to {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())