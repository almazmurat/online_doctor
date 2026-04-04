"""
analyze_dataset.py
Статистический анализ датасета doctor_kz_doctors.csv
Результаты сохраняются в dataset_statistics.json и dataset_statistics.md
"""

import csv
import json
import math
from collections import Counter, defaultdict

# --------------------------------------------------------------------------- #
# 1. Загрузка данных
# --------------------------------------------------------------------------- #
DATASET = "doctor_kz_doctors.csv"

with open(DATASET, encoding="utf-8", newline="") as f:
    reader = csv.DictReader(f)
    rows = list(reader)

FIELDS = [
    "doctor_name", "specialization", "experience_years",
    "clinic", "city", "rating", "reviews_count", "profile_url",
]

total_records = len(rows)

# --------------------------------------------------------------------------- #
# 2. Уникальные значения
# --------------------------------------------------------------------------- #
unique_doctors = len({r["doctor_name"].strip() for r in rows if r["doctor_name"].strip()})
unique_specializations = len({r["specialization"].strip() for r in rows if r["specialization"].strip()})
unique_cities = len({r["city"].strip() for r in rows if r["city"].strip()})

# --------------------------------------------------------------------------- #
# 3. Заполненность полей
# --------------------------------------------------------------------------- #
field_filled = {}
field_empty  = {}
for field in FIELDS:
    filled = sum(1 for r in rows if r.get(field, "").strip())
    field_filled[field] = filled
    field_empty[field]  = total_records - filled

# --------------------------------------------------------------------------- #
# 4. Топ-10 специализаций
# --------------------------------------------------------------------------- #
spec_counter = Counter()
for r in rows:
    spec = r["specialization"].strip()
    if spec:
        spec_counter[spec] += 1
top10_specializations = spec_counter.most_common(10)

# --------------------------------------------------------------------------- #
# 5. Топ-10 городов
# --------------------------------------------------------------------------- #
city_counter = Counter()
for r in rows:
    city = r["city"].strip()
    if city:
        city_counter[city] += 1
top10_cities = city_counter.most_common(10)

# --------------------------------------------------------------------------- #
# 6. Стаж (experience_years)
# --------------------------------------------------------------------------- #
exp_values = []
for r in rows:
    v = r["experience_years"].strip()
    if v:
        try:
            exp_values.append(float(v))
        except ValueError:
            pass

def median(lst):
    s = sorted(lst)
    n = len(s)
    if n == 0:
        return None
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2

exp_mean   = round(sum(exp_values) / len(exp_values), 2) if exp_values else None
exp_median = median(exp_values)
exp_min    = min(exp_values) if exp_values else None
exp_max    = max(exp_values) if exp_values else None

# --------------------------------------------------------------------------- #
# 7. Рейтинг (rating)
# --------------------------------------------------------------------------- #
rating_all = []
for r in rows:
    v = r["rating"].strip()
    if v:
        try:
            rating_all.append(float(v))
        except ValueError:
            pass

no_rating_count = sum(1 for r in rows if not r["rating"].strip() or r["rating"].strip() == "0")
rating_nonzero  = [x for x in rating_all if x > 0]

rating_mean   = round(sum(rating_nonzero) / len(rating_nonzero), 3) if rating_nonzero else None
rating_median = median(rating_nonzero)

# Распределение по диапазонам
rating_distribution = {
    "0 (нет рейтинга или 0)": 0,
    "0.1–1.9": 0,
    "2.0–2.9": 0,
    "3.0–3.9": 0,
    "4.0–4.4": 0,
    "4.5–4.9": 0,
    "5.0":     0,
}
for v in rating_all:
    if v == 0:
        rating_distribution["0 (нет рейтинга или 0)"] += 1
    elif v < 2:
        rating_distribution["0.1–1.9"] += 1
    elif v < 3:
        rating_distribution["2.0–2.9"] += 1
    elif v < 4:
        rating_distribution["3.0–3.9"] += 1
    elif v < 4.5:
        rating_distribution["4.0–4.4"] += 1
    elif v < 5:
        rating_distribution["4.5–4.9"] += 1
    else:
        rating_distribution["5.0"] += 1

# --------------------------------------------------------------------------- #
# 8. Отзывы (reviews_count)
# --------------------------------------------------------------------------- #
reviews_all = []
for r in rows:
    v = r["reviews_count"].strip()
    if v:
        try:
            reviews_all.append(int(float(v)))
        except ValueError:
            pass

reviews_with_comments = sum(1 for x in reviews_all if x > 0)
reviews_mean   = round(sum(reviews_all) / len(reviews_all), 2) if reviews_all else None
reviews_median = median(reviews_all)

# --------------------------------------------------------------------------- #
# 9. Топ-10 клиник
# --------------------------------------------------------------------------- #
clinic_counter = Counter()
for r in rows:
    clinic = r["clinic"].strip()
    if clinic:
        clinic_counter[clinic] += 1
top10_clinics = clinic_counter.most_common(10)

# --------------------------------------------------------------------------- #
# 10. Сводная таблица specialization × city (только топовые)
# --------------------------------------------------------------------------- #
TOP_SPEC = [s for s, _ in top10_specializations[:5]]
TOP_CITY = [c for c, _ in top10_cities[:5]]

cross_table = defaultdict(lambda: defaultdict(int))
for r in rows:
    spec = r["specialization"].strip()
    city = r["city"].strip()
    if spec in TOP_SPEC and city in TOP_CITY:
        cross_table[spec][city] += 1

# --------------------------------------------------------------------------- #
# Сборка результата
# --------------------------------------------------------------------------- #
stats = {
    "dataset_file": DATASET,
    "total_records": total_records,
    "unique_doctors": unique_doctors,
    "unique_specializations": unique_specializations,
    "unique_cities": unique_cities,
    "field_filled": field_filled,
    "field_empty": field_empty,
    "top10_specializations": [{"specialization": s, "count": c} for s, c in top10_specializations],
    "top10_cities": [{"city": c, "count": n} for c, n in top10_cities],
    "top10_clinics": [{"clinic": c, "count": n} for c, n in top10_clinics],
    "experience_years": {
        "filled": len(exp_values),
        "empty": total_records - len(exp_values),
        "mean": exp_mean,
        "median": exp_median,
        "min": exp_min,
        "max": exp_max,
    },
    "rating": {
        "filled_total": len(rating_all),
        "nonzero_count": len(rating_nonzero),
        "zero_or_missing": no_rating_count,
        "mean_nonzero": rating_mean,
        "median_nonzero": rating_median,
        "distribution": rating_distribution,
    },
    "reviews_count": {
        "filled": len(reviews_all),
        "with_reviews_gt0": reviews_with_comments,
        "mean": reviews_mean,
        "median": reviews_median,
    },
    "cross_table_spec_x_city": {
        spec: dict(cities) for spec, cities in cross_table.items()
    },
}

# --------------------------------------------------------------------------- #
# Сохранение JSON
# --------------------------------------------------------------------------- #
with open("dataset_statistics.json", "w", encoding="utf-8") as f:
    json.dump(stats, f, ensure_ascii=False, indent=2)

print("✓ dataset_statistics.json сохранён")

# --------------------------------------------------------------------------- #
# Формирование Markdown
# --------------------------------------------------------------------------- #
def pct(n, total):
    return f"{round(n / total * 100, 1)}%"

lines = []

lines += [
    "# Статистический анализ датасета doctor_kz_doctors.csv",
    "",
    "## 1. Общее описание",
    "",
    f"| Параметр | Значение |",
    f"|---|---|",
    f"| Файл | `doctor_kz_doctors.csv` |",
    f"| Всего записей | **{total_records:,}** |",
    f"| Уникальных врачей | **{unique_doctors:,}** |",
    f"| Уникальных специализаций | **{unique_specializations:,}** |",
    f"| Уникальных городов | **{unique_cities:,}** |",
    "",
]

lines += [
    "## 2. Заполненность полей",
    "",
    "| Поле | Заполнено | Пропущено | % заполнения |",
    "|---|---|---|---|",
]
for field in FIELDS:
    f = field_filled[field]
    e = field_empty[field]
    lines.append(f"| `{field}` | {f} | {e} | {pct(f, total_records)} |")

lines += [
    "",
    "## 3. Топ-10 специализаций",
    "",
    "| # | Специализация | Количество врачей |",
    "|---|---|---|",
]
for i, (s, c) in enumerate(top10_specializations, 1):
    lines.append(f"| {i} | {s} | {c} |")

lines += [
    "",
    "## 4. Топ-10 городов",
    "",
    "| # | Город | Количество врачей |",
    "|---|---|---|",
]
for i, (c, n) in enumerate(top10_cities, 1):
    lines.append(f"| {i} | {c} | {n} |")

lines += [
    "",
    "## 5. Топ-10 клиник",
    "",
    "| # | Клиника | Количество врачей |",
    "|---|---|---|",
]
for i, (c, n) in enumerate(top10_clinics, 1):
    lines.append(f"| {i} | {c} | {n} |")

lines += [
    "",
    "## 6. Статистика стажа (experience_years)",
    "",
    f"| Параметр | Значение |",
    f"|---|---|",
    f"| Заполнено | {len(exp_values)} ({pct(len(exp_values), total_records)}) |",
    f"| Пропущено | {total_records - len(exp_values)} ({pct(total_records - len(exp_values), total_records)}) |",
    f"| Среднее | {exp_mean} лет |",
    f"| Медиана | {exp_median} лет |",
    f"| Минимум | {exp_min} лет |",
    f"| Максимум | {exp_max} лет |",
    "",
]

lines += [
    "## 7. Статистика рейтинга (rating)",
    "",
    f"| Параметр | Значение |",
    f"|---|---|",
    f"| Всего с числовым рейтингом | {len(rating_all)} |",
    f"| С рейтингом > 0 | {len(rating_nonzero)} ({pct(len(rating_nonzero), total_records)}) |",
    f"| Рейтинг = 0 или отсутствует | {no_rating_count} ({pct(no_rating_count, total_records)}) |",
    f"| Среднее (по ненулевым) | {rating_mean} |",
    f"| Медиана (по ненулевым) | {rating_median} |",
    "",
    "### Распределение по диапазонам рейтинга",
    "",
    "| Диапазон | Количество |",
    "|---|---|",
]
for rng, cnt in rating_distribution.items():
    lines.append(f"| {rng} | {cnt} |")

lines += [
    "",
    "## 8. Статистика отзывов (reviews_count)",
    "",
    f"| Параметр | Значение |",
    f"|---|---|",
    f"| Заполнено | {len(reviews_all)} |",
    f"| С хотя бы одним отзывом (> 0) | {reviews_with_comments} ({pct(reviews_with_comments, total_records)}) |",
    f"| Среднее число отзывов | {reviews_mean} |",
    f"| Медиана отзывов | {reviews_median} |",
    "",
]

# Сводная таблица
lines += [
    "## 9. Сводная таблица: Топ-5 специализаций × Топ-5 городов",
    "",
]
header = "| Специализация | " + " | ".join(TOP_CITY) + " |"
sep    = "|---|" + "---|" * len(TOP_CITY)
lines += [header, sep]
for spec in TOP_SPEC:
    row_vals = [str(cross_table[spec].get(city, 0)) for city in TOP_CITY]
    lines.append("| " + spec + " | " + " | ".join(row_vals) + " |")

lines += [
    "",
    "---",
    "",
    "## 10. Краткие выводы",
    "",
    f"- Датасет содержит **{total_records}** записей о врачах, зарегистрированных на платформе doctor.kz.",
    f"- Охватывает **{unique_cities}** городов Казахстана; лидер — **{top10_cities[0][0]}** ({top10_cities[0][1]} врачей).",
    f"- Наиболее распространённая специализация: **{top10_specializations[0][0]}** ({top10_specializations[0][1]} врачей).",
    f"- Средний стаж составляет **{exp_mean}** лет, медиана — **{exp_median}** лет.",
    f"- Значительная часть профилей ({pct(no_rating_count, total_records)}) не имеет рейтинга или имеет рейтинг 0.",
    f"- Среди профилей с указанным рейтингом средний составляет **{rating_mean}** из 5.",
    f"- Лишь {reviews_with_comments} врачей ({pct(reviews_with_comments, total_records)}) имеют хотя бы один отзыв.",
    f"- Поле `experience_years` отсутствует у {field_empty['experience_years']} записей "
        f"({pct(field_empty['experience_years'], total_records)}).",
]

md_text = "\n".join(lines) + "\n"

with open("dataset_statistics.md", "w", encoding="utf-8") as f:
    f.write(md_text)

print("✓ dataset_statistics.md сохранён")

# --------------------------------------------------------------------------- #
# dataset_summary.txt — текст для вставки в учебный report
# --------------------------------------------------------------------------- #
report_text = f"""Статистический анализ собранного датасета

В рамках данной работы был собран датасет с открытого медицинского портала doctor.kz,
содержащий информацию о врачах, зарегистрированных на платформе. Итоговый датасет
включает {total_records} записей (строк) и {len(FIELDS)} полей: имя врача, специализация,
стаж работы, клиника, город, рейтинг, количество отзывов и URL профиля.

Географический охват

Датасет охватывает {unique_cities} уникальных городов Казахстана. Наибольшее
представительство врачей сосредоточено в городе {top10_cities[0][0]} — {top10_cities[0][1]}
записей ({pct(top10_cities[0][1], total_records)}). На втором месте расположен
{top10_cities[1][0]} ({top10_cities[1][1]} врачей, {pct(top10_cities[1][1], total_records)}),
на третьем — {top10_cities[2][0]} ({top10_cities[2][1]} врачей).
Это отражает неравномерное распределение медицинских специалистов по регионам страны.

Специализации

В датасете зафиксировано {unique_specializations} уникальных специализаций.
Наиболее распространённой является "{top10_specializations[0][0]}"
({top10_specializations[0][1]} врачей). Также часто встречаются специализации
"{top10_specializations[1][0]}" ({top10_specializations[1][1]}) и
"{top10_specializations[2][0]}" ({top10_specializations[2][1]}).

Стаж работы

Поле «стаж работы» (experience_years) заполнено у {len(exp_values)} записей
({pct(len(exp_values), total_records)}); у {total_records - len(exp_values)} профилей
({pct(total_records - len(exp_values), total_records)}) стаж не указан.
Среди заполненных значений средний стаж составляет {exp_mean} лет,
медиана — {exp_median} лет. Минимальное значение — {int(exp_min)} год,
максимальное — {int(exp_max)} лет, что свидетельствует о широком диапазоне
профессионального опыта среди представленных специалистов.

Рейтинг

Рейтинг врача представлен числовым значением от 0 до 5. Значение 0 или отсутствующий
рейтинг зафиксированы у {no_rating_count} врачей ({pct(no_rating_count, total_records)}),
что, вероятно, означает отсутствие достаточного числа оценок. Среди профилей
с ненулевым рейтингом средний показатель равен {rating_mean} балла,
медиана — {rating_median} балла. Большинство оценённых врачей ({rating_distribution.get("5.0", 0)}
профилей) имеют максимальный рейтинг 5.0.

Отзывы

Хотя бы один отзыв имеют {reviews_with_comments} врачей
({pct(reviews_with_comments, total_records)}). Среднее количество отзывов
на профиль составляет {reviews_mean}, медианное — {reviews_median}.
Столь низкие показатели объясняются высокой долей профилей без отзывов.

Качество данных

Анализ заполненности полей показал, что наиболее полными являются
поля «имя врача», «специализация», «город» и «URL профиля» (заполнены в подавляющем
большинстве записей). Поля «стаж» и «клиника» имеют значительную долю пропусков.
Это является характерной особенностью данных, собранных методом веб-скрейпинга:
не все медицинские учреждения и врачи заполняют профиль полностью.

Выводы

Собранный датасет позволяет получить репрезентативное представление о структуре
рынка медицинских услуг Казахстана в разрезе специализаций и географии. Несмотря
на наличие пропусков в ряде полей, объём данных ({total_records} записей) достаточен
для выявления ключевых тенденций. Алматы является абсолютным лидером по числу
зарегистрированных врачей, а стоматология — наиболее распространённой
специализацией на платформе doctor.kz.
"""

with open("dataset_summary.txt", "w", encoding="utf-8") as f:
    f.write(report_text)

print("✓ dataset_summary.txt сохранён")
print()
print("=== Ключевые числа ===")
print(f"  Всего записей:          {total_records}")
print(f"  Уникальных врачей:      {unique_doctors}")
print(f"  Специализаций:          {unique_specializations}")
print(f"  Городов:                {unique_cities}")
print(f"  Средний стаж:           {exp_mean} лет")
print(f"  Медиана стажа:          {exp_median} лет")
print(f"  Средний рейтинг (>0):   {rating_mean}")
print(f"  Врачей с отзывами:      {reviews_with_comments} ({pct(reviews_with_comments, total_records)})")
