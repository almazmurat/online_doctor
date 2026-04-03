# Data Cleaning Report

## 1. Sources and volumes

- doctor.kz: **2000** records
- ok.i-teka.kz: **395** records
- idoctor.kz: **281** records
- doctorline.kz(existing): **4** records
- merged_cleaned_dataset.csv: **2680** records

## 2. Site assessment summary

| Site | Status | Profile links (landing) | Page param | Score | Note |
|---|---:|---:|---|---:|---|
| https://doctorline.kz/doctors | 200 | 8 | True | 45 | Candidate for scraping. |
| https://ok.i-teka.kz/doctors | 200 | 468 | True | 85 | Candidate for scraping. |
| https://portal.metaclinic.kz/?m=Doctors | 200 | 0 | True | 40 | No substantial doctor listing links on landing page. |
| https://yesmed.kz/vse-vrachi | 200 | 0 | True | 40 | No substantial doctor listing links on landing page. |
| https://doq.kz/ | 200 | 0 | True | 40 | No substantial doctor listing links on landing page. |
| https://www.103.kz/list/konsultatsia-allergologa-online/kazakhstan/ | 200 | 0 | False | 20 | No substantial doctor listing links on landing page. |
| https://viamed.kz/doctor | 200 | 47 | True | 55 | Candidate for scraping. |
| https://docok.kz/vrach/ | 200 | 5 | True | 40 | Candidate for scraping. |
| https://idoctor.kz/ | 200 | 246 | True | 85 | Candidate for scraping. |

## 3. Cleaning logic

- Trimmed whitespace and removed technical artifacts.
- Kept records with missing fields (no drop due to null fields).
- Standardized common city variants and specialization formatting.
- Converted numeric fields (price, rating, reviews_count, experience_years).
- Deduplicated primarily by source + profile_url, fallback by profile attributes.

## 4. Duplicates

- Removed duplicates: **0**

## 5. Missing values

| Field | Missing | Missing % |
|---|---:|---:|
| doctor_name | 0 | 0.00% |
| specialization | 0 | 0.00% |
| city | 412 | 15.37% |
| clinic | 412 | 15.37% |
| rating | 555 | 20.71% |
| reviews_count | 555 | 20.71% |
| experience_years | 2144 | 80.00% |
| price | 2000 | 74.63% |
| consultation_format | 2281 | 85.11% |
| source | 0 | 0.00% |
| profile_url | 0 | 0.00% |

## 6. Outliers (1.5xIQR)

| Field | Q1 | Q3 | IQR | Lower | Upper | Outliers |
|---|---:|---:|---:|---:|---:|---:|
| price | 3800.000 | 9000.000 | 5200.000 | -4000.000 | 16800.000 | 57 |
| rating | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | 321 |
| reviews_count | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | 322 |
| experience_years | 8.000 | 19.000 | 11.000 | -8.500 | 35.500 | 27 |

## 7. Limitations

- Not all candidate sites are suitable for open reproducible scraping without authorization.
- Price and consultation format are source-dependent and unevenly distributed.
- Some platforms expose catalog links but provide limited structured profile fields.