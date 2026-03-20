# assignment3_online_doctor

Небольшой учебный проект для парсинга открытых данных со страницы doctor.kz/doctors.

## Что делает скрипт

Скрипт `parse_doctor_kz.py` проходит по страницам каталога врачей на `https://doctor.kz/doctors`, извлекает данные из карточек врачей и сохраняет результат сразу в три формата:

- `doctor_kz_doctors.csv`
- `doctor_kz_doctors.json`
- `doctor_kz_doctors.xml`

Если у конкретного врача часть информации отсутствует, соответствующее поле сохраняется пустым.

## Какие поля собираются

- `doctor_name`
- `specialization`
- `experience_years`
- `clinic`
- `city`
- `rating`
- `reviews_count`
- `profile_url`

## Как запустить

1. Перейдите в папку проекта:

```bash
cd ~/assignment3_online_doctor
```

2. Активируйте виртуальное окружение:

```bash
source .venv/bin/activate
```

3. При необходимости установите зависимости:

```bash
pip install -r requirements.txt
```

4. Запустите парсер:

```bash
python parse_doctor_kz.py
```

Для короткого тестового запуска можно ограничить число страниц:

```bash
python parse_doctor_kz.py --max-pages 5
```

## Какие файлы создаются

После запуска скрипт создаёт:

- `doctor_kz_doctors.csv`
- `doctor_kz_doctors.json`
- `doctor_kz_doctors.xml`

## Примечания

- Скрипт использует `requests` и `beautifulsoup4`.
- Данные берутся только из открытых страниц без авторизации.
- Поле `rating` может быть пустым, если на карточке врача рейтинг не указан.