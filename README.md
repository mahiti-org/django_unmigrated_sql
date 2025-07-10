# django-unmigrated-sql

A Django management command to execute and fake all unmigrated SQL migrations for any or all apps, ignoring errors and not using transactions. Useful for advanced migration troubleshooting and legacy database integration.

## Features
- Runs unmigrated SQL for all or a specific app
- Ignores SQL errors
- Marks migrations as fake
- No transaction wrapping

## Installation

    pip install django-unmigrated-sql

## Django Integration

Add to your `INSTALLED_APPS`:

    INSTALLED_APPS = [
        ...
        'django_unmigrated_sql',
        ...
    ]

## Usage

Run for all apps:

    python manage.py unmigrated-sql

Run for a specific app:

    python manage.py unmigrated-sql <app_label>

## License

MIT

---

Maintained by [mahiti.org](https://mahiti.org) | Contact: opensource@mahiti.org 