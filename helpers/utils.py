from datetime import datetime, date

# formatting - rounding
def format_number(value):
    return f"{value:,}"

def round_to_nearest_five(x):
    return 5 * round(x / 5)

def get_first_day_of_last_month(reference_date: datetime.date, months_ago: int) -> datetime.date:
    year = reference_date.year
    month = reference_date.month - months_ago

    while month < 1:
        month += 12
        year -= 1

    return date(year, month, 1)