# State Aid

Simple scraping of transparency state aid register from the EU State Aid Transparency Register.

## Environment Configuration

1. Copy the example environment file:
   ```
   cp .env.example .env
   ```

2. Edit the `.env` file with your database credentials and other settings:
   ```
   DB_NAME=state_aid_db
   DB_USER=postgres
   DB_PASSWORD=your_password
   DB_HOST=localhost
   DB_PORT=5432
   ```

## Usage

Run the scraper:
```
python main.py run
```

The scraper can be run recurrently (e.g., as a scheduled job) as it will:
1. Skip records that have already been inserted
2. Report how many new records were inserted and how many duplicates were skipped

## Implementation Details

The state aid data doesn't have a single unique identifier, so we use a composite key of these fields:
- SA Number (sa_number)
- Reference Number (ref_no)
- National ID (national_id)
- Beneficiary Name (beneficiary_name)
- Date Granted (date_granted)

This combination uniquely identifies each award. The scraper uses PostgreSQL's `ON CONFLICT DO NOTHING` to skip duplicates when inserting data.