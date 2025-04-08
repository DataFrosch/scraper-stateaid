import re
from requests import Session
from lxml.html import fromstring
from time import sleep
import os
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
import click


def configure_session(
    session,
):
    params = {
        "_countries": [],
        "_grantingAuthorityRegions": [],
        "grantingAuthorityRegions": [],
        "resetSearch": "true",
        "_selectAll": [
            "",
            "",
        ],
        "countries": [
            "CountryAUT",
            "CountryBEL",
            "CountryBGR",
            "CountryHRV",
            "CountryCYP",
            "CountryCZE",
            "CountryDNK",
            "CountryEST",
            "CountryEIB",
            "CountryFIN",
            "CountryFRA",
            "CountryDEU",
            "CountryGRC",
            "CountryHUN",
            "CountryISL",
            "CountryIRL",
            "CountryITA",
            "CountryLVA",
            "CountryLTU",
            "CountryLUX",
            "CountryMLT",
            "CountryNLD",
            "CountryPOL",
            "CountryPRT",
            "CountryROM",
            "CountrySVK",
            "CountrySVN",
            "CountryESP",
            "CountrySWE",
            "CountryGBR",
        ],
        "lang": "en",
    }

    response = session.get(
        "https://webgate.ec.europa.eu/competition/transparency/public",
        params=params,
    )

    try:
        match = re.search("LB_TRANSPARENCY=([^;]+)", response.headers.get("set-cookie"))
        session.cookies.set("LB_TRANSPARENCY", match.group(1))

    except TypeError:
        sleep(100)

        response = session.get(
            "https://webgate.ec.europa.eu/competition/transparency/public",
            params=params,
        )
        match = re.search("LB_TRANSPARENCY=([^;]+)", response.headers.get("set-cookie"))
        session.cookies.set("LB_TRANSPARENCY", match.group(1))

    data = [
        ("resetSearch", "true"),
        ("_countries", ""),
        ("countries", "CountryAUT"),
        ("_countries", ""),
        ("countries", "CountryBEL"),
        ("_countries", ""),
        ("countries", "CountryBGR"),
        ("_countries", ""),
        ("countries", "CountryHRV"),
        ("_countries", ""),
        ("countries", "CountryCYP"),
        ("_countries", ""),
        ("countries", "CountryCZE"),
        ("_countries", ""),
        ("countries", "CountryDNK"),
        ("_countries", ""),
        ("countries", "CountryEST"),
        ("_countries", ""),
        ("countries", "CountryEIB"),
        ("_countries", ""),
        ("countries", "CountryFIN"),
        ("_countries", ""),
        ("countries", "CountryFRA"),
        ("_countries", ""),
        ("countries", "CountryDEU"),
        ("_countries", ""),
        ("countries", "CountryGRC"),
        ("_countries", ""),
        ("countries", "CountryHUN"),
        ("_countries", ""),
        ("countries", "CountryISL"),
        ("_countries", ""),
        ("countries", "CountryIRL"),
        ("_countries", ""),
        ("countries", "CountryITA"),
        ("_countries", ""),
        ("countries", "CountryLVA"),
        ("_countries", ""),
        ("countries", "CountryLTU"),
        ("_countries", ""),
        ("countries", "CountryLUX"),
        ("_countries", ""),
        ("countries", "CountryMLT"),
        ("_countries", ""),
        ("countries", "CountryNLD"),
        ("_countries", ""),
        ("countries", "CountryPOL"),
        ("_countries", ""),
        ("countries", "CountryPRT"),
        ("_countries", ""),
        ("countries", "CountryROM"),
        ("_countries", ""),
        ("countries", "CountrySVK"),
        ("_countries", ""),
        ("countries", "CountrySVN"),
        ("_countries", ""),
        ("countries", "CountryESP"),
        ("_countries", ""),
        ("countries", "CountrySWE"),
        ("_countries", ""),
        ("countries", "CountryGBR"),
    ]

    response = session.post(
        "https://webgate.ec.europa.eu/competition/transparency/public/search",
        data=data,
    )


def scrape_and_process(db_params):
    """Scrape data and directly process it without writing to disk."""
    data = {
        "resetSearch": "true",
        "countries": [
            "CountryAUT",
            "CountryBEL",
            "CountryBGR",
            "CountryHRV",
            "CountryCYP",
            "CountryCZE",
            "CountryDNK",
            "CountryEST",
            "CountryEIB",
            "CountryFIN",
            "CountryFRA",
            "CountryDEU",
            "CountryGRC",
            "CountryHUN",
            "CountryISL",
            "CountryIRL",
            "CountryITA",
            "CountryLVA",
            "CountryLTU",
            "CountryLUX",
            "CountryMLT",
            "CountryNLD",
            "CountryPOL",
            "CountryPRT",
            "CountryROM",
            "CountrySVK",
            "CountrySVN",
            "CountryESP",
            "CountrySWE",
            "CountryGBR",
        ],
        "grantingAuthorityRegions": [],
        "aidMeasureTitle": "",
        "aidMeasureCaseNumber": "",
        "refNo": "",
        "beneficiaryMs-input": "",
        "thirdPartyNonEuCountry-input": "",
        "beneficiaryNationalId": "",
        "beneficiaryName": "",
        "beneficiaryTypes": "",
        "regions-input": "",
        "sectors-input": "",
        "aidInstruments": "",
        "objectives": "",
        "nominalAmountFrom": "",
        "nominalAmountTo": "",
        "grantedAmountFrom": "",
        "grantedAmountTo": "",
        "dateGrantedFrom": "",
        "dateGrantedTo": "",
        "grantingAuthorityNames": "",
        "entrustedEntities": "",
        "financialIntermediaries": "",
        "offset": 0,
        "max": 100,
    }

    session = Session()

    # Connect to the database
    conn, cursor = setup_database(db_params)
    total_rows = 0
    total_pages = 0

    while True:
        click.echo(f"Processing offset {data['offset']}...")
        response = session.post(
            "https://webgate.ec.europa.eu/competition/transparency/public/search/results?sort=serverReference&order=asc",
            data=data,
        )
        if (
            "Please choose a language" in response.text
            or "currently unable to handle the request" in response.text
        ):
            click.echo("Cookie expired or overload, retrying configuration...")
            configure_session(session)
        else:
            # Parse the HTML content directly
            tree = fromstring(response.text)

            # Extract data from the HTML
            data_rows = extract_data_from_html_content(tree)
            total_rows += len(data_rows)
            total_pages += 1

            # Insert into database
            insert_data(conn, cursor, data_rows, f"page_{data['offset']}")

            data["offset"] += 100

            # Get max offset to stop at some point
            max = int(tree.findall(".//a[@class='step']")[-1].text) * 100

            if data["offset"] > max:
                break

    cursor.close()
    conn.close()
    click.echo(
        f"Completed processing {total_pages} pages with {total_rows} total rows."
    )


def clean_text(text):
    """Remove excess whitespace and newlines from text."""
    if text is None:
        return None
    return re.sub(r"\s+", " ", text).strip()


def extract_data_from_html_content(tree):
    """Extract data from an HTML tree containing state aid transparency data."""
    # Find the data table using lxml
    data_table = tree.xpath("//table[@id='resultsTable']")
    if not data_table:
        click.echo("No data table found in HTML content")
        return []

    # Extract headers using lxml
    headers = []
    header_elements = tree.xpath("//table[@id='resultsTable']/thead/tr/th")
    for th in header_elements:
        # Get the text inside the <a> tag if it exists, otherwise get the th text
        a_tag = th.xpath("./a")
        if a_tag:
            header = clean_text(a_tag[0].text_content())
        else:
            header = clean_text(th.text_content())
        headers.append(header)

    # Extract rows using lxml
    rows = []
    row_elements = tree.xpath("//table[@id='resultsTable']/tbody/tr")
    for tr in row_elements:
        row = {}
        cells = tr.xpath("./td")

        for i, cell in enumerate(cells):
            if i < len(headers):
                header = headers[i]

                # Check if the cell has a title attribute (for truncated values)
                title = cell.get("title")
                if title:
                    value = clean_text(title)
                else:
                    value = clean_text(cell.text_content())

                # Special handling for National ID column
                if header == "National ID" and title:
                    row[header] = clean_text(cell.text_content())
                    row[header + " Type"] = clean_text(title)
                # Special handling for amount values
                elif (
                    "Amount" in header
                    or header == "Aid element, expressed as full amount"
                ):
                    # Try to extract the numeric part and currency
                    amount_match = (
                        re.match(r"([\d,.]+)\s*([A-Z]{3})?", value) if value else None
                    )
                    if amount_match:
                        amount = amount_match.group(1)
                        # Remove commas and convert to integer
                        amount = int(float(amount.replace(",", "")))
                        row[header] = amount
                    else:
                        row[header] = None
                else:
                    row[header] = value

        if row:  # Only add non-empty rows
            rows.append(row)

    return rows


def setup_database(db_params):
    """Set up the PostgreSQL database and table."""
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Create table if it doesn't exist
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS state_aid_awards (
        id SERIAL PRIMARY KEY,
        country TEXT,
        aid_measure_title TEXT,
        sa_number TEXT,
        ref_no TEXT,
        national_id TEXT,
        national_id_type TEXT,
        beneficiary_name TEXT,
        beneficiary_type TEXT,
        region TEXT,
        sector TEXT,
        aid_instrument TEXT,
        aid_objectives TEXT,
        nominal_amount NUMERIC,
        aid_element NUMERIC,
        date_granted DATE,
        granting_authority_name TEXT,
        entrusted_entity TEXT,
        financial_intermediaries TEXT,
        published_date DATE,
        beneficiary_ms TEXT,
        third_party_non_eu_country TEXT
    );
    """
    )

    conn.commit()
    return conn, cursor


def insert_data(conn, cursor, data_rows, file_name):
    """Insert extracted data into the database."""
    if not data_rows:
        return

    # Map the column names from HTML to database columns
    column_mapping = {
        "Country": "country",
        "Aid Measure Title": "aid_measure_title",
        "SA.Number": "sa_number",
        "Ref-no.": "ref_no",
        "National ID": "national_id",
        "National ID Type": "national_id_type",
        "Name of the beneficiary": "beneficiary_name",
        "Beneficiary Type": "beneficiary_type",
        "Region": "region",
        "Sector (NACE)": "sector",
        "Aid Instrument": "aid_instrument",
        "Objectives of the Aid": "aid_objectives",
        "Nominal Amount, expressed as full amount": "nominal_amount",
        "Aid element, expressed as full amount": "aid_element",
        "Date of granting": "date_granted",
        "Granting Authority Name": "granting_authority_name",
        "Entrusted Entity": "entrusted_entity",
        "Financial Intermediaries": "financial_intermediaries",
        "Published Date": "published_date",
        "Another Beneficiary Member State": "beneficiary_ms",
        "Third country outside of the EU": "third_party_non_eu_country",
    }

    # Prepare data for insertion
    insert_data = []
    for row in data_rows:
        db_row = {}

        for html_col, db_col in column_mapping.items():
            if html_col in row:
                value = row[html_col]

                # Convert dates to proper format
                if db_col in ["date_granted", "published_date"] and value:
                    # Try to parse date in format DD/MM/YYYY
                    try:
                        parts = value.split("/")
                        if len(parts) == 3:
                            value = f"{parts[2]}-{parts[1]}-{parts[0]}"  # Convert to YYYY-MM-DD
                    except:
                        pass  # Keep original value if parsing fails

                db_row[db_col] = value

        insert_data.append(db_row)

    # Prepare SQL statement with all possible columns
    columns = list(column_mapping.values())
    placeholders = [f"%({col})s" for col in columns]

    sql = f"""
    INSERT INTO state_aid_awards ({', '.join(columns)})
    VALUES ({', '.join(placeholders)})
    """

    # Execute batch insert
    execute_batch(cursor, sql, insert_data, page_size=100)
    conn.commit()
    print(f"Inserted {len(insert_data)} rows from {file_name}")


@click.group()
def cli():
    """CLI for EU State Aid Transparency Register."""
    # Load environment variables
    load_dotenv()


@cli.command()
def run():
    """Scrape data from the EU State Aid Transparency Register and directly import to the database."""
    # Database connection parameters from environment variables
    db_params = {
        "dbname": os.getenv("DB_NAME", "state_aid_db"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", ""),
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432"),
    }

    click.echo("Fetching and importing data directly to database...")
    scrape_and_process(db_params)
    click.echo("Process completed!")


if __name__ == "__main__":
    cli()
