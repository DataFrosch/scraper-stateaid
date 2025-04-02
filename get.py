import re
from requests import Session
from lxml.html import fromstring
from time import sleep


def configure(
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
            "CountryEIB",
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
        ("countries", "CountryEIB"),
    ]

    response = session.post(
        "https://webgate.ec.europa.eu/competition/transparency/public/search",
        data=data,
    )


data = {
    "resetSearch": "true",
    "countries": [
        "CountryEIB",
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
    "currency": "LOCAL",
    "dateGrantedFrom": "",
    "dateGrantedTo": "",
    "grantingAuthorityNames": "",
    "entrustedEntities": "",
    "financialIntermediaries": "",
    "offset": 0,
    "max": 100,
}

session = Session()

while True:
    print(f"getting offset {data['offset']} yay!")
    response = session.post(
        "https://webgate.ec.europa.eu/competition/transparency/public/search/results?sort=serverReference&order=asc",
        data=data,
    )
    if (
        "Please choose a language" in response.text
        or "currently unable to handle the request" in response.text
    ):
        print("Cookie expired or overload, retrying configuring")
        configure(session)
    else:
        with open(f"rawdata/{data['offset']}.html", "w") as fh:
            fh.write(response.text)

        data["offset"] += 100

        # Get max offset to stop at some point
        doc = fromstring(response.text)
        max = int(doc.findall(".//a[@class='step']")[-1].text) * 100

        if data["offset"] > max:
            break
