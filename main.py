import csv
import random
import shutil
import tempfile
import time
import zipfile
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

# Import your models
from tqdm import tqdm

BASE_FOLDER = Path(__file__).parent
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
    "Mozilla/5.0 (Linux; Android 11; SM-G960U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.72 Mobile Safari/537.36",
]

URLS = [
    "https://live.euronext.com/en/ajax/getFactsheetInfoBlock/WARRT/{}-{}/fs_generalinfo_warrants_block",
    "https://live.euronext.com/en/ajax/getFactsheetInfoBlock/WARRT/{}-{}/fs_underlying_block",
]


def load_from_csv_to_db(csv_path: Path) -> dict[str, dict[str, str]]:
    isin_data = {}
    print("Loading ISINs to memory...")
    with csv_path.open(newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=",")
        for row in reader:
            isin = row.pop("ISIN")
            isin_data[isin] = row
    print("Loaded ISINs to memory")
    return isin_data


def extract_isins_from_csvs(path: Path) -> list[tuple[str, str]]:
    print(f"Loading ISINs from {path.name!r}...")
    df = pd.concat([pd.read_csv(file) for file in path.iterdir()])
    df = df.loc[df["VenueOfPublication"].isin(["ETLX", "SEDX"])]
    print(f"ISINs loaded from {path.name!r}")
    return list(
        df[["MifidInstrumentID", "VenueOfPublication"]]
        .drop_duplicates(subset="MifidInstrumentID")
        .itertuples(index=False, name=None),
    )


def extract_from_title(soup: BeautifulSoup, title: str | list[str]) -> str | None:
    strings = title if isinstance(title, list) else [title]
    for string in strings:
        label = soup.find(
            "td",
            string=lambda text: text and string == text,
        )
        if label:
            return label.find_next_sibling("td").get_text(strip=True)
    return None


def extract_issuer(
    isin: str,
    mkt: str,
    already_loaded: dict[str, dict[str, str]],
) -> dict[str, str]:
    if isin in already_loaded:
        return already_loaded[isin]

    folder = BASE_FOLDER / "isins"
    folder.mkdir(parents=True, exist_ok=True)
    file = folder / f"{isin}.txt"
    if file.exists():
        soup = BeautifulSoup(file.read_text(encoding="utf-8"), "lxml")
    else:
        user_agent = random.choice(USER_AGENTS)
        headers = {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "priority": "u=1, i",
            "referer": "https://live.euronext.com/en/product/structured-products/XS2928979447-ETLX/market-information",
            "sec-ch-ua": '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
            "sec-ch-ua-mobile": "?1",
            "sec-ch-ua-platform": '"Android"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": user_agent,
            "x-requested-with": "XMLHttpRequest",
        }
        whole_data = ""
        for url_to_fill in URLS:
            url = url_to_fill.format(isin, mkt)
            try:
                r = requests.get(url, headers=headers, timeout=60)
            except requests.exceptions.ReadTimeout:
                tqdm.write("Ci stanno tracciando! Stacca, stacca!")
                time.sleep(30)
                r = requests.get(url, headers=headers, timeout=60)
            whole_data += r.text
        whole_data = whole_data.strip()
        file.write_text(whole_data, encoding="utf-8")
        soup = BeautifulSoup(whole_data, "lxml")
        t = random.random() * 2 + 1
        time.sleep(t)

    val = {
        "nome": extract_from_title(soup, "Product"),
        "strategy": extract_from_title(soup, "Strategy"),
        "eusipa_code": extract_from_title(soup, "EUSIPA Code"),
        "eusipa_name": extract_from_title(soup, "EUSIPA Name"),
        "issue_price": extract_from_title(soup, "Issue Price"),
        "emittente": extract_from_title(soup, ["Nom de l'émetteur", "Issuer Name"]),
        "sottostanti": extract_from_title(soup, "Name"),
    }
    tqdm.write(f"{isin}: {val}")

    return val


def write_csv_to_isin_info(
    isin_and_mkt: list[tuple[str, str]],
    isin_info_path: Path,
    already_loaded: dict[str, dict[str, str]],
) -> None:
    with isin_info_path.open(mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                "ISIN",
                "Nome",
                "Strategia",
                "EUSIPA Code",
                "EUSIPA Name",
                "Issue Price",
                "Emittente",
                "Sottostanti",
            ],
        )

        for isin, mkt in tqdm(isin_and_mkt):
            output = extract_issuer(isin=isin, mkt=mkt, already_loaded=already_loaded)
            writer.writerow([isin, *output.values()])


def update_mappings(
    isin_info_path: Path,
    type_and_subtype_path: Path,
    issuers_path: Path,
    underlyings_path: Path,
    und_mapping_path: Path,
) -> None:
    update_generic_mapping(
        input_path=isin_info_path,
        output_path=type_and_subtype_path,
        input_col="Nome",
        output_col="Category",
        default_use_same=False,
    )

    update_generic_mapping(
        input_path=isin_info_path,
        output_path=issuers_path,
        input_col="Emittente",
        output_col="Original",
        default_use_same=True,
    )

    update_generic_mapping(
        input_path=underlyings_path,
        output_path=und_mapping_path,
        input_col="Sottostante",
        output_col="Original",
        default_use_same=True,
    )


def summarize_csvs(input_folder: Path, output_folder: Path) -> None:
    for input_file in input_folder.iterdir():
        output_file = output_folder / input_file.with_suffix(".csv").name
        if output_file.exists():
            print(f"{output_file.name!r} already exists, skipping...")
            continue
        with tempfile.TemporaryDirectory() as tmpdir:
            with zipfile.ZipFile(input_file, "r") as z:
                z.extractall(tmpdir)

            # Construct full paths
            src_path = Path(tmpdir) / "Trades_WarrantCertificates.csv"
            input_df = pd.read_csv(
                src_path,
                header=1,
                parse_dates=[1, 2, 3],
                date_format="ISO8601",
            )
        input_df["DayEvent"] = input_df["TradingDateTime"].dt.date
        input_df = (
            input_df.loc[
                (input_df["VenueOfPublication"].isin(["ETLX", "SEDX"]))
                & (
                    input_df["TradingDateTime"].dt.strftime("%Y-%m-%d")
                    == input_file.stem
                )
            ]
            .pivot_table(
                index=["MifidInstrumentID", "VenueOfPublication", "DayEvent"],
                values=["MifidQuantity", "MifidNotionalAmount"],
                aggfunc="sum",
            )
            .round(2)
        )
        input_df.to_csv(output_file)
        print(f"Created {output_file.name!r}")


def download_file(save_folder: Path) -> None:
    zip_path = BASE_FOLDER / "downloaded_file.zip"

    url = "https://marketdata.euronext.com/data-reporting-service/trades-file/download"
    original_filename = "Trades_WarrantCertificates.csv"

    data = {
        "userID": "753530",
        "userToken": "wzidZjEnBZWckEigcHGrW2e_LJs2qx-0wAzdTyEU5Gg",
        "fileType": "WarrantCertificates",
    }

    print("Downloading newest file...")
    response = requests.post(url, data=data, timeout=60)

    with zip_path.open("wb") as f:
        f.write(response.content)
    print("Download completed")

    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(tmpdir)

        # Construct full paths
        src_path = Path(tmpdir) / original_filename
        new_filename = pd.read_csv(src_path, header=1, nrows=3)["TradingDateTime"].iloc[
            0
        ][:10]
        dst_path = save_folder / f"{new_filename}.zip"

        # 2. Move & rename
        shutil.move(zip_path, dst_path)
    print(f"Copied file to {dst_path.relative_to(BASE_FOLDER)}")


def create_underlying_table(isin_info_path: Path, output_path: Path) -> None:
    isin_info_df = pd.read_csv(isin_info_path)

    isin_info_df["underlying_list"] = isin_info_df["Sottostanti"].str.split(
        r"(?<!\d)/|/(?!\d)",
        regex=True,
    )

    # Step 2: Explode the new column
    df_long = isin_info_df.explode("underlying_list")

    # Step 3: Optional cleanup
    df_long = (
        df_long[["ISIN", "underlying_list"]]
        .rename(columns={"underlying_list": "Sottostante"})
        .reset_index(drop=True)
    )

    df_long.to_csv(output_path, index=False)


def update_generic_mapping(
    input_path: Path,
    output_path: Path,
    input_col: str,
    output_col: str,
    *,
    default_use_same: bool = True,
) -> None:
    input_df = pd.read_csv(input_path)
    mapping_df = pd.read_csv(output_path)

    all_names = input_df[input_col].str.lower()
    new_names = input_df.loc[
        ~all_names.isin(mapping_df[output_col].str.lower()) & (all_names.notna()),
        input_col,
    ].drop_duplicates()

    if new_names.empty:
        print(
            f"No new {input_col!r} found in {input_path.name!r} not in "
            f"{output_path.name!r}",
        )
        return
    print(
        f"New {input_col!r} found in {input_path.name!r} not in {output_path.name!r}: "
        f"{', '.join(repr(x) for x in new_names.to_list())}",
    )

    mapping_df = pd.concat(
        [
            mapping_df,
            new_names.to_frame(name=output_col).assign(
                **{
                    col: (lambda x: x[output_col] if default_use_same else None)
                    for col in mapping_df.columns
                    if col != output_col
                },
            ),
        ],
    )
    mapping_df.to_csv(output_path, index=False)


def main() -> None:
    input_folder = BASE_FOLDER / "input_csv"
    intermediate_folder = BASE_FOLDER / "intermediate_csv"
    isin_info_path = BASE_FOLDER / "isin_info.csv"
    type_and_subtype_path = BASE_FOLDER / "type_and_subtype.csv"
    underlyings_path = BASE_FOLDER / "underlyings.csv"
    und_mapping_path = BASE_FOLDER / "und_mapping.csv"
    issuers_path = BASE_FOLDER / "issuers.csv"

    # 1. download newest file, saves the .zip in 'input_csv' with name as day
    download_file(save_folder=input_folder)

    # 2. summarize CSVs and extract market (ETLX or SEDX)
    summarize_csvs(input_folder=input_folder, output_folder=intermediate_folder)
    isin_and_mkt = extract_isins_from_csvs(path=intermediate_folder)

    # 3. load existing ISIN info
    loaded_isins = load_from_csv_to_db(csv_path=isin_info_path)

    # 4. compiles 'isin_info.csv', scraping additional data, if needed
    write_csv_to_isin_info(
        isin_and_mkt=isin_and_mkt,
        isin_info_path=isin_info_path,
        already_loaded=loaded_isins,
    )

    # 5. create table for ISIN -> underlyings
    create_underlying_table(isin_info_path=isin_info_path, output_path=underlyings_path)

    # 6. update existing CSVs with newly scraped data
    update_mappings(
        isin_info_path=isin_info_path,
        type_and_subtype_path=type_and_subtype_path,
        issuers_path=issuers_path,
        underlyings_path=underlyings_path,
        und_mapping_path=und_mapping_path,
    )


if __name__ == "__main__":
    main()
