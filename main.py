import contextlib
import csv
import logging
import random
import shutil
import tempfile
import time
import zipfile
from collections import Counter
from collections.abc import Sequence
from datetime import date, datetime
from logging.handlers import RotatingFileHandler
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


class TqdmLoggingHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            tqdm.write(msg)  # Use tqdm.write instead of print
            self.flush()
        except Exception:
            self.handleError(record)


logger = logging.getLogger(__name__)


def default_wait_time_gen() -> float:
    return random.random() * 2 + 1


def load_from_csv_to_db(csv_path: Path) -> dict[str, dict[str, str]]:
    isin_data: dict[str, dict[str, str]] = {}
    logger.info("Loading ISINs metadata to memory...")
    if not csv_path.exists():
        csv_path.write_text(
            "ISIN,Nome,Strategy,EUSIPA Code,EUSIPA Name,Issue Price,Emittente,Issue Date,Expiry Date,Sottostanti,Coupon P.A.,Coupon Frequency,Autocall Frequency,Autocall First Date,Autocall Decrement,Autocall Initial Trigger,Autocall Minimum Trigger\n",
            encoding="utf-8-sig",
        )
        return isin_data

    with csv_path.open(newline="", encoding="utf-8-sig") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=",")
        for row in reader:
            isin = row.pop("ISIN")
            isin_data[isin] = row
    logger.info("Loaded ISINs metadata to memory")
    return isin_data


def extract_isins_from_csvs(path: Path) -> list[tuple[str, str]]:
    logger.info("Loading ISINs from %s...", repr(path.name))
    df = pd.concat([pd.read_csv(file) for file in path.iterdir()])
    df = df.loc[df["VenueOfPublication"].isin(["ETLX", "SEDX"])]
    logger.info("ISINs loaded from %s", repr(path.name))
    return list(
        df[["MifidInstrumentID", "VenueOfPublication"]]
        .drop_duplicates(subset="MifidInstrumentID")
        .itertuples(index=False, name=None),
    )


def extract_from_title(
    soup: BeautifulSoup,
    title: str | list[str],
    *,
    datetime_format: str | None = None,
) -> str | None:
    strings = title if isinstance(title, list) else [title]
    for string in strings:
        label = soup.find(
            "td",
            string=lambda text: text and string == text,
        )
        if label:
            value = label.find_next_sibling("td").get_text(strip=True)
            if datetime_format:
                value = datetime.strptime(value, datetime_format).date()
            return value
    return None


def parse_date(date_str: str) -> date | None:
    """Parses a date string (DD/MM/YYYY) into a datetime object."""
    if date_str:
        try:
            return datetime.strptime(date_str, "%d/%m/%Y").date()
        except ValueError:
            return None
    return None


def determine_frequency(dates: Sequence[date]) -> str:
    """Determine the frequency of events (e.g., weekly, monthly) given a list of dates.

    It looks for the most common difference in days between consecutive dates.
    """
    if len(dates) < 2:
        return "N/A (less than 2 dates)"

    # Filter out None dates and sort to ensure proper calculation
    valid_dates = sorted([d for d in dates if d is not None])

    if len(valid_dates) < 2:
        return "N/A (less than 2 valid dates)"

    diffs = [
        (valid_dates[i] - valid_dates[i - 1]).days for i in range(1, len(valid_dates))
    ]

    if not diffs:
        return "N/A (no valid date differences)"

    # Identify the most common difference in days
    most_common_diff = Counter(diffs).most_common(1)

    if not most_common_diff:
        return "Irregular"  # Should not happen if diffs is not empty

    common_diff_days = most_common_diff[0][0]

    # Map common differences to frequencies with a small tolerance
    if 4 <= common_diff_days <= 10:  # Around 7 days
        return "Weekly"
    if 26 <= common_diff_days <= 35:  # Around 30 days
        return "Monthly"
    if 80 <= common_diff_days <= 100:  # Around 90-91 days (3 months)
        return "Quarterly"
    if 160 <= common_diff_days <= 200:  # Around 182-183 days (6 months)
        return "Semiannual"
    if 350 <= common_diff_days <= 380:  # Around 365 days (12 months)
        return "Annual"
    return f"Irregular (most common diff: {common_diff_days} days)"


def parse_cd(soup: BeautifulSoup) -> dict[str, str | None]:
    sottostanti = get_sottostanti(soup)
    # --- 1. Derive Coupon P.A. ---
    coupon_pa = None
    coupon_frequency = None
    autocall_decrement = None
    minimum_autocall_trigger = None
    initial_autocall_trigger = None
    autocall_frequency = None
    autocall_dates = []

    # Find the "Date rilevamento" (Detection dates) table
    # This table is identified by the heading "Date rilevamento" inside a panel-info div
    date_relevamento_title = soup.find(
        "h3",
        class_="panel-title",
        string=lambda text: text and "date rilevamento" in text.lower(),
    )
    if date_relevamento_title is None:
        return {"sottostanti": sottostanti}

    date_relevamento_panel = date_relevamento_title.find_parent(
        "div",
        class_="panel panel-info",
    )

    if (
        date_relevamento_panel
        and date_relevamento_panel.find("h3", class_="panel-title")
        and date_relevamento_panel.find("h3", class_="panel-title").get_text(strip=True)
        == "Date rilevamento"
    ):
        coupon_autocall_table = date_relevamento_panel.find(
            "table",
            class_="table table-striped",
        )

    if coupon_autocall_table:
        headers = [
            th.get_text(strip=True)
            for th in coupon_autocall_table.find("thead").find_all("th")
        ]

        date_idx = -1
        coupon_idx = -1
        trigger_autocall_idx = -1

        date_idx = headers.index("DATA RILEVAMENTO")
        coupon_idx = headers.index("CEDOLA")
        trigger_autocall_idx = headers.index("TRIGGER AUTOCALLABLE")

        coupon_dates = []
        coupon_amounts = []
        autocall_schedule_entries = []

        for row in coupon_autocall_table.find("tbody").find_all("tr"):
            cells = row.find_all("td")
            if len(cells) > max(date_idx, coupon_idx, trigger_autocall_idx):
                # Extract Coupon Data
                date_str = cells[date_idx].get_text(strip=True)
                coupon_text = cells[coupon_idx].get_text(strip=True)

                parsed_date = parse_date(date_str)
                if parsed_date and coupon_text:  # Get the first non-empty coupon rate
                    coupon_dates.append(parsed_date)
                    with contextlib.suppress(ValueError):
                        coupon_amounts.append(
                            float(
                                coupon_text.replace("%", "").replace(",", ".").strip(),
                            ),
                        )

                # Extract Autocall Data
                autocall_trigger_text = cells[trigger_autocall_idx].get_text(strip=True)
                if autocall_trigger_text:  # Only record if autocall trigger is present
                    try:
                        autocall_trigger = float(
                            autocall_trigger_text.replace("%", "")
                            .replace(",", ".")
                            .strip(),
                        )
                        autocall_schedule_entries.append(
                            {"date": parsed_date, "trigger": autocall_trigger},
                        )
                    except ValueError:
                        pass  # Ignore unparseable autocall triggers

        coupon_frequency = determine_frequency(coupon_dates)
        coupon_amount = (
            sum(coupon_amounts) / len(coupon_amounts) if coupon_amounts else None
        )

        if coupon_amount:
            multiplier = 1
            if "Weekly" in coupon_frequency:
                multiplier = 52
            elif "Monthly" in coupon_frequency:
                multiplier = 12
            elif "Quarterly" in coupon_frequency:
                multiplier = 4
            elif "Semiannual" in coupon_frequency:
                multiplier = 2
            elif "Annual" in coupon_frequency:
                multiplier = 1
            # If irregular or unknown, can't derive annual rate easily
            elif "Irregular" in coupon_frequency or "N/A" in coupon_frequency:
                coupon_pa = f"N/A (Irregular/Unknown Coupon Frequency), Period Rate: {coupon_amounts[0]:.2f}%"

            if isinstance(coupon_pa, str):  # If it's already a descriptive string
                pass
            else:
                coupon_pa = coupon_amount * multiplier

        # Sort autocall entries by date for correct decrement calculation
        autocall_schedule_entries.sort(
            key=lambda x: x["date"] if x["date"] else datetime.min,
        )

        # Collect valid triggers for decrement calculation
        valid_autocall_triggers_values = [
            entry["trigger"]
            for entry in autocall_schedule_entries
            if entry["trigger"] is not None
        ]

        if len(valid_autocall_triggers_values) >= 2:
            # Assuming decrement is consistent and calculated from the first two available triggers
            # If triggers are descending, it's current - next. If ascending, it's next - current.
            # For 'step down' it should be current - next.
            autocall_decrement = (
                valid_autocall_triggers_values[0] - valid_autocall_triggers_values[1]
            )

        if valid_autocall_triggers_values:
            minimum_autocall_trigger = min(valid_autocall_triggers_values)
            initial_autocall_trigger = valid_autocall_triggers_values[0]

        autocall_dates = [
            entry["date"]
            for entry in autocall_schedule_entries
            if entry["date"] is not None
        ]
        autocall_frequency = determine_frequency(autocall_dates)

    return {
        "sottostanti": sottostanti,
        "coupon_pa": (
            round(coupon_pa, 2) if isinstance(coupon_pa, (float, int)) else None
        ),
        "coupon_frequency": coupon_frequency,
        "autocall_frequency": autocall_frequency,
        "autocall_first_date": autocall_dates[0] if autocall_dates else None,
        "autocall_decrement": autocall_decrement,
        "autocall_initial_trigger": initial_autocall_trigger,
        "autocall_minimum_trigger": minimum_autocall_trigger,
    }


def get_sottostanti(soup: BeautifulSoup) -> str | None:
    companies = []

    try:
        # 1. Find the h3 tag that contains "Scheda Sottostante"
        #    The lambda function handles potential extra text or non-breaking spaces around the title.
        h3_title_tag = soup.find(
            "h3",
            string=lambda text: text and "Scheda Sottostante" in text,
        )
        # 2. Go up the tree to find the parent div with class 'panel panel-info'.
        #    Use find_parent() instead of find_ancestor()
        panel_div = h3_title_tag.find_parent("div", class_="panel-info")

        # 3. Find the table within this identified panel div
        table = panel_div.find("table")

        # 4. Find all <tr> tags within the <tbody> of the table
        rows = table.find("tbody").find_all("tr")

        # 5. Iterate through rows and extract the text from the first <td>
        for row in rows:
            first_td = row.find("td")
            if first_td:
                companies.append(first_td.get_text(strip=True))
    except AttributeError:
        pass

    return "/".join(companies) if companies else None


def extract_from_cd(isin: str) -> tuple[dict[str, str | None], bool]:
    folder = BASE_FOLDER / "cd"
    folder.mkdir(parents=True, exist_ok=True)
    file = folder / f"{isin}.txt"
    made_request = False
    if file.exists():
        soup = BeautifulSoup(file.read_text(encoding="utf-8"), "lxml")
    else:
        try:
            r = requests.get(
                f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}",
                headers=get_headers(),
                timeout=60,
            )
            made_request = True
            r.raise_for_status()
        except requests.RequestException as e:
            logger.info(
                "Error fetching data for ISIN %s from CD: %s",
                repr(isin),
                repr(e),
            )
            return {}, made_request
        file.write_text(r.text, encoding="utf-8")
        soup = BeautifulSoup(r.text, "lxml")

    return parse_cd(soup), made_request


def get_headers() -> dict[str, str]:
    user_agent = random.choice(USER_AGENTS)
    return {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "priority": "u=1, i",
        "sec-ch-ua": '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
        "sec-ch-ua-mobile": "?1",
        "sec-ch-ua-platform": '"Android"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": user_agent,
        "x-requested-with": "XMLHttpRequest",
    }


def extract_data_for_isin(
    isin: str,
    mkt: str,
    already_loaded: dict[str, dict[str, str]],
) -> dict[str, str] | None:
    if isin.strip() in already_loaded:
        return already_loaded[isin]

    folder = BASE_FOLDER / "isins"
    folder.mkdir(parents=True, exist_ok=True)
    file = folder / f"{isin}.txt"
    t = None
    if file.exists():
        soup = BeautifulSoup(file.read_text(encoding="utf-8"), "lxml")
    else:
        whole_data = ""
        for url_to_fill in URLS:
            url = url_to_fill.format(isin, mkt)
            try:
                r = requests.get(url, headers=get_headers(), timeout=60)
                r.raise_for_status()
            except requests.exceptions.ReadTimeout:
                logger.info("Ci stanno tracciando! Stacca, stacca!")
                time.sleep(30)
                r = requests.get(url, headers=get_headers(), timeout=60)
            except requests.exceptions.HTTPError:
                logger.info("Error for ISIN %s %s, skipping...", repr(isin), repr(mkt))
                return None
            whole_data += r.text
        whole_data = whole_data.strip()
        file.write_text(whole_data, encoding="utf-8")
        soup = BeautifulSoup(whole_data, "lxml")
        t = default_wait_time_gen()
        time.sleep(t)

    val = {
        "nome": extract_from_title(soup, "Product"),
        "strategy": extract_from_title(soup, "Strategy"),
        "eusipa_code": extract_from_title(soup, "EUSIPA Code"),
        "eusipa_name": extract_from_title(soup, "EUSIPA Name"),
        "issue_price": extract_from_title(soup, "Issue Price"),
        "emittente": extract_from_title(
            soup,
            ["Nom de l'émetteur", "Issuer Name", "Nom émetteur"],
        ),
        "issue_date": extract_from_title(
            soup,
            "Issue Date",
            datetime_format="%d/%m/%Y",
        ),
        "expiry_date": extract_from_title(
            soup,
            "Expiry Date",
            datetime_format="%d/%m/%Y",
        ),
    }
    if val["eusipa_code"]:  # and val["eusipa_code"].startswith("1"):
        data, made_cd_request = extract_from_cd(isin)
        val.update(data)
        if made_cd_request is True and t is None:
            time.sleep(default_wait_time_gen())

    if not val.get("sottostanti"):
        val["sottostanti"] = extract_from_title(soup, "Name")

    # tqdm.write(f"{isin}: {val}")

    return val


def write_csv_to_isin_info(
    isin_and_mkt: list[tuple[str, str]],
    isin_info_path: Path,
    already_loaded: dict[str, dict[str, str]],
) -> None:
    old_isins = set(already_loaded.keys())
    isins_to_write = [
        (isin, mkt) for isin, mkt in isin_and_mkt if isin not in old_isins
    ]
    with isin_info_path.open(mode="a", newline="", encoding="utf-8-sig") as file:
        writer = csv.writer(file)
        for isin, mkt in tqdm(
            isins_to_write,
            bar_format="{l_bar}{bar}| {n:,}/{total:,} [{elapsed}<{remaining}, {rate_fmt}]",
        ):
            output = extract_data_for_isin(
                isin=isin,
                mkt=mkt,
                already_loaded=already_loaded,
            )
            if output is None:
                continue
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
        if (
            output_file.exists()
            and input_file.stat().st_mtime < output_file.stat().st_mtime
        ):
            logger.info("%s already exists, skipping...", repr(output_file.name))
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
        input_df.to_csv(output_file, encoding="utf-8-sig")
        logger.info("Created %s", repr(output_file.name))


def download_file(save_folder: Path) -> None:
    zip_path = BASE_FOLDER / "downloaded_file.zip"

    url = "https://marketdata.euronext.com/data-reporting-service/trades-file/download"
    original_filename = "Trades_WarrantCertificates.csv"

    data = {
        "userID": "753530",
        "userToken": "wzidZjEnBZWckEigcHGrW2e_LJs2qx-0wAzdTyEU5Gg",
        "fileType": "WarrantCertificates",
    }

    logger.info("Downloading newest file...")
    response = requests.post(url, data=data, timeout=60)

    with zip_path.open("wb") as f:
        f.write(response.content)
    logger.info("Download completed")

    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(tmpdir)

        # Construct full paths
        src_path = Path(tmpdir) / original_filename
        all_days = (
            pd.read_csv(
                src_path,
                header=1,
                parse_dates=[1, 2, 3],
                date_format="ISO8601",
            )["TradingDateTime"]
            .dt.strftime("%Y-%m-%d")
            .drop_duplicates()
        )
        logger.info("Found the following days in the file: %s", all_days.tolist())
        for day in all_days:
            dst_path = save_folder / f"{day}.zip"
            shutil.copy(zip_path, dst_path)
            logger.info("Copied file to %s", dst_path.relative_to(BASE_FOLDER))

    zip_path.unlink(missing_ok=True)


def create_underlying_table(isin_info_path: Path, output_path: Path) -> None:
    isin_info_df = pd.read_csv(isin_info_path, encoding="utf-8-sig")

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

    df_long["Sottostante"] = df_long["Sottostante"].str.strip()

    df_long.to_csv(output_path, index=False, encoding="utf-8-sig")


def update_generic_mapping(
    input_path: Path,
    output_path: Path,
    input_col: str,
    output_col: str,
    *,
    default_use_same: bool = True,
) -> None:
    """Update a mapping CSV file by adding new values from an input CSV file
    that are not already present in the mapping.

    This function reads two CSV files:
    - `input_path`: contains source data with potential new values.
    - `output_path`: a mapping file containing previously mapped values.

    It identifies new, case-insensitive and whitespace-trimmed unique entries
    in `input_col` of `input_path` that are not yet present in `output_col`
    of `output_path`, and appends them to the mapping file. If `default_use_same`
    is True, new rows will have the same value for all other columns (excluding `output_col`)
    as the new `output_col` value. Otherwise, those columns are set to `None`.

    Parameters
    ----------
        input_path (Path): Path to the input CSV file containing potential new entries.
        output_path (Path): Path to the mapping CSV file to be updated.
        input_col (str): Column name in `input_path` to check for new values.
        output_col (str): Column name in `output_path` to compare against and append to.
        default_use_same (bool, optional): If True, fill other columns with the same
            value as `output_col`. If False, fill them with `None`. Defaults to True.

    Returns
    -------
        None

    """
    input_df = pd.read_csv(input_path, encoding="utf-8-sig")
    mapping_df = pd.read_csv(output_path, encoding="utf-8-sig")

    all_names = input_df[input_col].str.lower().str.strip()
    new_names = input_df.loc[
        ~all_names.isin(mapping_df[output_col].str.lower().str.strip())
        & (all_names.notna()),
        input_col,
    ].drop_duplicates()
    new_names = new_names.to_frame(name=output_col)
    new_names["lower"] = new_names[output_col].str.lower()
    new_names = new_names.drop_duplicates(subset="lower").drop(columns=["lower"])

    new_names_list = new_names[output_col].to_list()

    if new_names.empty:
        logger.info(
            "No new %r found in %r not in %r",
            input_col,
            input_path.name,
            output_path.name,
        )
        return
    logger.info(
        "%d new %r found in %r not in %r: %s",
        len(new_names_list),
        input_col,
        input_path.name,
        output_path.name,
        ", ".join(repr(x) for x in new_names_list),
    )
    other_cols = [col for col in mapping_df.columns if col != output_col]
    new_names = new_names.assign(
        **{
            col: (lambda x: x[output_col] if default_use_same else None)
            for col in other_cols
        },
    )

    mapping_df = pd.concat([mapping_df, new_names])
    mapping_df.to_csv(output_path, index=False, encoding="utf-8-sig")


def update_all() -> None:
    input_folder = BASE_FOLDER / "input_csv"
    intermediate_folder = BASE_FOLDER / "intermediate_csv"
    isin_info_path = BASE_FOLDER / "isin_info.csv"
    type_and_subtype_path = BASE_FOLDER / "type_and_subtype.csv"
    underlyings_path = BASE_FOLDER / "underlyings.csv"
    und_mapping_path = BASE_FOLDER / "und_mapping.csv"
    issuers_path = BASE_FOLDER / "issuers.csv"

    input_folder.mkdir(parents=True, exist_ok=True)
    intermediate_folder.mkdir(parents=True, exist_ok=True)

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


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            TqdmLoggingHandler(),
            RotatingFileHandler(
                BASE_FOLDER / "app.log",
                maxBytes=5 * 1024 * 1024,  # 5 MB max size per file
                backupCount=3,  # Keep up to 3 backup files
            ),
        ],
    )
    update_all()


if __name__ == "__main__":
    main()
