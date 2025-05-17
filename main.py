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
from sqlalchemy import String, create_engine, select
from sqlalchemy.orm import (
    Mapped,
    Session,
    declarative_base,
    mapped_column,
)
from tqdm import tqdm

Base = declarative_base()


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
    "Mozilla/5.0 (Linux; Android 11; SM-G960U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.72 Mobile Safari/537.36",
]

URLS_BY_MKT = {
    "SEDX": "https://www.borsaitaliana.it/borsa/cw-e-certificates/scheda/{}.html?lang=it",
    "ETLX": "https://www.borsaitaliana.it/borsa/cw-e-certificates/eurotlx/scheda/{}.html?lang=it",
}


class Product(Base):
    __tablename__ = "products"

    isin: Mapped[str] = mapped_column(String, primary_key=True)
    nome: Mapped[str] = mapped_column(String)
    emittente: Mapped[str] = mapped_column(String)
    sottostanti: Mapped[str] = mapped_column(String)


engine = create_engine("sqlite:///strumenti.db", echo=False)
Base.metadata.create_all(engine)


def load_from_csv_to_db(csv_path: Path) -> None:
    print("Loading ISINs to database...")
    with csv_path.open(newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=",")
        with Session(engine) as session:
            for row in reader:
                strumento = Product(
                    isin=row["ISIN"],
                    nome=row["Nome"],
                    emittente=row["Emittente"],
                    sottostanti=row["Sottostanti"],
                )
                session.merge(strumento)
            session.commit()
    print("Loaded ISINs to database")


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


def extract_from_title(
    soup: BeautifulSoup,
    title: str,
    *,
    is_title_bold: bool = True,
) -> str | None:
    if not is_title_bold:
        label = soup.find(
            "td",
            string=lambda text: text and title in text,
        )
        if not label:
            return None
        return label.find_next_sibling("td").get_text(strip=True)
    label = soup.find(
        "strong",
        string=lambda text: text and title in text,
    )
    if not label:
        return None
    return label.find_parent("td").find_next_sibling("td").get_text(strip=True)


def extract_issuer(isin: str, mkt: str) -> tuple[str, str, str]:
    with Session(engine) as session:
        query = select(Product).where(Product.isin == isin)
        product = session.execute(query).scalars().first()
        if product:
            return product.nome, product.emittente, product.sottostanti

    folder = Path(__file__).parent / "isins"
    folder.mkdir(parents=True, exist_ok=True)
    file = folder / f"{isin}.txt"
    if file.exists():
        soup = BeautifulSoup(file.read_text(encoding="utf-8"))
    else:
        url_to_fill = URLS_BY_MKT[mkt]
        url = url_to_fill.format(isin)
        user_agent = random.choice(USER_AGENTS)
        headers = {"User-Agent": user_agent}
        try:
            r = requests.get(url, headers=headers, timeout=60)
        except requests.exceptions.ReadTimeout:
            tqdm.write("Ci stanno tracciando! Stacca, stacca!")
            time.sleep(30)
            r = requests.get(url, headers=headers, timeout=60)
        # file.write_text(r.text, encoding="utf-8")
        soup = BeautifulSoup(r.text, "lxml")
        t = random.random() * 3 + 2
        time.sleep(t)

    nome = extract_from_title(soup, "Tipologia ACEPI")
    if not nome:
        nome = f"{extract_from_title(soup, 'Nome Commerciale')}|||{extract_from_title(soup, 'Categoria di Borsa')}|||{extract_from_title(soup, 'Facolt&agrave')}"
    emittente = extract_from_title(soup, "Emittente")
    sottostanti = extract_from_title(soup, "Sottostante")
    tqdm.write(f"{isin}: {nome, emittente, sottostanti}")

    return nome, emittente, sottostanti


def write_csv_to_isin_info(
    isin_and_mkt: list[tuple[str, str]],
    isin_info_path: Path,
) -> None:
    with isin_info_path.open(mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["ISIN", "Nome", "Emittente", "Sottostanti"])

        for isin, mkt in tqdm(isin_and_mkt):
            output = extract_issuer(isin=isin, mkt=mkt)
            writer.writerow([isin, *output])


def insert_mapping(
    input_path: Path,
    isin_info_path: Path,
    mapping_path: Path,
    output_path: Path,
) -> bool:
    input_df = pd.read_csv(
        input_path,
        header=1,
        parse_dates=[1, 2, 3],
        date_format="ISO8601",
    )
    input_df = input_df.loc[input_df["VenueOfPublication"].isin(["ETLX", "SEDX"])]

    isin_info_df = pd.read_csv(isin_info_path)
    mapping_df = pd.read_csv(mapping_path)

    df = input_df.merge(
        isin_info_df.rename(columns={"Nome": "Category"}),
        left_on="MifidInstrumentID",
        right_on="ISIN",
        how="left",
    ).merge(
        mapping_df,
        on="Category",
        how="left",
    )

    dt_cols = df.select_dtypes(include=["datetime64[ns, UTC]"]).columns
    for col in dt_cols:
        df[col] = df[col].dt.tz_localize(None)

    df["DayEvent"] = df["TradingDateTime"].dt.date

    df = df.pivot_table(
        index=[
            "ISIN",
            "Venue",
            "Category",
            "Emittente",
            "Sottostanti",
            "Type",
            "SubType",
            "DayEvent",
        ],
        values=["MifidQuantity", "MifidNotionalAmount"],
        aggfunc="sum",
    ).reset_index()

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Data")

        # Access the workbook and worksheet objects
        workbook = writer.book
        worksheet = writer.sheets["Data"]

        # Define a format for numbers with comma as thousand separator and 2 decimal places
        number_format = workbook.add_format({"num_format": "#,##0.00"})

        # Apply the format to the appropriate columns
        for col_num, value in enumerate(df.columns.values):
            worksheet.set_column(col_num, col_num, None, number_format)


def update_type_mapping(isin_info_path: Path, mapping_path: Path) -> None:
    isin_info_df = pd.read_csv(isin_info_path)
    type_subtype_df = pd.read_csv(mapping_path)

    all_names = isin_info_df["Nome"]
    new_names = isin_info_df.loc[
        ~all_names.isin(type_subtype_df["Category"])
        & (all_names != "None|||None|||None"),
        "Nome",
    ].drop_duplicates()

    print(f"New names found {new_names.to_list()}")

    type_subtype_df = pd.concat(
        [
            type_subtype_df,
            new_names.to_frame(name="Category").assign(Type="NA", SubType="NA"),
        ],
    )
    type_subtype_df.to_csv(mapping_path, index=False)

    issuers_path = isin_info_path.with_name("issuers.csv")
    issuers_df = pd.read_csv(issuers_path)
    new_names = isin_info_df.loc[
        ~isin_info_df["Emittente"].isin(type_subtype_df["Category"])
        & isin_info_df["Emittente"].notna(),
        "Nome",
    ].drop_duplicates()
    if new_names.empty:
        return
    print(f"New issuers found: {new_names.to_list()}")

    issuers_df = pd.concat(
        [
            issuers_df,
            new_names.to_frame(name="Original").assign(Issuer=lambda x: x["Original"]),
        ],
    )
    issuers_df.to_csv(issuers_path, index=False)


def summarize_csvs(input_folder: Path, output_folder: Path) -> None:
    for input_file in input_folder.iterdir():
        output_file = output_folder / input_file.name
        if output_file.exists():
            print(f"{output_file.name!r} already exists, skipping...")
            continue
        input_df = pd.read_csv(
            input_file,
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
    zip_path = Path(__file__).parent / "downloaded_file.zip"

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
        dst_path = save_folder / f"{new_filename}.csv"

        # 2. Move & rename
        shutil.move(src_path, dst_path)
    print(f"Copied file to {dst_path}")


def main() -> None:
    input_folder = Path(__file__).parent / "input_csv"
    intermediate_folder = Path(__file__).parent / "intermediate_csv"
    isin_info_path = Path(__file__).parent / "isin_info.csv"
    mapping_path = Path(__file__).parent / "type_and_subtype.csv"

    # download_file(save_folder=input_folder)

    summarize_csvs(input_folder=input_folder, output_folder=intermediate_folder)
    isin_and_mkt = extract_isins_from_csvs(path=intermediate_folder)

    load_from_csv_to_db(csv_path=isin_info_path)

    write_csv_to_isin_info(isin_and_mkt=isin_and_mkt, isin_info_path=isin_info_path)
    update_type_mapping(isin_info_path=isin_info_path, mapping_path=mapping_path)

    # output_path = Path(__file__).parent / "output.xlsx"
    # insert_mapping(
    #     input_path=input_path,
    #     isin_info_path=isin_info_path,
    #     mapping_path=mapping_path,
    #     output_path=output_path,
    # )


if __name__ == "__main__":
    main()
