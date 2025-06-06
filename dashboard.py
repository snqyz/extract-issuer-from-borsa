import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="ISIN Dashboard", page_icon="📊", layout="wide")

BASE_FOLDER = Path(__file__).parent


@st.cache_data
def load_csv(filename: str, modified_time: float, **kwargs) -> pd.DataFrame:
    print("Loading CSV:", filename)
    path = BASE_FOLDER / filename
    if path.exists():
        return pd.read_csv(path, encoding="utf-8-sig", **kwargs)
    return pd.DataFrame()


def load_data_with_modified(filename: str, **kwargs) -> pd.DataFrame:
    return load_csv(filename, modified_time=os.path.getmtime(filename), **kwargs)


# Load data
sales_data = pd.concat(
    [
        load_data_with_modified(f, parse_dates=["DayEvent"])
        for f in (BASE_FOLDER / "intermediate_csv").iterdir()
    ],
)
isin_info = load_data_with_modified("isin_info.csv")
underlyings = load_data_with_modified("underlyings.csv")
type_and_subtype = load_data_with_modified("type_and_subtype.csv")
issuers = load_data_with_modified("issuers.csv")
und_mapping = load_data_with_modified("und_mapping.csv")


def issuers_page() -> None:
    st.title("Issuers dashboard")

    joined = get_joined_df()

    dates_filter, filter_type, filter_subtype, filter_issuer = get_standard_filters(
        joined,
    )

    # Filter data for the line chart
    filtered_by_date = joined[
        joined["DayEvent"].dt.date.between(
            dates_filter[0],
            dates_filter[1]
            if len(dates_filter) == 2
            else joined["DayEvent"].dt.date.max(),
        )
        & joined["Issuer"].isin(filter_issuer)
    ]

    top_10_issuers = (
        filtered_by_date.groupby("Issuer")["MifidNotionalAmount"]
        .sum()
        .nlargest(10)
        .index.tolist()
    )

    filtered_by_date_issuers = filtered_by_date[
        filtered_by_date["Issuer"].isin(top_10_issuers)
    ]
    filtered_by_subtype = filtered_by_date_issuers[
        filtered_by_date_issuers["SubType"].isin(filter_subtype)
        & filtered_by_date_issuers["Type"].isin(filter_type)
    ]
    aggregated = (
        filtered_by_subtype.groupby(
            ["DayEvent", "Issuer"],
        )
        .agg(
            MifidNotionalAmount=("MifidNotionalAmount", "sum"),
        )
        .reset_index()
    )
    aggregated["Turnover (M)"] = (aggregated["MifidNotionalAmount"] / 1_000_000).apply(
        lambda x: f"{x:,.2f}",
    )

    # ISIN Info Table
    st.subheader("By day")
    fig = px.line(
        data_frame=aggregated,
        x="DayEvent",
        y="Turnover (M)",
        color="Issuer",
        markers=True,
    )
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("By issuer")

    chart_data = filtered_by_subtype.groupby(
        ["Issuer", "SubType"],
    )["MifidNotionalAmount"].sum()
    issuer_order = (
        filtered_by_subtype.groupby("Issuer")["MifidNotionalAmount"]
        .sum()
        .sort_values(ascending=False)
        .index
    )
    chart_data = chart_data * 100 / chart_data.sum()
    chart_data = chart_data.reset_index()

    # Calculate total notional amount per issuer for sorting

    # Create the bar chart with plotly for proper sorting
    fig = px.bar(
        chart_data.rename(columns={"MifidNotionalAmount": "Turnover (%)"}),
        x="Turnover (%)",
        y="Issuer",
        color="SubType",
        orientation="h",
        category_orders={"Issuer": issuer_order},
        custom_data=["SubType"],
    )

    # Format axis and hover labels as percentage
    fig.update_xaxes(ticksuffix="%")
    fig.update_traces(
        hovertemplate="Issuer=%{y}<br>Turnover (%)=%{x:.1f}%<br>SubType=%{customdata[0]}<extra></extra>",
    )

    st.plotly_chart(fig, use_container_width=True)

    st.download_button(
        "Download CSV",
        data=filtered_by_subtype.to_csv(index=False),
        file_name="issuers.csv",
        mime="text/csv",
    )


def products_page() -> None:
    joined = get_joined_df()

    dates_filter, filter_type, filter_subtype, filter_issuer = get_standard_filters(
        joined,
    )

    st.title("Products dashboard")

    filtered_total = joined.loc[
        (
            joined["DayEvent"].dt.date.between(
                dates_filter[0],
                dates_filter[1]
                if len(dates_filter) == 2
                else joined["DayEvent"].dt.date.max(),
            )
        )
        & (joined["Type"].isin(filter_type))
        & (joined["SubType"].isin(filter_subtype))
        & (joined["Issuer"].isin(filter_issuer)),
        [
            "DayEvent",
            "ISIN",
            "Sottostanti",
            "Issuer",
            "Adjusted Turnover",
            "Type",
            "SubType",
        ],
    ]

    st.dataframe(
        filtered_total.groupby(["ISIN", "Issuer", "Sottostanti", "Type", "SubType"])
        .agg({"Adjusted Turnover": "sum"})
        .sort_values(by="Adjusted Turnover", ascending=False)
        .reset_index()
        .assign(
            **{
                "Adjusted Turnover": lambda df: (
                    df["Adjusted Turnover"] / 1_000_000
                ).apply(lambda x: f"{x:,.2f}"),
                "n": lambda df: range(1, len(df) + 1),
            },
        )
        .set_index("n"),
        use_container_width=True,
    )

    st.download_button(
        "Download CSV",
        data=filtered_total.to_csv(index=False),
        file_name="products.csv",
        mime="text/csv",
    )


def get_standard_filters(
    joined: pd.DataFrame,
) -> tuple[list[str], list[str], list[str], list[str]]:
    st.sidebar.header("Filters")

    dates_filter = st.sidebar.date_input(
        "Select days",
        min_value=joined["DayEvent"].min(),
        max_value=joined["DayEvent"].max(),
        value=(joined["DayEvent"].min(), joined["DayEvent"].max()),
    )

    filter_type = st.sidebar.multiselect(
        "Select Type",
        options=joined["Type"].unique(),
        default=joined["Type"].unique(),
    )
    filter_subtype = st.sidebar.multiselect(
        "Select SubType",
        options=joined["SubType"].unique(),
        default=["Yield Enhancement"],
    )
    filter_type = filter_type if filter_type else joined["Type"].unique()
    filter_subtype = filter_subtype if filter_subtype else joined["SubType"].unique()

    filter_issuer = st.sidebar.multiselect(
        "Select issuer",
        options=joined["Issuer"].unique(),
        default=[],
    )
    filter_issuer = filter_issuer if filter_issuer else joined["Issuer"].unique()
    return dates_filter, filter_type, filter_subtype, filter_issuer


@st.cache_data
def get_joined_df() -> pd.DataFrame:
    return (
        sales_data.merge(
            isin_info,
            how="left",
            left_on=["MifidInstrumentID"],
            right_on=["ISIN"],
        )
        .merge(
            issuers,
            how="left",
            left_on=["Emittente"],
            right_on=["Original"],
        )
        .merge(
            type_and_subtype,
            how="left",
            left_on=["Nome"],
            right_on=["Category"],
        )
        .assign(
            **{
                "Adjusted Turnover": lambda df: compute_adjusted_turnover(df),
            },
        )
        .drop(columns=["MifidInstrumentID", "Original", "Emittente", "Nome"])
    )


def compute_adjusted_turnover(df: pd.DataFrame) -> pd.Series:
    return df["MifidNotionalAmount"].where(
        df["Issue Price"].isna(),
        df["MifidQuantity"] * df["Issue Price"],
    )


def underlyings_page() -> None:
    n_underlyings = underlyings.groupby("ISIN")["Sottostante"].count()

    joined = (
        get_joined_df()
        .merge(
            n_underlyings.rename("n_underlyings").reset_index(),
            how="left",
            on=["ISIN"],
        )
        .merge(
            underlyings,
            how="left",
            on=["ISIN"],
        )
        .assign(
            Sottostante=lambda df: df["Sottostante"].str.lower(),
        )
        .merge(
            und_mapping,
            how="left",
            left_on=["Sottostante"],
            right_on=und_mapping["Original"].str.lower(),
            suffixes=("_x", ""),
        )
        .assign(
            **{
                "Adjusted Turnover (underlying)": lambda df: compute_adjusted_turnover(
                    df,
                ),
                "Adjusted Turnover": lambda df: (
                    df["Adjusted Turnover (underlying)"] / df["n_underlyings"]
                ),
            },
        )
        .drop(
            columns=[
                "Sottostante_x",
                "Original",
            ],
        )
    )

    dates_filter, filter_type, filter_subtype, filter_issuer = get_standard_filters(
        joined,
    )

    n_underlyings = st.sidebar.slider(
        label="Underlyings to show",
        min_value=5,
        max_value=20,
        value=10,
    )

    st.title("Underlyings dashboard")

    columns_to_group = [
        col
        for col in joined.columns
        if col
        not in [
            "DayEvent",
            "MifidNotionalAmount",
            "Adjusted Turnover",
            "Adjusted Turnover (underlying)",
            "MifidQuantity",
        ]
    ]
    ref_df = (
        joined.loc[
            (
                joined["DayEvent"].dt.date.between(
                    dates_filter[0],
                    dates_filter[1]
                    if len(dates_filter) == 2
                    else joined["DayEvent"].dt.date.max(),
                )
            )
            & (joined["Type"].isin(filter_type))
            & (joined["SubType"].isin(filter_subtype))
            & joined["Issuer"].isin(filter_issuer)
        ]
        .groupby(columns_to_group)[
            [
                "Adjusted Turnover",
                "MifidNotionalAmount",
                "Adjusted Turnover (underlying)",
                "MifidQuantity",
            ]
        ]
        .sum()
        .reset_index()
    )

    top_10_sottostanti = (
        ref_df.groupby("Sottostanti")["Adjusted Turnover"]
        .sum()
        .sort_values(ascending=True)
        .iloc[-n_underlyings:]
        .index
    )
    fig = px.bar(
        ref_df[ref_df["Sottostanti"].isin(top_10_sottostanti)]
        .groupby(["SubType", "Sottostanti"])["Adjusted Turnover"]
        .sum()
        .sort_values(ascending=True)
        .rename("Turnover (M)")
        .apply(lambda x: f"{x / 1_000_000:.2f}")
        .reset_index(),
        x="Turnover (M)",
        y="Sottostanti",
        color="SubType",
        orientation="h",
    )
    fig.update_layout(
        yaxis={
            "categoryorder": "array",
            "categoryarray": top_10_sottostanti.to_list(),
        },
    )
    st.header(f"Top {n_underlyings} baskets")
    st.plotly_chart(fig, use_container_width=True)

    top_10_sottostante = (
        ref_df.groupby("Sottostante")["Adjusted Turnover (underlying)"]
        .sum()
        .sort_values(ascending=True)
        .iloc[-n_underlyings:]
        .index
    )
    fig = px.bar(
        ref_df.loc[ref_df["Sottostante"].isin(top_10_sottostante)]
        .groupby(["Sottostante", "SubType"])["Adjusted Turnover (underlying)"]
        .sum()
        .sort_values(ascending=True)
        .rename("Turnover (M)")
        .apply(lambda x: f"{x / 1_000_000:.2f}")
        .reset_index(),
        x="Turnover (M)",
        y="Sottostante",
        color="SubType",
        orientation="h",
    )
    fig.update_layout(
        yaxis={
            "categoryorder": "array",
            "categoryarray": top_10_sottostante.to_list(),
        },
    )
    st.header(f"Top {n_underlyings} underlyings")
    st.plotly_chart(fig, use_container_width=True)

    # Step 1: Group and sum turnover
    grouped = (
        ref_df.groupby(["Sottostanti", "ISIN", "Issuer"])
        .agg({"Adjusted Turnover": "sum"})
        .reset_index()
    )

    # Step 2: Compute total turnover at each level
    # Create helper total columns to sort by
    sottostanti_total = grouped.groupby("Sottostanti")["Adjusted Turnover"].transform(
        "sum",
    )
    isin_total = grouped.groupby(["Sottostanti", "ISIN"])[
        "Adjusted Turnover"
    ].transform("sum")
    issuer_total = grouped.groupby(["Sottostanti", "ISIN", "Issuer"])[
        "Adjusted Turnover"
    ].transform("sum")

    # Step 3: Add these columns and sort
    grouped["Sottostanti Total"] = sottostanti_total
    grouped["ISIN Total"] = isin_total
    grouped["Issuer Total"] = issuer_total

    sorted_grouped = grouped.sort_values(
        by=["Sottostanti Total", "ISIN Total", "Issuer Total"],
        ascending=False,
    ).drop(
        columns=["Sottostanti Total", "ISIN Total", "Issuer Total"],
    )  # Optional cleanup
    sorted_grouped["Sottostanti"] = sorted_grouped["Sottostanti"].where(
        sorted_grouped["Sottostanti"].shift() != sorted_grouped["Sottostanti"],
        "//",
    )

    st.header("Top baskets")
    st.dataframe(
        sorted_grouped.assign(
            **{
                "Adjusted Turnover": lambda df: (
                    df["Adjusted Turnover"] / 1_000_000
                ).apply(lambda x: f"{x:,.2f}"),
                "n": lambda df: range(1, len(df) + 1),
            },
        ).set_index("n"),
        use_container_width=True,
    )

    grouped = (
        ref_df.groupby(["Sottostante", "Sottostanti", "ISIN", "Issuer"])
        .agg({"Adjusted Turnover (underlying)": "sum"})
        .reset_index()
    )

    # Step 2: Compute total turnover at each level
    # Create helper total columns to sort by
    sottostanti_total = grouped.groupby("Sottostante")[
        "Adjusted Turnover (underlying)"
    ].transform("sum")
    basket_total = grouped.groupby("Sottostanti")[
        "Adjusted Turnover (underlying)"
    ].transform("sum")
    isin_total = grouped.groupby(["Sottostante", "ISIN"])[
        "Adjusted Turnover (underlying)"
    ].transform("sum")
    issuer_total = grouped.groupby(["Sottostante", "ISIN", "Issuer"])[
        "Adjusted Turnover (underlying)"
    ].transform("sum")

    # Step 3: Add these columns and sort
    grouped["Sottostante Total"] = sottostanti_total
    grouped["Basket Total"] = basket_total
    grouped["ISIN Total"] = isin_total
    grouped["Issuer Total"] = issuer_total

    sorted_grouped = grouped.sort_values(
        by=["Sottostante Total", "Basket Total", "ISIN Total", "Issuer Total"],
        ascending=False,
    ).drop(
        columns=["Sottostante Total", "Basket Total", "ISIN Total", "Issuer Total"],
    )  # Optional cleanup
    sorted_grouped["Sottostante"] = sorted_grouped["Sottostante"].where(
        sorted_grouped["Sottostante"].shift() != sorted_grouped["Sottostante"],
        "//",
    )
    sorted_grouped["Sottostanti"] = sorted_grouped["Sottostanti"].where(
        sorted_grouped["Sottostanti"].shift() != sorted_grouped["Sottostanti"],
        "//",
    )

    st.header("Top underlyings")
    st.dataframe(
        sorted_grouped.assign(
            **{
                "Adjusted Turnover (underlying)": lambda df: (
                    df["Adjusted Turnover (underlying)"] / 1_000_000
                ).apply(lambda x: f"{x:,.2f}"),
                "n": lambda df: range(1, len(df) + 1),
            },
        ).set_index("n"),
        use_container_width=True,
    )

    st.download_button(
        "Download CSV",
        data=ref_df.to_csv(index=False),
        file_name="underlyings.csv",
        mime="text/csv",
    )


pages = {
    "Issuers": issuers_page,
    "Products": products_page,
    "Underlyings": underlyings_page,
}

selected_page = st.sidebar.selectbox("Navigate to:", list(pages.keys()))

pages[selected_page]()
