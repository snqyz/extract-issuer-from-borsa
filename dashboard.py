from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="ISIN Dashboard", page_icon="📊", layout="wide")

BASE_FOLDER = Path(__file__).parent


@st.cache_data
def load_csv(filename, **kwargs):
    print("Loading CSV:", filename)
    path = BASE_FOLDER / filename
    if path.exists():
        return pd.read_csv(path, encoding="utf-8-sig", **kwargs)
    return pd.DataFrame()


# Load data
sales_data = pd.concat(
    [
        load_csv(f, parse_dates=["DayEvent"])
        for f in (BASE_FOLDER / "intermediate_csv").iterdir()
    ],
)
isin_info = load_csv("isin_info.csv")
underlyings = load_csv("underlyings.csv")
type_and_subtype = load_csv("type_and_subtype.csv")
issuers = load_csv("issuers.csv")
und_mapping = load_csv("und_mapping.csv")


def issuers_page():
    st.title("Issuers Dashboard")

    joined = (
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
    )

    # Sidebar filters
    st.sidebar.header("Filters")

    start_day_filter = st.sidebar.date_input(
        "Select day",
        min_value=joined["DayEvent"].min(),
        max_value=joined["DayEvent"].max(),
        value=joined["DayEvent"].min(),
    )
    end_day_filter = st.sidebar.date_input(
        "Select end day",
        min_value=joined["DayEvent"].min(),
        max_value=joined["DayEvent"].max(),
        value=joined["DayEvent"].max(),
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

    # Filter data for the line chart
    filtered_by_date = joined[
        joined["DayEvent"].dt.date.between(start_day_filter, end_day_filter)
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
    aggregated["Turnover (M)"] = (aggregated["MifidNotionalAmount"] / 1_000_000).round(
        2,
    )

    # ISIN Info Table
    st.subheader("By Day")
    fig = px.line(
        data_frame=aggregated,
        x="DayEvent",
        y="Turnover (M)",
        color="Issuer",
        title="Turnover by Day and Issuer",  # Optional title
        markers=True,
    )
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("By Type")

    # Group and aggregate the data
    chart_data = (
        filtered_by_date_issuers[
            filtered_by_date_issuers["SubType"].isin(filter_subtype)
            & filtered_by_date_issuers["Type"].isin(filter_type)
        ]
        .groupby(
            ["Issuer", "SubType"],
        )
        .agg(
            MifidNotionalAmount=("MifidNotionalAmount", "sum"),
        )
        .reset_index()
    )

    # Calculate total notional amount per issuer for sorting
    issuer_totals = (
        chart_data.groupby("Issuer")["MifidNotionalAmount"].sum().reset_index()
    )
    issuer_totals = issuer_totals.sort_values("MifidNotionalAmount", ascending=True)

    # Create ordered list of issuers
    issuer_order = issuer_totals["Issuer"].tolist()

    # Sort the chart data by issuer order
    chart_data["Issuer"] = pd.Categorical(
        chart_data["Issuer"],
        categories=issuer_order,
        ordered=True,
    )
    chart_data = chart_data.sort_values("Issuer")

    # Create the bar chart with plotly for proper sorting
    fig = px.bar(
        chart_data,
        x="MifidNotionalAmount",
        y="Issuer",
        color="SubType",
        orientation="h",
        category_orders={"Issuer": issuer_order},
    )

    fig.update_layout(
        yaxis={"categoryorder": "array", "categoryarray": issuer_order},
    )

    st.plotly_chart(fig, use_container_width=True)


def products_page():
    st.sidebar.header("Filters")

    joined = (
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
                "Adjusted Turnover": lambda df: df["MifidNotionalAmount"].where(
                    df["Issue Price"].isna(),
                    df["MifidQuantity"] * df["Issue Price"],
                ),
            },
        )
    )

    start_day_filter = st.sidebar.date_input(
        "Select day",
        min_value=sales_data["DayEvent"].min(),
        max_value=sales_data["DayEvent"].max(),
        value=sales_data["DayEvent"].min(),
    )
    end_day_filter = st.sidebar.date_input(
        "Select end day",
        min_value=sales_data["DayEvent"].min(),
        max_value=sales_data["DayEvent"].max(),
        value=sales_data["DayEvent"].max(),
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

    st.subheader("Products Dashboard")

    st.dataframe(
        joined.loc[
            (joined["DayEvent"].dt.date.between(start_day_filter, end_day_filter))
            & (joined["Type"].isin(filter_type))
            & (joined["SubType"].isin(filter_subtype)),
            [
                "DayEvent",
                "ISIN",
                "Issuer",
                "Adjusted Turnover",
                "Type",
                "SubType",
            ],
        ]
        .groupby(["ISIN", "Issuer", "Type", "SubType"])
        .agg({"Adjusted Turnover": "sum"})
        .sort_values(by="Adjusted Turnover", ascending=False)
        .reset_index()
        .assign(
            **{
                "Adjusted Turnover": lambda df: (df["Adjusted Turnover"] / 1_000_000)
                .round(2)
                .astype(str)
                .str.ljust(4, "0"),
                "n": lambda df: range(1, len(df) + 1),
            },
        )
        .set_index("n"),
        use_container_width=True,
    )


def underlyings_page():
    st.sidebar.header("Filters")

    n_underlyings = underlyings.groupby("ISIN")["Sottostante"].nunique()

    joined = (
        sales_data.merge(
            isin_info,
            how="left",
            left_on=["MifidInstrumentID"],
            right_on=["ISIN"],
        )
        .merge(
            n_underlyings.rename("n_underlyings").reset_index(),
            how="left",
            on=["ISIN"],
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
                "Adjusted Turnover (underlying)": lambda df: df[
                    "MifidNotionalAmount"
                ].where(
                    df["Issue Price"].isna(),
                    df["MifidQuantity"] * df["Issue Price"],
                ),
                "Adjusted Turnover": lambda df: (
                    df["Adjusted Turnover (underlying)"] / df["n_underlyings"]
                ).round(2),
            },
        )
    )

    start_day_filter = st.sidebar.date_input(
        "Select day",
        min_value=sales_data["DayEvent"].min(),
        max_value=sales_data["DayEvent"].max(),
        value=sales_data["DayEvent"].min(),
    )
    end_day_filter = st.sidebar.date_input(
        "Select end day",
        min_value=sales_data["DayEvent"].min(),
        max_value=sales_data["DayEvent"].max(),
        value=sales_data["DayEvent"].max(),
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

    ref_df = joined.loc[
        (joined["DayEvent"].dt.date.between(start_day_filter, end_day_filter))
        & (joined["Type"].isin(filter_type))
        & (joined["SubType"].isin(filter_subtype)),
    ]

    top_10_sottostanti = (
        ref_df.groupby("Sottostanti")["Adjusted Turnover"]
        .sum()
        .sort_values(ascending=True)
        .iloc[-10:]
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
        title="Top 10 Baskets by Adjusted Turnover",
        orientation="h",
    )
    fig.update_layout(
        yaxis={
            "categoryorder": "array",
            "categoryarray": top_10_sottostanti.to_list(),
        },
    )
    st.plotly_chart(fig, use_container_width=True)

    top_10_sottostante = (
        ref_df.groupby("Sottostante")["Adjusted Turnover (underlying)"]
        .sum()
        .sort_values(ascending=True)
        .iloc[-10:]
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
        title="Top 10 Underlyings by Adjusted Turnover",
        orientation="h",
    )
    fig.update_layout(
        yaxis={
            "categoryorder": "array",
            "categoryarray": top_10_sottostante.to_list(),
        },
    )
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

    st.dataframe(
        sorted_grouped.assign(
            **{
                "Adjusted Turnover": lambda df: (df["Adjusted Turnover"] / 1_000_000)
                .round(2)
                .astype(str)
                .str.ljust(4, "0"),
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

    st.dataframe(
        sorted_grouped.assign(
            **{
                "Adjusted Turnover (underlying)": lambda df: (
                    df["Adjusted Turnover (underlying)"] / 1_000_000
                )
                .round(2)
                .astype(str)
                .str.ljust(4, "0"),
                "n": lambda df: range(1, len(df) + 1),
            },
        ).set_index("n"),
        use_container_width=True,
    )


pages = {
    "Issuers": issuers_page,
    "Products": products_page,
    "Underlyings": underlyings_page,
}

selected_page = st.sidebar.selectbox("Navigate to:", list(pages.keys()))


pages[selected_page]()
