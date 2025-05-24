import pandas as pd

# def fuzzy_normalize_name(name, choices, threshold=65):
#     match, score, _ = process.extractOne(name, choices)
#     return match if score >= threshold else None


names = (
    pd.read_csv("und_mapping.csv")[["Original", "Sottostante"]]
    .iloc[-55:]
    .merge(pd.read_csv("underlyings.csv"), left_on="Original", right_on="Sottostante")
    .merge(pd.read_csv("isin_info.csv"), on="ISIN")
    .merge(pd.read_csv("type_and_subtype.csv"), left_on="Nome", right_on="Category")
)
print(names.columns)
print(names[["Sottostante_x", "ISIN", "SubType"]])

# for idx, a in a["Name"].items():
# print(a, "|", fuzzy_normalize_name(a, existing))
