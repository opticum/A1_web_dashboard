import pandas as pd


def fmt_auto(x):
    if pd.isna(x):
        return ""
    x = float(x)
    if x.is_integer():
        return str(int(x))
    return f"{x:,.10f}".rstrip("0").rstrip(".")


def fmt_fixed(x, decimals):
    if pd.isna(x):
        return ""
    return f"{float(x):.{int(decimals)}f}"


def fmt_localized(x):
    if pd.isna(x):
        return ""
    return f"{x:,.0f}"


def fmt_percent(x):
    if pd.isna(x):
        return ""
    return f"{round(x * 100):.0f}%"