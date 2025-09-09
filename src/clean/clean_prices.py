import os, pandas as pd
RAW_DIR = "data/raw"
CLEAN_DIR = "data/clean"




def clean_one(path):
    df = pd.read_parquet(path)
    df = df.drop_duplicates(subset=["date","ticker"]).sort_values(["ticker","date"])
    df = df[df["volume"].fillna(0) >= 0]
    return df




def run_clean():
    os.makedirs(CLEAN_DIR, exist_ok=True)
    files = [f for f in os.listdir(RAW_DIR) if f.endswith(".parquet") and f != "KOSPI100_all.parquet"]
    all_list = []
    for f in files:
        df = clean_one(os.path.join(RAW_DIR, f))
        all_list.append(df)
        df.to_parquet(os.path.join(CLEAN_DIR, f), index=False)
    if all_list:
        full = pd.concat(all_list, ignore_index=True)
        full.to_parquet(os.path.join(CLEAN_DIR, "KOSPI100_all.parquet"), index=False)


if __name__ == "__main__":
    run_clean()