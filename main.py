import pandas as pd
from utils import time_it
import pyarrow.dataset as ds
import pyarrow as pa

@time_it
def step1_gendata():
    df = pd.DataFrame({
        "event_id": range(100_000),
        "user_id": (i % 1234 for i in range(100_000)),
        "country": ["US", "IN", "DE", "FR", "ES", "IT", "BR", "AR", "CL", "MX"] * 10_000,
        "event_date": pd.date_range(start="2025-01-01", periods=100_000, freq="min"),
        "value": range(100_000),
    })

    # Partition by date and country for easy filtering later
    df["event_date"] = df["event_date"].dt.floor("D")  # type: ignore[attr-defined]
    df.to_parquet(
        "data/events",
        partition_cols=["event_date", "country"],
        engine="pyarrow",
    )


@time_it
def step2_scan_data():
    dataset = ds.dataset("data/events", format="parquet", partitioning="hive")

    # Only load US events from January 2nd and only 3 columns
    scanner = dataset.scanner(
        columns=["event_id", "user_id", "value", "country", "event_date"],
        filter=(
            (ds.field("country") == "US") & 
            (ds.field("event_date") == "2025-01-02")
        )
    )
    table = scanner.to_table()

    df = table.to_pandas()
    df = df[["event_id", "user_id", "value"]]
    print(df)

@time_it
def stream_chunks_pipeline():
    dataset = ds.dataset("data/events", format="parquet", partitioning="hive")
    scanner = dataset.scanner(
        columns=["event_id", "user_id", "value", "country", "event_date"],
        filter=ds.field("country") == "IN"
    )

    import os
    os.makedirs("data/agg", exist_ok=True)

    for i, batch in enumerate(scanner.to_batches()):
        batch_df = batch.to_pandas()
        # process one chunk at a time
        agg = (
            batch_df
            .groupby("user_id", observed=True)["value"]
            .sum()
            .reset_index()
        )
        # write partial result somewhere
        agg.to_parquet(f"data/agg/part_{i}.parquet", engine="pyarrow")
    
    # merge all partial results
    agg = pd.read_parquet("data/agg")
    agg = agg.groupby("user_id", observed=True)["value"].sum()
    print(agg)

def pandas_on_arrow():
    dataset = ds.dataset("data/events", format="parquet", partitioning="hive")
    table = dataset.to_table(
        columns = ["event_id", "user_id", "value"]
    )

    df = table.to_pandas()
    
    # Convert all columns to string type
    for col in df.columns:
        df[col] = df[col].astype(str)

    print(df)
    print(df.dtypes)



if __name__ == "__main__":
    step1_gendata()
    step2_scan_data()
    stream_chunks_pipeline()
    pandas_on_arrow()