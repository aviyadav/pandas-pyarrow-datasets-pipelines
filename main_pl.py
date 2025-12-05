import polars as pl
from utils import time_it
import pyarrow.dataset as ds
import pyarrow as pa

@time_it
def step1_gendata():
    df = pl.DataFrame({
        "event_id": range(100_000),
        "user_id": [i % 1234 for i in range(100_000)],
        "country": ["US", "IN", "DE", "FR", "ES", "IT", "BR", "AR", "CL", "MX"] * 10_000,
        "event_date": pl.datetime_range(
            start=pl.datetime(2025, 1, 1),
            end=pl.datetime(2025, 1, 1, 0, 0) + pl.duration(minutes=99_999),
            interval="1m",
            eager=True
        ),
        "value": range(100_000),
    })

    # Partition by date and country for easy filtering later
    df = df.with_columns(pl.col("event_date").cast(pl.Date))
    df.write_parquet(
        "data/events_pl",
        use_pyarrow=True,
        pyarrow_options={
            "partition_cols": ["event_date", "country"]
        }
    )


@time_it
def step2_scan_data():
    dataset = ds.dataset("data/events_pl", format="parquet", partitioning="hive")

    # Only load US events from January 2nd and only 3 columns
    scanner = dataset.scanner(
        columns=["event_id", "user_id", "value", "country", "event_date"],
        filter=(
            (ds.field("country") == "US") & 
            (ds.field("event_date") == "2025-01-02")
        )
    )
    table = scanner.to_table()

    # Convert PyArrow table to Polars
    df = pl.from_arrow(table)  # type: ignore[assignment]
    assert isinstance(df, pl.DataFrame)
    df = df.select(["event_id", "user_id", "value"])
    print(df)

@time_it
def stream_chunks_pipeline():
    dataset = ds.dataset("data/events_pl", format="parquet", partitioning="hive")
    scanner = dataset.scanner(
        columns=["event_id", "user_id", "value", "country", "event_date"],
        filter=ds.field("country") == "IN"
    )

    import os
    os.makedirs("data/agg_pl", exist_ok=True)

    for i, batch in enumerate(scanner.to_batches()):
        # Convert PyArrow batch to Polars
        batch_df = pl.from_arrow(batch)  # type: ignore[assignment]
        assert isinstance(batch_df, pl.DataFrame)
        
        # process one chunk at a time
        agg = (
            batch_df
            .group_by("user_id")
            .agg(pl.col("value").sum())
        )
        # write partial result somewhere
        agg.write_parquet(f"data/agg_pl/part_{i}.parquet")
    
    # merge all partial results
    agg = pl.read_parquet("data/agg_pl/*.parquet")
    agg = agg.group_by("user_id").agg(pl.col("value").sum())
    print(agg)

def polars_on_arrow():
    dataset = ds.dataset("data/events_pl", format="parquet", partitioning="hive")
    table = dataset.to_table(
        columns = ["event_id", "user_id", "value"]
    )

    # Convert PyArrow table to Polars
    df = pl.from_arrow(table)  # type: ignore[assignment]
    assert isinstance(df, pl.DataFrame)
    
    # Convert all columns to string type
    df = df.with_columns([
        pl.col(col).cast(pl.Utf8) for col in df.columns
    ])

    print(df)
    print(df.dtypes)



if __name__ == "__main__":
    # step1_gendata()
    # step2_scan_data()
    # stream_chunks_pipeline()
    polars_on_arrow()