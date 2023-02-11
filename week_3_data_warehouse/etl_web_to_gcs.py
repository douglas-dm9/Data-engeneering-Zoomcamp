from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
import numpy as np

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url,encoding='utf-8')
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
    df["dropOff_datetime"] = pd.to_datetime(df["dropOff_datetime"])
    df["PUlocationID"] = df["PUlocationID"].astype('Int64')
    df["DOlocationID"] = df["DOlocationID"].astype('Int64')
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame,  dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"data/fhv-ny-taxi/fhv_tripdata_{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs(year: int, month: int,) -> None:
    """The main ETL function"""
    #color = "yellow"
    dataset_file = f"{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{dataset_file}.csv.gz"
    df = fetch(dataset_url)
    df_clean = clean(df)

    path = write_local(df_clean,  dataset_file)
    write_gcs(path)


@flow()
def etl_parent_flow(
    months: list[int] = [2,3,4,5,6,7,8,9,10,11,12], year: int = 2019,
):
    for month in months:
        etl_web_to_gcs(year, month)


if __name__ == "__main__":
    months = [2,3,4,5,6,7,8,9,10,11,12]
    year = 2019
    etl_parent_flow(months, year)