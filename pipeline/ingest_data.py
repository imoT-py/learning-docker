import pandas as pd
from sqlalchemy import create_engine
import click
from tqdm.auto import tqdm
import gzip
from urllib.request import urlretrieve


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2021, type=int, help='Year of the data')
@click.option('--month', default=1, type=int, help='Month of the data')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading CSV')

def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table, chunksize):

    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    filename = f"yellow_tripdata_{year}-{month:02d}.csv.gz"
    url = prefix + filename

    engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    # Download the file
    urlretrieve(url, filename)

    # Count rows and calculate the number of chunks
    with gzip.open(filename, "rt") as file:
        total_rows = sum(1 for _ in file) - 1

    total_chunks = (total_rows + chunksize - 1) // chunksize

    # Read CSV in chunks
    df_iter = pd.read_csv(
        filename,
        dtype=dtype,
        parse_dates=parse_dates,
        chunksize=chunksize
    )

    for chunk_number, df_chunk in enumerate(
        tqdm(df_iter, total=total_chunks, desc="Loading CSV")
    ):
        # Create the table on the first iteration
        if chunk_number == 0:
            df_chunk.head(0).to_sql(
                name="yellow_taxi_data",
                con=engine,
                if_exists="replace",
                index=False
            )
            tqdm.write("Table created")

        # Insert the current chunk
        df_chunk.to_sql(
            name="yellow_taxi_data",
            con=engine,
            if_exists="append",
            index=False
        )

if __name__ == '__main__':
    run()