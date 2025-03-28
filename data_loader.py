import pyarrow.parquet as pq
import pandas as pd
from neo4j import GraphDatabase
import time
import os


class DataLoader:
    def __init__(self, uri, user, password):
        """
        Connect to the Neo4j database and other init steps
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=False)
        self.driver.verify_connectivity()

    def close(self):
        """Close the connection to the Neo4j database"""
        self.driver.close()

    def load_transform_file(self, file_path):
        """
        Load the parquet file, clean it, convert to CSV, and load into Neo4j.
        """
        trips = pq.read_table(file_path).to_pandas()
        
        # Data Cleaning
        trips = trips[['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'trip_distance', 'fare_amount']]
        bronx = {3, 18, 20, 31, 32, 46, 47, 51, 58, 59, 60, 69, 78, 81, 94, 119, 126, 136, 147, 159, 167, 168, 169, 174, 182, 183, 184, 185, 199, 200, 208, 212, 213, 220, 235, 240, 241, 242, 247, 248, 250, 254, 259}
        trips = trips[trips['PULocationID'].isin(bronx) & trips['DOLocationID'].isin(bronx)]
        trips = trips[(trips['trip_distance'] > 0.1) & (trips['fare_amount'] > 2.5)]
        
        # Convert date-time columns
        trips['tpep_pickup_datetime'] = pd.to_datetime(trips['tpep_pickup_datetime'])
        trips['tpep_dropoff_datetime'] = pd.to_datetime(trips['tpep_dropoff_datetime'])
        
        # Save to CSV
        save_loc = os.path.join("/var/lib/neo4j/import", os.path.basename(file_path).replace(".parquet", ".csv"))
        trips.to_csv(save_loc, index=False)
        
        # Load into Neo4j
        with self.driver.session() as session:
            for _, row in trips.iterrows():
                session.run(
                    """
                    MERGE (pickup:Location {id: $PULocationID})
                    MERGE (dropoff:Location {id: $DOLocationID})
                    CREATE (pickup)-[:TRIP {distance: $trip_distance, fare: $fare_amount, pickup_time: $pickup_time, dropoff_time: $dropoff_time}]->(dropoff)
                    """,
                    PULocationID=row['PULocationID'],
                    DOLocationID=row['DOLocationID'],
                    trip_distance=row['trip_distance'],
                    fare_amount=row['fare_amount'],
                    pickup_time=row['tpep_pickup_datetime'].isoformat(),
                    dropoff_time=row['tpep_dropoff_datetime'].isoformat()
                )
        print("Data loaded into Neo4j successfully!")


def main():
    total_attempts = 10
    attempt = 0
    while attempt < total_attempts:
        try:
            data_loader = DataLoader("neo4j://localhost:7687", "neo4j", "project1phase1")
            data_loader.load_transform_file("yellow_tripdata_2022-03.parquet")
            data_loader.close()
            break  # Exit loop if successful
        except Exception as e:
            print(f"(Attempt {attempt+1}/{total_attempts}) Error: ", e)
            attempt += 1
            time.sleep(10)


if __name__ == "__main__":
    main()


