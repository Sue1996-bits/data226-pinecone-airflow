from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import time

import pandas as pd
import requests
from sentence_transformers import SentenceTransformer
from pinecone import Pinecone, ServerlessSpec


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="Medium_to_Pinecone",
    default_args=default_args,
    description="Build a Medium Posting Search Engine using Pinecone",
    schedule=timedelta(days=7),
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=["medium", "pinecone", "search-engine"],
) as dag:

    @task
    def download_data():
        data_dir = "/tmp/medium_data"
        os.makedirs(data_dir, exist_ok=True)
        file_path = f"{data_dir}/medium_data.csv"
        response = requests.get(
            "https://s3-geospatial.s3.us-west-2.amazonaws.com/medium_data.csv",
            timeout=120,
        )
        response.raise_for_status()
        with open(file_path, "wb") as output:
            for chunk in response.iter_content(chunk_size=8192):
                output.write(chunk)
        with open(file_path, "r") as input_file:
            print(f"Downloaded file has {sum(1 for _ in input_file)} lines")
        return file_path

    @task
    def preprocess_data(data_path):
        df = pd.read_csv(data_path)
        df["title"] = df["title"].fillna("").astype(str)
        df["subtitle"] = df["subtitle"].fillna("").astype(str)
        df["metadata"] = df.apply(
            lambda row: {"title": f"{row['title']} {row['subtitle']}"}, axis=1
        )
        df["id"] = df.reset_index(drop=True).index.astype(str)
        preprocessed_path = "/tmp/medium_data/medium_preprocessed.csv"
        df.to_csv(preprocessed_path, index=False)
        print(f"Preprocessed data saved to {preprocessed_path}")
        return preprocessed_path

    @task
    def create_pinecone_index():
        api_key = Variable.get("pinecone_api_key")
        pc = Pinecone(api_key=api_key)
        index_name = "semantic-search-fast"
        if index_name in [item["name"] for item in pc.list_indexes()]:
            pc.delete_index(index_name)
        pc.create_index(
            name=index_name,
            dimension=384,
            metric="dotproduct",
            spec=ServerlessSpec(cloud="aws", region="us-east-1"),
        )
        while not pc.describe_index(index_name).status["ready"]:
            time.sleep(1)
        return index_name

    @task
    def generate_embeddings_and_upsert(data_path, index_name):
        api_key = Variable.get("pinecone_api_key")
        df = pd.read_csv(data_path)
        model = SentenceTransformer("all-MiniLM-L6-v2", device="cpu")
        pc = Pinecone(api_key=api_key)
        index = pc.Index(index_name)
        batch_size = 100
        for start in range(0, len(df), batch_size):
            batch = df.iloc[start : start + batch_size].copy()
            metadata = batch["metadata"].apply(eval).tolist()
            embeddings = model.encode([item["title"] for item in metadata])
            index.upsert(
                vectors=[
                    {
                        "id": str(row["id"]),
                        "values": embeddings[offset].tolist(),
                        "metadata": metadata[offset],
                    }
                    for offset, (_, row) in enumerate(batch.iterrows())
                ]
            )
        print(f"Successfully upserted {len(df)} records to Pinecone")
        return index_name

    @task
    def test_search_query(index_name):
        api_key = Variable.get("pinecone_api_key")
        model = SentenceTransformer("all-MiniLM-L6-v2", device="cpu")
        index = Pinecone(api_key=api_key).Index(index_name)
        query = "what is ethics in AI"
        results = index.query(
            vector=model.encode(query).tolist(), top_k=5, include_metadata=True
        )
        for result in results["matches"]:
            print(
                f"ID: {result['id']}, Score: {result['score']}, "
                f"Title: {result['metadata']['title'][:50]}..."
            )

    data_path = download_data()
    preprocessed_path = preprocess_data(data_path)
    index_name = create_pinecone_index()
    final_index_name = generate_embeddings_and_upsert(preprocessed_path, index_name)
    test_search_query(final_index_name)

