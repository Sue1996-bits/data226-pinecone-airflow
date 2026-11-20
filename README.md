# DATA 226 Week 13 — Pinecone Airflow Job

This repository runs the instructor's Medium-to-Pinecone search pipeline in Dockerized Airflow.

## Run locally

```bash
docker compose down
docker compose up -d
```

Open Airflow at http://localhost:8080 with `airflow` / `airflow`. In **Admin → Variables**, add the variable `pinecone_api_key` with the Pinecone API key, then enable and trigger `Medium_to_Pinecone`.

Task dependency chain:

`download_data → preprocess_data → create_pinecone_index → generate_embeddings_and_upsert → test_search_query`

