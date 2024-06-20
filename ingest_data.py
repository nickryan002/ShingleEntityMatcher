import pysolr
import csv
from collections import defaultdict

# Initialize Solr client
solr = pysolr.Solr('http://localhost:8983/solr/search_queries', always_commit=True, timeout=10)

# Function to clean revenue strings
def clean_revenue(revenue_str):
    return float(revenue_str.replace('$', '').replace(',', ''))

# Function to delete all documents in the Solr collection
def delete_all_documents():
    solr.delete(q='*:*')
    solr.commit()
    print("All documents deleted successfully.")

# Function to read search queries and ingest them into Solr
def read_and_ingest_to_solr(filename):
    delete_all_documents()
    documents = []
    with open(filename, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        headers = next(reader)
        for row in reader:
            search_query = row[0]
            visits = int(row[4])
            revenue = clean_revenue(row[8])
            documents.append({
                "query": search_query,
                "visits": visits,
                "revenue": revenue
            })
    solr.add(documents)
    print("Data ingested into Solr successfully.")

