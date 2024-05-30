import time
import matplotlib.pyplot as plt
from elasticsearch import Elasticsearch, helpers
import pandas as pd
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

client = Elasticsearch(
    ["https://localhost:9200","https://localhost:9201","https://localhost:9202"],
    http_auth=('elastic', 'changeme'),
    verify_certs=False,  # Bypass SSL certificate verification
)

index_name = "covid_data_index"

if not client.indices.exists(index=index_name):
    # Create an index
    client.indices.create(
        index="covid_data_index",
        body={
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            },
            "mappings": {
                "properties": {
                    "USMER": {"type": "integer"},
                    "MEDICAL_UNIT": {"type": "integer"},
                    "SEX": {"type": "integer"},
                    "PATIENT_TYPE": {"type": "integer"},
                    "DATE_DIED": {"type": "date"},
                    "INTUBED": {"type": "integer"},
                    "PNEUMONIA": {"type": "integer"},
                    "AGE": {"type": "integer"},
                    "PREGNANT": {"type": "integer"},
                    "DIABETES": {"type": "integer"},
                    "COPD": {"type": "integer"},
                    "INMSUPR": {"type": "integer"},
                    "HIPERTENSION": {"type": "integer"},
                    "OTHER_DISEASE": {"type": "integer"},
                    "CARDIOVASCULAR": {"type": "integer"},
                    "OBESITY": {"type": "integer"},
                    "RENAL_CHRONIC": {"type": "integer"},
                    "TOBACCO": {"type": "integer"},
                    "CLASIFFICATION_FINAL": {"type": "integer"},
                    "ICU": {"type": "integer"},
                }
            }
        },
        ignore=400  # Ignore 400 already exists error
    )

# # Read the CSV file
# df = pd.read_csv('./CovidData.csv', delimiter=';')
#
# # Replace 'NaN' values with a default value
# df.fillna("unknown", inplace=True)
#
# # Convert DataFrame to a list of dictionaries
# data = df.to_dict('records')
#
# # Create an index action for each record
# actions = [
#     {
#         "_index": index_name,
#         "_source": record
#     }
#     for record in data
# ]
#
# # Use the bulk function to index the data
# helpers.bulk(client, actions)

# Benchmark searching
res = client.search(index=index_name, body={"query": {"match_all": {}}})
searching_time = res['took']

# Simple Query: Search for all male patients.
res1 = client.search(index=index_name, body={"query": {"match": {"SEX": 2}}})
simple_query_time = res1['took']

# Term Query: Search for all patients who were diagnosed with COVID in different degrees.
res = client.search(index=index_name, body={"query": {"range": {"CLASIFFICATION_FINAL": {"gte": 1, "lte": 3}}}})
term_query_time = res['took']

# Range Query: Search for all patients who are above 60 years old.
res = client.search(index=index_name, body={"query": {"range": {"AGE": {"gte": 60}}}})
range_query_time = res['took']

# Benchmark boolean query
res = client.search(index=index_name, body={
    "query": {
        "bool": {
            "must": [
                {"term": {"SEX": 2}},
                {"range": {"CLASIFFICATION_FINAL": {"gte": 1, "lte": 3}}},
                {"range": {"AGE": {"gte": 60}}}
            ]
        }
    }
})
boolean_query_time = res['took']

# Complex Query: Search for all male patients who were diagnosed with COVID, are above 60 years old, and have diabetes.
res = client.search(index=index_name, body={
    "query": {
        "bool": {
            "must": [
                {"term": {"SEX": 2}},
                {"range": {"CLASIFFICATION_FINAL": {"gte": 1, "lte": 3}}},
                {"range": {"AGE": {"gte": 60}}},
                {"term": {"DIABETES": 1}}
            ]
        }
    }
})
complex_query_time = res['took']

res = client.search(index=index_name, body={
    "query": {
        "bool": {
            "must": [
                {"term": {"SEX": 2}},
                {"range": {"CLASIFFICATION_FINAL": {"gte": 1, "lte": 3}}},
                {"range": {"AGE": {"gte": 60}}},
                {"term": {"DIABETES": 1}},
                {"bool": {
                    "should": [
                        {"term": {"CARDIOVASCULAR": 1}},
                        {"term": {"OBESITY": 1}}
                    ]
                }}
            ]
        }
    }
})
nested_boolean_query_time = res['took']
print(nested_boolean_query_time)

plt.figure(figsize=(14, 6))  # Adjust the width and height as needed
plt.bar(["Searching", "Simple Query", "Term Query", "Range Query", "Boolean Query", "Complex Query", "Nested Boolean Query"],
        [searching_time, simple_query_time, term_query_time, range_query_time, boolean_query_time, complex_query_time, nested_boolean_query_time])
plt.ylabel('Time (milliseconds)')
plt.title('Elasticsearch Benchmark')
plt.show()

index_settings = client.indices.get_settings(index=index_name)
number_of_shards = index_settings[index_name]['settings']['index']['number_of_shards']
number_of_replicas = index_settings[index_name]['settings']['index']['number_of_replicas']

print(f"Number of shards: {number_of_shards}")
print(f"Number of replicas: {number_of_replicas}")