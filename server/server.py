from flask import Flask, request, jsonify
from elasticsearch import Elasticsearch

app = Flask(__name__)
ELASTIC_URL = "https://site:33cec238f197ade0bf08eaa4cf1c5736@oin-us-east-1.searchly.com:443"
es = Elasticsearch([ELASTIC_URL], use_ssl=True, verify_certs=True)
index = 'tweet-sentiments'

@app.route('/search', methods=['GET'])
def search():
    query = request.args.get('q', '')

    if not query:
        return jsonify({'error': 'Query parameter "q" is required'}), 400

    # Perform a simple match query
    result = es.search(index=index, body={
        'query': {
            'match': {
                'Review': query
            }
        },
        'sort': [
            {'timestamp': {'order': 'desc'}}  # Sort by timestamp in descending order (latest first)
        ]
    })

    hits = result.get('hits', {}).get('hits', [])

    search_results = [{'id': hit['_id'], 'source': hit['_source']} for hit in hits]

    return jsonify({'results': search_results})
@app.route('/sentiment_aggregation', methods=['GET'])
def sentiment_aggregation():

    query = request.args.get('q', '')

    query_body = {
        "query": {
            "bool": {
                "must": [{"match": {"Review": query}}] if query else [],
            }
        },
        "size": 0,
        "aggs": {
            "sentiment_counts": {
                "terms": {
                    "field": "sentiment",
                    "size": 10
                }
            }
        }
    }

    result = es.search(index=index, body=query_body)

    buckets = result.get('aggregations', {}).get('sentiment_counts', {}).get('buckets', [])

    total_count = sum(bucket['doc_count'] for bucket in buckets)

    response_data = [{'sentiment': bucket['key'], 'count': bucket['doc_count'], 'percentage': (bucket['doc_count'] / total_count) * 100} for bucket in buckets]

    return jsonify({'sentiment_counts': response_data, 'total_count': total_count, 'search_query': query})

@app.route('/competitors', methods=['GET'])
def array_field_aggregation():

    search_query = request.args.get('q', '')

    query_body = {
        "query": {
            "bool": {
                "must": [{"match": {"Review": search_query}}] if search_query else [],
            }
        },
        "size": 0,
        "aggs": {
                "term_counts": {
                        "terms": {
                            "field": "competitors.keyword",
                            "size": 10
                        }
                }
        }
    }

    print(query_body)

    result = es.search(index=index, body=query_body)

    term_buckets = result.get('aggregations', {}).get('term_counts', {}).get('buckets', [])

    total_term_count = sum(bucket['doc_count'] for bucket in term_buckets)

    term_response_data = [{'term': bucket['key'], 'count': bucket['doc_count']} for bucket in term_buckets]

    return jsonify({'term_counts': term_response_data, 'total_term_count': total_term_count, 'search_query': search_query})


if __name__ == '__main__':
    app.run(debug=True, port=9999)
