import requests

def normalize(text_to_analyze):
    solr_url = "http://localhost:8983/solr"
    core_name = "search_queries"
    field_type = "dig_practice_char_syns"
    analysis_result = analyze_text_HTTP(solr_url, core_name, field_type, text_to_analyze)
    return get_normalized_text(analysis_result)


def analyze_text_HTTP(solr_url, core_name, field_type, text_to_analyze):
    analysis_url = f"{solr_url}/{core_name}/analysis/field"
    params = {
        'wt': 'json',
        'analysis.fieldtype': field_type,
        'analysis.fieldvalue': text_to_analyze
    }
    response = requests.get(analysis_url, params=params)
    # print(f"Text to analyze: {text_to_analyze}")
    # print(f"Request URL: {response.url}")
    # print(f"Response status code: {response.status_code}")
    # print(f"Response text: {response.text}")
    response.raise_for_status()  # Raise an exception for HTTP errors
    return response.json()

def get_normalized_text(response):
    tokens = []
    analysis_result = response.get('analysis', {}).get('field_types', {}).get('dig_practice_char_syns', {}).get('index', [])
    if analysis_result:
        # Get the last phase, which contains the final tokens
        last_phase = analysis_result[-1]
        if isinstance(last_phase, list):
            for token in last_phase:
                if 'text' in token:
                    tokens.append(token['text'])
    return ' '.join(tokens)

if __name__ == "__main__":
    text_to_analyze = "Hottie hot 25 inch"

    response = normalize(text_to_analyze)
    print(f"Final result: {response}")
