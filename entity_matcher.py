import csv
import visits_revenue_aggregator

# Global variables for filenames
INPUTS_CSV = 'inputs.csv'
MATCHED_TABLE_CSV = 'MatchedTable.csv'
UNMATCHED_TABLE_CSV = 'UnmatchedTable.csv'
LULU_TERMS_CSV = 'lulu_terms.csv'
LULU_TERMS_AGGREGATED_CSV = 'lulu_terms_Aggregated.csv'
POPULATED_DICT_TXT = 'populated_dict.txt'

# Global dictionary to store column values as keys and column names as values
global_dict = {}

# Read from a CSV file and populate the global dictionary
def read_csv_and_populate_dict(filename):
    print(f"Opening file {filename} to populate dictionary...")
    with open(filename, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        headers = next(reader)
        print("Reading headers...")
        for row in reader:
            for i, value in enumerate(row):
                global_dict[value.lower()] = headers[i]
    print("Dictionary populated successfully.")

# Function to generate shingles from a phrase
def generate_shingles(phrase):
    print(f"Generating shingles for the phrase: {phrase}")
    words = phrase.split()
    shingles = [' '.join(words[i:j+1]) for i in range(len(words)) for j in range(i, len(words))]
    print(f"Generated {len(shingles)} shingles.")
    return shingles

# Write to MatchedTable and UnmatchedTable CSVs
def write_to_csvs(search_query, shingles, visits, revenue):
    print(f"Writing to CSVs for the search query: {search_query}")
    with open(MATCHED_TABLE_CSV, mode='a', newline='', encoding='utf-8') as matched_file, \
         open(UNMATCHED_TABLE_CSV, mode='a', newline='', encoding='utf-8') as unmatched_file:
        
        matched_writer = csv.writer(matched_file)
        unmatched_writer = csv.writer(unmatched_file)
        
        for shingle in shingles:
            # Convert shingle to lowercase for case insensitive comparison
            lower_shingle = shingle.lower()
            if lower_shingle in global_dict:
                matched_writer.writerow([shingle, global_dict[lower_shingle], search_query, visits, revenue])
                print(f"Matched: {shingle}")
            else:
                unmatched_writer.writerow([shingle, search_query, visits, revenue])
                print(f"Unmatched: {shingle}")

read_csv_and_populate_dict(INPUTS_CSV)
# Output the sorted populated dictionary to a text file
with open(POPULATED_DICT_TXT, 'w') as f:
    for key, value in sorted(global_dict.items(), key=lambda item: item[1]):
        f.write(f'{key}: {value}\n')

# Clear existing data and add headers to MatchedTable and UnmatchedTable
with open(MATCHED_TABLE_CSV, mode='w', newline='', encoding='utf-8') as matched_file, \
     open(UNMATCHED_TABLE_CSV, mode='w', newline='', encoding='utf-8') as unmatched_file:
    matched_writer = csv.writer(matched_file)
    unmatched_writer = csv.writer(unmatched_file)
    matched_writer.writerow(["Matched Shingle", "Entity", "Search Query", "Visits", "Revenue"])
    unmatched_writer.writerow(["Unmatched Shingle", "Search Query", "Visits", "Revenue"])

visits_revenue_aggregator.normalize_and_aggregate(LULU_TERMS_CSV, LULU_TERMS_AGGREGATED_CSV)

# Read search queries from another CSV and process each
with open(LULU_TERMS_AGGREGATED_CSV, mode='r', newline='', encoding='utf-8') as search_queries_file:
    reader = csv.reader(search_queries_file)
    next(reader)  # Skip the header
    print("Processing search queries...")
    for row in reader:
        search_phrase = row[0]
        visits = row[4]
        revenue = row[8]
        shingles = generate_shingles(search_phrase)
        write_to_csvs(search_phrase, shingles, visits, revenue)
    print("Finished processing all search queries.")
