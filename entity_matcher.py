import csv
from sortedcontainers import SortedDict
import visits_revenue_aggregator

# Global variables for filenames
INPUTS_CSV = 'inputs.csv'
MATCHED_TABLE_CSV = '125_MatchedTable.csv'
UNMATCHED_TABLE_CSV = '125_UnmatchedTable.csv'
LULU_TERMS_CSV = 'searchTerms-125.csv'
LULU_TERMS_AGGREGATED_CSV = 'searchTerms-125_Aggregated.csv'
SYNONYMS_TXT = 'lulu_solr_synonyms.txt'
SYNONYM_MATCHES_CSV = 'SynonymMatches.csv'

# Global SortedDict to store shingles with their corresponding details
shingles_dict = SortedDict()

# Read from a CSV file and populate the shingles dictionary
def read_csv_and_populate_shingles_dict(filename):
    print(f"Opening file {filename} to populate shingles dictionary...")
    with open(filename, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        headers = next(reader)
        print("Reading headers...")
        for row in reader:
            for i, entity in enumerate(row):
                if entity.strip():
                    entity_type = headers[i]
                    entity_shingles = generate_shingles(entity)
                    for shingle in entity_shingles:
                        shingle_key = shingle.lower()
                        shingle_type = "full" if shingle.lower() == entity.lower() else "partial"
                        if shingle_key not in shingles_dict:
                            shingles_dict[shingle_key] = []
                        shingles_dict[shingle_key].append([shingle.lower(), entity, shingle_type, entity_type])
    print("Shingles dictionary populated successfully.")
    # Debug: Print the populated dictionary
    print("Populated shingles dictionary:")
    for k, v in shingles_dict.items():
        print(f"{k}: {v}")

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
            if lower_shingle in shingles_dict:
                for entry in shingles_dict[lower_shingle]:
                    matched_writer.writerow([shingle, entry[1], entry[2], entry[3], search_query, visits, revenue])
                    print(f"Matched: {shingle}")
            else:
                unmatched_writer.writerow([shingle, search_query, visits, revenue])
                print(f"Unmatched: {shingle}")

# Process synonyms and write to SynonymMatches CSV
def process_synonyms():
    with open(SYNONYMS_TXT, mode='r', encoding='utf-8') as synonyms_file, \
         open(SYNONYM_MATCHES_CSV, mode='w', newline='', encoding='utf-8') as synonym_matches_file:
        
        synonym_writer = csv.writer(synonym_matches_file)
        synonym_writer.writerow(["Left Term", "Entity", "Original Line", "Rewritten Line"])

        for line in synonyms_file:
            line = line.strip()

            if '=>' in line:
                left_term, right_terms = line.split('=>', 1)
                left_term = left_term.strip()
                right_terms = [term.strip() for term in right_terms.split(',')]
                
                lower_left_term = left_term.lower()
                
                # Check if the left term is in the shingles dictionary and not in the right terms
                if lower_left_term in shingles_dict:
                    if left_term not in right_terms:
                        print(f"Left term = {left_term} | Right term = {right_terms}")
                        # Retrieve the first matching entry for the left term
                        matching_entries = shingles_dict[lower_left_term]
                        for entry in matching_entries:
                            entity_type = entry[3]  # Get the entity type from the first match
                            original_line = line
                            rewritten_line = f"{left_term} => {left_term}, {', '.join(right_terms)}"
                            synonym_writer.writerow([left_term, entity_type, original_line, rewritten_line])
                            print(f"Processed synonym: {left_term}")
                            break

# Clear existing data and add headers to MatchedTable and UnmatchedTable
with open(MATCHED_TABLE_CSV, mode='w', newline='', encoding='utf-8') as matched_file, \
     open(UNMATCHED_TABLE_CSV, mode='w', newline='', encoding='utf-8') as unmatched_file:
    matched_writer = csv.writer(matched_file)
    unmatched_writer = csv.writer(unmatched_file)
    matched_writer.writerow(["Matched Shingle", "Original Entity", "Shingle Type", "Entity Type", "Search Query", "Visits", "Revenue"])
    unmatched_writer.writerow(["Unmatched Shingle", "Search Query", "Visits", "Revenue"])

read_csv_and_populate_shingles_dict(INPUTS_CSV)

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

process_synonyms()
