import csv
from collections import defaultdict
from decimal import Decimal
import sys
import normalizer  # Assuming you have a normalizer module with a normalize function

def normalize_and_aggregate(input_filename, output_filename):
    # Dictionary to hold aggregated data
    aggregated_data = defaultdict(lambda: [0, Decimal('0.00')])
    iteration_count = 0
    
    with open(input_filename, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.reader(infile)
        headers = next(reader)  # Read the header row

        for row in reader:
            iteration_count += 1
            # Update and print the iteration count every 1000 iterations
            if iteration_count % 1000 == 0:
                sys.stdout.write(f"\rIteration count: {iteration_count}")
                sys.stdout.flush()
            
            search_query = row[0]
            normalized_search_query = normalizer.normalize(search_query)
            visits = int(row[4])
            revenue = Decimal(row[8].replace('$', '').replace(',', ''))
            
            # Aggregate visits and revenue based on the normalized search query
            aggregated_data[normalized_search_query][0] += visits
            aggregated_data[normalized_search_query][1] += revenue

    # Write the aggregated data to a new CSV file
    with open(output_filename, mode='w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(headers)  # Write the header row

        with open(input_filename, mode='r', newline='', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            next(reader)  # Skip the header row
            for row in reader:
                search_query = row[0]
                normalized_search_query = normalizer.normalize(search_query)
                if normalized_search_query in aggregated_data:
                    visits, revenue = aggregated_data[normalized_search_query]
                    row[0] = normalized_search_query
                    row[4] = visits
                    row[8] = f"${revenue:,.2f}"
                    writer.writerow(row)
                    # Remove the entry after writing it to avoid duplicates
                    del aggregated_data[normalized_search_query]