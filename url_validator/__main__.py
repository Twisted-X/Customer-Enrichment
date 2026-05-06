"""CLI entry point: python -m url_validator [input.csv [output.csv]]"""
import sys
from datetime import datetime

from ._batch import validate_urls

input_file = "data/CustomCustomerSearchResults990.csv"
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
output_file = f"data/url_validation_{timestamp}.csv"

if len(sys.argv) > 1:
    input_file = sys.argv[1]
if len(sys.argv) > 2:
    output_file = sys.argv[2]

validate_urls(input_file, output_file)
