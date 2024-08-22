if [ -z "$1" ]; then
  echo "Usage: $0 input.json"
  exit 1
fi

input_file="$1"
output_file="${input_file%.json}.jsonl"

# Convert JSON to JSONL using jq
jq -c '.[]' "$input_file" > "$output_file"

# Check if the output file was created and is not empty
if [ -s "$output_file" ]; then
  echo "Conversion complete. Output saved to $output_file"
else
  echo "Conversion failed. Please check the input file format."
fi

