rm output.json
rm parsed_output.json
python3 p2.py -r hadoop ./json/train/*.json > output.json
python3 parse_output.py
