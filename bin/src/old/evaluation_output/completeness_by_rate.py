import math

FILE = "kafka_ingest_2_writers_500000_batch_1000_sources_20221009040321"

def parse_line(line):
    parts = line.split(",")
    rate = int(parts[1].split(":")[1])
    completeness = float(parts[4].split(":")[1])
    return rate, completeness


with open(FILE, "r") as f:
    lines = f.readlines()
    lines = list(filter(lambda x: x.startswith("Current"), lines))

rates = {}

for line in lines:
    rate, completeness = parse_line(line)
    if not (rate in rates):
        rates[rate] = []
    if not math.isnan(completeness):
        rates[rate].append(completeness)

results = []
print(rates)
for key, value in rates.items():
    value = value[: len(value)//2]
    results.append((key, sum(value) / len(value)))

results.sort()
print("Rate,Completeness")
fmt = "{},{}"
for result in results:
    print(fmt.format(result[0],result[1]))


