import re
FILE = "es_query_latency"

def parse_line(line):
    parts = line.split(",")
    query_id = int(parts[0].split(" ")[1])
    data_latency = float(parts[1].split(":")[1])
    execution_latency = float(parts[2].split(":")[1])
    return query_id, data_latency, execution_latency

with open(FILE, "r") as f:
    lines = f.readlines()
    lines = list(filter(lambda x: re.match("^Query [0-9]+, Data", x), lines))

print("query_id, data_latency, execution_latency");
for line in lines:
    query_id, data_latency, execution_latency = parse_line(line)
    print("{}, {:.10f}, {:.10f}".format(query_id, data_latency, execution_latency))



