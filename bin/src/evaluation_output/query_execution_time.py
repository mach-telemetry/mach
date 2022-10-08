FILE = "mach_simple_query_20221008215542"

def parse_line(line):
    parts = line.split(",")
    current_time = parts[0].split(" ")[2]
    query_id = int(parts[1].split(":")[1])
    data_latency = float(parts[6].split(":")[1])
    execution_latency = float(parts[7].split(":")[1])
    return current_time, query_id, data_latency, execution_latency


with open(FILE, "r") as f:
    lines = f.readlines()
    lines = list(filter(lambda x: x.startswith("Current"), lines))

print("current_time, query_id, data_latency, execution_latency");
for line in lines:
    current_time, query_id, data_latency, execution_latency = parse_line(line)
    print("{}, {}, {:.10f}, {:.10f}".format(current_time, query_id, data_latency, execution_latency))


