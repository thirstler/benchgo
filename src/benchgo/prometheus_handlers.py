
def prom_node_cpu_util_rate(data, mode):
    ttl_p = 0
    cpus = {}

    for metric in data:

        if metric["metric"]["mode"] == mode:
            if len(metric["values"]) < 2: continue
            cpu_id = "{}:{}".format(metric["metric"]["instance"], metric["metric"]["cpu"])
            try:
                cpus[cpu_id] = (float(metric["values"][-1][1]) - float(metric["values"][0][1])) /  (float(metric["values"][-1][0]) - float(metric["values"][0][0]))
            except:
                cpus[cpu_id] = 0
            ttl_p += cpus[cpu_id]

    try:
        rate = ttl_p/len(cpus)
    except:
        rate = -1

    return rate

def prom_node_net_rate(data):
    ttl_r = 0
    instances = {}

    for metric in data:
        if len(metric["values"]) < 2: continue
        instance = "{}".format(metric["metric"]["instance"])
        try:
            instances[instance] = (float(metric["values"][-1][1]) - float(metric["values"][0][1])) /  (float(metric["values"][-1][0]) - float(metric["values"][0][0]))
        except:
             instances[instance]  = 0
        ttl_r += instances[instance]

    return ttl_r


def prom_node_disk_rate(data):
    ttl_r = 0
    instances = {}
    for metric in data:
        if len(metric["values"]) < 2: continue
        instance = "{}".format(metric["metric"]["instance"])
        try:
            instances[instance] = (float(metric["values"][-1][1]) - float(metric["values"][0][1])) /  (float(metric["values"][-1][0]) - float(metric["values"][0][0]))
        except:
            instances[instance] = 0

        ttl_r += instances[instance]

    return ttl_r


def prom_node_cpu_count(data, ht=True):
    cpu_count = 0
    for metric in data:
        if metric["metric"]["mode"] == "idle":
            cpu_count += 1
            
    return cpu_count
