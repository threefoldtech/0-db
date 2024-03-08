import redis

master = redis.Redis(port=9900)
slave = redis.Redis(port=9901)
namespace = "default"

# freezing namespace
freeze = slave.execute_command(f"NSSET {namespace} freeze 1")
print(freeze)

# fetching master info
nsinfo = master.execute_command(f"NSINFO {namespace}")
nsinforaw = nsinfo.decode("utf-8").split("\n")
masterinfo = {}

for line in nsinforaw:
    data = line.split(": ")
    if len(data) > 1:
        masterinfo[data[0]] = data[1]

master_current_id = masterinfo['data_current_id']
master_current_offset = masterinfo['data_current_offset']

while True:
    nsinfo = slave.execute_command(f"NSINFO {namespace}")
    nsinforaw = nsinfo.decode("utf-8").split("\n")
    nsinfo = {}

    for line in nsinforaw:
        data = line.split(": ")
        if len(data) > 1:
            nsinfo[data[0]] = data[1]

    current_id = nsinfo['data_current_id']
    current_offset = nsinfo['data_current_offset']

    print(f"Current: {current_id}:{current_offset} [master {master_current_id}:{master_current_offset}]")

    try:
        segment = master.execute_command(f"DATA EXPORT {current_id} {current_offset}")
        result = slave.execute_command("DATA", "IMPORT", current_id, current_offset, segment)

    except redis.exceptions.ResponseError as error:
        if str(error) != "EOF":
            raise error

        if master_current_id == nsinfo['data_current_id']:
            if master_current_offset == nsinfo['data_current_offset']:
                print("DATA SYNC DONE")
                break

        print("End of file, switching")
        slave.execute_command("NSJUMP")

        continue
