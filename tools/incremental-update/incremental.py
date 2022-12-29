import redis
import time

class ZDBIncremental:
    def __init__(self, master, slave):
        self.master = redis.Redis(unix_socket_path="/tmp/zdb.sock")
        self.slave = redis.Redis(port=9900)

        # disable defaults callbacks
        for target in [self.master, self.slave]:
            target.set_response_callback("NSINFO", redis.client.parse_info)
            target.set_response_callback("DEL", redis.client.bool_ok)
            target.set_response_callback("SET", bytes)

    def sync(self, master, slave):
        raw = self.master.execute_command("DATA", "RAW", slave['dataid'], slave['offset'])

        print(raw)

        if raw[3] == 0:
            # SET
            response = self.slave.execute_command("SET", raw[0], raw[5], raw[4])
            if response != raw[0]:
                raise RuntimeError(f"incorrect set {response}")

        else:
            # DEL
            self.slave.execute_command("DEL", raw[0], raw[4])


    def run(self):
        namespace = "default"

        while True:
            master = {}
            slave = {}

            nsmaster = self.master.execute_command("NSINFO", namespace)
            master['dataid'] = int(nsmaster['data_current_id'])
            master['offset'] = int(nsmaster['data_current_offset'])

            nsslave = self.slave.execute_command("NSINFO", namespace)
            slave['dataid'] = int(nsslave['data_current_id'])
            slave['offset'] = int(nsslave['data_current_offset'])

            print("master: %d:%d" % (master['dataid'], master['offset']))
            print("slave: %d:%d" % (slave['dataid'], slave['offset']))

            if slave['dataid'] > master['dataid']:
                raise RuntimeError("slave ahead from master")

            if master['dataid'] == slave['dataid']:
                if master['offset'] == slave['offset']:
                    print("sync, waiting")
                    time.sleep(10)
                    continue

            self.sync(master, slave)


if __name__ == '__main__':
    incremental = ZDBIncremental("hello", "world")
    incremental.run()
