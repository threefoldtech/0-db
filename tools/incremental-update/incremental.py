import sys
import redis
import time
import hashlib

class ZDBIncremental:
    def __init__(self, master, mport, slave, sport):
        minfo = {"host": master, "port": mport, "namespace": "default", "name": "master"}
        sinfo = {"host": slave, "port": sport, "namespace": "default", "name": "slave"}

        self.master = redis.Redis(host=master, port=mport)
        self.master.__data = minfo

        self.slave = redis.Redis(host=slave, port=sport)
        self.slave.__data = sinfo

        # disable defaults callbacks
        for target in [self.master, self.slave]:
            target.set_response_callback("NSINFO", redis._parsers.helpers.parse_info)
            target.set_response_callback("DEL", redis._parsers.helpers.bool_ok)
            target.set_response_callback("SET", bytes)
            target.set_response_callback("AUTH", bytes)

    def authenticate(self, target, password):
        print(f"[+] authenticating: {target.__data['name']}")

        request = target.execute_command("AUTH", "SECURE", "CHALLENGE")
        challenge = request.decode('utf-8')

        encoded = f"{challenge}:{password}"
        response = hashlib.sha1(encoded.encode("utf-8")).hexdigest()

        # authenticate
        status = target.execute_command("AUTH", "SECURE", response)
        if status != b"OK":
            return False

        # authenticate on namespace as well
        status = target.execute_command("SELECT", target.__data["namespace"], password)

        return status == b"OK"

    def sync(self, master, slave):
        try:
            raw = self.master.execute_command("DATA", "RAW", slave['dataid'], slave['offset'])

        except redis.exceptions.ResponseError as e:
            if str(e) == "EOF":
                self.slave.execute_command("NSJUMP")
                return None

            raise e

        # print(raw)

        if raw[3] == 0:
            # SET
            response = self.slave.execute_command("SET", raw[0], raw[5], raw[4])
            if response != raw[0]:
                raise RuntimeError(f"incorrect set {response}")

        else:
            # DEL
            self.slave.execute_command("DEL", raw[0], raw[4])


    def run(self):
        print(f"[+] master host: {self.master.__data['host']}, port: {self.master.__data['port']}")
        print(f"[+] slave host: {self.slave.__data['host']}, port: {self.slave.__data['port']}")
        print(f"[+] syncing namespaces: {self.master.__data['namespace']} -> {self.slave.__data['namespace']}")

        while True:
            master = {}
            slave = {}

            nsmaster = self.master.execute_command("NSINFO", self.master.__data['namespace'])
            master['dataid'] = int(nsmaster['data_current_id'])
            master['offset'] = int(nsmaster['data_current_offset'])
            master['size'] = int(nsmaster['data_size_bytes'])

            nsslave = self.slave.execute_command("NSINFO", self.slave.__data['namespace'])
            slave['dataid'] = int(nsslave['data_current_id'])
            slave['offset'] = int(nsslave['data_current_offset'])
            slave['size'] = int(nsslave['data_size_bytes'])

            if slave['dataid'] > master['dataid']:
                raise RuntimeError("slave ahead from master")

            if master['dataid'] == slave['dataid']:
                if master['offset'] == slave['offset']:
                    sys.stdout.write("\r[+] syncing: %.2f / %.2f MB (%.1f %%), waiting changes \033[K" % (ssize, msize, progress))
                    time.sleep(10)
                    continue

            msize = master['size'] / 1024 / 1024
            ssize = slave['size'] / 1024 / 1024
            progress = (slave['size'] / master['size']) * 100
            dataid = slave['dataid']
            offset = slave['offset']

            sys.stdout.write("\r[+] syncing: %.2f / %.2f MB (%.1f %%) [request %d:%d] \033[K" % (ssize, msize, progress, dataid, offset))
            sys.stdout.flush()

            self.sync(master, slave)


if __name__ == '__main__':
    incremental = ZDBIncremental("hub.grid.tf", 9900, "127.0.0.1", 9900)
    incremental.authenticate(incremental.master, "master-password")
    incremental.authenticate(incremental.slave, "slave-password")
    incremental.run()
