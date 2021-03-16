import sys
import redis
import time
from datetime import datetime

class ZDBHooksMonitor:
    def __init__(self, host, port=9900):
        self.host = host
        self.port = port

        self.green = "\033[32;1m"
        self.yellow = "\033[33;1m"
        self.reset = "\033[0m"

        self.redis = redis.Redis(host, port)

    def live(self):
        while True:
            hooks = self.redis.execute_command("HOOKS")

            print("\033[2J\033[H")
            print(" Hook                 | Status       |  Started      | Ended           | Argument")
            print("----------------------+--------------+---------------+-----------------+---------------")

            for hook in hooks:
                name = hook[0].decode('utf-8')

                status = self.green + "finished" + self.reset + (" [%d]" % hook[5])

                if hook[4] == 0:
                    status = self.yellow + "running" + self.reset

                started = datetime.fromtimestamp(hook[3])
                ended = datetime.fromtimestamp(hook[4])
                command = hook[1][0].decode('utf-8') if len(hook[1]) > 0 else ""

                startstr = started.strftime("%m-%d %H:%M:%S")
                endstr = ended.strftime("%m-%d %H:%M:%S") if hook[4] != 0 else "..."

                print(" %-20s | %-23s | %-13s | %-14s | %s" % (name, status, startstr, endstr, command))

            print("")
            time.sleep(1)

if __name__ == '__main__':
    host = "localhost"
    port = 9900

    if len(sys.argv) > 1:
        host = sys.argv[1]

    if len(sys.argv) > 2:
        port = int(sys.argv[2])

    zls = ZDBHooksMonitor(host, port)
    zls.live()
