import redis
import random
import string

class ZeroDBCompactQuick:
    def __init__(self, host, port):
        self.r = redis.Redis(host, port)
        self.namespace = "quick"

    def ex(self, command):
        print(command)

        val = self.r.execute_command(command)
        print(val)

        return val

    def generate(self, length):
        return ''.join(random.choice(string.ascii_uppercase) for _ in range(length))

    def prepare(self):
        try:
            self.ex("NSNEW %s" % self.namespace)

        except redis.exceptions.ResponseError:
            self.ex("NSDEL %s" % self.namespace)
            self.ex("NSNEW %s" % self.namespace)

        self.ex("SELECT %s" % self.namespace)

    def run(self):
        self.prepare()

        # Fill datafile 00000 -> 00006
        for f in ['a', 'b', 'c', 'd', 'e', 'f', 'g']:
            for i in range(1, 128):
                self.ex("SET %c%d %s" % (f, i, self.generate(4096)))

        # Overwrite all keys on 00005
        for i in range(1, 128):
            self.ex("SET f%d %s" % (i, self.generate(64)))

        # Overwrite g1 and g6
        self.ex("SET g1 OVERWRITE-1")
        self.ex("SET g127 OVERWRITE-2")

        # Remove first and last entries
        self.ex("DEL a1")
        self.ex("DEL e127")

        # Remove full datafile 00002
        for i in range(1, 128):
            self.ex("DEL c%d" % i)

        # Remove some keys from 00003
        self.ex("DEL d1")
        self.ex("DEL d67")
        self.ex("DEL d94")
        self.ex("DEL d100")
        self.ex("DEL d108")


if __name__ == '__main__':
    db = ZeroDBCompactQuick("localhost", 9900)
    db.run()
