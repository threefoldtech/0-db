import redis
import random
import string

"""
Please run 0-db using '--datasize 512' in order to
make this working correctly

This test is made to fill database with well know edge and tricky
case which needs to be handled correctly on compaction process
and avoid using a huge database to try all of this

Compaction is cleaning unused keys from datafile and index,
but in order to keep everything clean, we just remove needed part
from file, we don't reorganize files themself, this keep the
"always append" idea true and improve incremental changes detection

This is why even a datafile without any data left inside,
should still exists after compaction, with just a header inside

All keys used to flag other keys (for eg, deletion etc.) should disapears

This generate a 7+ datafile db, filled with well known name
Each key name is defined 'XY', X is a letter (from a to g)
and Y a number, (from 1 to 6)

The first file, only the first entry is removed
The second file, nothing is done
The third file, everything is removed
The fourth file, some keys (1, 3, 4) are removed
The fifth file, only the last key is removed

The resulting compacted datafile should be:
00001: [a2, a3, a4, a5, a6]
00002: [b1, b2, b3, b4, b5, b6]
00003: []                        # empty but should still exists
00004: [d2, d5, d6]
00005: [e1, e2, e3, e4, e5]
00006: [f1, f2, f3, f4, f5, f6]  # with new values
00007: [g1, g2, g3, g4, g5, g6]  # with new values for g1 and g6
0000*: should be empty, since flag should be discarded
"""

class ZeroDBCompactTest:
    def __init__(self, host, port):
        self.r = redis.Redis(host, port)

    def ex(self, command):
        print(command)

        val = self.r.execute_command(command)
        print(val)

        return val

    def generate(self, length):
        return ''.join(random.choice(string.ascii_uppercase) for _ in range(length))

    def prepare(self):
        try:
            self.ex("NSNEW compactme")

        except redis.exceptions.ResponseError:
            self.ex("NSDEL compactme")
            self.ex("NSNEW compactme")

        self.ex("SELECT compactme")

    def run(self):
        self.prepare()

        # Fill datafile 00000 -> 00006
        for f in ['a', 'b', 'c', 'd', 'e', 'f', 'g']:
            for i in range(1, 7):
                self.ex("SET %c%d %s" % (f, i, self.generate(64)))

        # Overwrite all keys on 00005
        for i in range(1, 7):
            self.ex("SET f%d %s" % (i, self.generate(64)))

        # Overwrite g1 and g6
        self.ex("SET g1 OVERWRITE-1")
        self.ex("SET g6 OVERWRITE-2")

        # Remove first and last entries
        self.ex("DEL a1")
        self.ex("DEL e6")

        # Remove full datafile 00002
        for i in range(1, 7):
            self.ex("DEL c%d" % i)

        # Remove some keys from 00003
        self.ex("DEL d1")
        self.ex("DEL d3")
        self.ex("DEL d4")


if __name__ == '__main__':
    db = ZeroDBCompactTest("localhost", 9900)
    db.run()
