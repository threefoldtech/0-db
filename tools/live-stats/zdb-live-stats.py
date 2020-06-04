import sys
import redis
import time
import pprint
import curses

class ZDBLiveStats:
    def __init__(self, host, port=9900):
        self.host = host
        self.port = port

        self.redis = redis.Redis(host, port)
        self.screen = curses.initscr()

        self.wininit()

    def window(self, title, y, x, height=9, width=32):
        win = curses.newwin(height, width, y, x)
        win.box()
        self.write(win, 0, " %s " % title)
        win.refresh()

        return win


    def wininit(self):
        self.screen.border(0)
        curses.curs_set(0)

        self.write(self.screen, 1, "0-db live monitor")
        self.write(self.screen, 3, "Host: %s [port: %d]" % (self.host, self.port))

        self.screen.refresh()

        self.system = self.window("System", 5, 1, 7, 64)
        self.net = self.window("Network", 12, 1)
        self.index = self.window("Index", 12, 33)
        self.data = self.window("Data", 12, 65)

    def write(self, win, line, text):
        win.addstr(line, 2, text)

    def finish(self):
        curses.endwin()

    def diff(self, current, previous, key):
        return current[key] - previous[key]

    def kb(self, value):
        return value / 1024

    def size(self, value):
        units = ['KB', 'MB', 'GB', 'TB']
        uid = 0
        value = value / 1024

        while value / 1024 > 1024:
            value = value / 1024
            uid += 1

        return "%10.2f %s" % (value, units[uid])

    def uptime(self, value):
        if value < 600:
            return "%.0f minutes" % (value / 60)

        if value < 86400:
            return "%.1f hours" % (value / 3600)

        return "%d days, %.1f hours" % ((value / 86400), (value % 86400) / 3600)

    def stats(self):
        info = self.redis.info()
        # pprint.pprint(info)
        return info

    def live(self):
        previous = self.stats()

        while True:
            current = self.stats()

            stats = {
                'data-read': self.diff(current, previous, 'data_disk_read_bytes'),
                'data-write': self.diff(current, previous, 'data_disk_write_bytes'),
                'index-read': self.diff(current, previous, 'index_disk_read_bytes'),
                'index-write': self.diff(current, previous, 'index_disk_write_bytes'),
                'net-rx': self.diff(current, previous, 'network_rx_bytes'),
                'net-tx': self.diff(current, previous, 'network_tx_bytes'),
                'commands': self.diff(current, previous, 'commands_executed'),
                'clients': self.diff(current, previous, 'clients_lifetime'),
            }

            # pprint.pprint(stats)

            self.system.refresh()
            self.write(self.system, 2, " Uptime   : %s" % self.uptime(current['uptime']))
            self.write(self.system, 3, " Clients  : %-10d [%d]" % (stats['clients'], current['clients_lifetime']))
            self.write(self.system, 4, " Commands : %-10d [%d]" % (stats['commands'], current['commands_executed']))
            self.system.refresh()

            self.write(self.net, 2, " Download  %10.2f KB/s" % self.kb(stats['net-rx']))
            self.write(self.net, 3, "           %s" % self.size(current['network_rx_bytes']))
            self.write(self.net, 5, " Upload    %10.2f KB/s" % self.kb(stats['net-tx']))
            self.write(self.net, 6, "           %s" % self.size(current['network_tx_bytes']))
            self.net.refresh()

            self.write(self.index, 2, " Read      %10.2f KB/s" % self.kb(stats['index-read']))
            self.write(self.index, 3, "           %s" % self.size(current['index_disk_read_bytes']))
            self.write(self.index, 5, " Write     %10.2f KB/s" % self.kb(stats['index-write']))
            self.write(self.index, 6, "           %s" % self.size(current['index_disk_write_bytes']))
            self.index.refresh()

            self.write(self.data, 2, " Read      %10.2f KB/s" % self.kb(stats['data-read']))
            self.write(self.data, 3, "           %s" % self.size(current['data_disk_read_bytes']))
            self.write(self.data, 5, " Write     %10.2f KB/s" % self.kb(stats['data-write']))
            self.write(self.data, 6, "           %s" % self.size(current['data_disk_write_bytes']))
            self.data.refresh()

            self.screen.refresh()

            previous = current
            time.sleep(1)

        return True

if __name__ == '__main__':
    host = "localhost"
    port = 9900

    if len(sys.argv) > 1:
        host = sys.argv[1]

    if len(sys.argv) > 2:
        port = int(sys.argv[2])

    zls = ZDBLiveStats(host, port)

    try:
        zls.live()

    finally:
        zls.finish()
