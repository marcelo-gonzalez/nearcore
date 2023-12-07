import argparse
import datetime
import re
import numpy as np
import matplotlib.pyplot as plt
import sys

def parse_time(time_pattern, s):
    g = time_pattern.match(s).groups()
    amt = float(g[0])
    if g[1] == 's':
        return 1000000*amt
    if g[1] == 'ms':
        return 1000*amt
    if g[1] == 'µs' or g[1] == 'us':
        return amt
    if g[1] == 'ns':
        return amt/1000

def iter_times(filename, chunk_callback, head_update_callback):
    chunk_pattern = re.compile(r'.*do_apply_chunks{block_height=(\d+).*new_chunk{shard_id=(\d+)}: chain: close time.busy=([^\s]+) time.idle=([^\s]+)')
    head_update_pattern = re.compile(r'([^\s]+).*Head updated to .* at (\d+)')
    time_pattern = re.compile(r'([\d|.]+)(.*)')
    timestamp_pattern = re.compile(r'([^\s]+).*')
    first_timestamp = None
    with open(filename, 'r') as f:
        while True:
            l = f.readline()
            if len(l) < 1:
                break
            if first_timestamp is None:
                matches = timestamp_pattern.match(l)
                g = matches.groups()
                if g[0][-1] == 's':
                    first_timestamp = 'notneeded'
                else:
                    first_timestamp = datetime.datetime.fromisoformat(g[0])

            matches = chunk_pattern.match(l)
            if matches is None:
                matches = head_update_pattern.match(l)
                if matches is None:
                    continue
                g = matches.groups()
                timestamp = g[0]
                height = int(g[1])
                if timestamp[-1] == 's':
                    elapsed_seconds = float(timestamp[:-1])
                else:
                    timestamp = datetime.datetime.fromisoformat(timestamp)
                    diff = timestamp - first_timestamp
                    elapsed_seconds = diff.total_seconds()
                if not head_update_callback(elapsed_seconds, height):
                    return
                continue

            g = matches.groups()
            block_height = int(g[0])
            shard_id = int(g[1])
            busy = parse_time(time_pattern, g[2])
            idle = parse_time(time_pattern, g[3])

            if busy > 1000000:
                print(f'applying chunk at height {block_height} shard {shard_id} took a long time: {busy}')
            if not chunk_callback(block_height, shard_id, busy, idle):
                return 

class Plot:
    def __init__(self, start, end, shard_id):
        self.heights = []
        self.times = []
        self.trunced_times = []
        self.colors = []
        self.start = start
        self.end = end
        self.shard_id = shard_id
        self.max_time = 3000000

    def on_chunk_time(self, block_height, shard_id, busy, idle):
        if self.start is not None and block_height < self.start:
            return True
        if self.end is not None and block_height > self.end:
            return False
        if shard_id == self.shard_id:
            self.heights.append(block_height)
            self.times.append(busy)
            if busy <= self.max_time:
                self.trunced_times.append(busy)
                self.colors.append('blue')
            else:
                self.trunced_times.append(self.max_time)
                self.colors.append('red')
        return True

    def on_head_update(self, time, height):
        return True

def plot_cmd(args):
    plot = Plot(args.start, args.end, args.shard_id)
    iter_times(args.file, plot.on_chunk_time, plot.on_head_update)
    plt.bar(plot.heights, plot.trunced_times, color=plot.colors)
    plt.show()


class Summary:
    def __init__(self, start, end):
        self.times = {}
        self.start = start
        self.end = end
        self.update_times = []
        self.last_update = None

    def on_chunk_time(self, block_height, shard_id, busy, idle):
        if self.start is not None and block_height < self.start:
            return True
        if self.end is not None and block_height > self.end:
            return False
        try:
            t = self.times[shard_id]
        except KeyError:
            t = []
            self.times[shard_id] = t
        t.append([block_height, busy, idle])
        return True

    def on_head_update(self, time, height):
        if self.last_update is None:
            self.update_times.append((height, 0))
        else:
            self.update_times.append((height, time - self.last_update))

        self.last_update = time
        return True

def summary_cmd(args):
    summary = Summary(args.start, args.end)
    iter_times(args.file, summary.on_chunk_time, summary.on_head_update)
    updates = summary.update_times
    updates.sort(key=lambda x: x[1])
    i = 0
    avg_update_time = 0
    print('\nmax head update times')
    for u in reversed(updates):
        avg_update_time += u[1]

        if i < 5:
            print(f'time to update to block {u[0]}: {u[1]} seconds')
            i += 1
    avg_update_time /= len(updates)
    print(f'average update time {avg_update_time} seconds')
    print('')
    print('chunk application times:')
    print('')
    shards = sorted(list(summary.times.keys()))
    for shard_id in shards:
        print(f'---- shard {shard_id} ----')
        t = summary.times[shard_id]
        t = np.array(t)
        print(f'count: {len(t)}')
        m = np.mean(t, axis=0)
        s = np.std(t, axis=0)
        maxtime = 0
        maxheight = 0
        for sh, tbus, tid in t:
            if tbus > maxtime:
                maxtime = tbus
                maxheight = sh
        print(f'max: height {maxheight} time: {maxtime}')
        print(f'average time: busy: {round(m[1], 3)} µs idle: {round(m[2], 3)} µs')
        print(f'std dev: busy: {round(s[2], 3)} idle: {round(s[2], 3)}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title='subcommands',
                                       description='valid subcommands',
                                       help='additional help')
    parser.add_argument('--start', type=int)
    parser.add_argument('--end', type=int)
    parser.add_argument('--file', type=str, required=True)
    summary_parser = subparsers.add_parser('summary',
                                        help='''
    Summary
    ''')
    summary_parser.set_defaults(func=summary_cmd)
    plot_parser = subparsers.add_parser('plot',
                                        help='''
    Plot
    ''')
    plot_parser.add_argument('--shard-id', type=int, required=True)
    plot_parser.set_defaults(func=plot_cmd)

    args = parser.parse_args()
    args.func(args)
