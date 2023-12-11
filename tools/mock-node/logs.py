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

def us_pretty(time):
    if time < 1000:
        return f'{time} micros'
    if time < 1000000:
        return f'{round(time/1000, 3)} millis'
    return f'{round(time/1000000, 3)} seconds'


def iter_times(filename, chunk_callback=None, head_update_callback=None, applies_callback=None):
    if chunk_callback is None and head_update_callback is None and applies_callback is None:
        sys.exit(f'nothing to do')

    chunk_pattern = re.compile(r'.*do_apply_chunks{block_height=(\d+).*new_chunk{shard_id=(\d+)}: chain: close time.busy=([^\s]+) time.idle=([^\s]+)')
    head_update_pattern = re.compile(r'([^\s]+).*Head updated to .* at (\d+)')
    applies_pattern = re.compile(r'.*do_apply_chunks{block_height=(\d+).*new_chunk{shard_id=(\d+)}:process_state_update:apply{num_transactions=\d+}:([^{]+){([^{]+)}: runtime: close time.busy=([^\s]+) time.idle=([^\s]+)')
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
                    first_timestamp = datetime.datetime.strptime(g[0], "%Y-%m-%dT%H:%M:%S.%f%z")

            if chunk_callback is not None:
                matches = chunk_pattern.match(l)
                if matches is not None:
                    g = matches.groups()
                    block_height = int(g[0])
                    shard_id = int(g[1])
                    busy = parse_time(time_pattern, g[2])
                    idle = parse_time(time_pattern, g[3])

                    if busy > 1000000:
                        print(f'applying chunk at height {block_height} shard {shard_id} took a long time: {us_pretty(busy)}')
                    if not chunk_callback(block_height, shard_id, busy, idle):
                        return
                    continue
            if head_update_callback is not None:
                matches = head_update_pattern.match(l)
                if matches is not None:
                    g = matches.groups()
                    timestamp = g[0]
                    height = int(g[1])
                    if timestamp[-1] == 's':
                        elapsed_seconds = float(timestamp[:-1])
                    else:
                        timestamp = datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f%z")
                        diff = timestamp - first_timestamp
                        elapsed_seconds = diff.total_seconds()
                    if not head_update_callback(elapsed_seconds, height):
                        return
                    continue
            if applies_callback is not None:
                matches = applies_pattern.match(l)
                if matches is not None:
                    g = matches.groups()
                    block_height = int(g[0])
                    shard_id = int(g[1])
                    apply_type = g[2]
                    apply_item = g[3]
                    busy = parse_time(time_pattern, g[4])
                    idle = parse_time(time_pattern, g[5])

                    if not applies_callback(block_height, shard_id, apply_type, apply_item, busy, idle):
                        return
                    continue
            #print(f'other: {l}')



class Plot:
    def __init__(self, args):
        self.heights = []
        self.times = []
        self.trunced_times = []
        self.colors = []
        self.args = args
        self.max_time = 3000000

    def on_chunk_time(self, block_height, shard_id, busy, idle):
        if self.args.start is not None and block_height < self.args.start:
            return True
        if self.args.end is not None and block_height > self.args.end:
            return False
        if shard_id == self.args.shard_id:
            self.heights.append(block_height)
            self.times.append(busy)
            if busy <= self.max_time:
                self.trunced_times.append(busy)
                self.colors.append('blue')
            else:
                self.trunced_times.append(self.max_time)
                self.colors.append('red')
        return True


def plot_cmd(args):
    if args.shard_id is None:
        sys.exit('please give --shard-id')
    plot = Plot(args)
    iter_times(args.file, chunk_callback=plot.on_chunk_time)
    plt.bar(plot.heights, plot.trunced_times, color=plot.colors)
    plt.show()


class Summary:
    def __init__(self, args):
        self.times = {}
        self.args = args
        self.update_times = []
        self.last_update = None

    def on_chunk_time(self, block_height, shard_id, busy, idle):
        if self.args.shard_id is not None and shard_id != self.args.shard_id:
            return True
        if self.args.start is not None and block_height < self.args.start:
            return True
        if self.args.end is not None and block_height > self.args.end:
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
    summary = Summary(args)
    iter_times(args.file, chunk_callback=summary.on_chunk_time, head_update_callback=summary.on_head_update)
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
        badcount = 0
        for sh, tbus, tid in t:
            if tbus > maxtime:
                maxtime = tbus
                maxheight = sh
            if tbus > 1000000:
                badcount +=1
        print(f'percent above 1 sec {round(100*badcount/len(t), 2)}')
        print(f'max: height {maxheight} time: {us_pretty(maxtime)}')
        print(f'average time: busy: {round(m[1], 3)} µs idle: {round(m[2], 3)} µs')
        print(f'std dev: busy: {round(s[2], 3)} idle: {round(s[2], 3)}')

class ChunkApplication:
    def __init__(self):
        self.by_type = {}
        self.by_item = {}

    def add_event(self, apply_type, apply_item, busy):
        try:
            c = self.by_type[apply_type]
            self.by_type[apply_type] = (c[0] + 1, c[1] + busy)
        except KeyError:
            self.by_type[apply_type] = (1, busy)
        try:
            c = self.by_item[apply_item]
            self.by_item[apply_item] = (c[0] + 1, c[1] + busy)
        except KeyError:
            self.by_item[apply_item] = (1, busy)


class Applies:
    def __init__(self, args):
        self.applies = {}
        self.args = args

    def on_apply(self, block_height, shard_id, apply_type, apply_item, busy, idle):
        if self.args.shard_id is not None and shard_id != self.args.shard_id:
            return True
        if self.args.start is not None and block_height < self.args.start:
            return True
        if self.args.end is not None and block_height > self.args.end:
            return False
        try:
            c = self.applies[(block_height, shard_id)]
        except KeyError:
            c = ChunkApplication()
            self.applies[(block_height, shard_id)] = c
        c.add_event(apply_type, apply_item, busy)
        return True


def applies_cmd(args):
    applies = Applies(args)
    iter_times(args.file, applies_callback=applies.on_apply)
    for k, apply in applies.applies.items():
        print(f'--- height {k[0]} shard {k[1]} ---')
        for apply_type, stats in apply.by_type.items():
            print(f'{stats[0]} instances of {apply_type} with total time busy {us_pretty(stats[1])}')

        by_item = list(apply.by_item.items())
        by_item.sort(key=lambda x: x[1][1])
        x = 0
        for item in reversed(by_item):
            print(f'{item[0]}: {us_pretty(item[1][1])}')
            #print(f'{us_pretty(item[1][1])} for {item[1][0]} occurrence of {item[0]}')
            x += 1
            if x > 5:
                break


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title='subcommands',
                                       description='valid subcommands',
                                       help='additional help')
    parser.add_argument('--start', type=int)
    parser.add_argument('--end', type=int)
    parser.add_argument('--shard-id', type=int)
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
    plot_parser.set_defaults(func=plot_cmd)

    applies_parser = subparsers.add_parser('applies',
                                        help='''
    Apply times
    ''')
    applies_parser.set_defaults(func=applies_cmd)
    args = parser.parse_args()
    args.func(args)
