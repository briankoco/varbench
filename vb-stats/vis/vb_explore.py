#!/usr/bin/env python

"""
   This file is part of the 'varbench' project developed by the
   University of Pittsburgh with funding from the United States
   National Science Foundation and the Department of Energy.

   Copyright (c) 2017, Brian Kocoloski <briankoco@cs.pitt.edu>

   This is free software.  You are permitted to use, redistribute, and
   modify it as specified in the file "LICENSE.md".
"""

import os
import copy
import sys
import random
import getopt
import numpy as np
import scipy.stats
import matplotlib.pyplot as plt
import matplotlib.ticker



class VB_Histogram:
    """
     A V_Histogram consists of:
     (1) A parent VB_Selection, which contains the dataset
     (2) A figure/axes to plot in
     (3) A number of bins to plot
    """

    def __init__(self, selection=None, fig=None, ax=None, num_bins=None):
        self.selection = selection
        self.fig = fig 
        self.ax = ax
        self.num_bins = num_bins

    def __percentage_formatter__(self, x, pos=0):
        return '%.2f%%'%(100*x)

    def __usec_to_sec_formatter__(self, x, pos=0):
        return '%.2f'%(x/1000000)

    def __build_title__(self):
        # Build title based on current VB_Selection filters
        if self.selection is None:
            return 'Sample Title'

        if len(self.selection.filters) == 0:
            return 'All Data'

        s = "Filters: "

        for f in self.selection.filters:
            s += f.opt + " = " + str(f.arg_list) + ',' 

        return s

    def render(self):
        if self.selection is None:
            print 'Cannot render histogram: no data selected'
            return

        data = self.selection.get_time_data()
        if data is None:
            print 'Cannot render histogram: no data selected'
            return

        hist, bins = np.histogram(
            data,
            bins=self.num_bins,
            weights=[1.0/len(data) for p in range(len(data))])

        width  = 0.7 * (bins[1] - bins[0])

        # What the hell was this?
        # 'center' is the center of each bar, which should be
        # everything but the first bin minus the width that we want 
        #center = bins[:-1] + bins[1:] / 2
        center = bins[1:] - width

        self.ax.cla()
        self.ax.bar(center, hist, align='center', width=width)
        self.ax.yaxis.set_major_formatter(
            matplotlib.ticker.FuncFormatter(self.__percentage_formatter__))
        self.ax.xaxis.set_major_formatter(
            matplotlib.ticker.FuncFormatter(self.__usec_to_sec_formatter__))

        self.ax.set_title(self.__build_title__())
        self.ax.set_xlabel('Seconds')

        self.fig.show()


class VB_Filter:
    """
    A VB_Filter consists of:
    (1) A parent VB_Selection, which contains the dataset
    (2) A user-supplied filter to apply to the data
    """

    def __init__(self, selection=None, opt=None, arg_list=None):
        # Set up the default filters
        default_filters = [
            ["i", "instance=", "filter by instance"],
            ["r", "rank=", "filter by rank (same as -i)"],
            ["l", "local-instance=", "filter by local_instance"],
            ["R", "local-rank=", "filter by local_rank (same as -l)"],
            ["I", "iteration=", "filter by iteration"],
            ["n", "node=", "filter by node"],
            ["s", "socket=", "filter by socket"],
            ["c", "core=", "filter by core"],
            ["h", "hw-thread=", "filter by hw-thread"],
            ["o", "os-core=", "filter by os-core"],
        ]

        self.possible_filters = default_filters
        self.selection = selection
        self.opt = opt
        self.arg_list = arg_list

    def get_possible_filters(self):
        return self.possible_filters

    def _print(self):
        print self.opt + "=" + str(self.arg_list)

    def _apply(self):
        df = self.selection.get_dataset()
        vb = self.selection.get_vbobj()

        if self.opt == "i" or self.opt == "r":
            self.selection.set_dataset(
                vb.get_data(df=df, instance_list=self.arg_list)
            )

        elif self.opt == "l":
            self.selection.set_dataset(
                vb.get_data(df=df, local_instance_list=self.arg_list)
            )

        elif self.opt == "I":
            self.selection.set_dataset(
                vb.get_data(df=df, iteration_list=self.arg_list)
            )

        elif self.opt == "n":
            self.selection.set_dataset(
                vb.get_data(df=df, node_list=self.arg_list)
            )

        elif self.opt == "s":
            local_rank_list = self.__get_local_ranks_by_processor__(socket=True)
            self.selection.set_dataset(
                vb.get_data(df=df, local_instance_list=local_rank_list)
            )

        elif self.opt == "c":
            local_rank_list = self.__get_local_ranks_by_processor__(core=True)
            self.selection.set_dataset(
                vb.get_data(df=df, local_instance_list=local_rank_list)
            )

        elif self.opt == "h":
            local_rank_list = self.__get_local_ranks_by_processor__(hw_thread=True)
            self.selection.set_dataset(
                vb.get_data(df=df, local_instance_list=local_rank_list)
            )

        elif self.opt == "o":
            local_rank_list = self.__get_local_ranks_by_processor__(os_core=True)
            self.selection.set_dataset(
                vb.get_data(df=df, local_instance_list=local_rank_list)
            )


    # Use processor map to get all local ranks that correspond to
    # this resource
    def __get_local_ranks_by_processor__(self, socket=False,
            core=False, hw_thread=False, os_core=False):
        vb = self.selection.get_vbobj()
        local_rank_list = []

        for local_rank, [s, c, h, o] in enumerate(vb.processor_map):
            if (socket and s in self.arg_list) or \
               (core and c in self.arg_list) or \
               (hw_thread and h in self.arg_list) or \
               (os_core and o in self.arg_list):
               local_rank_list.append(local_rank)

        return local_rank_list


class VB_Selection:
    """ 
    A VB_Selection consists of:
    (1) A global index
    (2) A vbs dataset object
    (3) A current data frame
    (4) A list of VB_Filters
    (5) A VB_Histogram
    """

    def __init__(self, index, vb_obj, filters=[], histogram=None):
        self.index = index
        self.vb_obj = vb_obj
        self.dataset = vb_obj.get_data()
        self.filters = copy.deepcopy(filters)
        self.histogram = histogram

    def set_values(self, index=None, vb_obj=None, dataset=None, filters=[], histogram=None):
        if index is not None:
            self.index = index

        if vb_obj is not None:
            self.vb_obj = vb_obj

        if dataset is not None:
            self.dataset = dataset

        if filters is not None:
            self.filters = copy.deepcopy(filters)

        if histogram is not None:
            self.histogram = histogram

    def get_time_data(self):
        if self.dataset is None:
            print 'No data selected for VB_Selection'
            return None

        return self.vb_obj.get_time(df=self.dataset)

    def get_vbobj(self):
        return self.vb_obj

    def get_dataset(self):
        return self.dataset

    def set_dataset(self, dataset):
        self.dataset = dataset

    def add_filter(self, opt, arg):
        opt      = self.__opt_str_to_opt__(opt)
        arg_list = self.__arg_str_to_arg_list__(arg)

        assert opt is not None

        # Remove old filters with this option
        self.filters[:] = [
            f for f in self.filters if 
                (
                    (f.opt != opt)
                )
            ]

        # Add new filter
        new_filter = VB_Filter(
            opt=opt,
            arg_list=arg_list,
            selection=self
        )
        self.filters.append(new_filter)

        # We need to clear the df now and reapply all current filters
        self.dataset = self.vb_obj.get_data()
        self.apply_filters()

    def remove_filter(self, opt, arg):
        opt      = self.__opt_str_to_opt__(opt)
        arg_list = self.__arg_str_to_arg_list__(arg)

        assert opt is not None
        
        self.filters[:] = [
            f for f in self.filters if 
                (
                    (f.opt != opt) or 
                    (f.arg_list != arg_list)
                )
            ]

        # We need to clear the df now and reapply all current filters
        self.dataset = self.vb_obj.get_data()
        self.apply_filters()

    def apply_filters(self):
        for f in self.filters:
            f._apply()

    def print_filters(self):
        for f in self.filters:
            f._print()

    def print_statistics(self):
        times = self.get_time_data()
        mean  = np.mean(times)
        stdev = np.std(times)
        r     = max(times) - min(times)

        s =  "Selection " + str(self.index) + " statistics:"
        s += "\nmean:             "    + str(mean)
        s += "\nstdev:            " + str(stdev)
        s += "\n  (as % of mean): " + self.__print_as_perc_of__(stdev, mean)
        s += "\ncov:              " + str(scipy.stats.variation(times))
        s += "\nskewness:         " + str(scipy.stats.skew(times))
        s += "\nkurtosis:         " + str(scipy.stats.kurtosis(times, fisher=False))
        s += "\nrange:            " + str(r)
        s += "\n  (as % of mean): " + self.__print_as_perc_of__(r, mean)
        s += "\n"


        #  "\n  (as %% of mean): %.2f" %(r*100/mean) + "%"

        print s

    def __print_as_perc_of__(self, _as, _of):
        return str("%.2f" % (_as*100/_of)) + "%"
        

    def __opt_str_to_opt__(self, opt):
        dummy = VB_Filter()
        for f in dummy.get_possible_filters():
            if opt == f[1].strip('='):
                return f[0]
            elif opt == f[0]:
                return opt

        return None
        
    def __arg_str_to_arg_list__(self, arg_str):
        if ',' in arg_str:
            return [int(x.strip()) for x in arg_str.split(',')]
        elif '-' in arg_str:
            start, end = arg_str.split('-')
            return [x for x in range(int(start.strip()), int(end.strip())+1)]
        elif 'r' in arg_str:
            return ['r']
        else:
            return [int(arg_str)]




def print_cmds(cmds):
    for c in cmds:
        print "\t" + c[0] + ": " + c[1]

def print_filters(filters):
    for f in filters:
        print "\t-" + f[0] + " (--"  + f[1] + "):\n\t\t" + f[2] 

def parse_filter(idx, mode, argv, selections, possible_filters):

    opts = []
    args = []

    if mode == 'list':
        selections[idx].print_filters()
        return

    try:
        f_short = ""
        f_long = []
        for f in possible_filters:
            f_short += f[0]
            f_long.append(f[1])
            if f[1][-1] == "=":
                f_short += ":"

        opts, args = getopt.getopt(argv, f_short, f_long)

    except getopt.GetoptError, err:
        print str(err)
        print_filters(filters)
        return

    for o, a in opts:
        if mode == 'add':
            selections[idx].add_filter(o.strip('-'), a)
        else:
            selections[idx].remove_filter(o.strip('-'), a)

        selections[idx].histogram.render()



def parse_input(inp, selections):
    dummy = VB_Filter()
    filters = dummy.get_possible_filters()

    cmds = [
        ["help", "list all possible commands", []],
        ["quit", "quit this shell", []],
        ["filter <add/remove/list> <set> [OPTIONS ...]", "filter data in <set> by:\n\t\tspecific value (e.g., 10)\n\t\trandom value (r)\n\t\tcomma-delimited list (e.g., 1,3,5)\n\t\trange (e.g., 1-10)", filters],
        ["stats <set> [OPTIONS ...]", "print summary statistics for data in <set>\n", []],
    ]


    words = inp.split()

    if len(words) == 0:
        print_cmds(cmds)
        return False

    cmd = words[0]
    remainder = words[1:]

    if cmd == "quit":
        return True

    elif cmd == "help":
        print_cmds(cmds)

    elif cmd == "filter":
        if len(remainder) < 2:
            print "filter <add/remove/list> <set> [OPTIONS ...]"
            print_filters(filters)
        else:
            mode = remainder[0]
            idx  = int(remainder[1])

            if idx < 0 or idx >= len(selections):
                print 'Invalid data set ' + str(idx) + '. Only ' + str(len(selections)) + ' sets.'
            elif mode != 'add' and mode != 'remove' and mode != 'list':
                print 'Invalid mode: ' + mode + '. Must be add/remove/list'
            else:
                parse_filter(idx, mode, remainder[2:], selections, filters)

    elif cmd == "stats":
        if len(remainder) < 1:
            print "stats <set> [OPTIONS ...]"
        else:
            idx  = int(remainder[0])

            if idx < 0 or idx >= len(selections):
                print 'Invalid data set ' + str(idx) + '. Only ' + str(len(selections)) + ' sets.'
            else:
                selections[idx].print_statistics()

    return False

def usage(argv):
    print 'Usage: ' + argv[0] + ' [OPTIONS] <vb meta file>'
    print '  -h (--help):           print help and exit'
    print '  -H (--nr-histograms=): number of histograms to show'
    print '  -r (--nr-rows):        number of rows'
    print '  -c (--nr-columns):     number of columns'
    print '  -v (--vbs-path=):      path to VB_Stats python module (default: $HOME/varbench/vb-stats)'

def parse_argv(argv):
    opts = []
    args = []
    nr      = 1
    rows    = 1
    columns = 1
    vb_path = os.path.expanduser('~' + '/varbench/vb-stats')

    try:
        opts, args = getopt.getopt(argv[1:],
            "hH:r:c:v:",
            ["help", "nr-histograms=", "nr-rows=", "nr-columns=", "vbs-path="])
    except getopt.GetoptError, err:
        usage(argv)
        sys.exit(2)

    for o, a in opts:
        if o in ("-h", "--help"):
            usage(argv)
            sys.exit(0)

        elif o in ("-H", "--nr-histograms"):
            nr = int(a)

        elif o in ("-r", "--nr-rows"):
            rows = int(a)

        elif o in ("-c", "--nr-columns"):
            columns = int(a)

        elif o in ("-v", "--vbs-path"):
            vb_path = a

        else:
            print 'Unhandled option'
            usage(argv)
            sys.exit(2)


    if len(args) != 1:
        usage(argv)
        sys.exit(2)

    if rows * columns < nr:
        print 'Not enough rows/columns to plot the histograms'
        usage(argv)
        sys.exit(0)

    return vb_path, args[0], nr, rows, columns


def main(argc, argv):
    vb_path, meta_file, nr_histograms, nr_rows, nr_columns = parse_argv(argv) 

    sys.path.insert(0, vb_path)
    import vb_stats as vbs
    vb = vbs.VB_Stats(meta_file, load_data=True)

    print 'vb metadata:'
    print '\tnum_instances:       ' + str(vb.num_instances)
    print '\tnum_nodes:           ' + str(vb.num_nodes)
    print '\tnum_local_instances: ' + str(vb.num_local_instances)
    print '\titerations:          ' + str(vb.iterations)
    print '\tmemory_affinity:     ' + str(vb.memory_affinity)
    print '\tprocessor_pinning:   ' + str(vb.processor_pinning)
    print '\tkernel_name:         ' + str(vb.kernel_name)
    print '\tkernel_args:         ' + str(vb.kernel_args)

    """
    print '\tnode_map:' 
    for idx, node in enumerate(vb.node_map):
        print '\t\t   node_id ' + str(idx) + ': ' + str(node)
    print '\tprocessor_map:' 
    for idx, processor in enumerate(vb.processor_map):
        print '\t\tlocal_rank ' + str(idx) + ': ' + str(processor)
    """


    # Create the subplots 
    font = {'size' : 10}
    matplotlib.rc('font', **font)
    f, axarr = plt.subplots(nr_rows, nr_columns, sharex=True,
    sharey=False,
        figsize=(16, 10))

    plt.ion()

    # Create the selections
    selections = [None for s in range(nr_histograms)]
    for idx, ax in enumerate(f.axes):
        selections[idx] = VB_Selection(idx, copy.deepcopy(vb))

        #  Create the histogram
        _histogram = VB_Histogram(
            selections[idx],
            fig=f,
            ax=ax,
            num_bins=30
        )

        # Store them in the selection      
        selections[idx].set_values(
            histogram=_histogram,
        )

        # Render the histogram
        _histogram.render()


    while True:
        if sys.version_info[0] < 3:
            inp = raw_input('$$: ')
        else:
            inp = input('$$: ')

        die = parse_input(inp, selections)

        if die:
            break

if __name__ == "__main__":
    argv = sys.argv
    argc = len(argv)
    main(argc, argv)
