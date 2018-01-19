#!/usr/bin/python

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
import getopt
import scipy.stats
import numpy as np

import dash
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output

import plotly.graph_objs as go


class VB_Filter:
    """
    A VB_Filter consists of:
    (1) A parent VB_Selection, which contains the dataset
    (2) A user-supplied filter to apply to the data
    """

    def __init__(self, selection=None, opt=None, arg_list=None):
        # Set up the default filters
        default_filters = {
            'instance' : ["i", "instance=", "filter by instance"],
            'local instance' : ["l", "local-instance=", "filter by local_instance"],
            'iteration' : ["I", "iteration=", "filter by iteration"],
            'node' : ["n", "node=", "filter by node"],
            'socket' : ["s", "socket=", "filter by socket"],
            'core' : ["c", "core=", "filter by core"],
            'HW thread' : ["h", "hw-thread=", "filter by hw-thread"],
            'OS core' : ["o", "os-core=", "filter by os-core"],
        }

        self.possible_filters = default_filters
        self.selection = selection
        self.opt = opt
        self.arg_list = arg_list

    def _get_unique_values(self):
        vb = self.selection.get_vbobj()

        if self.opt == "i" or self.opt == "r":
            return range(vb.num_instances)

        elif self.opt == "l":
            return range(vb.num_local_instances)

        elif self.opt == "I":
            return range(vb.iterations)

        elif self.opt == "n":
            return range(vb.num_nodes)

        elif self.opt == "s":
            return range(vb.num_sockets_per_node)

        elif self.opt == "c":
            return range(vb.num_cores_per_socket)

        elif self.opt == "h":
            return range(vb.num_hw_threads_per_core)

        elif self.opt == "o":
            return range(vb.num_local_instances)

        assert 1 == 0

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
    (1) A vb dataset object
    (2) A current data frame
    (3) A list of VB_Filters
    """

    def __init__(self, vb_obj, filters=[]):
        self.vb_obj = vb_obj
        self.dataset = vb_obj.get_data()
        self.filters = copy.deepcopy(filters)

    def set_values(self, vb_obj=None, dataset=None, filters=None):
        if vb_obj is not None:
            self.vb_obj = vb_obj

        if dataset is not None:
            self.dataset = dataset

        if filters is not None:
            self.filters = copy.deepcopy(filters)

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
        filters = dummy.get_possible_filters()

        for f in filters:
            if opt == filters[f][1].strip('='):
                return filters[f][0]
            elif opt == filters[f][0]:
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
        v = filters[f]
        print "\t-" + v[0] + " (--"  + v[1] + "):\n\t\t" + v[2] 

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
            val = possible_filters[f]
            f_short += val[0]
            f_long.append(val[1])
            if val[1][-1] == "=":
                f_short += ":"

        opts, args = getopt.getopt(argv, f_short, f_long)

    except getopt.GetoptError, err:
        print str(err)
        print_filters(possible_filters)
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

class VB_Dash_App:
    """ 
    A VB_Dash_App consists of:
    (1) A VB dataset object
    (2) A VB_Selection object for storing data filters
    (3) A Dash application
    """

    def __init__(self, vb_obj):
        self.vb_obj = vb_obj
        self.selection = VB_Selection(self.vb_obj)

        dummy = VB_Filter(selection=self.selection)
        filters = dummy.get_possible_filters()


        column_style = {
            'min-height' : '100px',
            'padding'    : '5px',
            'width'      : '400px'
        }

        self.app = dash.Dash()
        self.app.layout = html.Div([
            html.H1('Varbench Data Anaylsis Tool'),

            html.Div([
                html.Div([
                    html.H3('Filter Selection'),

                    dcc.Dropdown(
                        id='filter-dropdown',
                        placeholder = 'Select filter type ...',
                        options = [{'label' : f, 'value' : f} for f in filters],
                    ),

                    dcc.Dropdown(
                        id='filter-dropdown-2',
                        placeholder = 'Select filter value ...',
                        multi=True
                    ),

                    html.Button('Add Filter', id='filter-button', style={
                        'display'    : 'block', 
                        'text-align' : 'center',
                        'margin'     : '0 auto',}
                    )

                ], style=column_style),

                html.Div([
                    html.H3('Active Filters'),

                    dcc.Dropdown(
                        id='test'
                    )
                ], style=column_style),
            ], style={'columnCount': 2}),

            html.Div([
                dcc.Graph(
                    id='histogram'
                )
            ], style={'columnCount': 1})
        ])

        # **** Callbacks that alter state of the first dropdown ****

        # Clear dropdown when button is clicked
        @self.app.callback(
            Output('filter-dropdown', 'value'),
            [Input('filter-button', 'n_clicks')]
        )
        def dropdown_clear(button_n_clicks):
            return None

        # **** Callbacks that alter state of second dropdown ****

        # Enable the 2nd dropdown on selection of 1st
        @self.app.callback(
            Output('filter-dropdown-2', 'disabled'),
            [Input('filter-dropdown', 'value')]
        )
        def dropdown2_enable(value):
            if value:
                return False
            else:
                return True

        # Clear value in 2nd dropdown on selection of 1st, or on button click'
        @self.app.callback(
            Output('filter-dropdown-2', 'value'),
            [Input('filter-dropdown', 'value'),
             Input('filter-button', 'n_clicks')])
        def dropdown2_clear(dropdown_value, button_n_clicks):
            return None

        # Enable options in 2nd dropdown based on selection of 1st
        @self.app.callback(
            Output('filter-dropdown-2', 'options'),
            [Input('filter-dropdown', 'value')]
        )
        def dropdown2_populate(value):
            if value:
                dummy = VB_Filter(selection=self.selection)
                possible_filters = dummy.get_possible_filters()
                dummy.opt = possible_filters[value][0]

                return [{'label' : d, 'value' : d} for d in dummy._get_unique_values()]
            else:
                return []

        # **** Callbacks that alter state of the button ****

        # Enable button on selection of 2nd dropdown
        @self.app.callback(
            Output('filter-button', 'disabled'),
            [Input('filter-dropdown-2', 'value')]
        )
        def button_enable(value):
            if value:
                return False
            else:
                return True


        # **** Callbacks that alter the state of the figure ****
        @self.app.callback(
            Output('histogram', 'figure'),
            [Input('filter-button', 'n_clicks')]
        )
        def graph_update(click):
            time_usec = self.vb_obj.get_time(as_list=True)
            time_sec  = [t/1000000. for t in time_usec]

            # Generate a histogram
            data = [go.Histogram(x=time_sec, histnorm='percent', nbinsx=100)]

            return {
                'data': data,
                'layout': {
                    'title' : self.vb_obj.kernel_name.replace('_', ' ').capitalize(),
                    'xaxis' : dict(title='Seconds'),
                    'yaxis' : dict(title='Percentage')
                }
            }


        """
        def callback(radio_value):
            if radio_value == 'OFF':
                return {}

            time_usec = self.vb_obj.get_time(as_list=True)
            time_sec  = [t/1000000. for t in time_usec]

            # Generate a histogram
            data = [go.Histogram(x=time_sec, histnorm='percent', nbinsx=100)]

            return {
                'data': data,
                'layout': {
                    'title' : self.vb_obj.kernel_name.replace('_', ' ').capitalize(),
                    'xaxis' : dict(title='Seconds'),
                    'yaxis' : dict(title='Percentage')
                }
            }
        """

    # Launch the server app
    def launch_app(self, hostname='localhost', port=8050):
        self.app.server.run(host=hostname, port=port, debug=True)


def usage(argv):
    print 'Usage: ' + argv[0] + ' [OPTIONS] <vb meta file>'
    print '  -h (--help)    : print help and exit'
    print '  -H (--Host=)   : hostname for Dash HTPP server (default: localhost)'
    print '  -p (--port=)   : port for Dash HTTP server (default: 8050)'
    print '  -v (--vb-path=): path to VB_Stats python module (default: $HOME/varbench/vb-stats)'

def parse_argv(argv):
    opts = []
    args = []
    hostname = 'localhost'
    port     = 8050
    vb_path  = os.path.expanduser('~' + '/varbench/vb-stats')

    try:
        opts, args = getopt.getopt(argv[1:],
            "hH:p:v:",
            ["help", "Host=", "port=", "vb-path="])
    except getopt.GetoptError, err:
        usage(argv)
        sys.exit(2)

    for o, a in opts:
        if o in ("-h", "--help"):
            usage(argv)
            sys.exit(0)

        elif o in ("-H", "--Host"):
            hostname = a

        elif o in ("-p", "--port"):
            port = int(a)

        elif o in ("-v", "--vb-path"):
            vb_path = a

        else:
            print 'Unhandled option'
            usage(argv)
            sys.exit(2)


    if len(args) != 1:
        usage(argv)
        sys.exit(2)

    return vb_path, hostname, port, args[0]


def main(argc, argv):
    vb_path, hostname, port, meta_file = parse_argv(argv) 

    # Insert vb path in the user's environment
    sys.path.insert(0, vb_path)
    import vb_stats as vbs

    # Load the VB module
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

    print hostname, port

    # Create the Dash app
    app = VB_Dash_App(vb)
    app.launch_app(hostname=hostname, port=port)

    return


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
