#!/usr/bin/env python

import os
import sys
import getopt
import json

def usage(argv, exit=None):
    print "Usage: %s [OPTIONS] <VB metadata file> <VB JSON topology file (output)>" % argv[0]
    print " -h (--help)      : print help and exit"
    print " -v (--vbs-path=) : path to VB Stats python module"

    if exit is not None:
        sys.exit(exit)


def parse_cmd_line(argc, argv):
    opts = []
    args = []

    cur_path = os.path.dirname(os.path.realpath(__file__))
    vb_path = cur_path + "/../vb-stats/"

    try:
        opts, args = getopt.getopt(
            argv[1:],
            "hv:",
            ["help", "vb-path="]
        )
    except getopt.GetoptError, err:
        print >> sys.stderr, err
        usage(argv, exit=1)

    for o, a in opts:
        if o in ("-h", "--help"):
            usage(argv, exit=0)

        elif o in ("-v", "--vb-path"):
            vb_path = a

        else:
            usage(argv, exit=1)

    if len(args) != 2:
        usage(argv, exit=1)

    return vb_path, args[0], args[1]

def main(argc, argv, envp):
    vb_path, meta, json_file = parse_cmd_line(argc, argv)
    procs = []

    # Try to import vb-path
    try:
        sys.path.insert(0, vb_path)
        from vb_stats import VB_Stats as vbs
    except ImportError:
        print >> sys.stderr, "Could not import VB_Stats. Please specify path to VB_Stats with '--vbs-path'"
        usage(argv, exit=2)

    with vbs(meta, load_data=False) as vb:
        with open(json_file, "w") as f:
            num_processors = vb.num_sockets_per_node * vb.num_cores_per_socket * vb.num_hw_threads_per_core
            json.dump({
                "processor_info" : {
                    "num_processors"      : num_processors,
                    "num_sockets"         : vb.num_sockets_per_node,
                    "cores_per_socket"    : vb.num_cores_per_socket,
                    "hw_threads_per_core" : vb.num_hw_threads_per_core
                },

                # The format of p: [socket, core, hw_thread, os_core]
                "processor_list" : [
                    {
                        "os_core"   : p[3],
                        "socket"    : p[0],
                        "core"      : p[1],
                        "hw_thread" : p[2]
                    } for p in vb.processor_map
                ]
            }, f, indent=4)

if __name__ == "__main__":
    argv = sys.argv
    argc = len(argv)
    envp = os.environ
    sys.exit(main(argc, argv, envp))
