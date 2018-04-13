#!/usr/bin/env python

import glob
import sys
import re
import getopt
import os
import pandas as pd
import csv
import math
import numpy as np

def usage(argv):
    print 'Usage: ' + argv[0] + ' [OPTIONS] <metadata file 1> <metadata file 2>'
    print '  --vb-path=<v>         : path to Python VB_Stats module (default: $HOME/varbench/vb-stats/'
    print '  --help                : print help and exit'

def parseCmdLine(argc, argv):
    opts = []
    args = []
    vb_path = os.path.expanduser('~') + '/varbench/vb-stats'

    try:
        opts,args = getopt.getopt(argv[1:],
            "hv:",
            ["help", "vb-path="])
    except getopt.GetoptError, err:
        print str(err)
        usage(argv)

    for o,a in opts:
        if o in ("-h", "--help"):
            usage(argv)
            sys.exit(0)
        elif o in ("-v", "--vb-path"):
            vb_path = a
        else:
            print "unhandled option"
            usage(argv)
            sys.exit(2)

    if len(args) != 2:
        usage(argv)
        sys.exit(2)

    return vb_path, args[0], args[1]


def main(argc, argv, envp):
    # Parse command line
    vb_path, meta1, meta2 = parseCmdLine(argc, argv)

    sys.path.insert(0, vb_path)
    import vb_stats as vbs

    with vbs.VB_Stats(meta1, load_data=False) as vb1:
        with vbs.VB_Stats(meta2, load_data=False) as vb2:
            if vb1.kernel_name != "operating_system" or\
               vb2.kernel_name != "operating_system":
               print >> sys.stderr, "The two metadata files must be for 'operating_system' kernels"
               return 1

            if vb1.misc["selection_seed"] != vb2.misc["selection_seed"]:
                print >> sys.stderr, "Warning: you are comparing program sets with different selection seeds"
            
            set1 = vb1.misc["program_set"]
            set2 = vb2.misc["program_set"]

            overlap = set1.intersection(set2)

            if len(set1) != len(set2):
                print >> sys.stderr, "Warning: you are comparing program sets of different sizes"
                print "%d sets overlap" % len(overlap) 
            else:
                print "%d sets overlap (%.2f%% overlap rate)" % (len(overlap), (float(len(overlap)*100. / len(set1))))

if __name__ == "__main__":
    argv = sys.argv
    argc = len(argv)
    envp = os.environ

    sys.exit(main(argc, argv, envp))
