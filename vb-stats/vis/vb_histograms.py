#!/usr/bin/python

"""
   This file is part of the 'varbench' project developed by the
   University of Pittsburgh with funding from the United States
   National Science Foundation and the Department of Energy.

   Copyright (c) 2017, Brian Kocoloski <briankoco@cs.pitt.edu>

   This is free software.  You are permitted to use, redistribute, and
   modify it as specified in the file "LICENSE.md".
"""

import sys
import random
import numpy as np
import vb_stats as vbs
import matplotlib.pyplot as plt
import matplotlib.ticker


def percentage_formatter(x, pos=0):
    return '%.2f%%'%(100*x)

def usec_to_sec_formatter(x, pos=0):
    return '%.2f'%(x/1000000)

def plot_histo(vb, ax, data, title, num_bins=30):
    hist, bins = np.histogram(data, bins=num_bins,  
        weights=[1.0/len(data) for p in range(len(data))])

    width  = 0.7 * (bins[1] - bins[0])
    center = (bins[:-1] + bins[1:]) / 2

    ax.bar(center, hist, align='center', width=width)
    ax.yaxis.set_major_formatter(
        matplotlib.ticker.FuncFormatter(percentage_formatter))
    ax.xaxis.set_major_formatter(
        matplotlib.ticker.FuncFormatter(usec_to_sec_formatter))
    ax.set_title(title)


def main():
    argv = sys.argv
    argc = len(argv)

    if (argc != 2):
        print 'Usage: ./vb_driver.py <meta file>'
        sys.exit(-1)

    vb = vbs.VB_Stats(argv[1])
    if vb.load_data() != 0:
        print 'Could not load vb data'
        sys.exit(-1)

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

    # Setup plot
    font = {'size' : 14}
    matplotlib.rc('font', **font)
    f, axarr = plt.subplots(2, 2, sharex=True, sharey=False,
        figsize=(16, 10))

    # Plot 1: variability across all ranks and instances
    data = vb.get_data()
    plot_histo(vb, axarr[0, 0], data, 
        'Variability Across all Instances'\
        + '\nand Iterations')

    # Plot 2: histogram of a random collective's finish times
    iteration = random.randint(0, vb.iterations - 1)
    iter_data = vb.get_data(iteration=iteration)
    plot_histo(vb, axarr[0, 1], iter_data, 
        'Cross-Instance Variability at'\
        + '\nRandom Iteration ' + str(iteration))

    # Plot 3: histogram of a random instance's finish times
    instance = random.randint(0, vb.num_instances - 1)
    # Find node id for instance 'instance'
    node = vb.get_node_id_of(instance)

    instance_data = vb.get_data(instance=instance)
    plot_histo(vb, axarr[1, 0], instance_data,
        'Cross-Iteration Variability at'\
        + '\nRandom Instance ' + str(instance)\
        + ' (Node = ' + str(node) + ')')
    axarr[1, 0].set_xlabel('Seconds')

    # Plot 4: histogram of a random node's finsih times at this
    # plot 2's iteration

    
    node_data = vb.get_data(iteration=iteration, node=node)
    plot_histo(vb, axarr[1, 1], node_data,
        'Cross-Instance Variability at'\
        + '\nNode ' + str(node)\
        + ', Iteration ' + str(iteration))
    axarr[1, 1].set_xlabel('Seconds')

    plt.show()

if __name__ == "__main__":
    main()
