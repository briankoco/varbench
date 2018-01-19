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
import sys
import xml.etree.ElementTree
from contextlib import contextmanager

""" A very pythonic pushd """
@contextmanager
def pushd(newDir):
    if not newDir:
        yield
    else:
        previousDir = os.getcwd()
        os.chdir(newDir)
        yield
        os.chdir(previousDir)

try:
    import pandas as pd
except ImportError:
    print >> sys.stderr, "ImportError: no pandas module available"
    raise

class VB_Stats:
    def __clear__(self):
        # Data representation
        self.data_frame          = pd.DataFrame()
        self.data_directory      = None
        self.meta_file           = None
        self.data_file           = None

        # Misc information from meta file
        self.num_instances       = 0
        self.num_nodes           = 0
        self.num_local_instances = 0
        self.iterations          = 0
        self.memory_affinity     = 'none'
        self.processor_pinning   = 'none'
        self.kernel_name         = 'none'
        self.kernel_args         = []
        self.node_map            = []
        self.processor_map       = []

        self.num_sockets_per_node    = 0
        self.num_cores_per_socket    = 0
        self.num_hw_threads_per_core = 0

        # Dictionary for kernel-specific misc. info
        self.misc = {}

    def __init__(self, meta, load_data=False):
        self.__clear__()
        self.data_directory, self.meta_file = os.path.split(meta)
        self.__load_metadata__()

        if load_data:
            self.load_data()

        return None

    def __enter__(self):
        return self

    def __exit__(self, *err):
        self.__clear__()

    def __as_list__(self, df):
        return df.as_matrix().tolist()

    def __process_miscellaneous_tag__(self, tag):
        for m_tag in tag:
            if m_tag.tag == "operating_system_misc":
                for s in m_tag:
                    if s.tag == "selection_seed":
                        self.misc["selection_seed"] = s.attrib.get("seed")

            elif m_tag.tag == "programs":
                program_set = set([])
                for program in m_tag:
                    program_name = program.attrib.get("name")
                    program_set.add(program_name)

                self.misc["program_set"] = program_set

    def __load_metadata__(self):
        with pushd(self.data_directory):
            tree = xml.etree.ElementTree.parse(self.meta_file)
        root = tree.getroot()

        for tag in root:
            if tag.tag == 'general':
                self.num_instances       = int(tag.find('num_instances').attrib.get('val'))
                self.num_nodes           = int(tag.find('num_nodes').attrib.get('val'))
                self.num_local_instances = int(tag.find('num_local_instances').attrib.get('val'))
                self.data_file = tag.find('data_file').attrib.get('val')

            elif tag.tag == 'options':
                for option in tag:
                    opt_id  = option.attrib.get('id')
                    opt_val = option.attrib.get('val')

                    if opt_id == 'iterations':
                        self.iterations = int(opt_val)
                    elif opt_id == 'memory_affinity':
                        self.memory_affinity = opt_val
                    elif opt_id == 'processor_pinning':
                        self.processor_pinning = opt_val

            elif tag.tag == 'kernel':
                self.kernel_name = tag.attrib.get('name')
                for arg in tag:
                    self.kernel_args.append(arg.attrib.get('val'))

            elif tag.tag == 'nodes':
                for idx, node in enumerate(tag):
                    assert(int(node.attrib.get('id')) == idx)
                    self.node_map.append(node.attrib.get('hostname'))

            elif tag.tag == 'processors':
                self.num_sockets_per_node    = int(tag.attrib.get('num_sockets'))
                self.num_cores_per_socket    = int(tag.attrib.get('cores_per_socket'))
                self.num_hw_threads_per_core = int(tag.attrib.get('hw_threads_per_core'))

                for idx, processor in enumerate(tag):
                    assert(int(processor.attrib.get('local_rank')) == idx)
                    self.processor_map.append(\
                        [int(processor.attrib.get('socket')),
                        int(processor.attrib.get('core')),
                        int(processor.attrib.get('hw_thread')),
                        int(processor.attrib.get('os_core'))])

            elif tag.tag == 'miscellaneous':
                self.__process_miscellaneous_tag__(tag)
            
            else:
                print 'Unknown XML tag: ' + str(tag.tag)

        return 0

    def load_data(self):
        with pushd(self.data_directory):
            self.data_frame = pd.read_csv(self.data_file)

    def get_node_id_of(self, instance, df=None):
        if df is None:
            df = self.data_frame

        # Just grab the node_id from the first row of 'instance'
        return df[df['rank'] == instance].iloc[0]['node_id']

    def get_data(self, df=None, column=None, as_list=False,
                 iteration=None,      iteration_list=None, 
                 local_instance=None, local_instance_list=None,
                 instance=None,       instance_list=None,
                 node=None,           node_list=None):

        if df is None:
            df = self.data_frame

        if iteration is not None:
            df = df[df['iteration'] == iteration]

        if iteration_list is not None:
            df = df[df['iteration'].isin(iteration_list)]

        if local_instance is not None:
            df = df[df['local_rank'] == local_instance]

        if local_instance_list is not None:
            df = df[df['local_rank'].isin(local_instance_list)]

        if instance is not None:
            df = df[df['rank'] == instance]

        if instance_list is not None:
            df = df[df['rank'].isin(instance_list)]

        if node is not None:
            df = df[df['node_id'] == node]

        if node_list is not None:
            df = df[df['node_id'].isin(node_list)]

        if column is not None:
            if column == 'instance':
                column = 'rank'
            elif column == 'local_instance':
                column = 'local_rank'

            df = df[column]

        if as_list:
            return self.__as_list__(df)
        else:
            return df

    def get_data_kv(self, key_value_string=None, column=None, as_list=False):
        _iteration = None
        _local_instance = None
        _instance = None
        _node = None
        
        if key_value_string is None:
            return self.get_data()

        for f in key_value_string.split(','):
            k, v = f.split('=')

            if k == 'iteration':
                _iteration = int(v)
            elif k == 'local_instance':
                _local_instance = int(v)
            elif k == 'instance':
                _instance = int(v)
            elif k == 'node_id':
                _node = int(v)
            else:
                print 'Unknown key: ' + k
                return pd.DataFrame()

        return self.get_data(\
            column=column,
            as_list=as_list,
            iteration=_iteration,\
            local_instance=_local_instance,\
            instance=_instance,\
            node=_node\
        )

    def get_time(self, df=None, as_list=False):
        return self.get_data(column='time_usec', df=df, as_list=as_list)

    def get_runtime(self, df=None):
        if df is None:
            df = self.data_frame

        return sum([max(self.get_time(df=self.get_data(df=df,iteration=iteration))) for iteration in range(self.iterations)])
