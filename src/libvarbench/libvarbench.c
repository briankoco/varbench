/*
 * This file is part of the 'varbench' project developed by the
 * University of Pittsburgh with funding from the United States
 * National Science Foundation and the Department of Energy.
 *
 * Copyright (c) 2017, Brian Kocoloski <briankoco@cs.pitt.edu>
 *
 * This is free software.  You are permitted to use, redistribute, and
 * modify it as specified in the file "LICENSE.md".
 */


#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <fcntl.h>
#include <errno.h>

#include <sys/mman.h>

#include <varbench.h>
#include <libvarbench.h>


vb_instance_t this_instance = { 
    .as_library = true,
    .options = {
        .map_by = 'n',
        .mem_affinity = 'n'
    }
};

int
libvb_init_instance(int    * argc,
                    char *** argv)
{
    int status, error;

    status = vb_build_rank_info(argc, argv, &this_instance);
    if (status != VB_SUCCESS) {
        error = -1;
        goto err;
    }

    status = vb_init_instance(&this_instance, true, *argc, *argv);
    if (status != VB_SUCCESS) {
        error = -1;
        goto err;
    }

    return 0;

err:
    errno = -error;
    return error;
}

int
libvb_deinit_instance(int status)
{
    vb_deinit_instance(&this_instance, status);
    return 0;
}

int
libvb_gather_kernel_results(unsigned long long iteration,
                            struct timeval   * start,
                            struct timeval   * end)
{
    return vb_gather_kernel_results(&this_instance, iteration, start, end);
}
