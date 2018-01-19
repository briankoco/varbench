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

#include <varbench.h>

int 
vb_kernel_hello_world(int             argc,
                      char         ** argv,
                      vb_instance_t * instance)
{
    int arg;

    vb_print("Hello World from instance %d\n", instance->rank_info.global_id);

    for (arg = 0; arg < argc; arg++) {
        vb_print_root("argv[%d] = %s\n", arg, argv[arg]);
    }

    return VB_SUCCESS;
}
