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

#ifndef __LIBVARBENCH_H__
#define __LIBVARBENCH_H__

#include <varbench.h>

/* Application Programming Interface */

int libvb_init_instance(int *, char ***);
int libvb_deinit_instance(int);
int libvb_gather_kernel_results(unsigned long long, struct timeval *, struct timeval *);


#endif /* __LIBVARBENCH_H__ */
