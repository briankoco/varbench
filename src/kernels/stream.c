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

/*-----------------------------------------------------------------------*/
/* Program: STREAM                                                       */
/* Revision: $Id: stream.c,v 5.10 2013/01/17 16:01:06 mccalpin Exp mccalpin $ */
/* Original code developed by John D. McCalpin                           */
/* Programmers: John D. McCalpin                                         */
/*              Joe R. Zagar                                             */
/*                                                                       */
/* This program measures memory transfer rates in MB/s for simple        */
/* computational kernels coded in C.                                     */
/*-----------------------------------------------------------------------*/
/* Copyright 1991-2013: John D. McCalpin                                 */
/*-----------------------------------------------------------------------*/
/* License:                                                              */
/*  1. You are free to use this program and/or to redistribute           */
/*     this program.                                                     */
/*  2. You are free to modify this program for your own use,             */
/*     including commercial use, subject to the publication              */
/*     restrictions in item 3.                                           */
/*  3. You are free to publish results obtained from running this        */
/*     program, or from works that you derive from this program,         */
/*     with the following limitations:                                   */
/*     3a. In order to be referred to as "STREAM benchmark results",     */
/*         published results must be in conformance to the STREAM        */
/*         Run Rules, (briefly reviewed below) published at              */
/*         http://www.cs.virginia.edu/stream/ref.html                    */
/*         and incorporated herein by reference.                         */
/*         As the copyright holder, John McCalpin retains the            */
/*         right to determine conformity with the Run Rules.             */
/*     3b. Results based on modified source code or on runs not in       */
/*         accordance with the STREAM Run Rules must be clearly          */
/*         labelled whenever they are published.  Examples of            */
/*         proper labelling include:                                     */
/*           "tuned STREAM benchmark results"                            */
/*           "based on a variant of the STREAM benchmark code"           */
/*         Other comparable, clear, and reasonable labelling is          */
/*         acceptable.                                                   */
/*     3c. Submission of results to the STREAM benchmark web site        */
/*         is encouraged, but not required.                              */
/*  4. Use of this program or creation of derived works based on this    */
/*     program constitutes acceptance of these licensing restrictions.   */
/*  5. Absolutely no warranty is expressed or implied.                   */
/*-----------------------------------------------------------------------*/
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <math.h>
#include <float.h>
#include <limits.h>
#include <sys/time.h>
#include <fcntl.h>

#include <varbench.h>

#define NTIMES   10

#ifndef MIN
#define MIN(x,y) ((x)<(y)?(x):(y))
#endif
#ifndef MAX
#define MAX(x,y) ((x)>(y)?(x):(y))
#endif

static double 
mysecond(void)
{
    struct timeval tp;
    struct timezone tzp;
    int i;

    i = gettimeofday(&tp, &tzp);
    return ( (double) tp.tv_sec + (double) tp.tv_usec * 1.e-6);
}

# define M 20
static int
checktick(void)
{
    int     i, minDelta, Delta;
    double  t1, t2, timesfound[M];

    /* Collect a sequence of M unique time values from the system. */

    for (i = 0; i < M; i++) {
        t1 = mysecond();
        while( ((t2=mysecond()) - t1) < 1.0E-6 );
        timesfound[i] = t1 = t2;
    }

    /*
     * Determine the minimum difference between these M values.
     * This result will be our estimate (in microseconds) for the
     * clock granularity.
     */
    minDelta = 1000000;
    for (i = 1; i < M; i++) {
        Delta = (int)( 1.0E6 * (timesfound[i]-timesfound[i-1]));
        minDelta = MIN(minDelta, MAX(Delta,0));
    }

    return(minDelta);
}


static uint8_t
iteration(vb_instance_t * instance,
          double        * a,
          double        * b,
          double        * c,
          unsigned long   array_size)
{
    int           k;
    uint8_t       ret;
    unsigned long j;
    int           scalar;

    for (j=0; j<array_size; j++) {
        a[j] = 1.0;
        b[j] = 2.0;
        c[j] = 0.0;
    }
   
    scalar = 3.0;
    for (k=0; k<NTIMES; k++)
    {
        for (j=0; j<array_size; j++)
            c[j] = a[j];

        for (j=0; j<array_size; j++)
            b[j] = scalar*c[j];
        
        for (j=0; j<array_size; j++)
            c[j] = a[j]+b[j];
        
        for (j=0; j<array_size; j++)
            a[j] = b[j]+scalar*c[j];
    
        /* prevent compiler optimizations */
        ret += a[j - 1] + b[j - 1] + c[j - 1];
    }

    return ret;
}

static int
__run_kernel(vb_instance_t    * instance,
             unsigned long long iterations,
             double           * a,
             double           * b,
             double           * c,
             unsigned long      array_size)
{
    unsigned long long iter;
    struct timeval t1, t2;
    int fd, status;
    uint64_t ops;
    uint8_t dummy;

    for (iter = 0; iter < iterations; iter++) {
        MPI_Barrier(MPI_COMM_WORLD);

        gettimeofday(&t1, NULL);
        dummy = iteration(instance, a, b, c, array_size);
        gettimeofday(&t2, NULL);

        vb_print_root("Iteration %llu finished\n", iter);

        status = vb_gather_kernel_results(
                instance,
                iter,
                &t1,
                &t2
            );

        /* Prevent compiler optimization by writing the dummy val somewhere */
        fd = open("/dev/null", O_RDWR);
        assert(fd > 0);
        write(fd, &dummy, sizeof(uint8_t));
        close(fd);

        if (status != 0) {
            vb_error("Could not gather kernel results\n");
            return status;
        }
    }

    return VB_SUCCESS;
}


static int
run_kernel(vb_instance_t    * instance,
           unsigned long      array_size,
           unsigned long long iterations)
{
    double *a, *b, *c, t;
    int quantum, ret;
    unsigned long j, nr_elements = array_size / sizeof(double);

    a = malloc(sizeof(double) * nr_elements);
    b = malloc(sizeof(double) * nr_elements);
    c = malloc(sizeof(double) * nr_elements);

    if (!a || !b || !c) {
        vb_error("Out of memory\n");
        ret = VB_GENERIC_ERROR;
        goto out;
    }

    if  ( (quantum = checktick()) >= 1) {
        vb_debug("Your clock granularity/precision appears to be "
            "%d microseconds.\n", quantum);
    } else {
        vb_debug("Your clock granularity appears to be "
            "less than one microsecond.\n");
        quantum = 1;
    }

    t = mysecond();
    for (j = 0; j < nr_elements; j++) {
        a[j] = 2.0E0 * a[j];
    }

    t = 1.0E6 * (mysecond() - t);

    vb_debug("Each test below will take on the order"
        " of %d microseconds.\n"
        "   (= %d clock ticks)\n", 
        (int) t, (int) (t/quantum) );

    if ((int)(t/quantum) < 20) {
        vb_error("You are not getting at least 20 clock ticks per test: increase array size\n");
        ret = VB_GENERIC_ERROR;
        goto out;
    }
     
    ret = __run_kernel(instance, iterations, a, b, c, nr_elements);

out:
    if (a)
        free(a);

    if (b)
        free(b);

    if (c)
        free(c);

    return ret;
}

static void
usage(void)
{
    vb_error_root("\nstream requires args:\n"
        "  <memory size (MB) per array (there are 3 total)\n"
        );
}

int 
vb_kernel_stream(int             argc,
                 char         ** argv,
                 vb_instance_t * instance)
{
    unsigned long cache_size = 0;
    unsigned long mem_size;
    float mem_size_mb;

    hwloc_topology_t hwloc;
    hwloc_obj_t obj;

    /* Arg must be array size */
    if (argc != 1) {
        usage();
        return VB_BAD_ARGS;
    }

    mem_size_mb = atof(argv[0]);
    mem_size = mem_size_mb * (1ULL << 20);

    hwloc_topology_init(&hwloc);
    hwloc_topology_load(hwloc);

    /* Make sure the each array is at least 4 times the total cache
     * size, as per stream specs */
    for (obj = hwloc_get_obj_by_type(hwloc,
        HWLOC_OBJ_PU, 0);
         obj;
         obj = obj->parent)
    {
        if (obj->type == HWLOC_OBJ_CACHE)
            cache_size += obj->attr->cache.size;
    }

    if ((cache_size * 4) > mem_size) {
        vb_error_root("stream array size too small: must be at least %lu bytes\n",
            (cache_size * 4));
        return VB_BAD_ARGS;
    }

    vb_print_root("\nRunning stream\n"
        "  Num iterations: %llu\n"
        "  Array size:     %fMB\n",
        instance->options.num_iterations,
        mem_size_mb
    );

    return run_kernel(instance, mem_size,
            instance->options.num_iterations);
}
