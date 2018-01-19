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

/*
  ONE-SIDED HPCC RandomAccess benchmark

  This test implements the HPCC RandomAccess benchmark using passive one-sided
  communication with MPI_Accumulate.

  According to the rules, there are to be 4 times as many updates as
  there are entries in the global table. A random number is generated,
  and the lower bits are used as an index into the global table. The
  value of that global table entry is XORed with the random number and
  then stored.

  The local index is formed by masking the lower bits from the global
  index, and the rank of the process where the entry is located is
  found from the higher bits (which are the middle bits in the random number).

  The next random number is generated using a shift-register and an HPCC-spec
  polynomial.

  Each table entry is a 64-bit integer.
  
  The test is supposed to be run with the largest table size at an
  even power of 2 that will fit into available memory. (Note that each
  entry is an 8-byte integer.) In practice, this one-sided
  communication-based version is too slow to do this, and the results
  change little above 128k entries/processor.

*/


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <limits.h>
#include <math.h>
#include <time.h>
#include <fcntl.h>

#include <sys/time.h>

#include <varbench.h>

#define ZERO64B 0LL
#define POLY 0x0000000000000007ULL
#define PERIOD 1317624576693539401LL

static uint8_t 
iteration(vb_instance_t * instance,
          uint64_t      * table,
          unsigned long   tbl_entries,
          uint64_t        ran)
{
    uint64_t idx;
    unsigned long long access;
    int offset;


    /* 
     The HPCC spec is 4 * global table size updates total. The lower
     random bits are used to calculate the offset within each local
     table, and the next highest several bits are used to calculate
     the rank of the processor that owns that entry in the global
     table.

     The polynomial used in the shift register is provided by the
     HPCC.
    */
    for (idx = 0; idx < tbl_entries * 4; idx++) {
        /* mask lower bits */
        offset = ran & (tbl_entries - 1);
        table[offset] ^= ran;
        ran = (ran << 1) ^ ((int64_t) ran < ZERO64B ? POLY : ZERO64B); 
    }

    return (uint8_t)ran;
}

static int
__run_kernel(vb_instance_t    * instance,
             unsigned long long iterations,
             uint64_t         * array,
             unsigned long      array_entries,
             uint64_t           ran)
{
    unsigned long long iter;
    struct timeval t1, t2;
    int fd, status;
    uint8_t dummy;

    for (iter = 0; iter < iterations; iter++) {
        MPI_Barrier(MPI_COMM_WORLD);

        gettimeofday(&t1, NULL);
        dummy = iteration(instance, array, array_entries, ran);
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

/* 
   Utility routine to start random number generator at Nth step
   (shameless lifted from public domain HPCC version)
 */
uint64_t
HPCC_starts(int64_t n)
{
  int i, j;
  uint64_t m2[64];
  uint64_t temp, ran;

  while (n < 0) n += PERIOD;
  while (n > PERIOD) n -= PERIOD;
  if (n == 0) return 0x1;

  temp = 0x1;
  for (i=0; i<64; i++) {
    m2[i] = temp;
    temp = (temp << 1) ^ ((int64_t) temp < 0 ? POLY : 0);
    temp = (temp << 1) ^ ((int64_t) temp < 0 ? POLY : 0);
  }

  for (i=62; i>=0; i--)
    if ((n >> i) & 1)
      break;

  ran = 0x2;
  while (i > 0) {
    temp = 0;
    for (j=0; j<64; j++)
      if ((ran >> j) & 1)
        temp ^= m2[j];
    ran = temp;
    i -= 1;
    if ((n >> i) & 1)
      ran = (ran << 1) ^ ((int64_t) ran < 0 ? POLY : 0);
  }

  return ran;
}



static int
run_kernel(vb_instance_t    * instance,
           unsigned long      tbl_entries,
           unsigned long long iterations)
{
    uint64_t * table;
    uint64_t   ran;
    int        local_id;
    int        ret;

    table = (uint64_t *)malloc(sizeof(uint64_t) * tbl_entries);
    if (table == NULL) {
        vb_error("Out of memory\n");
        return VB_GENERIC_ERROR;
    }

    local_id = instance->rank_info.local_id;
    ran = HPCC_starts(4*local_id*tbl_entries);    

    ret = __run_kernel(instance, iterations, table, tbl_entries, ran);

    free(table);

    return ret;

}

static void
usage(void)
{
    vb_error_root("random_access requires args:\n"
        "  <table size (MB) per proc> (must be power of 2)\n"
    );
}

int
vb_kernel_random_access(int             argc,
                        char         ** argv,
                        vb_instance_t * instance)
{
    unsigned long t_size, t_entries;
    float t_size_mb;
    uint64_t * table;

    if (argc != 1) {
        usage();
        return VB_BAD_ARGS;
    }

    t_size_mb = atof(argv[0]);
    t_size    = t_size_mb * (1ULL << 20);
    t_entries = t_size / sizeof(uint64_t);

    if (t_entries & (t_entries - 1)) {
        usage();
        return VB_BAD_ARGS;
    }

    vb_print_root("Running random access\n"
        "  Num iterations:  %llu\n"
        "  Mem per process: %fMB\n",
        instance->options.num_iterations,
        t_size_mb);

    return run_kernel(instance, t_entries,
        instance->options.num_iterations);
}

