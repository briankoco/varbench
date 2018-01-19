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
#include <fcntl.h>
#include <math.h>
#include <errno.h>
#include <sys/mman.h>

#include <sys/time.h>

#include <varbench.h>

#define PAGE_SIZE sysconf(_SC_PAGESIZE)
#define INDICES_PER_PERMUTATION     64


/* Make somewhat of an attempt to make l1/l2/l3 runtimes similar 
 * Loop through 4GB of data.
 */
#define LOOP_THROUGH_CACHE_TIMES(array_size) \
    ((unsigned long)array_size > (1ULL << 32)) ? 1 : (1ULL << 32) / (unsigned long)array_size

static void
get_random_permutation(int   id,
                       int   nr_indices,
                       int * permutation)
{
    int i, j, temp;

    /* seed with the local instance's id */
    srand(VB_BASE_SEED + id);

    for (i = nr_indices - 1; i >= 0; --i) {
        j = rand() % (i + 1);

        temp = permutation[i];
        permutation[i] = permutation[j];
        permutation[j] = temp;
    }
}

static uint8_t
iteration(vb_instance_t * instance,
          char            mode,
          int             nr_indices,
          int           * permutation,
          uint8_t       * array,
          unsigned long   array_size,
          int             line_size)
{
    int num_loops, loops;
    unsigned long line_idx, base_idx, num_lines;
    volatile uint8_t read_val = 0;

    num_lines = array_size / line_size;
    loops = LOOP_THROUGH_CACHE_TIMES(array_size);

    vb_debug_root("Looping through cache %d times\n", loops);

    /* How many times to loop through the entire cache */
    for (num_loops = 0; num_loops < loops; num_loops++) {

        /* Iterate through each cache line and perform a read/write */
        for (base_idx = 0; base_idx < num_lines; base_idx++) {
            if (mode == 'r') {
                /* Iterate through the array by:
                 *  ((base_idx / nr_indices) * nr_indices) 
                 *      -- increments by nr_indices every time through the
                 *      permutation array
                 *
                 *          +
                 *
                 *  permutation[base_idx % nr_indices]
                 *      -- random permutation of the next nr_indices to
                 *      access 
                 *
                 * 
                 *  line_idx gives us the index of the cache line. To get
                 *  the actual address, multiple by the line_size
                 */

                line_idx = ((base_idx / nr_indices) * nr_indices) + 
                            permutation[base_idx % nr_indices];
            } else {
                /* sequential */
                line_idx = base_idx;
            }

            /* Alternate between reads/writes */
            if (line_idx % 2) {
                read_val += array[line_idx * line_size] + line_idx;
            } else {
                array[line_idx * line_size] = read_val;
            }
        }
    }

    return read_val;
}

static int
__run_kernel(vb_instance_t    * instance,
             char               mode,
             unsigned long long iterations,
             uint8_t          * array,
             unsigned long      array_size,
             unsigned long      cache_size,
             int                line_size)
{
    unsigned long long iter;
    struct timeval t1, t2;
    int fd, status, local_id, i, permutation[INDICES_PER_PERMUTATION];
    uint64_t ops;
    uint8_t dummy;

    local_id = instance->rank_info.local_id;

    /* Generate a permutation of the numbers 0 through 63 */
    if (mode == 'r') {
        for (i = 0; i < INDICES_PER_PERMUTATION; i++)
            permutation[i] = i;

        get_random_permutation(local_id, INDICES_PER_PERMUTATION, permutation);
    }

    for (iter = 0; iter < iterations; iter++) {
        /* Start with a fresh array */
        memset(array, 0, array_size);

        MPI_Barrier(MPI_COMM_WORLD);

        gettimeofday(&t1, NULL);
        dummy = iteration(
            instance, 
            mode,
            INDICES_PER_PERMUTATION,
            permutation, 
            array, 
            array_size, 
            line_size
        );
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
alloc_array(unsigned long   cache_size,
            int             line_size,
            int             num_local_instances,
            uint8_t      ** array_p,
            unsigned long * array_size_p)
{
    uint8_t * array;
    unsigned long array_size;
    int status, mmap_fd;

    *array_p      = NULL;
    *array_size_p = 0;

    /* Each instance allocates an array such that array_size *
     * num_local_instances = cache_size  * 2.
     *
     * i.e., things will be evicted.
     *
     * Each instance either randomly or sequentially accesses its
     * array instances, reading/writing every other access
     */
    array_size = (cache_size * 2) / num_local_instances;

    status = posix_memalign((void **)&array, PAGE_SIZE, array_size);
    if (status == -1) {
        vb_error("posix_memalign(): failed to allocate array: %s\n", strerror(errno));
        return VB_GENERIC_ERROR;
    }

    /* double check that this is cache aligned */
    assert(((unsigned long)array & (line_size - 1)) == 0);

    vb_debug("Allocated local array at %p\n", (void *)array);

    *array_p      = array;
    *array_size_p = array_size;

    return VB_SUCCESS;
}

static void
free_array(uint8_t     * array,
           unsigned long array_size)
{
    free(array);
}

static int
run_kernel(vb_instance_t    * instance,
           float              rw_ratio,
           unsigned long long iterations,
           unsigned long      cache_size,
           int                line_size)
{
    uint8_t * array;
    unsigned long array_size;
    int status;

    status = alloc_array(cache_size, line_size, 
            instance->rank_info.num_local_instances,
            &array, &array_size);
    if (status != VB_SUCCESS) {
        vb_error("Could not allocate array\n");
        return status;
    }

    status = __run_kernel(instance, rw_ratio, iterations, 
            array, array_size, cache_size, line_size);

    free_array(array, array_size);

    return status;
}


static void
usage(void)
{
    vb_error_root("\ncache_capacity requires args:\n"
        "  arg 0: <r/s> (sequential/random access to local array)\n"
        "  arg 1: <mode> ('a' OR 'm')\n" 
        "  arg 2:\n"
        "    if <mode> == 'a':\n"
        "      <cache depth> (1, 2, ...)\n"
        "    else:\n"
        "      <cache size (bytes)\n"
        "  arg 3:\n"
        "    if <mode> == 'a':\n"
        "      N/A\n"
        "    else:\n"
        "      <line size (bytes)\n"
        );
}

int 
vb_kernel_cache_capacity(int             argc,
                         char         ** argv,
                         vb_instance_t * instance)
{
    int  cache_depth, line_size;
    float cache_size, tmp;
    char cache_size_tag = 'B';
    char mode, access_mode;
    hwloc_topology_t hwloc;
    hwloc_obj_t obj;

    if (argc < 3) {
        usage();
        return VB_BAD_ARGS;
    }

    access_mode = argv[0][0];
    switch (access_mode) {
        case 's':
        case 'r':
            break;

        default:
            vb_error_root("access_mode must be 's' or 'r'\n");
            usage();
            return VB_BAD_ARGS;
    }

    mode = argv[1][0];
    switch (mode) {
        case 'a':
            hwloc_topology_init(&hwloc);
            hwloc_topology_load(hwloc);

            cache_depth = atoi(argv[2]);

            /* find associated hwloc object */
            for (obj = hwloc_get_obj_by_type(hwloc, HWLOC_OBJ_PU, 0);
                 obj;
                 obj = obj->parent)
            {
                if ((obj->type              == HWLOC_OBJ_CACHE) &&
                    (obj->attr->cache.depth == cache_depth) &&
                    (obj->attr->cache.type  != HWLOC_OBJ_CACHE_INSTRUCTION))
                {

                    cache_size = obj->attr->cache.size;
                    line_size  = obj->attr->cache.linesize;
                    break;
                }
            }

            if (!obj) {
                vb_error_root("Could not find HWLOC object for cache L%u\n", cache_depth);
                return VB_GENERIC_ERROR;
            }

            break;

        case 'm':
            if (argc != 4) {
                usage();
                return VB_BAD_ARGS;
            }

            cache_size = atof(argv[2]);
            line_size  = atoi(argv[3]);

            break;

        default:
            vb_error_root("<mode> must be 'a' (automatic) or 'm' (manual)\n");
            return VB_BAD_ARGS;
    }

    for (tmp = cache_size; tmp > (1 << 10); tmp /= (1 << 10)) {
        switch(cache_size_tag) {
            case 'B':
                cache_size_tag = 'K';
                break;

            case 'K':
                cache_size_tag = 'M';
                break;

            case 'M':
                cache_size_tag = 'G';
                break;

            default:
                assert(1 == 0);
                break;
        }
    }

    vb_print_root("\nRunning cache_capacity\n"
        "  Num iterations: %llu\n"
        "  Access mode:    %s\n"
        "  Cache specs:\n"
        "   depth:         L%d\n"
        "   size:          %.2f%c\n"
        "   line_size:     %uB\n",
        instance->options.num_iterations,
        (access_mode == 's') ? "sequential" : "random",
        (mode == 'a') ? cache_depth : -1,
        tmp, cache_size_tag, line_size);

    return run_kernel(instance, access_mode,
            instance->options.num_iterations,
            cache_size, line_size);
}
