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
#define EPSILON 8192

#define READ    0
#define WRITE   1


/* Make somewhat of an attempt to make l1/l2/l3 runtimes similar 
 * Loop through 4GB of data.
 */
#define LOOP_THROUGH_CACHE_TIMES(array_size) \
    ((unsigned long)array_size > (1ULL << 32)) ? 1 : (1ULL << 32) / (unsigned long)array_size


#define VB_COHERENCE_FILE   "/dev/shm/vb-coherence"
#define MAX_FNAME_LEN       64


/* We want to ensure each rank has the same number of RD/WR
 * operations, but we also don't want an entire cache line to
 * only be read or written
 *
 * Our strategy is to figure out how many of the 64 ops are writes,
 * say W, and to set each 64/Wth element in *ops to be a write, with
 * everything else being a read.
 *
 * To add some cross-rank randomness, we calculate the index from
 * where we start filling *ops randomly
 */

static void
get_rw_operations(float      rw_ratio,
                  int        id,
                  uint64_t * ops)
{
    int i, nr_read = 0, nr_write = 0, start_idx;
    float idx, write_inc;

    *ops = 0;

    nr_write = (int)(64. * (1. - rw_ratio));
    write_inc = (float)(64. / nr_write);

    /* seed with local instance's id */
    srand(VB_BASE_SEED + id);
    start_idx = rand() % 64;

    for (i = 0; i < nr_write; i++) {
        idx = (start_idx + (int)(i * write_inc)) % 64;
        *ops |= (1ULL << (int)idx);
    }
}

static uint8_t
iteration(vb_instance_t * instance,
          uint64_t        ops,
          uint8_t       * array,
          unsigned long   array_size,
          int             line_size)
{
    int op_idx = 0;
    unsigned long line_idx, ar_idx, num_loops, loop, num_lines;
    uint8_t line_off = instance->rank_info.local_id % line_size;
    volatile uint8_t read_val = 0;

    num_lines = array_size / line_size;
    num_loops = LOOP_THROUGH_CACHE_TIMES(array_size);

    vb_debug_root("Looping through cache %lu times\n", num_loops);

    /* How many times to loop through the entire cache */
    for (loop = 0; loop < num_loops; loop++) {
        ar_idx = 0;

        /* Iterate through each cache line and perform the operation */
        for (line_idx = 0; line_idx < num_lines; line_idx++) {
#if 0
            assert((addr & 0x3F) == line_off);
#endif
            if ((ops & (1 << op_idx)) == READ) {
                read_val += array[ar_idx + line_off] + op_idx;
            } else {
                array[ar_idx + line_off] = read_val;
            }

            op_idx = (op_idx + 1) % 64;
            ar_idx += line_size;
        }
    }

    return read_val;
}

static int
__run_kernel(vb_instance_t    * instance,
             float              rw_ratio,
             unsigned long long iterations,
             uint8_t          * array,
             unsigned long      array_size,
             unsigned long      cache_size,
             int                line_size)
{
    unsigned long long iter;
    struct timeval t1, t2;
    int fd, status;
    uint64_t ops;
    uint8_t dummy;

    //assert(instance->rank_info.local_id < line_size);

    /* Generate Read or Write for each cache line */
    get_rw_operations(rw_ratio, instance->rank_info.local_id, &ops);

    for (iter = 0; iter < iterations; iter++) {
        /* Start with a fresh array */
        memset(array, 0, array_size);

        MPI_Barrier(MPI_COMM_WORLD);

        gettimeofday(&t1, NULL);
        dummy = iteration(instance, ops, array, array_size, line_size);
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

static void
create_coherence_file(vb_instance_t * instance,
                      char          * file_name)
{
    char * fmt_str;
    vb_build_fmt_str(instance, NULL, &fmt_str);
    snprintf(file_name, MAX_FNAME_LEN, "%s-%s.shm", VB_COHERENCE_FILE, fmt_str);
    free(fmt_str);
}


static int
alloc_array(char          * fname,
            unsigned long   cache_size,
            int             line_size,
            uint8_t      ** array_p,
            unsigned long * array_size_p)
{
    uint8_t * array;
    unsigned long array_size;
    int status, mmap_fd;

    /* Make sure this fits in cache by providing a little padding for
     * all the other data we might touch
     */
    array_size = cache_size - EPSILON;

    *array_p      = NULL;
    *array_size_p = 0;

    /* Create a memory-mapped file */
    mmap_fd = open(fname, O_CREAT | O_TRUNC | O_RDWR, S_IRUSR | S_IWUSR);
    if (mmap_fd == -1) {
        vb_error("Could not create file %s: %s\n", fname, strerror(errno));
        return VB_GENERIC_ERROR;
    }

    /* Update its size */
    status = ftruncate(mmap_fd, array_size);
    if (status != 0) {
        vb_error("Could not truncate file %s: %s\n", fname, strerror(errno));
        close(mmap_fd);
        return VB_GENERIC_ERROR;
    }

    /* mmap it */
    array = mmap(NULL, array_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, mmap_fd, 0);
    close(mmap_fd);

    if (array == MAP_FAILED) {
        vb_error("Could not mmap file %s: %s\n", fname, strerror(errno));
        return VB_GENERIC_ERROR;
    }

    /* double check that this is cache aligned */
    assert(((unsigned long)array & (line_size - 1)) == 0);

    /* we populated the array with pages, just synchronize to make sure attachers see this */
    msync(array, array_size, MS_SYNC);

    *array_p      = array;
    *array_size_p = array_size;

    return VB_SUCCESS;
}

static void
free_array(char        * fname,
           uint8_t     * array,
           unsigned long array_size)
{
    int status;

    status = munmap(array, array_size);
    if (status != 0) {
        vb_error("Could not munmap() file %s: %s\n", fname, strerror(errno));
    }
}

static int
attach_array(char          * fname,
             unsigned long   cache_size,
             int             line_size,
             uint8_t      ** array_p,
             unsigned long * array_size_p)
{
    uint8_t * array;
    unsigned long array_size;
    int status, mmap_fd;
    array_size = cache_size - EPSILON;

    *array_p      = NULL;
    *array_size_p = 0;

    /* Get existing array via mmap'd file */
    do {
        mmap_fd = open(fname, O_RDWR, S_IRUSR | S_IWUSR);
        if ((mmap_fd == -1) && (errno != ENOENT)) {
            vb_error("Could not open() %s: %s\n", fname, strerror(errno));
            return VB_GENERIC_ERROR;
        }
    } while (mmap_fd == -1);

    /* Attach it locally */
    array = mmap(NULL, array_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, mmap_fd, 0);
    close(mmap_fd);

    if (array == (void *)-1) {
        vb_error("Could not mmap file %s: %s\n", fname, strerror(errno));
        return VB_GENERIC_ERROR;
    }

    /* double check that this is cache aligned */
    assert(((unsigned long)array & (line_size - 1)) == 0);

    *array_p      = array;
    *array_size_p = array_size;

    return VB_SUCCESS;
}

static void
detach_array(char        * fname,
             uint8_t     * array,
             unsigned long array_size)
{
    int status;

    status = munmap(array, array_size);
    if (status != 0) {
        vb_error("Could not munmap() file %s: %s\n", fname, strerror(errno));
    }
}

static int
run_kernel_local_root(vb_instance_t    * instance,
                      float              rw_ratio,
                      unsigned long long iterations,
                      unsigned long      cache_size,
                      int                line_size)
{
    uint8_t * array;
    unsigned long array_size;
    int status;
    char fname[MAX_FNAME_LEN];

    /* Generate a unique file_name for the coherence memory */
    create_coherence_file(instance, (char *)fname);

    status = alloc_array(fname, cache_size, line_size, &array, &array_size);
    if (status != VB_SUCCESS) {
        vb_error("Could not allocate array\n");
        return status;
    }

    vb_debug("Allocated array at %p\n", array);

    /* Broadcast file_name to local ranks */
    MPI_Bcast(
        fname,
        MAX_FNAME_LEN,
        MPI_CHAR,
        0,
        instance->rank_info.local_node_comm
    );

    /* Wait until everyone is here */
    MPI_Barrier(MPI_COMM_WORLD);

    status = __run_kernel(instance, rw_ratio, iterations, 
            array, array_size, cache_size, line_size);

    free_array(fname, array, array_size);

    unlink(fname);

    return status;
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
    char fname[MAX_FNAME_LEN];

    /* Receive fname from local root */
    MPI_Bcast(
        fname,
        MAX_FNAME_LEN,
        MPI_CHAR,
        0,
        instance->rank_info.local_node_comm
    );

    status = attach_array((char *)fname, cache_size, line_size, &array, &array_size);
    if (status != VB_SUCCESS) {
        vb_error("Could not attach array\n");
        return status;
    }

    vb_debug("%d: Attached array at %p\n", getpid(), array);

    /* Wait until everyone is here */
    MPI_Barrier(MPI_COMM_WORLD);

    status = __run_kernel(instance, rw_ratio, iterations, 
            array, array_size, cache_size, line_size);

    detach_array((char *)fname, array, array_size);

    return status;
}

static void
usage(void)
{
    vb_error_root("\ncache_coherence requires args:\n"
        "  arg 0: <read/write ratio> (e.g., 0.75=75%% reads,25%% writes)\n"
        "  arg 1: <mode> ('a' OR 'm')\n"
        "  arg 2:\n"
        "    if <mode> == 'a'>:\n"
        "      <cache depth> (1, 2, ...)\n"
        "    else:\n"
        "      <cache size (bytes)\n"
        "  arg 3:\n"
        "    if <mode == 'a'>\n"
        "      N/A\n"
        "    else:\n"
        "      <line size (bytes)>\n"
        );
}

int 
vb_kernel_cache_coherence(int             argc,
                          char         ** argv,
                          vb_instance_t * instance)
{
    int  cache_depth, line_size;
    float rw_ratio;
    float cache_size, tmp;
    char mode, cache_size_tag = 'B';
    hwloc_topology_t hwloc;
    hwloc_obj_t obj;

    if (argc < 3) {
        usage();
        return VB_BAD_ARGS;
    }

    rw_ratio = atof(argv[0]);
    if ((rw_ratio < 0) || (rw_ratio > 1)) {
        vb_error_root("rw_ratio must be between 0 and 1\n");
        usage();
        return VB_BAD_ARGS;
    }

    mode = argv[1][0];
    switch (mode) {
        case 'a':
            cache_depth = atoi(argv[2]);

            hwloc_topology_init(&hwloc);
            hwloc_topology_load(hwloc);

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
            usage();
            return VB_BAD_ARGS;
    }


    /* Num instances per node must be less than the cache line size */
    if (instance->rank_info.num_local_instances > line_size) {
        vb_debug_root("Running cache_coherence with more instances per node (%d) than bytes in cache line (%d)\n",
        instance->rank_info.num_local_instances, line_size);
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

    vb_print_root("\nRunning cache_coherence\n"
        "  Num iterations: %llu\n"
        "  R/W ratio:      %.2f\n"
        "  Cache specs:\n"
        "   depth:         L%d\n"
        "   size:          %.2f%c\n"
        "   line_size:     %uB\n",
        instance->options.num_iterations,
        rw_ratio,
        (mode == 'a') ? cache_depth : -1,
        tmp, cache_size_tag,
        line_size);

    /* All local node masters allocate an array */
    if (instance->rank_info.local_id == 0)
        return run_kernel_local_root(instance, rw_ratio,
                instance->options.num_iterations,
                cache_size, line_size);
    else
        return run_kernel(instance, rw_ratio,
                instance->options.num_iterations,
                cache_size, line_size);
}
