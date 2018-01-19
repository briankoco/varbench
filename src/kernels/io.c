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

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/mman.h>

#include <varbench.h>

#define VB_IO           "VB_IO"
#define VB_IO_MODE      0660
#define MAX_FNAME_LEN   64

static inline bool
do_write_block(vb_instance_t * instance,
               unsigned long   first_write_idx,
               unsigned long   block_idx,
               unsigned long   nr_blocks,
               float           rd_ratio)
{
    unsigned long nr_write, write_inc;
 
    nr_write = (unsigned long)(nr_blocks * (1. - rd_ratio));
    if (nr_write == 0)
        return false;

    write_inc = nr_blocks / nr_write;

    if (labs(block_idx - first_write_idx) % write_inc == 0)
        return true;
    else
        return false;
}

static void
get_random_permutation(int             id,
                       unsigned long   nr_indices,
                       unsigned long * permutation)
{
    long i, j, temp;

    /* seed with the local instance's id */
    srand(VB_BASE_SEED + id);

    for (i = nr_indices - 1; i >= 0; --i) {
        j = rand() % (i + 1);

        temp = permutation[i];
        permutation[i] = permutation[j];
        permutation[j] = temp;
    }
}

static int
iteration(vb_instance_t * instance,
          unsigned long   block_size_kb,
          float           rd_ratio,
          unsigned long * permutation,
          unsigned long   nr_indices,
          char            map_mode,
          int             data_fd,
          void          * src_buf,
          void          * dst_buf,
          void          * data_addr,
          uint8_t       * dummy_val)
{
    unsigned long i, idx, first_write_idx;
    off_t off, res;
    ssize_t bytes;

    /* Calculate the first index where I will write instead of read */
    first_write_idx = instance->rank_info.local_id % nr_indices;

    for (i = 0; i < nr_indices; i++) {
        idx = permutation[i];
        off = idx * (block_size_kb * 1024);

        if (map_mode == 'm') {
            /* mmap - use memcpy */
            if (do_write_block(instance, first_write_idx, idx, nr_indices, rd_ratio)) {
                memcpy(data_addr + off, src_buf, block_size_kb * 1024);
            } else {
                memcpy(dst_buf, data_addr + off, block_size_kb * 1024);

                /* to prevent optimization */
                *dummy_val += ((uint8_t *)dst_buf)[instance->rank_info.local_id % block_size_kb * 1024];
            }
        } else {
            /* regular or direct - use lseek/read/write */
            res = lseek(data_fd, off, SEEK_SET);

            if (res == (off_t)-1) {
                vb_error("lseek failed: %s\n", strerror(errno));
                return VB_GENERIC_ERROR;
            } else if (off != res) {
                vb_error("lseek set the field offset to %lu, but %lu was requested\n", 
                    res, off);
                return VB_GENERIC_ERROR;
            } 

            /* read/write */
            if (do_write_block(instance, first_write_idx, idx, nr_indices, rd_ratio)) {

                bytes = write(data_fd, src_buf, block_size_kb * 1024);
                if (bytes == -1) {
                    vb_error("write failed: %s\n", strerror(errno));
                    return VB_GENERIC_ERROR;
                } else if (bytes != block_size_kb * 1024) {
                    vb_error("wrote %ld bytes, but %ld was requested\n",
                        bytes, block_size_kb * 1024);
                    return VB_GENERIC_ERROR;
                }

            } else {
                bytes = read(data_fd, dst_buf, block_size_kb * 1024);

                if (bytes == -1) {
                    vb_error("read failed: %s\n", strerror(errno));
                    return VB_GENERIC_ERROR;
                } else if (bytes != block_size_kb * 1024) {
                    vb_error("read %ld bytes, but %ld was requested\n",
                        bytes, block_size_kb * 1024);
                    return VB_GENERIC_ERROR;
                }

                /* to prevent optimization */
                *dummy_val += ((uint8_t *)dst_buf)[instance->rank_info.local_id % block_size_kb * 1024];
            }
        }
    }

    return VB_SUCCESS;
}

static int
__run_kernel(vb_instance_t    * instance,
             unsigned long long iterations,
             unsigned long      data_size_kb,
             unsigned long      block_size_kb,
             float              rd_ratio,
             unsigned long    * permutation,
             unsigned long      nr_indices,
             char               map_mode,
             int                data_fd,
             void *             data_addr)
{
    unsigned long long iter;
    struct timeval t1, t2;
    int i, fd, status, local_id;
    uint8_t dummy;
    void * src_buf;
    void * dst_buf;

    /* ensure src_buf and dst_buf are aligned on a 1K increment (needed for O_DIRECT) */
    status = posix_memalign(&src_buf, 1024, block_size_kb * 1024);
    if (status != 0) {
        vb_error("posix_memalign failed: %s\n", strerror(errno));
        return VB_GENERIC_ERROR;
    }

    status = posix_memalign(&dst_buf, 1024, block_size_kb * 1024);
    if (status != 0) {
        vb_error("posix_memalign failed: %s\n", strerror(errno));
        free(src_buf);
        return VB_GENERIC_ERROR;
    }

    /* Set each byte in the src buffer */
    for (i = 0; i < block_size_kb * 1024; i++) {
        ((uint8_t *)src_buf)[i] = (uint8_t)i;
    }

    for (iter = 0; iter < iterations; iter++) {
        MPI_Barrier(MPI_COMM_WORLD);

        gettimeofday(&t1, NULL);
        status = iteration(instance, block_size_kb, rd_ratio, permutation, nr_indices,
                map_mode, data_fd, src_buf, dst_buf, data_addr, &dummy);
        gettimeofday(&t2, NULL);

        MPI_Barrier(MPI_COMM_WORLD);

        if (status != VB_SUCCESS) {
            vb_error("Iteration %llu failed\n", iter);
            goto out;
        }

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

        if (status != VB_SUCCESS) {
            vb_error("Could not gather kernel results\n");
            return status;
        }

        vb_print_root("Iteration %llu finished\n", iter);
    }

    status = VB_SUCCESS;

out:
    free(dst_buf);
    free(src_buf);
    return status;
}

static void
create_storage_filename(vb_instance_t * instance,
                        char          * directory,
                        char          * file_name,
                        size_t          file_name_len)
{
    char * fmt_str;
    char * prefix;

    asprintf(&prefix, "%s-%d", VB_IO, instance->rank_info.local_id);
    vb_build_fmt_str(instance, prefix, &fmt_str); 
    snprintf(file_name, file_name_len, "%s/%s-%s.vbio", directory, prefix, fmt_str);

    free(prefix);
    free(fmt_str);
}

static void 
delete_file(vb_instance_t * instance,
            char          * fname,
            int             fd)
{
    close(fd);
    unlink(fname);
}

static int
create_file(vb_instance_t * instance,
            unsigned long   data_size_kb,
            char            map_mode,
            char          * fname,
            int           * fd_p)
{
    int fd, flags, status;

    flags = O_CREAT | O_TRUNC | O_RDWR;
    if (map_mode == 'd') {
        flags |= O_DIRECT | O_SYNC;
    }

    /* Create file */
    fd = open(fname, flags, VB_IO_MODE);
    if (fd == -1) {
        vb_error("Failed to create file %s for IO kernel: %s\n", fname, strerror(errno));
        return VB_GENERIC_ERROR;
    }

    /* Set its size */
    status = ftruncate(fd, data_size_kb * 1024);
    if (status != 0) {
        vb_error("Failed to resize file %s: %s\n", fname, strerror(errno));
        delete_file(instance, fname, fd);
        return VB_GENERIC_ERROR;
    }

    *fd_p = fd;
    return VB_SUCCESS;
}

static int
open_file(vb_instance_t * instance,
          char            map_mode,
          char          * fname,
          int           * fd_p)
{
    int fd, flags, status;

    flags = O_RDWR;
    if (map_mode == 'd') {
        flags |= O_DIRECT | O_SYNC;
    }

    do {
        fd = open(fname, flags, VB_IO_MODE);
        if ((fd == -1) && (errno != ENOENT)) {
            vb_error("Failed to open file %s for IO kernel: %s\n", fname, strerror(errno));
            return VB_GENERIC_ERROR;
        }
    } while (fd == -1);

    *fd_p = fd;
    return VB_SUCCESS;
}


static int
mmap_file(vb_instance_t * instance,
          unsigned long   data_size_kb,
          char          * fname,
          int             fd,
          void         ** map_addr)
{
    void * data_addr;

    data_addr = mmap(NULL, data_size_kb * 1024, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (data_addr == MAP_FAILED) {
        vb_error("Failed to mmap file %s: %s\n", fname, strerror(errno));
        return VB_GENERIC_ERROR;
    }

    *map_addr = data_addr;
    return VB_SUCCESS;
}

static void
munmap_file(vb_instance_t * instance,
            unsigned long   data_size_kb,
            void          * map_addr)
{
    msync(map_addr, data_size_kb * 1024, MS_SYNC);
    munmap(map_addr, data_size_kb * 1024);
}


/* Each rank uses its own private storage. No special handling for
 * local root processes
 */
static int
run_kernel(vb_instance_t    * instance,
           unsigned long long iterations,
           unsigned long      data_size_kb,
           unsigned long      block_size_kb,
           float              rd_ratio,
           char               share_mode,
           char               access_mode,
           char               map_mode,
           char             * directory)
{
    char fname[MAX_FNAME_LEN];
    void * data_addr;
    unsigned long * permutation;
    unsigned long nr_indices, i;
    int fd, status;

    /* Generate file(s) for storage */
    if ((share_mode == 'p') || (instance->rank_info.local_id == 0)) {
        create_storage_filename(instance, directory, fname, MAX_FNAME_LEN);

        /* Create a file at this location with a specified size */
        status = create_file(instance, data_size_kb, map_mode, fname, &fd);
        if (status != VB_SUCCESS) {
            vb_error("Could not create IO file\n");
            return status;
        }
    } 
    
    if (share_mode == 's') {
        if (instance->rank_info.local_id == 0) {
            /* Broadcast file_name to local ranks */
            MPI_Bcast(
                fname,
                MAX_FNAME_LEN,
                MPI_CHAR,
                0,
                instance->rank_info.local_node_comm
            );
        } else {
            /* local ranks receive filename from local root */

            /* Receive fname from local root */
            MPI_Bcast(
                fname,
                MAX_FNAME_LEN,
                MPI_CHAR,
                0,
                instance->rank_info.local_node_comm
            );

            /* open it */
            status = open_file(instance, map_mode, fname, &fd);
            if (status != VB_SUCCESS) {
                vb_error("Could not open IO file\n");
                return status;
            }
        }
    }
 
    /* mmap the file if desired */
    if (map_mode == 'm') {
        status = mmap_file(instance, data_size_kb, fname, fd, &data_addr);
        if (status != VB_SUCCESS) {
            vb_error("Could not mmap IO file\n");
            goto out;
        }
    }

    /* Generate order in which data will be accessed */
    nr_indices = data_size_kb / block_size_kb;

    permutation = malloc(sizeof(unsigned long) * nr_indices);
    if (permutation == NULL) {
        vb_error("Out of memory\n");
        status = VB_GENERIC_ERROR;
        goto out_munmap;
    }

    for (i = 0; i < nr_indices; i++) {
        permutation[i] = i;
    }

    /* If random access, generate a permutation */
    if (access_mode == 'r') {
        get_random_permutation(instance->rank_info.local_id, nr_indices, permutation); 
    }

    /* run the kernel */
    status = __run_kernel(instance, iterations, data_size_kb, block_size_kb, rd_ratio,
            permutation, nr_indices, map_mode, fd, data_addr);

    free(permutation);

out_munmap:
    if (map_mode == 'm') {
        munmap_file(instance, data_size_kb, data_addr);
    }

out:
    /* Delete file */
    if ((share_mode == 'p') || (instance->rank_info.local_id == 0))
        delete_file(instance, fname, fd);
    else
        close(fd);

    return status;
}

static void
usage(void)
{
    vb_error_root("io requires args:\n"
        "  <directory to operate in>\n"
        "  <data size (KB)>\n"
        "  <block size (KB)>\n"
        "  <read ratio)>\n"
        "  <'s' OR 'p'> (share_mode: shared or private)\n"
        "  <'s' OR 'r'> (access_mode: sequential or random access)\n"
        "  <'r' or 'd' or 'm'> (map_mode: regular or direct (no page cache) or mmap)\n"
    );
}

int
vb_kernel_io(int             argc,
             char         ** argv,
             vb_instance_t * instance)
{
    unsigned long data_size_kb;
    unsigned long block_size_kb;
    float rd_ratio;
    char share_mode, access_mode, map_mode;
    char * directory;

    if (argc != 7) {
        usage();
        return VB_BAD_ARGS;
    }

    directory = argv[0];
    data_size_kb = strtoul(argv[1], NULL, 10);
    block_size_kb = strtoul(argv[2], NULL, 10);
    rd_ratio = atof(argv[3]);
    share_mode = argv[4][0];
    access_mode = argv[5][0];
    map_mode = argv[6][0];

    /* check args */
    if ((data_size_kb < block_size_kb) ||
        (data_size_kb % block_size_kb != 0))
    {
        vb_error_root("Data size must be a constant size factor of block size\n");
        return VB_BAD_ARGS;
    }

    if ((rd_ratio < 0) || (rd_ratio > 1)) {
        vb_error_root("rd_ratio must be between 0 and 1\n");
        usage();
        return VB_BAD_ARGS;
    }

    switch (share_mode) {
        case 's':
        case 'p':
            break;

        default:
            vb_error_root("share_mode must be 's' or 'p'\n");
            usage();
            return VB_BAD_ARGS;
    }

    switch (access_mode) {
        case 's':
        case 'r':
            break;

        default:
            vb_error_root("access_mode must be 's' or 'r'\n");
            usage();
            return VB_BAD_ARGS;
    }

    switch (map_mode) {
        case 'r':
        case 'd':
        case 'm':
            break;

        default:
            vb_error_root("map_mode must be 'r' or 'd' or 'm'\n");
            usage();
            return VB_BAD_ARGS;
    }

    vb_print_root("\nRunning io\n"
        "  Num iterations: %llu\n"
        "  Data directory: %s\n"
        "  Data size:      %luKB\n"
        "  Block size:     %luKB\n"
        "  R/W ratio:      %.2f\n"
        "  Shared mode:    %s\n"
        "  Access mode:    %s\n"
        "  Map mode:       %s\n",
        instance->options.num_iterations,
        directory,
        data_size_kb,
        block_size_kb,
        rd_ratio,
        (share_mode == 's') ? "shared" : "private",
        (access_mode == 's') ? "sequential" : "random",
        (map_mode == 'r') ? "regular IO" : (map_mode == 'd') ? "direct IO" : "mmap'd IO"
    );

    return run_kernel(
            instance,
            instance->options.num_iterations,
            data_size_kb,
            block_size_kb,
            rd_ratio,
            share_mode,
            access_mode,
            map_mode,
            directory
        );
}
