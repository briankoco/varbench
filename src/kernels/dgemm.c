/*
 * This file is part of the 'varbench' project developed by the
 * University of Pittsburgh with funding from the United States
 * National Science Foundation and the Department of Energy.
 *
 * Copyright (c) 2017, Brian Kocoloski <briankoco@cs.pitt.edu>
 *
 * This is free software.  You are permitted to use, redistribute, and
 * modify it as specified in the file "LICENSE.md".
 *
 * NOTE: this kernel is based on a DGEMM MPI 1 sided implementation
 * provided by Xin Zhao:
 *
 *****************************
 *
 * URL:
 * http://wgropp.cs.illinois.edu/courses/cs598-s16/examples/ga_mpi_ddt_rma.c
 *
 * Copyright (c) 2014 Xin Zhao. All rights reserved.
 *
 * Author(s): Xin Zhao <xinzhao3@illinois.edu>
 *
 *****************************
 */

#include <varbench.h>


#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <sys/types.h>
#include <time.h>
#include <fcntl.h>
#include <sys/time.h>


#define RAND_RANGE (10)

typedef enum {
    VB_DGEMM_LOCAL,
    VB_DGEMM_GLOBAL
} dgemm_mode_t;


static void 
init_mats(int      mat_dim, 
          double * mat_a, 
          double * mat_b, 
          double * mat_c)
{
    int i, j;

    srand(time(NULL));

    for (j = 0; j < mat_dim; j++) {
        for (i = 0; i < mat_dim; i++) {
            mat_a[j+i*mat_dim] = (double) rand() / (RAND_MAX / RAND_RANGE + 1);
            mat_b[j+i*mat_dim] = (double) rand() / (RAND_MAX / RAND_RANGE + 1);
            mat_c[j+i*mat_dim] = (double) 0.0;
        }
    }
}

static void 
dgemm(double * local_a, 
      double * local_b, 
      double * local_c, 
      int      blk_dim)
{
    int i, j, k;

    memset(local_c, 0, blk_dim*blk_dim*sizeof(double));

    for (j = 0; j < blk_dim; j++) {
        for (i = 0; i < blk_dim; i++) {
            for (k = 0; k < blk_dim; k++)
                local_c[j+i*blk_dim] += local_a[k+i*blk_dim] * local_b[j+k*blk_dim];
        }
    }
}

#if 0
static void 
check_mats(double * mat_a, 
           double * mat_b, 
           double * mat_c, 
           int      mat_dim)
{
    int i, j, k;
    int bogus = 0;
    double temp_c;
    double diff, max_diff = 0.0;

    for (j = 0; j < mat_dim; j++) {
        for (i = 0; i < mat_dim; i++) {
            temp_c = 0.0;
            for (k = 0; k < mat_dim; k++)
                temp_c += mat_a[k+i*mat_dim] * mat_b[j+k*mat_dim];
            diff = mat_c[j+i*mat_dim] - temp_c;
            if (fabs(diff) > 0.00001) {
                bogus = 1;
                if (fabs(diff) > fabs(max_diff))
                    max_diff = diff;
            }
        }
    }

    if (bogus) {
        vb_print_error("\nTEST FAILED: (%.5f MAX diff)\n\n", max_diff);
    }
}
#endif

static void
kernel_preconfig(vb_instance_t * instance,
                 int             mat_dim,
                 int             blk_dim,
                 int             px,
                 int             pt,
                 dgemm_mode_t    mode,
                 int           * rank,
                 double       ** local_mem,
                 double       ** mat_a,
                 double       ** mat_b,
                 double       ** mat_c,
                 MPI_Datatype  * blk_dtp,
                 MPI_Comm      * comm,
                 MPI_Win       * win)
{
    int blk_num, rx, ry, bx, by;
    double *win_mem;
    MPI_Aint disp_a, disp_b, disp_c;
    MPI_Aint offset_a, offset_b, offset_c;
    int i, j, k;
    int global_i, global_j;

    if (mode == VB_DGEMM_GLOBAL) {
        *rank = instance->rank_info.global_id;
        *comm = MPI_COMM_WORLD;
    } else {
        *rank = 0;
            
        /* Create communicator with just self */
        MPI_Comm_split(
            MPI_COMM_WORLD,
            instance->rank_info.global_id,
            instance->rank_info.global_id,
            comm
        );
    }

    if (!*rank) {
        /* create RMA window */
        MPI_Win_allocate(3*mat_dim*mat_dim*sizeof(double), sizeof(double),
                         MPI_INFO_NULL, *comm, &win_mem, win);

        *mat_a = win_mem;
        *mat_b = *mat_a + mat_dim * mat_dim;
        *mat_c = *mat_b + mat_dim * mat_dim;
    } else {
        MPI_Win_allocate(0, sizeof(double), MPI_INFO_NULL, *comm,
                         &win_mem, win);
    }

    /* allocate local buffer */
    MPI_Alloc_mem(3*blk_dim*blk_dim*sizeof(double), MPI_INFO_NULL, local_mem);

    /* create block datatype */
    MPI_Type_vector(blk_dim, blk_dim, mat_dim, MPI_DOUBLE, blk_dtp);
    MPI_Type_commit(blk_dtp);
}

static void
kernel_postconfig(vb_instance_t * instance,
                  dgemm_mode_t    mode,
                  double        * local_mem,
                  MPI_Datatype  * blk_dtp,
                  MPI_Comm      * comm,
                  MPI_Win       * win)
{
    MPI_Type_free(blk_dtp);
    MPI_Free_mem(local_mem);
    MPI_Win_free(win);

    if (mode == VB_DGEMM_LOCAL)
        MPI_Comm_free(comm);
}

static uint8_t
iteration(vb_instance_t * instance,
          int             rank,
          int             mat_dim,
          int             blk_dim,
          int             px,
          int             py,
          double        * local_mem,
          double        * mat_a,
          double        * mat_b,
          double        * mat_c,
          MPI_Datatype    blk_dtp,
          MPI_Win         win)
{
    int blk_num, rx, ry, bx, by;
    double *local_a, *local_b, *local_c;
    MPI_Aint disp_a, disp_b, disp_c;
    MPI_Aint offset_a, offset_b, offset_c;
    int i, j, k;
    int global_i, global_j;
    uint8_t val;

    /* number of blocks in one dimension */
    blk_num = mat_dim / blk_dim;

    /* determine my coordinates (x,y) -- r=x*a+y in the 2d processor array */
    rx = rank % px;
    ry = rank / px;

    /* determine distribution of work */
    bx = blk_num / px;
    by = blk_num / py;

    /* setup local buffer */
    local_a = local_mem;
    local_b = local_a + blk_dim * blk_dim;
    local_c = local_b + blk_dim * blk_dim;

    disp_a = 0;
    disp_b = disp_a + mat_dim * mat_dim;
    disp_c = disp_b + mat_dim * mat_dim;

    MPI_Win_lock(MPI_LOCK_SHARED, 0, 0, win);

    for (i = 0; i < by; i++) {
        for (j = 0; j < bx; j++) {

            global_i = i + by * ry;
            global_j = j + bx * rx;

            /* get block from mat_a */
            offset_a = global_i * blk_dim * mat_dim + global_j * blk_dim;
            MPI_Get(local_a, blk_dim*blk_dim, MPI_DOUBLE,
                    0, disp_a+offset_a, 1, blk_dtp, win);

            MPI_Win_flush(0, win);

            for (k = 0; k < blk_num; k++) {

                /* get block from mat_b */
                offset_b = global_j * blk_dim * mat_dim + k * blk_dim;
                MPI_Get(local_b, blk_dim*blk_dim, MPI_DOUBLE,
                        0, disp_b+offset_b, 1, blk_dtp, win);

                MPI_Win_flush(0, win);

                /* local computation */
                dgemm(local_a, local_b, local_c, blk_dim);

                /* accumulate block to mat_c */
                offset_c = global_i * blk_dim * mat_dim + k * blk_dim;
                MPI_Accumulate(local_c, blk_dim*blk_dim, MPI_DOUBLE,
                               0, disp_c+offset_c, 1, blk_dtp, MPI_SUM, win);

                MPI_Win_flush(0, win);

                /* save some crap */
                val += local_c[k];

            }
        }
    }

    MPI_Win_unlock(0, win);

#if 0
    if (rank == 0) {
        check_mats(mat_a, mat_b, mat_c, mat_dim);

        print_mat(mat_a, mat_dim);
        print_mat(mat_b, mat_dim);
        print_mat(mat_c, mat_dim);

        printf("[%i] time: %f\n", rank, t2-t1);
    }
#endif

    return val;
}


static int
run_kernel(vb_instance_t    * instance,
           unsigned long long iterations,
           int                mat_dim,
           int                blk_dim,
           int                px,
           int                py,
           dgemm_mode_t       mode)
{
    unsigned long long iter;
    struct timeval t1, t2;
    int fd, status, rank;
    uint8_t dummy;

    double * local_mem, * mat_a, * mat_b, * mat_c;
    MPI_Comm comm;
    MPI_Win win;
    MPI_Datatype blk_dtp;

    /* Preconfig the matrices, MPI windows, etc. */
    kernel_preconfig(
            instance,
            mat_dim,
            blk_dim,
            px,
            py,
            mode,
            &rank,
            &local_mem,
            &mat_a,
            &mat_b,
            &mat_c,
            &blk_dtp,
            &comm,
            &win
        );

    for (iter = 0; iter < iterations; iter++) {

        /* initialize matrices */
        if (rank == 0)
            init_mats(mat_dim, mat_a, mat_b, mat_c);

        MPI_Barrier(MPI_COMM_WORLD);

        gettimeofday(&t1, NULL);
        dummy = iteration(
                instance,
                rank,
                mat_dim,
                blk_dim,
                px,
                py,
                local_mem,
                mat_a,
                mat_b,
                mat_c,
                blk_dtp,
                win
            );
        gettimeofday(&t2, NULL);

        MPI_Barrier(MPI_COMM_WORLD);

#if 0
        /* check result */
        if (rank == 0)
            check_mats(mat_a, mat_b, mat_c, mat_dim);
#endif

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

    kernel_postconfig(
        instance,
        mode,
        local_mem,
        &blk_dtp,
        &comm,
        &win
    );
        

    return VB_SUCCESS;
}
                

static void
usage(void)
{
    vb_error_root("\ndgemm required args:\n"
        "  <matrix dimension>\n"
        "  <block dimension>\n"
        "  <px (x dimension of processor grid)>\n"
        "  <py (y dimension of processor grid)>\n"
        " Constraints:\n"
        "  (1) (mat_dim %% blk_dim) == 0\n"
        "  (2) ((mat_dim / blk_dim) %% px) == 0\n"
        "  (3) ((mat_dim / blk_dim) %% py) == 0\n"
        "  (4) (px * py) == {\n"
        "         1: each instance runs private dgemm;\n"
        "         total number of processors: global dgemm\n"
        "      }\n"
    );
}

int 
vb_kernel_dgemm(int             argc, 
                char         ** argv,
                vb_instance_t * instance)
{
    int mat_dim, blk_dim, px, py;
    dgemm_mode_t mode;

    /* Get args */
    if (argc != 4) {
        usage();
        return VB_BAD_ARGS;
    }

    mat_dim = atoi(argv[0]);
    blk_dim = atoi(argv[1]);
    px      = atoi(argv[2]);
    py      = atoi(argv[3]);

    if (
        (mat_dim % blk_dim        != 0) ||
        ((mat_dim / blk_dim) % px != 0) ||
        ((mat_dim / blk_dim) % py != 0)) 
    {
        usage();
        return VB_BAD_ARGS;
    }

    if (px * py == instance->rank_info.num_instances) {
        mode = VB_DGEMM_GLOBAL;
    } else if (px * py == 1) {
        mode = VB_DGEMM_LOCAL;
    } else {
        usage();
        return VB_BAD_ARGS;
    }

    return run_kernel(instance,
        instance->options.num_iterations,
        mat_dim,
        blk_dim,
        px,
        py,
        mode
    );

}
