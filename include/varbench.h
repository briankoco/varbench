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

#ifndef __VARBENCH_H__
#define __VARBENCH_H__

#define VB_SUCCESS          0
#define VB_GENERIC_ERROR    -1
#define VB_BAD_ARGS         -2

#define VB_BASE_SEED        1024

#include <stdbool.h>
#include <hwloc.h>
#include <mpi.h>

#ifdef USE_PAPI
#include <papi.h>
#endif

#define MAX_NAME_LEN 64

typedef struct varbench_options {
    char               kernel_name[MAX_NAME_LEN];
    bool               debug_on;
    char               map_by[3];
    char               mem_affinity;
    char             * topo_file;
    unsigned int       turbo_pin;
    unsigned long long num_iterations;

    int              * cpu_array;
    int                cpu_array_len;
} vb_options_t;

/* forward decalaration of struct in src/modules/topology.c */
struct varbench_hw_topology;

typedef struct varbench_topology {
    struct varbench_hw_topology * hw_topology;

    /* Information about where we are pinned */
    signed int os_core_id;    /* logical cpu id that the OS exposes (i.e., top/mpstat) */
    signed int socket_id;     /* socket we're pinned to */
    signed int core_id;       /* physical core we're pinned to on this socket */
    signed int hw_thread_id;  /* e.g., hyperthread number */

    /* Info for each node */
    unsigned int sockets_per_node;
    unsigned int cores_per_socket;
    unsigned int hw_threads_per_core;
} vb_topology_t;

typedef struct varbench_node_info {
    char         hostname[HOST_NAME_MAX];
    unsigned int rank_list[0]; /* of length num_local instances */
} vb_node_info_t;

typedef struct varbench_rank_id {
    unsigned int local_id;
    unsigned int node_id;
} vb_rank_id_t;

typedef struct varbench_rank_info {
    MPI_Comm     local_root_comm;
    MPI_Comm     local_node_comm;

    unsigned int global_id;
    unsigned int local_id;
    unsigned int node_id;

    unsigned int num_instances;
    unsigned int num_local_instances;
    unsigned int num_nodes;

    /* The following 2 structures are allocated by local rank 0 on
     * each node, and exported via shared memory (mmap) to all
     * other local ranks
     */
    /* global_id (MPI rank) ---> {local_id, node_id} */
    vb_rank_id_t * rank_map;
    int rank_map_fd;
    char rank_map_fname[MAX_NAME_LEN];

    /* node_id --->
     *      (1) hostname
     *      (2) [array of local_id] ---> global_id (MPI rank) 
     */
    vb_node_info_t ** node_map; 
    int node_map_fd;
    char node_map_fname[MAX_NAME_LEN];
} vb_rank_info_t;

struct varbench_instance;
typedef struct varbench_kernel {
    char * name;
    int (*fn)(int argc, char ** argv, struct varbench_instance * instance);
} vb_kernel_t;

typedef struct varbench_perfctrs {

} vb_perfctrs_t;

#define VB_FMT_LEN    64
#define VB_SUFFIX_LEN 16
#define VB_FILE_LEN   VB_FMT_LEN + VB_SUFFIX_LEN

typedef struct varbench_instance {
    bool           as_library;

    vb_options_t   options;
    vb_topology_t  topology;
    vb_rank_info_t rank_info;
    vb_kernel_t    kernel;
    vb_perfctrs_t  perf_ctrs;

    struct {
        FILE * file;
        char name[VB_FILE_LEN];
    } data_csv;

    struct {
        FILE * file;
        char name[VB_FILE_LEN];
    } meta_xml;
} vb_instance_t;




extern vb_instance_t this_instance;


#define VB_PRINT_ANY    1
#define VB_PRINT_ROOT   0

#define __vb_print(vb_type, format, args...) \
    if ((vb_type == VB_PRINT_ANY) || ((vb_type == VB_PRINT_ROOT) && this_instance.rank_info.global_id == 0)) {\
        printf("[in_%d:%s:%d]:  INFO: "format, this_instance.rank_info.global_id, __FILE__, __LINE__, ##args);\
        fflush(stdout);\
    }

#define __vb_debug(vb_type, format, args...) \
    if (this_instance.options.debug_on) \
        if ((vb_type == VB_PRINT_ANY) || ((vb_type == VB_PRINT_ROOT) && this_instance.rank_info.global_id == 0)) {\
            printf("[in_%d:%s:%d]: DEBUG: "format, this_instance.rank_info.global_id, __FILE__, __LINE__, ##args);\
            fflush(stdout);\
        }

#define __vb_error(vb_type, format, args...) \
    if ((vb_type == VB_PRINT_ANY) || ((vb_type == VB_PRINT_ROOT) && this_instance.rank_info.global_id == 0)) {\
        printf("[in_%d:%s:%d]: ERROR: "format, this_instance.rank_info.global_id, __FILE__, __LINE__, ##args);\
        fflush(stdout);\
    }

#define vb_print_root(format, args...)  __vb_print(VB_PRINT_ROOT, format, ##args)
#define vb_debug_root(format, args...)  __vb_debug(VB_PRINT_ROOT, format, ##args)
#define vb_error_root(format, args...)  __vb_error(VB_PRINT_ROOT, format, ##args)
#define vb_print(format, args...)       __vb_print(VB_PRINT_ANY, format, ##args)
#define vb_debug(format, args...)       __vb_debug(VB_PRINT_ANY, format, ##args)
#define vb_error(format, args...)       __vb_error(VB_PRINT_ANY, format, ##args)

/* Kernels */

int vb_kernel_hello_world(int, char **, vb_instance_t *);

/* Cache kernels */
int vb_kernel_cache_coherence(int, char **, vb_instance_t *);
int vb_kernel_cache_capacity(int, char **, vb_instance_t *);

/* Memory kernels */
int vb_kernel_stream(int, char **, vb_instance_t *);
int vb_kernel_random_access(int, char **, vb_instance_t *);

/* CPU kernels */

/* Network kernels */
int vb_kernel_alltoall(int, char **, vb_instance_t *);
int vb_kernel_random_neighbor(int, char **, vb_instance_t *);
int vb_kernel_nearest_neighbor(int, char **, vb_instance_t *);
int vb_kernel_io(int, char **, vb_instance_t *);

/* Application kernels */
int vb_kernel_dgemm(int, char **, vb_instance_t *);

/* Other kernels */
int vb_kernel_operating_system(int, char **, vb_instance_t *);




/* Other modules */

int  vb_build_rank_info(int *, char ***, vb_instance_t *);
void vb_deinit_rank_info(vb_instance_t *);

int  vb_init_instance(vb_instance_t *, bool, int, char **);
void vb_deinit_instance(vb_instance_t *, int);

int  vb_build_hw_topology(vb_instance_t *);
void vb_destroy_hw_topology(vb_instance_t *);
int  vb_bind_instance(vb_instance_t *);

void vb_add_misc_meta_info(vb_instance_t *, char *, char *, int, char **, char **);

int  vb_get_processor_os_core(vb_instance_t *, unsigned int, unsigned int,
            unsigned int, unsigned int *);
int  vb_get_processor_for_instance(vb_instance_t *, int, char[3],
            unsigned int *, unsigned int *, unsigned int *);

int  vb_gather_kernel_results_time_spent(vb_instance_t *, unsigned long long,
            unsigned long long);
int  vb_abort_kernel(vb_instance_t *);



#include <sys/time.h>
static inline unsigned long long
time_spent(struct timeval * tv1,
           struct timeval * tv2)
{
    return (tv2->tv_sec * 1000000. + tv2->tv_usec) - (tv1->tv_sec * 1000000. + tv1->tv_usec);
}

static inline int 
vb_gather_kernel_results(vb_instance_t    * instance, 
                         unsigned long long iteration,
                         struct timeval   * tv1,
                         struct timeval   * tv2)
{
    return vb_gather_kernel_results_time_spent(instance, iteration, time_spent(tv1, tv2));
}


static inline void
vb_build_fmt_str(vb_instance_t * instance, 
                 char          * prefix,
                 char         ** str)
{
    struct timeval tv;

    time_t nowtime;
    struct tm * nowtm;
    char tmbuf[VB_FMT_LEN];
    char tmbuf_micro[VB_FMT_LEN*2 + 2];

    gettimeofday(&tv, NULL);
    nowtime = tv.tv_sec;
    nowtm = localtime(&nowtime);
    strftime(tmbuf, sizeof(tmbuf), "%Y-%m-%d-%H-%M-%S", nowtm);
    snprintf(tmbuf_micro, sizeof(tmbuf_micro), "%s-%lu", tmbuf, tv.tv_usec);

    if (prefix != NULL) {
        asprintf(str, "%s-%s", prefix, tmbuf_micro);
    } else {
        asprintf(str, "%s", tmbuf_micro);
    }
}

#endif /* __VARBENCH_H__ */
