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
#include <sys/time.h>

#include <varbench.h>

typedef enum {
    VB_ALL,
    VB_RANDOM,
    VB_NEAREST
} vb_mp_mode_t;



/* Uses MPI one sided message transfers via remote memory access (RMA)
 *
 * Each rank has a list of neighbors, and it fetches 'message_size' bytes from
 * each neighbor via MPI_Get(). There is no blocking due to synchronization,
 * only to wait for data
 */
static uint8_t
iteration(vb_instance_t    * instance,
          unsigned long      message_size,
          int                group_size,
          int              * neighbors,
          int              * data,
          int              * recvbuf,
          MPI_Win            win)
{
    vb_rank_info_t * info = &(instance->rank_info);
    unsigned long size, num_elements, num_elements_per_instance, i, j;
    uint8_t val;

    num_elements_per_instance = message_size / sizeof(int);
    num_elements = info->num_instances *
        num_elements_per_instance;

    for (j = 0; j < group_size; j++) {
        int target = neighbors[j];
        /* There will never be conflicting locks, as the shared buffer is never written, so these calls
         * should never block
         */
        MPI_Win_lock(MPI_LOCK_SHARED, target, MPI_MODE_NOCHECK, win);
        MPI_Get(
                //&data[target*num_elements_per_instance], /* local buffer where data is stored */
                recvbuf,
                num_elements_per_instance, /* number of entries in local buffer */
                MPI_INT, /* type */
                target, /* target rank */
                target, /* displacement (in units of 'target count') */
                num_elements_per_instance,  /* target count */
                MPI_INT, /* type */
                win /* window */
            );
        MPI_Win_unlock(target, win);

        /* do something with the data to prevent the compiler from optimizing */
        val += recvbuf[j];

    }

    return val;
}

/* Example of MPI 1 sided RMA with active synchronization (fences) */
#if 0
#define NUM_ELEMENT 4
static int
run_kernel(vb_instance_t    * instance,
           unsigned long      message_size,
           int                group_size,
           int              * neighbors,
           int              * data,
           int              * recvbuf,
           MPI_Win            fake_win, 
           unsigned long long iterations)
{
    int i, id, num_procs, len, localbuffer[NUM_ELEMENT], sharedbuffer[NUM_ELEMENT];
    char name[MPI_MAX_PROCESSOR_NAME];
    MPI_Win win;

    id = instance->rank_info.global_id;
    num_procs = instance->rank_info.num_instances;

    MPI_Get_processor_name(name, &len);

    printf("Rank %d running on %s\n", id, name);

    //MPI_Win_create(sharedbuffer, NUM_ELEMENT, sizeof(int), MPI_INFO_NULL, sub_comm, &win);
    MPI_Win_create(sharedbuffer, NUM_ELEMENT, sizeof(int), MPI_INFO_NULL, MPI_COMM_WORLD, &win);

    for (i = 0; i < NUM_ELEMENT; i++)
    {
      sharedbuffer[i] = 10*id + i;
      localbuffer[i] = 0;
    }

    printf("Rank %d sets data in the shared memory:", id);

    for (i = 0; i < NUM_ELEMENT; i++)
      printf(" %02d", sharedbuffer[i]);

    printf("\n");

    MPI_Win_fence(0, win);
      

    if (id != 0)
      MPI_Get(&localbuffer[0], NUM_ELEMENT, MPI_INT, id-1, 0, NUM_ELEMENT, MPI_INT, win);
    else
      MPI_Get(&localbuffer[0], NUM_ELEMENT, MPI_INT, num_procs-1, 0, NUM_ELEMENT, MPI_INT, win);

    MPI_Win_fence(0, win);

    printf("Rank %d gets data from the shared memory:", id);

    for (i = 0; i < NUM_ELEMENT; i++)
      printf(" %02d", localbuffer[i]);

    printf("\n");

    MPI_Win_fence(0, win);

    if (id < num_procs-1)
      MPI_Put(&localbuffer[0], NUM_ELEMENT, MPI_INT, id+1, 0, NUM_ELEMENT, MPI_INT, win);
    else
      MPI_Put(&localbuffer[0], NUM_ELEMENT, MPI_INT, 0, 0, NUM_ELEMENT, MPI_INT, win);

    MPI_Win_fence(0, win);

    printf("Rank %d has new data in the shared memory:", id);

    for (i = 0; i < NUM_ELEMENT; i++)
      printf(" %02d", sharedbuffer[i]);

    printf("\n");

    MPI_Win_free(&win);

    return VB_SUCCESS;
}
#else
static int
run_kernel(vb_instance_t    * instance,
           unsigned long      message_size,
           int                group_size,
           int              * neighbors,
           int              * data,
           int              * recvbuf,
           MPI_Win            win, 
           unsigned long long iterations)
{

    unsigned long long iter;
    struct timeval t1, t2;
    int fd, status;
    uint8_t dummy;

    /* Allocate an array */

    for (iter = 0; iter < iterations; iter++) {
        MPI_Barrier(MPI_COMM_WORLD);

        gettimeofday(&t1, NULL);
        dummy = iteration(instance, message_size, group_size, neighbors, 
                data, recvbuf, win);
        gettimeofday(&t2, NULL);

        MPI_Barrier(MPI_COMM_WORLD);

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

    return VB_SUCCESS;
}
#endif

/* This function is only executed by the root instance (id=0)
 *
 * Create a single permutation of all nodes. When assigning a 
 * group for a given node, start from its offset, in the permutation
 * and count 'group_size' elements from there.
 *
 * In this way, we ensure that each node has the same number of
 * neighbors in the system
 */
static void
root_generate_random_permutation(int   nr_indices,
                                 int * permutation)
{
    int i, j, temp;

    for (i = 0; i < nr_indices; i++)
        permutation[i] = i;

    /* This is only done once, so we can seed with anything. There's
     * no need to be consistent across nodes as only node 0 does this
     */
    srand(time(NULL));

    for (i = nr_indices - 1; i>= 0; --i) {
        j = rand() % (i + 1);

        temp = permutation[i];
        permutation[i] = permutation[j];
        permutation[j] = temp;
    }
}


/*
 * Create a list of rank IDs to be accessed by this instance
 */
static void 
get_neighbor_list(vb_instance_t * instance,
                  vb_mp_mode_t    mode,
                  int           * node_permutation,
                  int             group_size,
                  int           * group)
{
    vb_rank_info_t * rank_info = &(instance->rank_info);
    int i, node, local_offset;

    if (mode == VB_ALL) {
        assert(group_size == rank_info->num_nodes - 1);

        /* Grab all nodes but self */
        for (i = 0, node = 0; i < group_size; i++) {
            if (node == rank_info->node_id) {
                node = (node == rank_info->num_nodes)
                    ? 0
                    : node + 1;
            }

            group[i] = node++;
        }

    } else if (mode == VB_RANDOM) {
        /* Select 'group_size' random nodes */
        local_offset = rank_info->node_id;

        for (i = 0; i < group_size; i++) {
            node = node_permutation[(rank_info->node_id + local_offset) % rank_info->num_nodes];

            if (node == rank_info->node_id)
                node = node_permutation[(rank_info->node_id + ++local_offset) % rank_info->num_nodes];

            group[i] = node;
            ++local_offset;
        }
    } else {
        assert(mode == VB_NEAREST);
        int next;

        /* Select the "nearest" 'group_size'/2 nodes to the "left," and the
         * "nearest" 'group_size'/2 to the "right"
         */
        next = rank_info->node_id;
        for (i = 0; i < group_size/2; i++) {
            next = (next == 0) ? rank_info->num_nodes - 1 : next - 1;
            group[i] = next;
        }
        
        next = rank_info->node_id;
        for (; i < group_size; i++) {
            next = (next == rank_info->num_nodes - 1) ? 0 : next + 1;
            group[i] = next;
        }
    }

    /* Convert node list to rank list.
     * We use the node_map to get the list of ranks on the target node, and
     * offset the list to find the rank on that node with the same local id
     * as us
     */
    for (i = 0; i < group_size; i++) {
        group[i] = rank_info->node_map[group[i]]->rank_list[rank_info->local_id];
    }
}
                        

static int
vb_kernel_message_passing(vb_instance_t    * instance,
                          vb_mp_mode_t       mode,
                          unsigned long      message_size,
                          int                group_size,
                          unsigned long long iterations)
{
    vb_rank_info_t * rank_info = &(instance->rank_info);
    unsigned long size, num_elements, num_elements_per_instance, i, j;
    int ret, * permutation = NULL, * neighbors = NULL, * data = NULL, * recvbuf = NULL;
    MPI_Win win;

    /* See if there are enough nodes */
    if (group_size >= rank_info->num_nodes) {
        vb_error_root("Only %d nodes in the experiment, but %d neighbors requested\n",
            rank_info->num_nodes, group_size
        );

        return VB_GENERIC_ERROR;
    }

    neighbors = malloc(sizeof(int) * group_size);
    if (neighbors == NULL) {
        vb_error("Out of memory\n");
        ret = VB_GENERIC_ERROR;
        goto out;
    }

    /* Generate a random permutation of all node id's if we want
     * random neighbors
     */
    if (mode == VB_RANDOM) {
        permutation = malloc(sizeof(int) * rank_info->num_nodes);
        if (permutation == NULL) {
            vb_error("Out of memory\n");
            ret = VB_GENERIC_ERROR;
            goto out;
        }

        /* Root generates permutation */
        if (rank_info->global_id == 0)
            root_generate_random_permutation(rank_info->num_nodes, permutation);

        /* Distribute permutation array to each rank */
        MPI_Bcast(
            permutation,
            rank_info->num_nodes,
            MPI_INT,
            0,
            MPI_COMM_WORLD
        );
    }

    /* Generate our list of neighbors */
    get_neighbor_list(
        instance,
        mode,
        permutation,
        group_size,
        neighbors
    );

    {
        char * lst, * old_lst;
        asprintf(&lst, "Rank %d neighbors:", rank_info->global_id);
        for (i = 0; i < group_size; i++) {
            old_lst = lst;
            asprintf(&lst, "%s [node=%d, rank=%d]", old_lst, 
                rank_info->rank_map[neighbors[i]].node_id,
                neighbors[i]
            );
            free(old_lst);
        }

        vb_debug("%s\n", lst);
        free(lst);
    }

    /* Create shared data and MPI window for RMA transfers */
    num_elements_per_instance = message_size / sizeof(int);
    num_elements = rank_info->num_instances *
        num_elements_per_instance;

    MPI_Alloc_mem(sizeof(int) * num_elements, MPI_INFO_NULL, &data);

    recvbuf = malloc(sizeof(int) * num_elements_per_instance);
    if (recvbuf == NULL) {
        vb_error("Out of memory\n");
        goto out;
    }

/* Debugging stuff - not needed */
#if 0
    for (i = 0; i < rank_info->num_instances; i++) {
        unsigned long b = i*num_elements_per_instance; 
        for (j = b; j < b + num_elements_per_instance; j++) {
            if (i == rank_info->global_id) {
                data[j] = 1000 * rank_info->global_id + j;
            } else {
                data[j] = 0;
            }
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);
#endif

    MPI_Win_create(data, rank_info->num_instances, message_size, 
            MPI_INFO_NULL, MPI_COMM_WORLD, &win);

    ret = run_kernel(
        instance,
        message_size,
        group_size,
        neighbors,
        data,
        recvbuf,
        win,
        iterations
    );

    MPI_Win_free(&win);

out:
    if (data)
        MPI_Free_mem(data);

    if (recvbuf)
        free(recvbuf);

    if (neighbors)
        free(neighbors);

    if (permutation)
        free(permutation);

    return ret;
}


static void
usage_alltoall(void)
{
    vb_error_root("alltoall requires args:\n"
        "  <message size (B)>\n"
    );
}

static void
usage_random_neighbor(void)
{
    vb_error_root("random_neighbor requires args:\n"
        "  <message size (B)>\n"
        "  <number of neighbors per node>\n"
    );
}

static void
usage_nearest_neighbor(void)
{
    vb_error_root("nearest_neighbor requires args:\n"
        "  <message size (B)>\n"
        "  <number of neighbors per node>\n"
    );
}



int 
vb_kernel_alltoall(int             argc,
                   char         ** argv,
                   vb_instance_t * instance)
{
    if (argc != 1) {
        usage_alltoall();
        return VB_BAD_ARGS;
    }

    return vb_kernel_message_passing(
            instance,
            VB_ALL,
            atol(argv[0]),
            instance->rank_info.num_nodes - 1,
            instance->options.num_iterations
        );
}

int
vb_kernel_random_neighbor(int             argc,
                          char         ** argv,
                          vb_instance_t * instance)
{
    if (argc != 2) {
        usage_random_neighbor();
        return VB_BAD_ARGS;
    }

    vb_debug("vb_kernel_message_passing\n");

    return vb_kernel_message_passing(
            instance,
            VB_RANDOM,
            atol(argv[0]),
            atoi(argv[1]),
            instance->options.num_iterations
        );
}

int
vb_kernel_nearest_neighbor(int             argc,
                           char         ** argv,
                           vb_instance_t * instance)
{
    if (argc != 2) {
        usage_nearest_neighbor();
        return VB_BAD_ARGS;
    }

    return vb_kernel_message_passing(
            instance,
            VB_NEAREST,
            atol(argv[0]),
            atoi(argv[1]),
            instance->options.num_iterations
        );
}



