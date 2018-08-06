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
#include <limits.h>
#include <fcntl.h>

#include <sys/mman.h>

#include <varbench.h>

#include "vb_hashtable.h"
#include "vb_list.h"

#define PAGE_SIZE    sysconf(_SC_PAGESIZE)


static inline void
create_file_name(char * prefix,
                 char * unique,
                 char * post_fix,
                 char * fname,
                 int    fname_len)
{
    snprintf(fname, fname_len, "%s%s%s", prefix, unique, post_fix);
}

#define VB_TOPO_FILE "/dev/shm/vb-topo"

#define VB_NODE_MAP_FILE(unique_str, fname, fname_len) \
    create_file_name(VB_TOPO_FILE "-node-", unique_str, ".shm", fname, fname_len)

#define VB_RANK_MAP_FILE(unique_str, fname, fname_len) \
    create_file_name(VB_TOPO_FILE "-rank-", unique_str, ".shm", fname, fname_len)

#define MAX_FNAME_LEN 64

typedef unsigned int vb_hash_val_t;

static uint32_t
vb_hash_fn(uintptr_t key)
{
    char * k = (char *)key;
    return vb_hash_buffer((uint8_t *)k, strlen(k));
}

static int
vb_eq_fn(uintptr_t key1,
         uintptr_t key2)
{
    char * k1 = (char *)key1;
    char * k2 = (char *)key2;

    return (strcmp(k1, k2) == 0);
}

typedef struct {
    unsigned int     global_id;
    struct list_head list_node;
} vb_node_entry_t;

static void
vb_add_local_rank(struct list_head * node_map,
                  unsigned int       global_rank)
{
    vb_node_entry_t * new_entry, * tmp;

    new_entry = malloc(sizeof(vb_node_entry_t));
    assert(new_entry);

    INIT_LIST_HEAD(&(new_entry->list_node));
    new_entry->global_id = global_rank;

    list_for_each_entry(tmp, node_map, list_node) {
        if (tmp->global_id > new_entry->global_id) {
            list_add_tail(&(new_entry->list_node), &(tmp->list_node));
            return;
        }
    }

    list_add_tail(&(new_entry->list_node), node_map);
}

static void
vb_delete_local_node_map(struct list_head * node_map)
{
    vb_node_entry_t * tmp, * next;

    list_for_each_entry_safe(tmp, next, node_map, list_node) {
        list_del(&(tmp->list_node));
        free(tmp);
    }
}

static int
vb_build_system_topology(vb_rank_info_t   * info,
                         vb_options_t     * options,
                         char               hostname[HOST_NAME_MAX],
                         struct list_head * node_map,
                         unsigned int     * node_id_list)
{
    struct hashtable * hostname_table = NULL;
    char * namebuf, * cur_hostname;
    int status;
    unsigned int i, node_id;

    /* Global root is responsible for allocating node IDs and needs
     * a hashtable to do so
     */
    if (info->global_id == 0) {
        hostname_table = (struct hashtable *)vb_create_htable(0, vb_hash_fn, vb_eq_fn);
        if (hostname_table == NULL) {
            vb_error("Out of memory\n");
            return VB_GENERIC_ERROR;
        }
    }

    namebuf = malloc(sizeof(char) * HOST_NAME_MAX * info->num_instances);
    if (namebuf == NULL) {
        vb_error("Out of memory\n");
        status = VB_GENERIC_ERROR;
        goto out_namebuf;
    }

    MPI_Allgather(hostname, HOST_NAME_MAX, MPI_CHAR, namebuf, 
                            HOST_NAME_MAX, MPI_CHAR, MPI_COMM_WORLD);

    info->num_local_instances = 0;
    info->local_id            = 0;
    info->num_nodes           = 0;

    for (i = 0; i < info->num_instances; i++) {
        cur_hostname = &(namebuf[i*HOST_NAME_MAX]);

        /* Global root - determine if we've seen this hostname before,
         * and determine the node_id for this rank
         */
        if (info->global_id == 0) {
            node_id = (unsigned int)vb_htable_search(hostname_table, (uintptr_t)cur_hostname);
            if (node_id == 0) {
                /* Not found - add it now (note that we add the
                 * node_id + 1, as 0 cannot be a successful return
                 * value
                 */

                status = vb_htable_insert(hostname_table, (uintptr_t)cur_hostname, (uintptr_t)++info->num_nodes);
                if (status == 0) {
                    vb_error("Could not update hashtable\n");
                    goto out_htable_insert;
                }

                node_id = info->num_nodes - 1;
            } else {
                node_id = node_id - 1;
            }

            node_id_list[i] = node_id;
        }
            
        if (strcmp(hostname, cur_hostname) == 0) {
            info->num_local_instances++;

            if (i < info->global_id) {
                info->local_id++;

                /* We can forget about the local node map now; it will be created by
                 * local rank 0 and that aint us
                 */
                if (info->local_id == 1)
                    vb_delete_local_node_map(node_map);
            }

            /* Remember rank if we could still be the local root */
            if (info->local_id == 0) {
                vb_add_local_rank(node_map, i);
            }
        }
    }

    status = VB_SUCCESS;

out_htable_insert:
    free(namebuf);

out_namebuf:
    if (hostname_table != NULL)
        vb_free_htable(hostname_table, 0, 0);

    return VB_SUCCESS;
}

static int
vb_exchange_local_root_maps(vb_rank_info_t   * info,
                            char               hostname[HOST_NAME_MAX],
                            struct list_head * node_map)
{
    int status;
    unsigned int i, j, * node_array, *instances_per_node;
    vb_node_entry_t * tmp, * next;
 
    /* (1) Ensure each node has the same number of local instances */
    {
        instances_per_node = malloc(sizeof(unsigned int) * info->num_nodes);
        if (instances_per_node == NULL) {
            vb_error("Out of memory\n");
            return VB_GENERIC_ERROR;
        }

        MPI_Allgather(
            &(info->num_local_instances),
            1,
            MPI_UNSIGNED,
            instances_per_node,
            1,
            MPI_UNSIGNED,
            info->local_root_comm
        );

        for (i = 0; i < info->num_nodes; i++) {
            if (instances_per_node[i] != info->num_local_instances) {
                vb_error("Node %d has %d local instances, whereas node %d has %d\n",
                    i, instances_per_node[i], info->node_id, info->num_local_instances);
                status = VB_GENERIC_ERROR;
                goto out_compare;
            }
        }
    }

    /* (2) Exchange node_maps */
    {
        MPI_Datatype node_map_dt;

        /* Allocate virtually contiguous space for the global node map */ 
        info->node_map = malloc(sizeof(vb_node_info_t *) * info->num_nodes);
        if (info->node_map == NULL) {
            vb_error("Could not allocate global node map: %s\n", strerror(errno));
            status = VB_GENERIC_ERROR;
            goto out_map_alloc;
        }
        info->node_map[0] = malloc(
            info->num_nodes * 
                (sizeof(vb_node_info_t) + (sizeof(unsigned int) * info->num_local_instances))
            );
        if (info->node_map[0] == NULL) {
            vb_error("Could not allocate global node map: %s\n", strerror(errno));
            status = VB_GENERIC_ERROR;
            goto out_map_alloc;
        }

        for (i = 0; i < info->num_nodes; i++) {
            info->node_map[i] = (vb_node_info_t *)(
                (uintptr_t)info->node_map[0] + 
                (i * (sizeof(vb_node_info_t) + (sizeof(unsigned int) * info->num_local_instances)))
            );
        }

        /* Convert list-head list to array */
        i = 0;
        list_for_each_entry_safe(tmp, next, node_map, list_node) {
            info->node_map[info->node_id]->rank_list[i++] = tmp->global_id;
            list_del(&(tmp->list_node));
            free(tmp);
        }

        strncpy(info->node_map[info->node_id]->hostname, hostname, HOST_NAME_MAX);

        /* Build a custom datatype */
        {
            int count = 2;
            int array_of_blocklengths[] = {
                HOST_NAME_MAX,
                info->num_local_instances
            };
            MPI_Aint array_of_displacements[] = {
                offsetof(vb_node_info_t, hostname),
                offsetof(vb_node_info_t, rank_list)
            };
            MPI_Datatype array_of_types[] = {
                MPI_CHAR,
                MPI_UNSIGNED
            };

            MPI_Type_create_struct(
                count,
                array_of_blocklengths,
                array_of_displacements,
                array_of_types,
                &node_map_dt
            );
            MPI_Type_commit(&node_map_dt);
        }

        /* Perform the exchange */
        {
            /* The MPI standard does not allow overlapping address ranges on the send and recvbufs of an Allgather.
             * So, we need to copy to a separate buffer the data we want to send
             */
            vb_node_info_t * send = malloc(sizeof(vb_node_info_t) + (sizeof(unsigned int) * info->num_local_instances));
            assert(send != NULL);
            memcpy(send, info->node_map[info->node_id], sizeof(vb_node_info_t) + (sizeof(unsigned int) * info->num_local_instances));

            MPI_Allgather(
                send,
                1,
                node_map_dt,
                info->node_map[0],
                1,
                node_map_dt,
                info->local_root_comm
            );

            free(send);
        }
    }

    /* (3) Build the rank_map for this node by walking through the
     * node maps we just gathered
     */
    {
        info->rank_map = malloc(sizeof(vb_rank_id_t) * info->num_instances);
        if (info->rank_map == NULL) {
            vb_error("Out of memory\n");
            status = VB_GENERIC_ERROR;
            goto out_rank_map;
        }

        for (i = 0; i < info->num_nodes; i++) {
            vb_node_info_t * node_info = info->node_map[i];

            for (j = 0; j < info->num_local_instances; j++) {
                unsigned int global_id = node_info->rank_list[j];
                assert(global_id < info->num_instances);

                info->rank_map[global_id].node_id  = i;
                info->rank_map[global_id].local_id = j;
            }
        }
    }

    status = VB_SUCCESS;

out_rank_map:
out_map_alloc:
    if (status != VB_SUCCESS) {
        if (info->node_map[0])
            free(info->node_map[0]);
        free(info->node_map);
    }
out_compare:
    free(instances_per_node);

    return status;
}

int
vb_export_local_root_maps(vb_rank_info_t * info)
{
    int status;
    size_t size;
    void * local_map;

    /* Export node map */
    {
        info->node_map_fd = open(info->node_map_fname, O_CREAT | O_TRUNC | O_RDWR, S_IRUSR | S_IWUSR);
        if (info->node_map_fd == -1) {
            vb_error("Could not create file %s: %s\n", info->node_map_fname, strerror(errno));
            status = VB_GENERIC_ERROR;
            goto out_node_map_open;
        }

        size = info->num_nodes * (
            sizeof(vb_node_info_t) + 
            (sizeof(unsigned int) * info->num_local_instances)
        );

        status = ftruncate(info->node_map_fd, size);
        if (status != 0) {
            vb_error("Could not truncate file %s: %s\n", info->node_map_fname, strerror(errno));
            status = VB_GENERIC_ERROR;
            goto out_node_map_truncate;
        }

        local_map = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, 
            info->node_map_fd, 0);
        if (local_map == MAP_FAILED) {
            vb_error("Could not mmap file %s: %s\n", info->node_map_fname, strerror(errno));
            status = VB_GENERIC_ERROR;
            goto out_node_map_mmap;
        }

        memcpy(local_map, info->node_map[0], size);
        munmap(local_map, size);
    }

    /* Export rank map */
    {
        info->rank_map_fd = open(info->rank_map_fname, O_CREAT | O_TRUNC | O_RDWR, S_IRUSR | S_IWUSR);
        if (info->rank_map_fd == -1) {
            vb_error("Could not create file %s: %s\n", info->rank_map_fname, strerror(errno));
            status = VB_GENERIC_ERROR;
            goto out_rank_map_open;
        }

        size = sizeof(vb_rank_id_t) * info->num_instances;

        status = ftruncate(info->rank_map_fd, size);
        if (status != 0) {
            vb_error("Could not truncate file %s: %s\n", info->rank_map_fname, strerror(errno));
            status = VB_GENERIC_ERROR;
            goto out_rank_map_truncate;
        }

        local_map = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED,
            info->rank_map_fd, 0);
        if (local_map == MAP_FAILED) {
            vb_error("Could not mmap file %s: %s\n", info->rank_map_fname, strerror(errno));
            status = VB_GENERIC_ERROR;
            goto out_rank_map_mmap;
        }

        memcpy(local_map, info->rank_map, size);
        munmap(local_map, size);
    }

    status = VB_SUCCESS;

out_rank_map_mmap:
out_rank_map_truncate:
    close(info->rank_map_fd);
    if (status != VB_SUCCESS)
        unlink(info->rank_map_fname);

out_rank_map_open:
out_node_map_mmap:
out_node_map_truncate:
    close(info->node_map_fd);
    if (status != VB_SUCCESS)
        unlink(info->node_map_fname);

out_node_map_open:
    return status;
}

int 
vb_attach_local_root_maps(vb_rank_info_t * info)
{
    unsigned int i;
    int status;
    size_t node_size, rank_size;

    /* Allocate the outer double pointers */
    info->node_map = malloc(sizeof(vb_node_info_t *) *
        info->num_nodes);
    if (info->node_map == NULL) {
        vb_error("Out of memory\n");
        return VB_GENERIC_ERROR;
    }

    /* Attach node map */
    {
        do {
            info->node_map_fd = open(info->node_map_fname, O_RDONLY, S_IRUSR);
            if ((info->node_map_fd == -1) && (errno != ENOENT)) {
                vb_error("Could not open() file %s: %s\n", info->node_map_fname, strerror(errno));
                status = VB_GENERIC_ERROR;
                goto out_node_map_open;
            }
        } while (info->node_map_fd == -1);

        node_size = info->num_nodes * (
            sizeof(vb_node_info_t) + 
            (sizeof(unsigned int) * info->num_local_instances)
        );


        info->node_map[0] = mmap(NULL, node_size, PROT_READ, MAP_SHARED | MAP_POPULATE, 
            info->node_map_fd, 0);
        if (info->node_map[0] == MAP_FAILED) {
            vb_error("Could not mmap file %s: %s\n", info->node_map_fname, strerror(errno));
            status = VB_GENERIC_ERROR;
            goto out_node_map_mmap;
        }

        /* Update the double pointers */
        for (i = 0; i < info->num_nodes; i++) {
            info->node_map[i] = (vb_node_info_t *)(
                (uintptr_t)info->node_map[0] + 
                (i * (sizeof(vb_node_info_t) + (sizeof(unsigned int) * info->num_local_instances)))
            );
        }
    }

    /* Attach rank map */
    {
        do {
            info->rank_map_fd = open(info->rank_map_fname, O_RDONLY, S_IRUSR);
            if ((info->rank_map_fd == -1) && (errno != ENOENT)) {
                vb_error("Could not open() file %s: %s\n", info->rank_map_fname, strerror(errno));
                status = VB_GENERIC_ERROR;
                goto out_rank_map_open;
            }
        } while (info->rank_map_fd == -1);

        rank_size = sizeof(vb_rank_id_t) * info->num_instances;

        info->rank_map = mmap(NULL, rank_size, PROT_READ, MAP_SHARED | MAP_POPULATE, 
            info->rank_map_fd, 0);
        if (info->rank_map == MAP_FAILED) {
            vb_error("Could not mmap file %s: %s\n", info->rank_map_fname, strerror(errno));
            status = VB_GENERIC_ERROR;
            goto out_rank_map_mmap;
        }
    }

    status = VB_SUCCESS;

out_rank_map_mmap:
    close(info->rank_map_fd);

out_rank_map_open:
    if (status != VB_SUCCESS)
        munmap(info->node_map[0], node_size);

out_node_map_mmap:
    close(info->node_map_fd);

out_node_map_open:
    if (status != VB_SUCCESS)
        free(info->node_map);

    return status;
}

int
vb_build_rank_info(int           * argc,
                   char        *** argv,
                   vb_instance_t * instance)
{
    int status, i;
    MPI_Datatype discovery_type, map_type;
    char hostname[HOST_NAME_MAX];
    LIST_HEAD(node_map);

    vb_rank_info_t * info = &(instance->rank_info);
    unsigned int * node_id_list = NULL;

    MPI_Comm_rank(MPI_COMM_WORLD, &(info->global_id));
    MPI_Comm_size(MPI_COMM_WORLD, &(info->num_instances));

    gethostname(hostname, HOST_NAME_MAX);

    /* (1) Determine locally unique ID within local node.
     *    (1a) If we're the local root, also create a mapping of local
     *      node IDs to global IDs 
     *    (1b) If we're global root, also create the global rank map,
     *      determine number of nodes, and assign node ID to each
     *      global rank
     */
    if (info->global_id == 0) {
        node_id_list = malloc(sizeof(unsigned int) * info->num_instances);
        if (node_id_list == NULL) {
            vb_error("Out of memory\n");
            goto out_node_id;
        }
    }

    status = vb_build_system_topology(info, &(instance->options), hostname, &node_map, node_id_list);
    if (status != VB_SUCCESS) {
        vb_error("Could not build local node map\n");
        goto out_build_local;
    }

    /* (2) Root broadcasts number of nodes and scatters node IDs to every rank */
    {
        MPI_Bcast(&(info->num_nodes),
            1,
            MPI_UNSIGNED,
            0,
            MPI_COMM_WORLD
        );

        MPI_Scatter(node_id_list,
            1,
            MPI_UNSIGNED,
            &(info->node_id),
            1,
            MPI_UNSIGNED,
            0,
            MPI_COMM_WORLD
        );
    }

    /* (3) Build a couple new communicators: one to communicate among all
     * local root instances, and one to communicate amoung all local node
     * instances
     */
     MPI_Comm_split(
        MPI_COMM_WORLD,
        (info->local_id == 0) ? 1 : MPI_UNDEFINED,
        info->global_id,
        &(info->local_root_comm)
    );

     MPI_Comm_split(
        MPI_COMM_WORLD,
        info->node_id,
        info->global_id,
        &(info->local_node_comm)
    );

    /* (4) Distribute node_map and rank_map among all local roots */
    if (info->local_id == 0) {
        status = vb_exchange_local_root_maps(info, hostname, &node_map);
        if (status != VB_SUCCESS) {
            vb_error("Could not exchange local root maps\n");
            goto out_exchange_local;
        }
    }

    /* (5) Everything is built at this point - just export to all
     * local ranks
     */
    {
        if (info->local_id == 0) {
            char * fmt_str;

            /* Generate unique file names for the node and rank maps */
            vb_build_fmt_str(instance, NULL, &fmt_str);
            VB_NODE_MAP_FILE(fmt_str, info->node_map_fname, MAX_FNAME_LEN);
            VB_RANK_MAP_FILE(fmt_str, info->rank_map_fname, MAX_FNAME_LEN);
            free(fmt_str);

            /* Export them */
            status = vb_export_local_root_maps(info);

            /* Broadcast file names to all local ranks */
            MPI_Bcast(
                info->node_map_fname,
                MAX_FNAME_LEN,
                MPI_CHAR,
                0,
                info->local_node_comm
            );

            MPI_Bcast(
                info->rank_map_fname,
                MAX_FNAME_LEN,
                MPI_CHAR,
                0,
                info->local_node_comm
            );
        } else {
            /* Get fnames from local root */
            MPI_Bcast(
                info->node_map_fname,
                MAX_FNAME_LEN,
                MPI_CHAR,
                0,
                info->local_node_comm
            );

            MPI_Bcast(
                info->rank_map_fname,
                MAX_FNAME_LEN,
                MPI_CHAR,
                0,
                info->local_node_comm
            );

            /* Attach to the maps */
            status = vb_attach_local_root_maps(info);
        }

        /* Wait until everyone is here */
        MPI_Barrier(MPI_COMM_WORLD);
    }

    if (status != 0) {
        vb_error("Could not export/attach local root maps\n");
        goto out_export_attach;
    }

    status = VB_SUCCESS;

out_export_attach:
out_exchange_local:
    if (status != VB_SUCCESS) {
        MPI_Comm_free(&(info->local_root_comm));
        MPI_Comm_free(&(info->local_node_comm));
    }

    vb_delete_local_node_map(&node_map);
    
out_build_local:
    if (node_id_list)
        free(node_id_list);

out_node_id:
    if (status != VB_SUCCESS)
        MPI_Abort(MPI_COMM_WORLD, status);

    return status;
}

void
vb_deinit_rank_info(vb_instance_t * instance)
{
    vb_rank_info_t * info = &(instance->rank_info);
    size_t rank_size, node_size;

    if (info->local_id == 0)
        MPI_Comm_free(&(info->local_root_comm));

    MPI_Comm_free(&(info->local_node_comm));

    if (info->local_id != 0) {
        rank_size = sizeof(vb_rank_id_t) * info->num_instances;
        node_size = info->num_nodes * (
            sizeof(vb_node_info_t) + 
            (sizeof(unsigned int) * info->num_local_instances)
        );

        munmap(info->node_map[0], node_size);
        munmap(info->rank_map, rank_size);
    } else {
        free(info->node_map[0]);
        free(info->rank_map);

        /* Unlink the map files */
        unlink(info->node_map_fname);
        unlink(info->rank_map_fname);
    }

    free(info->node_map);
}
