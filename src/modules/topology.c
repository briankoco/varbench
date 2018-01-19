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


#include <sched.h>

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <numa.h>

#include <varbench.h>
#include <cJSON.h>

#define MIN(x,y) (x < y) ? (x) : (y)

typedef enum {
    VB_HW_TOPO_NONE,
    VB_HW_TOPO_HWLOC,
    VB_HW_TOPO_JSON
} vb_hw_topo_mode_t;

typedef struct varbench_processor {
    unsigned int socket_id;
    unsigned int core_id;
    unsigned int hw_thread_id;
} vb_processor_t;

typedef struct varbench_hw_topology {
    /* Mode: hwloc or json */
    vb_hw_topo_mode_t topo_mode;

    /* Only one can be active */
    union {
        hwloc_topology_t hwloc;
        FILE           * json;
    };

    /* This is created by the global root process and broadcast to all other ranks */
    /* Array enumerating physical processors, indexed by logical OS core */
    unsigned int     nr_processors;
    vb_processor_t * processor_map;
} vb_hw_topology_t;

static void
dump_processor_map(vb_hw_topology_t * hw_topology)
{
    int i;

    vb_print("%u processors\n", hw_topology->nr_processors);
    for (i = 0; i < hw_topology->nr_processors; i++) {
        vb_processor_t * proc = &(hw_topology->processor_map[i]);

        vb_print("%d : %u, %u, %u\n",
            i, proc->socket_id, proc->core_id, proc->hw_thread_id
        );
    }
}

/* If there are no inherited pinnings, pin each instance starting sequentially */
int
vb_get_processor_for_instance(vb_instance_t * instance,
                              int             local_id,
                              char            map_by[3],
                              unsigned int  * socket,
                              unsigned int  * logical_core,
                              unsigned int  * hw_thread)
{
    vb_options_t     * options     = &(instance->options);
    vb_topology_t    * topology    = &(instance->topology);
    vb_hw_topology_t * hw_topology = topology->hw_topology;

    unsigned int low_val             = 0;
    unsigned int mid_val             = 0;
    unsigned int sockets_per_node    = topology->sockets_per_node;
    unsigned int cores_per_socket    = topology->cores_per_socket;
    unsigned int hw_threads_per_core = topology->hw_threads_per_core;


    /* There are 2 approaches here.
     *  (1) If the user gave an explicit CPU list, what that translates
     *      to is local_rank i should select the ith CPU in the list as
     *      its OS core. 
     *      
     *  (2) The user gave no such list. In this case, we simply offset 
     *      into the socket/core/hw_thread topology based onour local rank
     */

    if (options->cpu_array_len > 0) {

        vb_processor_t * processor;
        unsigned int     os_core;

        if (local_id > options->cpu_array_len) {
            vb_error("Could not determine processor: user-specified cpu list too short\n");
            return VB_GENERIC_ERROR;
        }

        os_core   = options->cpu_array[local_id];
        processor = &(hw_topology->processor_map[os_core]);

        *socket       = processor->socket_id;
        *logical_core = processor->core_id;
        *hw_thread    = processor->hw_thread_id;

        return VB_SUCCESS;
    }


    switch (map_by[0]) {
        case 's':
            low_val = sockets_per_node;
            *socket = local_id % sockets_per_node;
            break;

        case 'c':
            low_val       = cores_per_socket;
            *logical_core = local_id % cores_per_socket;
            break;

        case 'h':
            low_val    = hw_threads_per_core;
            *hw_thread = local_id % hw_threads_per_core;
            break;

        default:
            vb_error_root("Invalid mapping strategy: %s\n", options->map_by);
            return VB_GENERIC_ERROR;
    }

    switch (map_by[1]) {
        case 's':
            mid_val = sockets_per_node;
            *socket = (local_id / low_val) % sockets_per_node;
            break;

        case 'c':
            mid_val       = cores_per_socket;
            *logical_core = (local_id / low_val) % cores_per_socket;
            break;

        case 'h':
            mid_val    = hw_threads_per_core;
            *hw_thread = (local_id / low_val) % hw_threads_per_core;
            break;

        default:
            vb_error_root("Invalid mapping strategy: %s\n", options->map_by);
            return VB_GENERIC_ERROR;
    }

    switch (map_by[2]) {
        case 's':
            *socket = local_id / (low_val * mid_val);
            break;

        case 'c':
            *logical_core = local_id / (low_val * mid_val);
            break;

        case 'h':
            *hw_thread = local_id / (low_val * mid_val);
            break;

        default:
            vb_error_root("Invalid mapping strategy: %s\n", options->map_by);
            return VB_GENERIC_ERROR;
    }
    
    if ((*socket >= sockets_per_node) ||
        (*logical_core >= cores_per_socket) ||
        (*hw_thread >= hw_threads_per_core))
    {
        vb_error("Local id %d maps to invalid processor (s:%u,c:%u,h:%u). Overcommitting ...\n",
            local_id,
            *socket, *logical_core, *hw_thread
        );

        *socket       %= sockets_per_node;
        *logical_core %= cores_per_socket;
        *hw_thread    %= hw_threads_per_core;
    }

    return VB_SUCCESS;
}

static int
vb_find_os_core_hwloc(vb_instance_t * instance,
                      unsigned int    socket_id,
                      unsigned int    logical_core,
                      unsigned int    hw_thread,
                      unsigned int  * os_core_p)
{
    vb_topology_t    * topology    = &(instance->topology);
    vb_hw_topology_t * hw_topology = topology->hw_topology;

    unsigned int sockets_per_node    = topology->sockets_per_node;
    unsigned int cores_per_socket    = topology->cores_per_socket;
    unsigned int hw_threads_per_core = topology->hw_threads_per_core;

    unsigned int i, os_core, logical_index, depth, nr_hw_threads;
    unsigned int socket_off, core_off, hw_thread_off;

    hwloc_obj_t obj, pu_obj, core_obj, sock_obj;
    hwloc_obj_t last_sock_obj = NULL;
    hwloc_obj_t last_core_obj = NULL;

    hwloc_topology_t * hwloc = &(hw_topology->hwloc);

    depth = hwloc_get_type_depth(*hwloc, HWLOC_OBJ_PU);
    if (depth == HWLOC_TYPE_DEPTH_UNKNOWN) {
        vb_error("*** The number of PUs is unknown\n");
        return VB_GENERIC_ERROR;
    }
    nr_hw_threads = hwloc_get_nbobjs_by_depth(*hwloc, depth);

    /* Start at the PU level. Walk across horizontally until we find
     * the correct core/socket parents for the given PU 
     */
    socket_off = core_off = hw_thread_off = 0;
    for (i = 0; i < nr_hw_threads; i++) {
        pu_obj = hwloc_get_obj_by_type(*hwloc, HWLOC_OBJ_PU, i);

        /*
        { 
            extern vb_instance_t this_instance; 
            if (this_instance.rank_info.global_id == 255) {
                vb_print("pu %d: sock=%d, core=%d, hw_thread=%d\n",
                    i, socket_off, core_off, hw_thread_off);
            }
        }
        */

        /* find core/socket parent */
        core_obj = sock_obj = NULL;
        for (obj = pu_obj->parent; obj; obj = obj->parent) {
            if (obj->type == HWLOC_OBJ_CORE) {
                core_obj = obj;
            } else if (obj->type == HWLOC_OBJ_NUMANODE || obj->type == HWLOC_OBJ_PACKAGE) {
                sock_obj = obj;
                break;
            }
        }

        assert(sock_obj);
        assert(core_obj);

        if (last_sock_obj && last_sock_obj != sock_obj) {
            socket_off++;
            core_off = 0;
            hw_thread_off = 0;
        } else if (last_core_obj && last_core_obj != core_obj) {
            core_off++;
            hw_thread_off = 0;
        } else if (i != 0) {
            hw_thread_off++;
        } 

        if ((socket_off == socket_id) &&
           (core_off == logical_core) &&
           (hw_thread_off == hw_thread))
        {
            *os_core_p = pu_obj->os_index; 
            return VB_SUCCESS;
        }
 
        last_sock_obj = sock_obj;
        last_core_obj = core_obj;
    }

    return VB_GENERIC_ERROR;
}

int
vb_get_processor_os_core(vb_instance_t * instance,
                         unsigned int    socket_id,
                         unsigned int    logical_core,
                         unsigned int    hw_thread,
                         unsigned int  * os_core_p)
{
    vb_topology_t    * topology    = &(instance->topology);
    vb_hw_topology_t * hw_topology = topology->hw_topology;
    vb_processor_t   * processor   = NULL;
    unsigned int       os_core;

    if (hw_topology == NULL) {
        vb_error("Cannot query HW topology: enumeration failed\n");
        return VB_GENERIC_ERROR;
    }

    if (socket_id > topology->sockets_per_node)
        return VB_GENERIC_ERROR;

    if (logical_core > topology->cores_per_socket)
        return VB_GENERIC_ERROR;

    if (hw_thread > topology->hw_threads_per_core)
        return VB_GENERIC_ERROR;

    for (os_core = 0; os_core < hw_topology->nr_processors; os_core++) {
        processor = &(hw_topology->processor_map[os_core]);

        if ((processor->socket_id    == socket_id) &&
            (processor->core_id      == logical_core) &&
            (processor->hw_thread_id == hw_thread))
        {
            *os_core_p = os_core;
            return VB_SUCCESS;
        }
    }

    return VB_GENERIC_ERROR;
}

static int
vb_bind_processor_numa(vb_instance_t * instance,
                       unsigned int    os_core)
{
    vb_topology_t  * topology = &(instance->topology);
    struct bitmask * bitmask  = NULL;
    int status;

    bitmask = numa_allocate_cpumask();
    if (bitmask == NULL) {
        vb_error("Could not allocate cpumask\n");
        return VB_GENERIC_ERROR;
    }

    bitmask = numa_bitmask_clearall(bitmask);
    bitmask = numa_bitmask_setbit(bitmask, os_core);

    status = numa_sched_setaffinity(getpid(), bitmask); 
    if (status != 0) {
        vb_error("Could not set numa_sched_setaffinity: %s\n", strerror(errno));
    }

    numa_free_cpumask(bitmask);
    
    if (status == 0)
        return VB_SUCCESS;
    else
        return VB_GENERIC_ERROR;
}

static int 
vb_bind_processor_hwloc(vb_instance_t * instance,
                        unsigned int    os_core)
{
    vb_topology_t    * topology    = &(instance->topology);
    vb_hw_topology_t * hw_topology = topology->hw_topology;
    hwloc_topology_t * hwloc       = &(hw_topology->hwloc);
    hwloc_cpuset_t cpuset;
    int status;

    assert(hw_topology->topo_mode == VB_HW_TOPO_HWLOC);

    /* Pin self to this processor */
    cpuset = hwloc_bitmap_alloc();
    hwloc_bitmap_set(cpuset, os_core);
    status = hwloc_set_cpubind(*hwloc, cpuset, 0);
    hwloc_bitmap_free(cpuset);

    if (status != 0) {
        vb_error("Could not bind to cpuset: %s\n", strerror(errno));
        return VB_GENERIC_ERROR;
    }
    
    return VB_SUCCESS;
}

static int
vb_bind_processor(vb_instance_t * instance)
{
    vb_topology_t * topology = &(instance->topology);
    vb_options_t  * options  = &(instance->options);
    unsigned int socket, core, hw_thread, os_core;
    int status;

    /* No pinning */
    if (instance->options.map_by[0] == 'n')
        return VB_SUCCESS;

    /* Determine which processor this instance should use
     * based on its local id and the mapping strategy
     */
    status = vb_get_processor_for_instance(
        instance,
        instance->rank_info.local_id,
        instance->options.map_by,
        &socket,
        &core,
        &hw_thread
    );
    if (status != VB_SUCCESS) {
        vb_error("Could not determine processor for instance\n");
        return status;
    }

    /* Determine which OS core this maps to */
    status = vb_get_processor_os_core(
        instance,
        socket,
        core,
        hw_thread,
        &os_core
    );
    if (status != VB_SUCCESS) {
        vb_error("Could not determine OS core for processor\n");
        return status;
    }

    /* Bind it */
    if (topology->hw_topology->topo_mode == VB_HW_TOPO_JSON)
        status = vb_bind_processor_numa(instance, os_core);
    else
        status = vb_bind_processor_hwloc(instance, os_core);

    if (status != VB_SUCCESS)
        return status;

    /* We're pinned */
    topology->os_core_id   = os_core;
    topology->socket_id    = socket;
    topology->core_id      = core;
    topology->hw_thread_id = hw_thread;

    return VB_SUCCESS;
}

static int
vb_bind_memory_numa(vb_instance_t * instance,
                    int             os_cpu)
{
    vb_options_t     * options     = &(instance->options);
    vb_topology_t    * topology    = &(instance->topology);
    vb_hw_topology_t * hw_topology = topology->hw_topology;
    struct bitmask   * bitmask     = NULL;

    int status = 0, numa_node;
    unsigned int nr_numa_zones, i;

    bitmask = numa_allocate_nodemask();
    if (bitmask == NULL) {
        vb_error("Could not allocate nodemask\n");
        return VB_GENERIC_ERROR;
    }
    bitmask = numa_bitmask_clearall(bitmask);

    switch (options->mem_affinity) {
        case 'l':
            if (os_cpu == -1) {
                vb_error_root("Cannot apply local memory binding without a processor binding\n");
                goto err;
            }

            numa_node = numa_node_of_cpu(os_cpu);
            if (numa_node == -1) {
                vb_error("Cannot find numa zone for cpu %d: :%s. Cannot bind to local memory\n",
                    os_cpu, strerror(errno));
                goto err;
            }

            bitmask = numa_bitmask_setbit(bitmask, numa_node);
            numa_set_membind(bitmask);

            break;

        case 'i': 
            nr_numa_zones = numa_num_configured_nodes();

            for (i = 0; i < nr_numa_zones; i++) {
                bitmask = numa_bitmask_setbit(bitmask, i);
            }

            numa_set_interleave_mask(bitmask);
            break;

        case 'n':
            break;

        default:
            vb_error_root("Invalid memory affinity: %c\n", options->mem_affinity);
            goto err;
    }

out:
    numa_free_nodemask(bitmask);

    if (status == 0)
        return VB_SUCCESS;
    else
        return VB_GENERIC_ERROR;
err:
    status = VB_GENERIC_ERROR;
    goto out;
}

static int
vb_bind_memory_hwloc(vb_instance_t * instance,
                     int             os_cpu)
{
    vb_options_t     * options     = &(instance->options);
    vb_topology_t    * topology    = &(instance->topology);
    vb_hw_topology_t * hw_topology = topology->hw_topology;
    hwloc_topology_t * hwloc       = &(hw_topology->hwloc);

    assert(hw_topology->topo_mode == VB_HW_TOPO_HWLOC);

    hwloc_obj_t obj;
    hwloc_nodeset_t nodeset;
    hwloc_membind_policy_t policy;
    int depth, status;
    unsigned int nr_numa_zones, i;

    /* Determine number of numa nodes */
    nodeset = hwloc_bitmap_alloc();
    nr_numa_zones = hwloc_get_nbobjs_by_type(*hwloc, HWLOC_OBJ_NUMANODE);

    /* Nothing to do with only one numa socket */
    if (nr_numa_zones <= 1)
        return VB_SUCCESS;

    switch (options->mem_affinity) {
        case 'l':
            if (os_cpu == -1) {
                vb_error_root("Cannot apply local memory binding without a processor binding\n");
                return VB_GENERIC_ERROR;
            }

            /* Figure out which numa zone covers this cpu */
            for (i = 0; i < nr_numa_zones; i++) {
                obj = hwloc_get_obj_by_type(*hwloc, HWLOC_OBJ_NUMANODE, i);

                if (hwloc_bitmap_isset(obj->cpuset, os_cpu)) {
                    vb_debug("Binding memory for instance (cpu=%d, numa zone=%d)\n",
                        os_cpu, i);

                    hwloc_bitmap_set(nodeset, i);
                    break;
                }
            }

            if (i == nr_numa_zones) {
                vb_error("Cannot find numa zone for cpu %d: cannot bind to local memory\n",
                    os_cpu);
                return VB_GENERIC_ERROR;
            }

            policy = HWLOC_MEMBIND_BIND;
            break;

        case 'i': 
            for (i = 0; i < nr_numa_zones; i++) {
                hwloc_bitmap_set(nodeset, i);
            }

            {
                char * str;

                hwloc_bitmap_asprintf(&str, nodeset);

                vb_debug("Interleaved binding memory for instance (NUMAset=%s)\n",
                    str);
                free(str);
            }

            policy = HWLOC_MEMBIND_INTERLEAVE;
            break;

        case 'n':
            hwloc_bitmap_free(nodeset);
            return VB_SUCCESS;

        default:
            vb_error_root("Invalid memory affinity: %c\n", options->mem_affinity);
            hwloc_bitmap_free(nodeset);
            return VB_GENERIC_ERROR;
    }

    status = hwloc_set_membind_nodeset(
        *hwloc,
        nodeset,
        policy,
        HWLOC_MEMBIND_STRICT | HWLOC_MEMBIND_NOCPUBIND
    );

    hwloc_bitmap_free(nodeset);

    if (status != 0) {
        vb_error("Could not bind to memory zones: %s\n", strerror(errno));
        return VB_GENERIC_ERROR;
    }

    return VB_SUCCESS;
}

static int
vb_bind_memory(vb_instance_t * instance)
{
    vb_topology_t    * topology    = &(instance->topology);
    vb_hw_topology_t * hw_topology = topology->hw_topology;

    if (hw_topology->topo_mode == VB_HW_TOPO_JSON)
        return vb_bind_memory_numa(instance, topology->os_core_id);
    else
        return vb_bind_memory_hwloc(instance, topology->os_core_id);
}

static int
vb_broadcast_topology(vb_instance_t * instance)
{
    vb_topology_t    * topology    = &(instance->topology);
    vb_hw_topology_t * hw_topology = topology->hw_topology; 
    vb_rank_info_t   * rank_info   = &(instance->rank_info);

    MPI_Datatype processor_map_dt;
    MPI_Comm     comm;

    /* If this is JSON mode, broadcast to everyone - only root parses the JSON */
    /* If it's hwloc mode, all local roots broadcast to their local nodes */
    if (hw_topology->topo_mode == VB_HW_TOPO_JSON) {
        comm = MPI_COMM_WORLD;
    } else {
        comm = rank_info->local_node_comm;
    }

    /* Advertise the processor mappings */
    MPI_Bcast(&(topology->sockets_per_node), 1, MPI_UNSIGNED, 0, comm);
    MPI_Bcast(&(topology->cores_per_socket), 1, MPI_UNSIGNED, 0, comm);
    MPI_Bcast(&(topology->hw_threads_per_core), 1, MPI_UNSIGNED, 0, comm);
    MPI_Bcast(&(hw_topology->nr_processors), 1, MPI_UNSIGNED, 0, comm);
    
    /* Everyone but the broadcaster allocates space for the array */
    if ( 
            (
                (hw_topology->topo_mode == VB_HW_TOPO_JSON) &&
                (rank_info->global_id   != 0)
            ) 
       || 
            (
                (hw_topology->topo_mode == VB_HW_TOPO_HWLOC) &&
                (rank_info->local_id    != 0)
            )
       )
    {
        hw_topology->processor_map = malloc(sizeof(vb_processor_t) * hw_topology->nr_processors);
        if (hw_topology->processor_map == NULL) {
            vb_error("Out of memory\n");
            return VB_GENERIC_ERROR;
        }
    }
         
    /* Build a new MPI datatype for this transfer */
    {
        int count = 3;
        int array_of_blocklengths[] = {
            1,
            1,
            1
        };
        MPI_Aint array_of_displacements[] = {
            offsetof(vb_processor_t, socket_id),
            offsetof(vb_processor_t, core_id),
            offsetof(vb_processor_t, hw_thread_id)
        };
        MPI_Datatype array_of_types[] = {
            MPI_UNSIGNED,
            MPI_UNSIGNED,
            MPI_UNSIGNED
        };

        MPI_Type_create_struct(
            count,
            array_of_blocklengths,
            array_of_displacements,
            array_of_types,
            &processor_map_dt
        );
        MPI_Type_commit(&processor_map_dt);
    }

    /* Broadcast the processor map */
    MPI_Bcast(
        hw_topology->processor_map,
        hw_topology->nr_processors,
        processor_map_dt,
        0,
        comm
    );

    return VB_SUCCESS; 
}

static int
vb_parse_json(vb_instance_t * instance,
              char          * json_file,
              cJSON         * json_obj)
{
    vb_topology_t    * topology      = &(instance->topology); 
    vb_hw_topology_t * hw_topology   = topology->hw_topology;
    hwloc_topology_t * hwloc         = &(hw_topology->hwloc);
    vb_rank_info_t   * rank_info     = &(instance->rank_info);
    vb_processor_t   * processor_map = NULL;

    int nr_processors, nr_sockets, cores_per_socket, hw_threads_per_core, os_core, i, array_size,
        socket, core, hw_thread;

    cJSON * processor_info, * processor_list, * entry, * p_entry;

    processor_info = cJSON_GetObjectItemCaseSensitive(json_obj, "processor_info");
    if (!cJSON_IsObject(processor_info)) {
        vb_error("Invalid format in %s: no processor_info dictionary\n", json_file);
        goto out;
    }

    /* Sanity check processor info */
    {
        entry = cJSON_GetObjectItemCaseSensitive(processor_info, "num_processors");
        if (!cJSON_IsNumber(entry)) {
            vb_error("Invalid format in %s: no num_processors entry in processor_info object\n", json_file);
            goto out;
        }
        nr_processors = entry->valueint;

        entry = cJSON_GetObjectItemCaseSensitive(processor_info, "num_sockets");
        if (!cJSON_IsNumber(entry)) {
            vb_error("Invalid format in %s: no num_sockets entry in processor_info object\n", json_file);
            goto out;
        }
        nr_sockets = entry->valueint;

        entry = cJSON_GetObjectItemCaseSensitive(processor_info, "cores_per_socket");
        if (!cJSON_IsNumber(entry)) {
            vb_error("Invalid format in %s: no cores_per_socket entry in processor_info object\n", json_file);
            goto out;
        }
        cores_per_socket = entry->valueint;

        entry = cJSON_GetObjectItemCaseSensitive(processor_info, "hw_threads_per_core");
        if (!cJSON_IsNumber(entry)) {
            vb_error("Invalid format in %s: no hw_threads_per_core entry in processor_info object\n", json_file);
            goto out;
        }
        hw_threads_per_core = entry->valueint;

        if (nr_processors != (nr_sockets * cores_per_socket * hw_threads_per_core)) {
            vb_error("Invalid data in %s: num_processors does not equal (num_sockets * cores_per_socket * hw_threads_per_core)\n", json_file);
            goto out;
        }
    }

    processor_list = cJSON_GetObjectItemCaseSensitive(json_obj, "processor_list");
    if (!cJSON_IsArray(processor_list)) {
        vb_error("Invalid format in %s: no processor_list\n", json_file);
        goto out;
    }

    array_size = cJSON_GetArraySize(processor_list);
    processor_map = malloc(sizeof(vb_processor_t) * nr_processors);
    if (processor_map == NULL) {
        vb_error("Out of memory\n");
        goto out;
    }

    if (array_size != nr_processors) {
        vb_error("Invalid format in %s: %d processors declared, but %d processor entries\n",
            json_file, nr_processors, array_size);
        goto out_free;
    }

    for (i = 0; i < nr_processors; i++) {
        vb_processor_t * processor = NULL;

        p_entry = cJSON_GetArrayItem(processor_list, i);
        if (!cJSON_IsObject(p_entry)) {
            vb_error("Invalid format in %s: malformed entry in processor_list\n", json_file);
            goto out_free;
        }

        entry = cJSON_GetObjectItemCaseSensitive(p_entry, "os_core");
        if (!cJSON_IsNumber(entry)) {
            vb_error("Invalid format in %s: processor entry missing os_core\n", json_file);
            goto out_free;
        }
        os_core = entry->valueint;

        entry = cJSON_GetObjectItemCaseSensitive(p_entry, "socket");
        if (!cJSON_IsNumber(entry)) {
            vb_error("Invalid format in %s: processor entry missing socket\n", json_file);
            goto out_free;
        }
        socket = entry->valueint;

        entry = cJSON_GetObjectItemCaseSensitive(p_entry, "core");
        if (!cJSON_IsNumber(entry)) {
            vb_error("Invalid format in %s: processor entry missing core\n", json_file);
            goto out_free;
        }
        core = entry->valueint;

        entry = cJSON_GetObjectItemCaseSensitive(p_entry, "hw_thread");
        if (!cJSON_IsNumber(entry)) {
            vb_error("Invalid format in %s: processor entry missing hw_thread\n", json_file);
            goto out_free;
        }
        hw_thread = entry->valueint;

        /* Some sanity checking */
        if ((socket    < 0 || socket > nr_sockets) ||
            (core      < 0 || core > cores_per_socket) ||
            (hw_thread < 0 || hw_thread > hw_threads_per_core) ||
            (os_core   < 0 || os_core > nr_processors))
        {
            vb_error("Invalid data in %s: some value in processor entry is out of range\n", json_file);
            goto out_free;
        }

        /* Ok */
        processor = &(processor_map[os_core]);

        processor->socket_id    = socket;
        processor->core_id      = core;
        processor->hw_thread_id = hw_thread;
    }

    /* Success */
    topology->sockets_per_node    = nr_sockets;
    topology->cores_per_socket    = cores_per_socket;
    topology->hw_threads_per_core = hw_threads_per_core;

    hw_topology->nr_processors = nr_processors;
    hw_topology->processor_map = processor_map;

    return VB_SUCCESS;

out_free:
    free(processor_map);
out:
    return VB_GENERIC_ERROR;
}


static int
vb_build_and_scatter_json_topology(vb_instance_t * instance,
                                   char          * json_file)
{
    vb_topology_t  * topology  = &(instance->topology); 
    vb_rank_info_t * rank_info = &(instance->rank_info);
    cJSON          * json_obj  = NULL;
    int              status;

    /* Root gets the json info */
    if (rank_info->global_id == 0) {
        json_obj = cJSON_GetObjectFromFile(json_file, NULL, NULL);
        if (json_obj == NULL) {
            vb_error("Could not read JSON object from file: %s\n", json_file);
            return VB_GENERIC_ERROR;
        }

        status = vb_parse_json(instance, json_file, json_obj);
        cJSON_Delete(json_obj);

        if (status != VB_SUCCESS) {
            vb_error("Could not parse topology from JSON object\n");
            return status;
        }
    }

    /* Broadcast the topology to all other ranks */
    status = vb_broadcast_topology(instance);
    if (status != VB_SUCCESS) {
        vb_error("Could not broadcast JSON topology to other instances\n");
        if (rank_info->global_id == 0)
            free(topology->hw_topology->processor_map);
    }

    return status;
}

/* Use hwloc to fill out the hw_topo */
static int
vb_build_hw_topo_hwloc(vb_instance_t * instance)
{
    vb_topology_t    * topology    = &(instance->topology);
    vb_hw_topology_t * hw_topology = topology->hw_topology;
    hwloc_topology_t * hwloc       = &(hw_topology->hwloc);

    unsigned int nr_sockets, nr_empty_sockets, nr_cores, nr_empty_cores,
        nr_hw_threads, cores_per_socket, hw_threads_per_core, i;
    unsigned int * child_count;
    int status, depth;
    bool numa;
    hwloc_obj_t obj, sock_obj, core_obj, pu_obj;
    hwloc_obj_t * cores, * sockets;

    /* Get number of machine sockets, cores, and hw threads */
    depth = hwloc_get_type_depth(*hwloc, HWLOC_OBJ_NUMANODE);
    if (depth == HWLOC_TYPE_DEPTH_UNKNOWN) {
        vb_error("*** The number of numa_nodes is unknown. Looking up packages\n");

        depth = hwloc_get_type_depth(*hwloc, HWLOC_OBJ_PACKAGE);
        if (depth == HWLOC_TYPE_DEPTH_UNKNOWN) {
            vb_error("*** The number of sockets is unknown\n");
            return VB_GENERIC_ERROR;
        }

        numa = false;
    } else {
        numa = true;
    }
    nr_sockets = hwloc_get_nbobjs_by_depth(*hwloc, depth);

    depth = hwloc_get_type_depth(*hwloc, HWLOC_OBJ_CORE);
    if (depth == HWLOC_TYPE_DEPTH_UNKNOWN) {
        vb_error("*** The number of cores is unknown\n");
        return VB_GENERIC_ERROR;
    }
    nr_cores = hwloc_get_nbobjs_by_depth(*hwloc, depth);

    depth = hwloc_get_type_depth(*hwloc, HWLOC_OBJ_PU);
    if (depth == HWLOC_TYPE_DEPTH_UNKNOWN) {
        vb_error("*** The number of PUs is unknown\n");
        return VB_GENERIC_ERROR;
    }
    nr_hw_threads = hwloc_get_nbobjs_by_depth(*hwloc, depth);

    child_count = malloc(sizeof(unsigned int) * nr_sockets);
    assert(child_count);
    memset(child_count, 0, sizeof(unsigned int) * nr_sockets);

    /* Determine number of cores per socket */
    for (i = 0; i < nr_cores; i++) {
        core_obj = hwloc_get_obj_by_type(*hwloc, HWLOC_OBJ_CORE, i);

        for (sock_obj = core_obj->parent; 
             sock_obj && ((numa && sock_obj->type != HWLOC_OBJ_NUMANODE) ||
                         (!numa && sock_obj->type != HWLOC_OBJ_PACKAGE));
            sock_obj = sock_obj->parent);

        if (!sock_obj) {
            vb_error("Could not find parent socket for core %d\n", i);
            free(child_count);
            return VB_GENERIC_ERROR;
        }

        assert(sock_obj->logical_index < nr_sockets);
        child_count[sock_obj->logical_index]++;
    }

    /* Determine how many sockets actually have cores */
    cores_per_socket = nr_cores;
    nr_empty_sockets = 0;
    for (i = 0; i < nr_sockets; i++) {
        if (child_count[i] == 0) {
            vb_debug_root("Socket %d has no cores - assuming this is a"
                " \"memory only\" socket\n", i);
            nr_empty_sockets++;
        } else {
            cores_per_socket = MIN(child_count[i], cores_per_socket);
        }
    }

    if (cores_per_socket != (nr_cores / nr_sockets)) {
        vb_debug_root("Sockets on this machine are non-uniform."
            " Setting a hard max of %u cores per socket\n",
            cores_per_socket);
    }

    if (nr_empty_sockets > 0) {
        vb_debug_root("%d sockets have no cores\n", nr_empty_sockets);
    }

    /* Do not attempt to pin things to empty sockets */
    nr_sockets -= nr_empty_sockets;

    free(child_count);
    child_count = malloc(sizeof(unsigned int) * nr_cores);
    assert(child_count);
    memset(child_count, 0, sizeof(unsigned int) * nr_cores);

    /* Determine number of HW threads per core */
    for (i = 0; i < nr_hw_threads; i++) {
        pu_obj = hwloc_get_obj_by_type(*hwloc, HWLOC_OBJ_PU, i);

        for (core_obj = pu_obj->parent; core_obj && core_obj->type != HWLOC_OBJ_CORE;
            core_obj = core_obj->parent);

        if (!core_obj) {
            vb_error("Could not find parent core for PU %d\n", i);
            free(child_count);
            return VB_GENERIC_ERROR;
        }

        assert(core_obj->logical_index < nr_cores);
        child_count[core_obj->logical_index]++;
    }

    /* Determine how many cores actually have HW threads */
    hw_threads_per_core = nr_hw_threads;
    nr_empty_cores      = 0;
    for (i = 0; i < nr_cores; i++) {
        if (child_count[i] == 0) {
            vb_debug_root("Core %d has no HW threads - wtf??\n", i);
            free(child_count);
            return VB_GENERIC_ERROR;
        } else {
            hw_threads_per_core = MIN(child_count[i], hw_threads_per_core);
        }
    }

    if (hw_threads_per_core != (nr_hw_threads / nr_cores)) {
        vb_print_root("Cores on this machine are non-uniform."
            " Setting a hard max of %u HW threads per core\n",
            hw_threads_per_core);
    }

    free(child_count);

    /* Debug once per node */
    if (instance->rank_info.local_id == 0) {
        vb_debug("Node topology: " \
            "Sockets: %d, " \
            "Cores per socket: %d, " \
            "HW threads per core: %d\n",
            nr_sockets, cores_per_socket, hw_threads_per_core);
    }

    /* OK, we've built the topology. Now, fill in the map */
    topology->sockets_per_node    = nr_sockets;
    topology->cores_per_socket    = cores_per_socket;
    topology->hw_threads_per_core = hw_threads_per_core;

    hw_topology->nr_processors =
            topology->sockets_per_node *
            topology->cores_per_socket *
            topology->hw_threads_per_core;

    hw_topology->processor_map = malloc(sizeof(vb_processor_t) * hw_topology->nr_processors);
    if (hw_topology->processor_map == NULL) {
        vb_error("Out of memory\n");
        return VB_GENERIC_ERROR;
    }

    /* Fill in the map for each os_core_id */
    {
        unsigned int socket, core, hw_thread, os_core;
        vb_processor_t * vb_processor;

        for (socket = 0; socket < nr_sockets; socket++) {
            for (core = 0; core < cores_per_socket; core++) {
                for (hw_thread = 0; hw_thread < hw_threads_per_core; hw_thread++) {
                    status = vb_find_os_core_hwloc(instance, socket, core, hw_thread, &os_core);
                    if (status != VB_SUCCESS) {
                        vb_error("Failed to determine OS core for socket/core/hw_thread combo\n");
                        free(hw_topology->processor_map);
                        return VB_GENERIC_ERROR;
                    }

                    assert(os_core < hw_topology->nr_processors);

                    vb_processor = &(hw_topology->processor_map[os_core]);
                    vb_processor->socket_id    = socket;
                    vb_processor->core_id      = core;
                    vb_processor->hw_thread_id = hw_thread;
                }
            }
        }
    }

    return VB_SUCCESS;
}

static int
vb_build_and_broadcast_hwloc_topology(vb_instance_t * instance)
{
    vb_topology_t    * topology    = &(instance->topology); 
    vb_hw_topology_t * hw_topology = topology->hw_topology;
    hwloc_topology_t * hwloc       = &(hw_topology->hwloc);
    vb_rank_info_t   * rank_info   = &(instance->rank_info);

    hwloc_topology_init(hwloc);
    hwloc_topology_load(*hwloc);

    int status = 0;
    int failed = 0;
    int global = 0;

    /* Each local root does determines local hw topology */
    if (rank_info->local_id == 0) {
        status = vb_build_hw_topo_hwloc(instance);
        if (status != VB_SUCCESS) {
            failed = 1;
        }
    }

    /* Determine whether this succeeded or not */
    MPI_Allreduce(&failed, &global, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
    if (global != 0) {
        return VB_GENERIC_ERROR;
    }
    
    /* Broadcast to all local node instances */
    status = vb_broadcast_topology(instance);
    if (status != VB_SUCCESS) {
        vb_error("Could not broadcast HWLOC topology to other local node instances\n");

        if (rank_info->local_id == 0) {
            free(hw_topology->processor_map);
        }
    }

    return status;
}

int 
vb_build_hw_topology(vb_instance_t * instance)
{
    vb_options_t     * options     = &(instance->options);
    vb_topology_t    * topology    = &(instance->topology);
    vb_hw_topology_t * hw_topology = NULL; 
    int status;

    hw_topology = malloc(sizeof(vb_hw_topology_t));
    if (hw_topology == NULL) {
        vb_error("Out of memory\n");
        return VB_GENERIC_ERROR;
    }
    topology->hw_topology = hw_topology;

    if (options->topo_file != NULL) {
        hw_topology->topo_mode = VB_HW_TOPO_JSON;
        status = vb_build_and_scatter_json_topology(instance, options->topo_file);
    } else {
        hw_topology->topo_mode = VB_HW_TOPO_HWLOC;
        status = vb_build_and_broadcast_hwloc_topology(instance);
    }

    if (status != VB_SUCCESS) {
        free(hw_topology);
        topology->hw_topology = NULL;
    }

    return status;
}

void
vb_destroy_hw_topology(vb_instance_t * instance)
{
    vb_topology_t    * topology    = &(instance->topology);
    vb_hw_topology_t * hw_topology = topology->hw_topology; 
    
    if (hw_topology->topo_mode != VB_HW_TOPO_NONE) {
        free(hw_topology->processor_map);
    }

    free(hw_topology);
}

/* Bind each instance a core or HW thread and a set of numa nodes */
int
vb_bind_instance(vb_instance_t * instance)
{
    vb_topology_t * topology = &(instance->topology);
    vb_options_t  * options  = &(instance->options);
    int             status   = 0;

    topology->os_core_id   = -1;
    topology->socket_id    = -1;
    topology->core_id      = -1;
    topology->hw_thread_id = -1;

    /* Bail if we don't have a valid topology */
    if (topology->hw_topology == NULL) {
        vb_error("Cannot bind process: topology not enumerated\n");
        return VB_GENERIC_ERROR;
    }

    /* Bind this instance to a processor */
    status = vb_bind_processor(instance);
    if (status != VB_SUCCESS) {
        vb_error_root("Could not bind instance to processor\n");
        return status;
    }

    /* Bind this instance to some memory */
    status = vb_bind_memory(instance);
    if (status != VB_SUCCESS) {
        vb_error_root("Could not bind instance to memory regionsy\n");
        return status;
    }  

    vb_debug("Local instance pinned to: " \
        "socket = %d, " \
        "phys core = %d, " \
        "hw thread = %d, " \
        "OS core = %d, " \
        "mem_policy = %s\n",
        topology->socket_id,
        topology->core_id,
        topology->hw_thread_id,
        topology->os_core_id,
        (instance->options.mem_affinity == 'l') ? "local" : 
            (instance->options.mem_affinity == 'i') ? "interleaved" : "none"
    );

    return VB_SUCCESS;
}
