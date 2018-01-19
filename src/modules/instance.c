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
#include <stdarg.h>
#include <limits.h>
#include <time.h>
#include <pthread.h>
#include <fcntl.h>
#include <sys/time.h>

#include <varbench.h>


#define MAX_NR_STRINGS 4096

#define CHECK_KERNEL_STR(i, s) \
    if (!strcmp(i->options.kernel_name, #s)) { \
        i->kernel.name = i->options.kernel_name; \
        i->kernel.fn = &vb_kernel_##s; \
        return VB_SUCCESS; \
    }

static int
vb_str_to_kernel(vb_instance_t * instance)
{
    CHECK_KERNEL_STR(instance, hello_world);
    CHECK_KERNEL_STR(instance, cache_coherence);
    CHECK_KERNEL_STR(instance, cache_capacity);
    CHECK_KERNEL_STR(instance, stream);
    CHECK_KERNEL_STR(instance, random_access);
    CHECK_KERNEL_STR(instance, alltoall);
    CHECK_KERNEL_STR(instance, random_neighbor);
    CHECK_KERNEL_STR(instance, nearest_neighbor);
    CHECK_KERNEL_STR(instance, io);
    CHECK_KERNEL_STR(instance, dgemm);
    CHECK_KERNEL_STR(instance, operating_system);

    return VB_BAD_ARGS;
}

static int
vb_perfctr_init(vb_instance_t * instance)
{
    return 0;
}

static char *
map_by_to_str(char map_by[3],
              int  idx)
{
    if (map_by[0] == 'n')
        return "none";

    switch (map_by[idx]) {
        case 's':
            return "socket";

        case 'c':
            return "core";

        default:
            assert(map_by[idx] == 'h');
            return "hw_thread";
    }
}

static char *
mem_affinity_to_str(char mem_affinity)
{
    switch (mem_affinity) {
        case 'l':
            return "local";

        case 'i':
            return "interleaved";

        default:
            assert(mem_affinity == 'n');
            return "none";
    }
}

void
vb_add_misc_meta_info(vb_instance_t * instance,
                      char          * tag,
                      char          * attrib,
                      int             nr_attributes,
                      char         ** names,
                      char         ** vals)
{
    int i;

    fprintf(instance->meta_xml.file, "\t\t<%s>\n", tag);
    {
        for (i = 0; i < nr_attributes; i++) {
            fprintf(instance->meta_xml.file, "\t\t\t<%s %s=\"%s\"/>\n",
                attrib, names[i], vals[i]
            );
        }
    }
    fprintf(instance->meta_xml.file, "\t\t</%s>\n", tag);
    fflush(instance->meta_xml.file);
}

static void 
vb_complete_meta_xml(vb_instance_t * instance)
{
    fprintf(instance->meta_xml.file, "\t</miscellaneous>\n");
    fprintf(instance->meta_xml.file, "</vb_metadata>\n");
    fflush(instance->meta_xml.file);
}

static int
vb_write_meta_xml(vb_instance_t * instance,
                  int             argc,
                  char         ** argv)
{
    vb_options_t   * options   = &(instance->options);
    vb_topology_t  * topology  = &(instance->topology);
    vb_rank_info_t * rank_info = &(instance->rank_info);
    int i, status;

    /* Print misc. information */
    {
        fprintf(instance->meta_xml.file, "<!-- Varbench 1.0 metadata file -->\n");
        fprintf(instance->meta_xml.file, "<vb_metadata>\n");

        /* general stuff */ 
        fprintf(instance->meta_xml.file, "\t<general>\n");
        {
            fprintf(instance->meta_xml.file, 
                "\t\t<data_file val=\"%s\"/>\n",
                instance->data_csv.name
            );

            fprintf(instance->meta_xml.file, 
                "\t\t<num_instances val=\"%d\"/>\n",
                rank_info->num_instances
            );

            fprintf(instance->meta_xml.file, 
                "\t\t<num_nodes val=\"%d\"/>\n",
                rank_info->num_nodes
            );

            fprintf(instance->meta_xml.file, 
                "\t\t<num_local_instances val=\"%d\"/>\n",
                rank_info->num_local_instances
            );
        }
        fprintf(instance->meta_xml.file, "\t</general>\n");

        /* options */
        fprintf(instance->meta_xml.file, "\n\t<options>\n");
        {
            fprintf(instance->meta_xml.file,
                "\t\t<option id=\"%s\" val=\"%s\"/>\n",
                "debugging", (options->debug_on) ? "on" : "off"
            );

            fprintf(instance->meta_xml.file,
                "\t\t<option id=\"%s\" val=\"%llu\"/>\n",
                "iterations", options->num_iterations
            );

            fprintf(instance->meta_xml.file,
                "\t\t<option id=\"%s\" val=\"%s\"/>\n",
                "memory_affinity", mem_affinity_to_str(options->mem_affinity)
            );

            fprintf(instance->meta_xml.file,
                "\t\t<option id=\"%s\" val=\"%s:%s:%s\"/>\n",
                "processor_pinning",
                map_by_to_str(options->map_by, 0),
                map_by_to_str(options->map_by, 1),
                map_by_to_str(options->map_by, 2)
            );

            fprintf(instance->meta_xml.file,
                "\t\t<option id=\"%s\" val=\"%d\"/>\n",
                "turbo_pin", options->turbo_pin
            );
        }
        fprintf(instance->meta_xml.file, "\t</options>\n");

        /* Kernel arguments */
        fprintf(instance->meta_xml.file, "\n\t<kernel name=\"%s\" argc=\"%u\">\n",
            options->kernel_name, argc);
        for (i = 0; i < argc; i++) {
            fprintf(instance->meta_xml.file,
                "\t\t<arg val=\"%s\"/>\n", argv[i]
            );
        }
        fprintf(instance->meta_xml.file, "\t</kernel>\n");

        /* Node ID to hostname */
        fprintf(instance->meta_xml.file, "\n\t<nodes>\n");
        for (i = 0; i < rank_info->num_nodes; i++) {
            fprintf(instance->meta_xml.file,
                "\t\t<node id=\"%d\" hostname=\"%s\"/>\n",
                i, rank_info->node_map[i]->hostname
            );
        }
        fprintf(instance->meta_xml.file, "\t</nodes>\n");

        /* Processor topology */
        fprintf(instance->meta_xml.file, 
            "\n\t<processors num_sockets=\"%d\" cores_per_socket=\"%d\" hw_threads_per_core=\"%d\">\n",
            topology->sockets_per_node,
            topology->cores_per_socket,
            topology->hw_threads_per_core
        );
        {
            if (options->map_by[0] != 'n') {
                for (i = 0; i < rank_info->num_local_instances; i++) {
                    signed int socket_id, core_id, hw_thread_id, os_core_id;

                    status = vb_get_processor_for_instance(
                        instance,
                        i,
                        options->map_by,
                        &socket_id,
                        &core_id,
                        &hw_thread_id
                    );
                    assert(status == VB_SUCCESS);

                    status = vb_get_processor_os_core(
                        instance,
                        socket_id,
                        core_id,
                        hw_thread_id,
                        &os_core_id
                    );
                    assert(status == VB_SUCCESS);

                    fprintf(instance->meta_xml.file,
                        "\t\t<processor local_rank=\"%d\" socket=\"%d\" core=\"%d\" hw_thread=\"%d\" os_core=\"%d\"/>\n",
                        i, socket_id, core_id, hw_thread_id, os_core_id
                    );
                }
            }
        }
        fprintf(instance->meta_xml.file, "\t</processors>\n");

        /* Miscellaneous things (to be filled in a per-kernel basis if needed */
        fprintf(instance->meta_xml.file, "\n\t<miscellaneous>\n");
    }

    fflush(instance->meta_xml.file);

    return VB_SUCCESS;
}

static int
vb_init_root_instance(vb_instance_t * instance,
                      int             argc,
                      char         ** argv)
{
    int status;
    char * fmt_str;
    
    /* Build various files */
    vb_build_fmt_str(instance, instance->kernel.name, &fmt_str);

    snprintf(instance->data_csv.name, VB_FMT_LEN, "%s", fmt_str);
    snprintf(instance->meta_xml.name, VB_FMT_LEN, "%s", fmt_str);

    free(fmt_str);

    strncat(instance->data_csv.name, "-data.csv", VB_SUFFIX_LEN);
    strncat(instance->meta_xml.name, "-meta.xml", VB_SUFFIX_LEN);

    /* Init the data csv */
    {
        instance->data_csv.file = fopen(instance->data_csv.name, "w");
        if (instance->data_csv.file == NULL) {
            vb_error_root("Could not create csv file %s\n", instance->data_csv.name);
            return VB_GENERIC_ERROR;
        }

        fprintf(instance->data_csv.file, "node_id,local_rank,rank,iteration,time_usec\n");
        fflush(instance->data_csv.file);
    }

    /* Init and write the meta xml */
    {
        instance->meta_xml.file = fopen(instance->meta_xml.name, "w");
        if (instance->meta_xml.file == NULL) {
            vb_error_root("Could not create csv file %s\n", instance->meta_xml.name);
            return VB_GENERIC_ERROR;
        }

        status = vb_write_meta_xml(instance, argc, argv);
        if (status != VB_SUCCESS) {
            vb_error_root("Could not write topo csv file %s\n", instance->meta_xml.name);
            return VB_GENERIC_ERROR;
        }
    }

    return VB_SUCCESS;
}


static pthread_t     * turbo_threads;
static int             turbo_state;
#define TURBO_RUN      0
#define TURBO_DIE      1


static void *
__turbo_hack(void * arg)
{
    while (turbo_state == TURBO_RUN) {
        __sync_synchronize();
    }

    return NULL;
}

int
vb_init_local_root_instance(vb_instance_t * instance)
{
    vb_options_t   * options   = &(instance->options);
    vb_rank_info_t * rank_info = &(instance->rank_info);
    vb_topology_t  * topology  = &(instance->topology);
    int i, status;
    pthread_attr_t attr;

    if (options->turbo_pin == 0)
        return VB_SUCCESS;

    turbo_threads = malloc(sizeof(pthread_t) * options->turbo_pin);
    if (!turbo_threads) {
        vb_error("Out of memory\n");
        return VB_GENERIC_ERROR;
    }

    turbo_state = TURBO_RUN;

    pthread_attr_init(&attr);

    for (i = 0; i < options->turbo_pin; i++) {
        unsigned int socket, logical_core, hw_thread, os_core;
        cpu_set_t cpus;

        /* No pinning */
        if (options->map_by[0] == 'n') {
            vb_debug_root("Burning some core to \"disable\" Turbo Boost\n");
            status = pthread_create(&(turbo_threads[i]), NULL, __turbo_hack, NULL); 
        } else {
            /* Fake like we're a real local instance to determine
             * where we would have been pinned
             */
            status = vb_get_processor_for_instance(
                    instance,
                    rank_info->num_local_instances + i,
                    options->map_by,
                    &socket,
                    &logical_core,
                    &hw_thread);
            if (status != VB_SUCCESS) {
                vb_error("Could not get processor for (fake) instance\n");
                goto out;
            }

            /* Get the OS core */
            status = vb_get_processor_os_core(
                    instance,
                    socket,
                    logical_core,
                    hw_thread,
                    &os_core);
            if (status != VB_SUCCESS) {
                vb_error("Could not get OS core for processor\n");
                goto out;
            }

            vb_debug_root("Burning socket=%u,core=%u,hw_thread=%u (os_core=%u) to \"disable\" Turbo Boost\n",
                socket,
                logical_core,
                hw_thread,
                os_core
            );

            CPU_ZERO(&cpus);
            CPU_SET(os_core, &cpus);

            pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
            status = pthread_create(&(turbo_threads[i]), &attr, __turbo_hack, NULL); 
        }

        if (status != 0) {
            vb_error("Failed to create pthread\n");
            goto out;
        }
    }

    pthread_attr_destroy(&attr);

    return VB_SUCCESS;

out:
    /* Kill threads */
    turbo_state = TURBO_DIE;

    while (--i >= 0)
        pthread_join(turbo_threads[i], NULL);

    return VB_GENERIC_ERROR;
}

void
vb_deinit_local_root_instance(vb_instance_t * instance)
{
    int i;

    /* Kill threads */
    turbo_state = TURBO_DIE;
    __sync_synchronize();

    for (i = 0; i < instance->options.turbo_pin; i++) {
        pthread_join(turbo_threads[i], NULL);
    }
}

void
vb_deinit_root_instance(vb_instance_t * instance)
{
    vb_complete_meta_xml(instance);
}

static void
vb_build_kernel_name(vb_instance_t * instance)
{
    int fd = open("/proc/self/cmdline", O_RDONLY);

    if (fd < 0) {
        strncpy(instance->options.kernel_name, "null", 4);
        return;
    }

    read(fd, &(instance->options.kernel_name), 63);
    close(fd);
}

int
vb_init_instance(vb_instance_t * instance,
                 bool            lib_invocation,
                 int             argc,
                 char         ** argv)
{
    int status;
    char * str;

    /* Check for valid kernel */
    if (!lib_invocation) {
        if (instance->options.kernel_name == NULL) {
            vb_error_root("No kernel name provided\n");
            status = VB_BAD_ARGS;
            goto out_1;
        }

        status = vb_str_to_kernel(instance);
        if (status != VB_SUCCESS) {
            vb_error_root("Bad kernel: %s not defined as a kernel\n",
                instance->options.kernel_name);
            goto out_1;
        }
    } else if (instance->rank_info.global_id == 0) {
        vb_build_kernel_name(instance);
        instance->kernel.name = instance->options.kernel_name;
    }

    /* Enumerate hardware topology */
    status = vb_build_hw_topology(instance);
    if (status != VB_SUCCESS) {
        vb_error_root("Could not determine HW topology\n");
        goto out_1;
    }

    /* Bind instance to local resources */
    status = vb_bind_instance(instance);
    if (status != VB_SUCCESS) {
        vb_error_root("Could not bind instance to HW resources\n");
        goto out_2;
    }

    /* Setup performance counter framework, if user wants them */
    status = vb_perfctr_init(instance);
    if (status != VB_SUCCESS) {
        vb_error_root("Could not initialize perf_ctr framework\n");
        goto out_2;
    }

    /* Root-specific stuff */
    if (instance->rank_info.global_id == 0) {
        status = vb_init_root_instance(instance, argc, argv);
        if (status != 0) {
            vb_error_root("Root-specific initialization failed\n");
            goto out_2;
        }
    }

    /* Local root specific stuff */
    if (instance->rank_info.local_id == 0) {
        status = vb_init_local_root_instance(instance);
        if (status != 0) {
            vb_error_root("Local root-specific initialization failed\n");
            goto out_3;
        }
    }

    status = VB_SUCCESS;

out_3:
    if (status != VB_SUCCESS) {
        if (instance->rank_info.global_id == 0)
            vb_deinit_root_instance(instance);
    }

out_2:
    if (status != VB_SUCCESS)
        vb_destroy_hw_topology(instance);

out_1:
    return status;
}

void
vb_deinit_instance(vb_instance_t * instance,
                   int             exit_status)
{
    vb_destroy_hw_topology(instance);

    if (instance->rank_info.local_id == 0) {
        vb_deinit_local_root_instance(instance);
    }

    if (instance->rank_info.global_id == 0) {
        vb_deinit_root_instance(instance);
    }

    if (instance->data_csv.file) {
        fclose(instance->data_csv.file);
        if (exit_status != VB_SUCCESS) {
            unlink(instance->data_csv.name);
        }
    }

    if (instance->meta_xml.file) {
        fclose(instance->meta_xml.file);
        if (exit_status != VB_SUCCESS) {
            unlink(instance->meta_xml.name);
        }
    }

    instance->data_csv.file = NULL;
    instance->meta_xml.file = NULL;
}


int
vb_gather_kernel_results_time_spent(vb_instance_t    * instance,
                                    unsigned long long iteration,
                                    unsigned long long time)
{
    vb_rank_info_t * rank_info = &(instance->rank_info);
    unsigned int id                  = rank_info->global_id;
    unsigned int num_instances       = rank_info->num_instances;
    unsigned long long * times;

    if (id == 0) {
        times = malloc(sizeof(unsigned long long) * num_instances);
        if (!times) {
            vb_error_root("Out of memory\n");
            return VB_GENERIC_ERROR;
        }
    }

    /* Gather times */
    MPI_Gather(&time, 1, MPI_UNSIGNED_LONG_LONG, times, 1, MPI_UNSIGNED_LONG_LONG, 0, MPI_COMM_WORLD);

    if (id == 0) {
        unsigned int rid;
        for (rid = 0; rid < num_instances; rid++) {
            fprintf(instance->data_csv.file, "%u,%u,%u,%llu,%llu\n",
                rank_info->rank_map[rid].node_id,
                rank_info->rank_map[rid].local_id,
                rid,
                iteration,
                times[rid]
            );
            fflush(instance->data_csv.file);
        }

        free(times);
    }

    return VB_SUCCESS;
}

int
vb_abort_kernel(vb_instance_t * instance)
{
    /* restart the performance tracking */
    fclose(instance->data_csv.file);

    instance->data_csv.file = fopen(instance->data_csv.name, "w");
    if (instance->data_csv.file == NULL) {
        vb_error_root("Could not create csv file %s\n", instance->data_csv.name);
        return VB_GENERIC_ERROR;
    }

    fprintf(instance->data_csv.file, "node_id,local_rank,rank,iteration,time_usec\n");
    fflush(instance->data_csv.file);

    return VB_SUCCESS;
}
