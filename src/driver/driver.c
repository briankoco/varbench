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
#include <signal.h>
#include <getopt.h>

#include <varbench.h>

#define DEFAULT_NUM_ITERATIONS 100
#define CPU_ALLOC_GRAN         64

/* Global used in various places. Do NOT change the name or type */
vb_instance_t this_instance = { 0 };

static void
usage(void) 
{
    vb_print_root("Usage: varbench\n" \
        " Required options:\n" \
        "  -k (--kernel=)<kernel>\n" \
        " Optional options:\n" \
        "  -h (--help)\n" \
        "  -d (--debug_on)\n" \
        "  -l (--list-kernels)\n" \
        "  -i (--iterations=)<iterations> (default=%d)\n" \
        "  -p (--processor-pin-by=)<'n', OR string permutation of 'csh'> (default='csh')\n" \
                "\t  'csh': map consecutive instances by 'c'ore, then by 's'ocket, then by 'h'w thread\n" \
                "\t  'n' disables core pinning\n" \
        "  -c (--cpu-list=<list of cpus>))\n" \
                "\t comma (or semicolon) delimited list of CPUs to run on\n" \
                "\t supersedes -p option" \
        "  -m (--memory-pin=<l,i,n>) (default='l')\n" \
                "\t allocate memory from 'l'ocal socket, 'i'nterleaved across sockets, or 'n'one\n"
        "  -t (--topology-file=)<file>\n" \
                "\t load topology information from JSON file <file> rather than query via hwloc\n" \
                "\t (see README for format specification)\n" \
        "  -b (--burn-cores=<b>)\n" \
                "\t pin a NO-OP busy loop on <t> cores per node to prevent Turbo Boost anomalies\n"
                "\t (NOTE: You need to provide enough MPI ranks to run your <t> NO-OPs)\n"\
        , 
        DEFAULT_NUM_ITERATIONS
    );
}

/* Ensure the list is valid and parse into an integer array */
static int
vb_parse_cpu_list(char * cpu_list,
                  int ** cpu_ar_p,
                  int  * nr_cpus_p)
{
    char * iter_str = NULL;
    int  * cpu_ar = NULL, * tmp_ar = NULL;
    int    nr_cpus  = 0;
    long cpu;

    while ((iter_str = strsep(&cpu_list, ",;")) != NULL) {
        
        cpu = strtol(iter_str, NULL, 10);
        if (cpu < 0) {
            vb_error_root("Invalid CPU string: CPUs must be non-negative\n");
            return VB_BAD_ARGS;
        }

        if (nr_cpus % CPU_ALLOC_GRAN == 0) {
            tmp_ar = realloc(cpu_ar, sizeof(int) * CPU_ALLOC_GRAN);
            if (!tmp_ar) {
                vb_error("Out of memory\n");
                free(cpu_ar);
                return VB_GENERIC_ERROR;
            }

            cpu_ar = tmp_ar;
        }

        cpu_ar[nr_cpus++] = cpu;
    }

    *nr_cpus_p = nr_cpus;
    *cpu_ar_p = cpu_ar;

    return VB_SUCCESS;
}

static int 
vb_parse_options(int           * argc,
                 char        *** argv,
                 vb_options_t  * opts)
{
    bool kernel_opt = false;
    int status;

    memset(opts, 0, sizeof(vb_options_t));
    opterr = 0; // quell error messages

    opts->num_iterations = DEFAULT_NUM_ITERATIONS;
    opts->map_by[0] = 'c';
    opts->map_by[1] = 's';
    opts->map_by[2] = 'h';
    opts->mem_affinity = 'l';

    while (1) {
        int c, option_index;
        struct option long_options[] =
        {
            {"help",                no_argument,        0,  'h'},
            {"kernel",              required_argument,  0,  'k'},
            {"debug-on",            no_argument,        0,  'd'},
            {"list-kernels",        no_argument,        0,  'l'},
            {"iterations",          no_argument,        0,  'i'},
            {"processor-pin-by",    required_argument,  0,  'p'},
            {"cpu-list",            required_argument,  0,  'c'},
            {"memory-pin",          required_argument,  0,  'm'},
            {"topology-file",       required_argument,  0,  't'},
            {"burn-cores",          required_argument,  0,  'b'},
            {0,                     0,                  0,  0}
        };

        c = getopt_long_only(*argc, *argv, "hk:dli:p:m:c:t:b:",
                long_options, &option_index);

        if (c == -1)
            break;

        switch (c) {
            case 'h':
                usage();
                exit(EXIT_SUCCESS);

            case 'k':
                kernel_opt = true;
                strncpy(opts->kernel_name, optarg, 63);
                break;
 
            case 'd':
                opts->debug_on = true;
                break;

            case 'l':
                vb_print_root("Valid kernels:\n" \
                    "  hello_world\n" \
                    " Cache kernels:\n"
                    "  cache_coherence\n" \
                    "  cache_capacity\n"
                    " Memory kernels:\n"
                    "  stream\n" \
                    "  random_access\n" \
                    " Network/IO kernels:\n" \
                    "  alltoall\n" \
                    "  random_neighbor\n" \
                    "  nearest_neighbor\n" \
                    "  io\n" \
                    " Application kernels:\n" \
                    "  dgemm\n"
                    " Other:\n" \
                    "  operating_system\n"
                );
                MPI_Finalize();
                exit(EXIT_SUCCESS);

            case 'i':
                opts->num_iterations = strtoull(optarg, NULL, 10);
                break;

            case 'p':
                strncpy(opts->map_by, optarg, 3);
                break;

            case 'c':
                status = vb_parse_cpu_list(optarg, &(opts->cpu_array), &(opts->cpu_array_len));
                if (status != VB_SUCCESS) {
                    vb_error("Failed to parse CPU array\n");
                    return VB_GENERIC_ERROR;
                }
                break;  

            case 'm':
                opts->mem_affinity = optarg[0];
                break;

            case 't':
                opts->topo_file = optarg;
                break;
            
            case 'b':
                opts->turbo_pin = atoi(optarg);
                if (opts->turbo_pin < 0) {
                    vb_error_root("--turbo_hack must be non-negative\n");
                    return VB_BAD_ARGS;
                }
                break;

            default:
                return VB_BAD_ARGS;
        }
    }

    if (!kernel_opt) {
        vb_error_root("You must supply --kernel\n");
        usage();
        exit(EXIT_FAILURE);
    }

    *argc -= optind;
    *argv += optind;

    return VB_SUCCESS;
}

int 
main(int     argc,
     char ** argv,
     char ** envp)
{
    int status;

    /* Get node/process topology */
    status = vb_build_rank_info(&argc, &argv, &this_instance);
    if (status != 0) {
        vb_error_root("Could not determine process topology\n");
        exit(EXIT_FAILURE);
    }

    /* Parse cmd line options */
    status = vb_parse_options(&argc, &argv, &(this_instance.options));
    if (status != VB_SUCCESS) {
        vb_error_root("Could not parse command line options\n");
        usage();
        exit(EXIT_FAILURE);
    }

    /* Various initialization */
    status = vb_init_instance(&this_instance, false, argc, argv);
    if (status != VB_SUCCESS) {
        vb_error_root("Could not initialize instance\n");
        exit(EXIT_FAILURE);
    }

    MPI_Barrier(MPI_COMM_WORLD);

    vb_debug_root("Launching kernel %s\n", this_instance.kernel.name);
    status = this_instance.kernel.fn(argc, argv, &this_instance);

    vb_deinit_instance(&this_instance, 0 /* status */);
    vb_deinit_rank_info(&this_instance);
    MPI_Finalize();

    exit(status);
}
