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

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <math.h>
#include <float.h>
#include <limits.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <dlfcn.h>

#include <varbench.h>
#include <libsyzcorpus.h>

#include <cJSON.h>

#define VB_OS_RESTART      1024
#define MAX_NAME_LEN       64
#define TEST_ITERATION_CNT 1

//#define VB_NO_CHECK

/* TODO: per-system call tracking */




typedef int (*syzkaller_prototype_t)(syscall_info *scall_info, int *num_calls);

typedef enum {
    VB_INITING,
    VB_DISABLED,
    VB_ENABLED,
} vb_program_status_t;;

typedef struct {
    char name[MAX_NAME_LEN];
    syzkaller_prototype_t fn;
    vb_program_status_t status;
} vb_program_t;

typedef struct {
    vb_program_t * program_list;
    cJSON        * json_list;
    unsigned int   nr_programs; /* length of program_list/json_list */
    unsigned int   nr_programs_enabled;
    int            last_failed_program;
} vb_program_list_t;


/* Just here to cause EINTR to break us out of waitpid() */
void alrm(int s) {}

static void
call_fn(syzkaller_prototype_t fn,
        int                 * exit_status,
        int                 * normal_exit,
        struct timeval      * t1,
        struct timeval      * t2,
		syscall_info          * scall_info_arr,
		int                 * num_scalls) 
{
    pid_t pid;
    int st, ret, to_child_fd[2], to_parent_fd[2], devnl;
    char go[1];

    *exit_status = -1;
    *normal_exit = -1;

    ret = pipe(to_child_fd);
    if (ret != 0) {
        vb_error("Failed to create pipe: %s\n", strerror(errno));
        return;
    }

    ret = pipe(to_parent_fd);
    if (ret != 0) {
        vb_error("Failed to create pipe: %s\n", strerror(errno));
        return;
    }

    switch ((pid = fork())) {
        case -1:
            perror("Failed to fork()\n");

        case 0:
            close(to_child_fd[1]);
            close(to_parent_fd[0]);

            /* Put myself in a new pgid */
            setpgid(0, 0);

            /* Prevent any of my children from zombifying, because we are not going to wait for them */
            signal(SIGCHLD, SIG_IGN);

            /* Send my stdout/stderr to dev/null */
            devnl = open("/dev/null", 'w');
            assert(devnl > 0);
            assert(dup2(devnl, STDOUT_FILENO) == STDOUT_FILENO);
            assert(dup2(devnl, STDERR_FILENO) == STDERR_FILENO);
            close(devnl);

            /* Wait for go signal */
            read(to_child_fd[0], &go, 1);
            assert(go[0] == '1');
            close(to_child_fd[0]);

            /* Tell parent to start counting */
            write(to_parent_fd[1], "1", 1);
            close(to_parent_fd[1]);

            /* Go */
            fn(scall_info_arr, num_scalls);
            exit(0);

        default:
            close(to_parent_fd[1]);
            close(to_child_fd[0]);

            /* Wait for everyone to reach this barrier to distill out the fork/exec overheads */
            //MPI_Barrier(MPI_COMM_WORLD);

            /* Send go */
            write(to_child_fd[1], "1", 1);
            close(to_child_fd[1]);

            /* Wait for child to finish its preinitialization */
            read(to_parent_fd[0], &go, 1);
            assert(go[0] == '1');
            close(to_parent_fd[0]);

            /* Start timer */
            gettimeofday(t1, NULL);

            /* Give it 3 seconds */
            alarm(3);

            while (1) {
                ret = waitpid(pid, &st, 0);
                if (ret == -1) {
                    if (errno == EINTR) {
                        kill(pid, SIGKILL);
                        continue;
                    }
                }
                break;
            }

            /* End timer */
            gettimeofday(t2, NULL);

            alarm(0);

            /* Determine exit status */
            if (WIFEXITED(st)) {
                *exit_status = WEXITSTATUS(st);
                *normal_exit = 1;
            } else if (WIFSIGNALED(st)) {
                *exit_status = WTERMSIG(st);
                *normal_exit = 0;
            } else {
                assert(2+2 != 4);
            }

            /* Kill any could be grand-children (they don't need reaped) */
            kill(-pid, SIGKILL);

            break;
    }
}


/* Retrieve a syzkaller program via its name */
static syzkaller_prototype_t
get_syzkaller_program(void  * dlopen_handle,
                      char  * program_name)
{
    syzkaller_prototype_t prototype;
    char * symbol_name;

    asprintf(&symbol_name, "_%s", program_name); 
    prototype = (syzkaller_prototype_t)dlsym(dlopen_handle, symbol_name);
    free(symbol_name);

    return prototype;
}

static syzkaller_prototype_t
get_syzkaller_program_by_offset(vb_program_list_t * program_list,
                                void              * dlopen_handle,
                                int                 offset)
{
    return get_syzkaller_program(dlopen_handle, program_list->program_list[offset].name);
}

static int
exec_program(vb_instance_t      * instance,
             vb_program_t       * program,
             char                 execution_mode,
             unsigned long long * time,
			 syscall_info       * scall_info_arr)
{
    struct timeval t1, t2;
    int exit_status, normal_exit, num_scalls;

    if (execution_mode == 'd') {
        /* Here we go ... */
        gettimeofday(&t1, NULL);
        program->fn(scall_info_arr, &num_scalls);
        gettimeofday(&t2, NULL);
    } else {
        call_fn(program->fn, &exit_status, &normal_exit, &t1, &t2, scall_info_arr, &num_scalls);
        if (!normal_exit) {
            vb_debug("Program %s exited via signal: %d (%s)\n", program->name, exit_status, strsignal(exit_status));

            //if (exit_status == SIGKILL) {
                return 1;
            //}
        }
    }

    *time = time_spent(&t1, &t2);
    return 0;
}

static void 
exec_and_check_program(vb_instance_t      * instance,
                       vb_program_t       * program,
                       char                 execution_mode,
                       unsigned long long * time,
                       int                * local_status,
                       int                * global_status,
					   syscall_info         * scall_info_arr)
{
    *local_status = exec_program(instance, program, execution_mode, time, scall_info_arr);
    MPI_Allreduce(local_status, global_status, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
}

/*
 * Create a single permutation of the numbers in range 0 to nr_indices-1
 */
static void
generate_random_permutation(int   nr_indices,
                            int * permutation,
                            bool  list_inited)
{
    int i, j, temp;

    if (!list_inited) {
        for (i = 0; i < nr_indices; i++)
            permutation[i] = i;
    }

    for (i = nr_indices - 1; i>= 0; --i) {
        j = rand() % (i + 1);

        temp = permutation[i];
        permutation[i] = permutation[j];
        permutation[j] = temp;
    }
}


/* iteration_all: execute all programs in a list */
static int
iteration(vb_instance_t      * instance,
          vb_program_list_t  * program_list,
          char                 concurrency_mode,
          char                 execution_mode,
          int                  nr_programs_per_iteration,
          int                * program_indices,
          bool                 dry_run,
          unsigned long long * time_p,
		  syscall_info		 * scall_info_arr)
{
    struct timeval t1, t2;
    int total_errors = 0, local_status, global_status;
    unsigned long long time, total_time;
    int i;

    total_time = 0;

    /* Scramble the program indices if we want random orders in each instance */
    if (concurrency_mode == 'r') {
        generate_random_permutation(nr_programs_per_iteration, program_indices, true);
    }

    /* Nothing's failed (yet ...) */
    program_list->last_failed_program = -1;


    /* Allocation of array of syscall info */


    for (i = 0; i < nr_programs_per_iteration; i++) {
        vb_program_t * program = &(program_list->program_list[program_indices[i]]);
        
        if (dry_run) {
            assert(program->status == VB_INITING);

            exec_and_check_program(instance, program, execution_mode, &time, &local_status, &global_status, scall_info_arr);
            if (local_status != 0) {
                program_list->last_failed_program = program_indices[i];
            }

            if (global_status != 0) {
                return global_status;
            }
        } else {
            assert(program->status == VB_ENABLED);
            local_status = exec_program(instance, program, execution_mode, &time, scall_info_arr);
            if (local_status != 0) {
                program_list->last_failed_program = program_indices[i];
                total_errors += 1;
            }
        }

        total_time += time;
    }

    if (!dry_run) {
        vb_print_root("Verifying iteration\n");
        MPI_Allreduce(&total_errors, &global_status, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
    }

    *time_p = total_time;
    return global_status;
}

static int
__run_kernel(vb_instance_t     * instance,
             unsigned long long  iterations,
             vb_program_list_t * program_list,
             char                concurrency_mode,
             char                execution_mode,
             int                 nr_programs_per_iteration,
             int               * program_indices)
{
    unsigned long long iter, time;
    int fd, status, prog_off = 0;
    int dummy, failure_count;
    syzkaller_prototype_t prototype;

	syscall_info *scall_info_arr = malloc(sizeof(syscall_info) * MAX_SYSCALLS);
	if (!scall_info_arr){
		vb_error("Out of memory\n");
        return VB_GENERIC_ERROR;
	}

    /* Let's be smart about program failure.
     * Simple metric that will allow us to survive an occasional
     * failure but that will also not stall us if errors are frequent:
     *
     *   -- always have at least as many successes as failures --
     */

    vb_print_root("Starting benchmark ...\n");

    failure_count = 0;
    for (iter = 0; iter < iterations; iter++) {
        do {
            MPI_Barrier(MPI_COMM_WORLD);

            /* execute all programs in the set */
            dummy = iteration(instance, program_list, concurrency_mode, execution_mode, nr_programs_per_iteration, 
                    program_indices, false, &time, scall_info_arr);

            if (dummy != 0) {
                vb_print_root("Iteration %llu failed\n", iter);

                ++failure_count;

                /* Determine if we need to bail */
                if (
                    (failure_count > iter) &&
                    (failure_count > 10)
                   )
                {
                    vb_error_root("Detected high program failure rate. %d iterations have failed, while only %llu have completed. Restarting.\n",
                        failure_count, iter);

                    if (instance->rank_info.global_id == 0) {
                        if (vb_abort_kernel(instance) != VB_SUCCESS) {
                            vb_error_root("Failed to abort kernel. Bailing out of program entirely\n");
							free(scall_info_arr); /*Make Sure this is right*/
                            return VB_GENERIC_ERROR;
                        }
                    }

                    /* Restart */
					free(scall_info_arr); /*Make Sure this is right*/
                    return VB_OS_RESTART;
                }

                vb_print_root("Retrying iteration %llu\n", iter);
            }
        } while (dummy != 0);

        vb_print_root("Iteration %llu finished\n", iter);

        status = vb_gather_kernel_results_time_spent(instance, iter, time);
        if (status != 0) {
            vb_error("Could not gather kernel results\n");
			free(scall_info_arr); /*Make Sure this is right*/
            return status;
        }
    }

	free(scall_info_arr);

    return VB_SUCCESS;
}

static int
get_json_object(vb_instance_t * instance,
                char          * json_file,
                cJSON        ** root)
{
    off_t off_len;
    int   len;
    char * file_contents;

    /* Root gets the json */
    if (instance->rank_info.global_id == 0) {
        *root = cJSON_GetObjectFromFile(json_file, (void **)&file_contents, &off_len);
        len = (int)off_len;
    }

    /* Broadcast size */
    MPI_Bcast(
        &len,
        1,
        MPI_INT,
        0,
        MPI_COMM_WORLD
    );

    /* check error */
    if (len == -1)
        return VB_GENERIC_ERROR;

    /* Non root specific */
    if (instance->rank_info.global_id != 0) {
        file_contents = malloc(sizeof(char) * len);
        if (file_contents == NULL) {
            vb_error("Could not allocate space for JSON object (%s)\n", strerror(errno));
            return VB_GENERIC_ERROR;
        }
    }

    /* Broadcast data */
    MPI_Bcast(
        file_contents,
        len,
        MPI_CHAR,
        0,
        MPI_COMM_WORLD
    );

    /* You gotta save it somewhere dude ...*/
    if (instance->rank_info.global_id != 0) {
        *root = cJSON_Parse(file_contents);
    }

    /* Teardown */
    if (instance->rank_info.global_id == 0) {
        munmap(file_contents, len);
    } else {
        free(file_contents);
    }

    return VB_SUCCESS;
}

static int
build_vb_program_list(cJSON              * json_list,
                      void               * dlopen_handle,
                      vb_program_list_t ** list)
{
    vb_program_list_t * new_list;
    vb_program_t      * new_program;
    int i;

    new_list = malloc(sizeof(vb_program_list_t));
    if (!new_list) {
        vb_error("Out of memory\n");
        return VB_GENERIC_ERROR;
    }

    new_list->json_list = json_list;
    new_list->nr_programs = cJSON_GetArraySize(json_list);
    new_list->nr_programs_enabled = 0;
    new_list->last_failed_program = -1;

    new_list->program_list = malloc(sizeof(vb_program_t) * new_list->nr_programs);
    if (!new_list->program_list) {
        vb_error("Out of memory\n");
        free(new_list);
        return VB_GENERIC_ERROR;
    }

    for (i = 0; i < new_list->nr_programs; i++) {
        new_program = &(new_list->program_list[i]);
        new_program->status = VB_INITING;
        snprintf(new_program->name, MAX_NAME_LEN, "%s", cJSON_GetArrayItem(new_list->json_list, i)->valuestring);
        new_program->fn = get_syzkaller_program_by_offset(new_list, dlopen_handle, i);
        assert(new_program->fn != NULL);
    }

    *list = new_list;
    return VB_SUCCESS;
}

static int
gather_last_failed_programs(vb_instance_t     * instance,
                            vb_program_list_t * program_list,
                            int              ** last_failed_programs)
{
    int * last_failed_list;
    int local_id;

    last_failed_list = malloc(sizeof(int) * instance->rank_info.num_instances);
    if (last_failed_list == NULL) {
        vb_error("Out of memory\n");
        return VB_GENERIC_ERROR;
    }

    MPI_Allgather(
        &(program_list->last_failed_program),
        1,
        MPI_INT,
        last_failed_list,
        1,
        MPI_INT,
        MPI_COMM_WORLD
    );

    *last_failed_programs = last_failed_list;
    return VB_SUCCESS;
}

static int
disable_failed_programs(vb_instance_t     * instance,
                        vb_program_list_t * program_list)
{
    int * failed_list;
    int i, st, nr_failed = 0;

    /* Gather the programs that failed and disable them */
    st = gather_last_failed_programs(instance, program_list, &failed_list);
    if (st != VB_SUCCESS) {
        vb_error("Could not gather program lists\n");
        return st;
    }

    /* Disable these programs */ 
    for (i = 0; i < instance->rank_info.num_instances; i++) {
        vb_program_t * program;
        int program_id;

        program_id = failed_list[i];
        if (program_id == -1)
            continue;

        program = &(program_list->program_list[program_id]);

        if (program->status != VB_DISABLED) {
            nr_failed++;
            program->status = VB_DISABLED;
            vb_print_root("Deleted program %s\n", program->name);
        }
    }

    free(failed_list);

    if (nr_failed > 0) {
        vb_print_root("Deleted %d failed programs\n", nr_failed);
    }

    return VB_SUCCESS;
}

static int
derive_program_set(vb_instance_t     * instance,
                   vb_program_list_t * program_list,
                   char                concurrency_mode,
                   int                 nr_programs_per_iteration,
                   int                 selection_seed,
                   int              ** success_indices_p)
{
    int   i, status, local_status, global_status, successful;
    int * permutation;
    int * success_indices;
    unsigned long long time;

    /* Root broadcast selection seed */
    MPI_Bcast(
        &selection_seed,
        1,
        MPI_UNSIGNED,
        0,
        MPI_COMM_WORLD
    );

    success_indices = malloc(sizeof(int) * nr_programs_per_iteration);
    if (!success_indices) {
        vb_error("Out of memory\n");
        return VB_GENERIC_ERROR;
    }
	
	syscall_info *scall_info_arr = malloc(sizeof(syscall_info) * MAX_SYSCALLS);
	if (!scall_info_arr){
		vb_error("Out of memory\n");
		free(success_indices);
        return VB_GENERIC_ERROR;
	}

    /* Here's the algorithm:
     * (1) Using seed, generate a permutation with which to walk through the program corpus
     * (2) All instances test this program concurrently
     * (3) Agree/disagree on each program
     * (4) If agree, add to final list
     */

    /* (1) */
    srand(selection_seed);
    permutation = malloc(sizeof(int) * program_list->nr_programs);
    if (!permutation) {
        vb_error("Out of memory\n");
        free(success_indices);
		free(scall_info_arr);
        return VB_GENERIC_ERROR;
    }
    generate_random_permutation(program_list->nr_programs, permutation, false); 

    do {

        vb_print_root("Deriving set of programs ...\n");

        for ( i = 0, successful = 0;
             (i < program_list->nr_programs) && (successful < nr_programs_per_iteration);
              i++
            ) 
        {
            vb_program_t * program = &(program_list->program_list[permutation[i]]);

            /* Program could be permanently disabled */
            if (program->status == VB_DISABLED)
                continue;

            MPI_Barrier(MPI_COMM_WORLD);

            /* (2) (always run via 'fork' when probing the corpus) */
            exec_and_check_program(instance, program, 'f', &time, &local_status, &global_status, scall_info_arr);

            /* (3) */
            if (global_status != 0) {
                vb_debug_root("Program %s failed on %d out of %d instances\n",
                    program->name, global_status, instance->rank_info.num_instances
                );

                program->status = VB_DISABLED;
            } else {
                /* (4) */
                success_indices[successful++] = permutation[i];

                vb_debug_root("Program %s succeeded on all %d instances. Found %d out of %d programs\n",
                    program->name, instance->rank_info.num_instances,
                    successful, nr_programs_per_iteration 
                );
            }
        }

        if (successful != nr_programs_per_iteration) {
            vb_error_root("Only found %d successful programs out of %d requested\n", 
                successful, nr_programs_per_iteration);

            free(permutation);
            free(success_indices);
			free(scall_info_arr);
            return VB_GENERIC_ERROR;
        }

        /* Re-seed our random number generator with our local instance ID + the global seed */
        srand(selection_seed + instance->rank_info.local_id);

        /***** Test this corpus, and proceed if and only if it works *****/
        vb_print_root("Testing program corpus to make sure at least %d iterations succeed ...\n", TEST_ITERATION_CNT);
        {
            int i = 0;

            for (i = 0; i < TEST_ITERATION_CNT; i++) {
                status = iteration(instance, program_list, concurrency_mode, 'f', 
                        nr_programs_per_iteration, success_indices, true, &time, scall_info_arr);

                if (status) {
                    int st;
                    st = disable_failed_programs(instance, program_list);
                    if (st != VB_SUCCESS) {
                        vb_error("Could not disable programs\n");
                        free(permutation);
                        free(success_indices);
						free(scall_info_arr);
                        return VB_GENERIC_ERROR;
                    }

                    vb_print_root("Corpus failed: restarting with new corpus\n");

                    break;
                } else {
                    vb_print_root("Test iteration %d succeeded\n", i);
                }

            }

        }

    } while (status != 0);

    vb_print_root("Corpus succeeded\n");
    free(permutation);
	free(scall_info_arr);
    /* Enable everything in the success list */
    for (i = 0; i < nr_programs_per_iteration; i++) {
        vb_program_t * program = &(program_list->program_list[success_indices[i]]);
        program->status = VB_ENABLED;
    }
    program_list->nr_programs_enabled = nr_programs_per_iteration;

    *success_indices_p = success_indices;

    return VB_SUCCESS;
}

static void 
save_program_info(vb_instance_t     * instance,
                  vb_program_list_t * program_list,
                  int                 nr_programs_per_iteration,
                  int               * program_indices,
                  int                 selection_seed)
{
    /* First, add in the seed */
    {
        char seed[] = "seed";
        char * names[1];
        char * vals[1];

        names[0] = seed;
        asprintf(&(vals[0]), "%d", selection_seed);

        vb_add_misc_meta_info(instance, "operating_system_misc", "selection_seed", 1, names, vals);

        free(vals[0]);
    }
    
    /* Now, add program list */
    {
        char ** names;
        char ** vals;
        char name[] = "name";
        int i;

        names = malloc(sizeof(char *) * nr_programs_per_iteration);
        assert(names);

        vals = malloc(sizeof(char *) * nr_programs_per_iteration);
        assert(vals);

        for (i = 0; i < nr_programs_per_iteration; i++) {
            vb_program_t * program = &(program_list->program_list[program_indices[i]]);
            names[i] = name;
            vals[i]  = (char *)program->name;
        }

        vb_add_misc_meta_info(instance, "programs", "program", nr_programs_per_iteration, names, vals);

        free(names);
        free(vals);
    }
}
    

static int
run_kernel(vb_instance_t    * instance,
           unsigned long long iterations,
           char             * json_file,
           char               concurrency_mode,
           char               execution_mode,
           int                nr_programs_per_iteration,
           int                selection_seed)
{
    int i, status, global_seed, nr_programs;
    int * program_indices;
    cJSON * json_obj, *program_dict, * true_list;
    void * lib_handle;
    struct sigaction act, old_act;
    vb_program_list_t * program_list;

    /* Catch SIGALRM */
    act.sa_handler = alrm;
    act.sa_flags   = 0;
    sigemptyset(&act.sa_mask);

    status = sigaction(SIGALRM, &act, &old_act);
    if (status != 0) {
        vb_error("Could not install signal handler for SIGALRM: %s\n", strerror(errno));
        return VB_GENERIC_ERROR;
    }

    /* Get a handle to the syzkaller program library */
    lib_handle = dlopen(NULL, RTLD_NOW|RTLD_GLOBAL);
    if (lib_handle == NULL) {
        vb_error("dlopen failed\n");
        return VB_GENERIC_ERROR;
    }

    /* Get JSON object describing programs */
    status = get_json_object(instance, json_file, &json_obj);
    if (status != VB_SUCCESS) {
        vb_error("Unable to build JSON object\n");
        return status;
    }

    program_dict = cJSON_GetObjectItemCaseSensitive(json_obj, "program_dict");
    if (!cJSON_IsObject(program_dict)) {
        vb_error("Unable to retrieve 'program_dict' object from JSON object\n");
        goto out_json;
    }

    true_list = cJSON_GetObjectItemCaseSensitive(program_dict, "true");
    if (!cJSON_IsArray(true_list)) {
        vb_error("Unable to retrieve 'true' object from JSON object\n");
        status = VB_GENERIC_ERROR;
        goto out_json;
    }

    /* Build list of programs to execute */
    status = build_vb_program_list(true_list, lib_handle, &program_list);
    if (status != VB_SUCCESS) {
        vb_error("Unable to build program list\n");
        goto out_json;
    }


    /* Do this until we get a successful program run */
    do {

        /* Disable any failed programs */
        status = disable_failed_programs(instance, program_list);
        if (status != VB_SUCCESS) {
            vb_error("Unable to disable failed programs\n");
            goto out_prog_list;
        }

        /* To start, set everything to INITING, unless some programs are already disabled */
        for (i = 0; i < program_list->nr_programs; i++) {
            vb_program_t * program = &(program_list->program_list[i]);

            if (program->status == VB_ENABLED)
                program->status = VB_INITING;
        }

        /* Derive set of programs to execute */
        status = derive_program_set(instance, program_list, concurrency_mode, nr_programs_per_iteration, 
                selection_seed, &program_indices);
        if (status != VB_SUCCESS) {
            vb_error_root("Unable to derive set of successful programs\n");
            goto out_prog_list;
        }
        
        /* Run the kernel */
        status = __run_kernel(instance, iterations, program_list, concurrency_mode, 
                    execution_mode, nr_programs_per_iteration, program_indices);
            
        /* Store the program list in the XML */
        if ((status == VB_SUCCESS) && (instance->rank_info.global_id == 0)) {
            save_program_info(instance, program_list, nr_programs_per_iteration, program_indices, selection_seed);
        }

        free(program_indices);
    } while (status == VB_OS_RESTART);


out_prog_list:
    free(program_list->program_list);
    free(program_list);

out_json:
    cJSON_Delete(json_obj);

    return status;
}


static void
usage(void)
{
    vb_error_root("\noperating_system requires args:\n"
        "  arg 0: <path to corpus json file>\n"
        "  arg 1: <concurrency mode>: 's' OR 'r' (single or random)\n"
        "       single: each instance concurrently executes the same program order\n"
        "       ramdom: each instance concurrently executes a random program order\n"
        "  arg 2: <execution mode>: 'f' OR 'd' (fork or direct)\n"
        "       fork: instances fork off a child to execute the programs\n"
        "       direct: instances directly execute programs, and potentially die if they crash\n"
        "  arg 3: <number of programs per iteration>\n"
        "  arg 4: <seed for random program selection order> (optional: default = time(NULL))\n"
        );
}

#ifdef USE_CORPUS
int 
vb_kernel_operating_system(int             argc,
                           char         ** argv,
                           vb_instance_t * instance)
{
    char * json_file, * program_id;
    char selection_mode, concurrency_mode, execution_mode;
    int nr_programs_per_iter, selection_seed;

    /* Arg must be array size */
    if (argc != 4 && argc != 5) {
        usage();
        return VB_BAD_ARGS;
    }

    json_file            = argv[0];
    concurrency_mode     = argv[1][0];
    execution_mode       = argv[2][0];
    nr_programs_per_iter = atoi(argv[3]);

    if (argc == 5) 
        selection_seed = atoi(argv[4]);
    else
        selection_seed = time(NULL);

    switch (concurrency_mode) {
        case 's':
        case 'r':
            break;

        default:
            vb_error_root("Invalid concurrency mode: %c\n", concurrency_mode);
            usage();
            return VB_BAD_ARGS;
    }

    switch (execution_mode) {
        case 'd':
            vb_print_root("Direct execution mode is dangerous: your benchmark may die abruptly\n");
        case 'f':
            break;

        default:
            vb_error_root("Invalid execution mode: %c\n", execution_mode);
            usage();
            return VB_BAD_ARGS;
    }

    vb_print_root("\nRunning operating_system\n"
        "  Num iterations    : %llu\n"
        "  JSON file         : %s\n"
        "  Concurrency mode  : %s\n"
        "  Execution mode    : %s\n"
        "  Programs per iter : %d\n"
        "  Selection seed    : %d\n",
        instance->options.num_iterations,
        json_file,
        (concurrency_mode == 's') ? "single"   : "random",
        (  execution_mode == 'd') ? "direct"   : "fork",
        nr_programs_per_iter,
        selection_seed
    );

    return run_kernel(instance, instance->options.num_iterations, json_file,
                concurrency_mode, execution_mode, nr_programs_per_iter,
                selection_seed
        );
}
#else /* USE_CORPUS */
int 
vb_kernel_operating_system(int             argc,
                           char         ** argv,
                           vb_instance_t * instance)
{
    vb_error_root("You must build varbench against a syzkaller corpus to run this kernel. See the README and Makefile options\n");
    return VB_GENERIC_ERROR;
}
#endif
