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
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <dlfcn.h>
#include <dirent.h>

#include <varbench.h>
#include <libsyzcorpus.h>

#include <cJSON.h>

#define NO_SYSCALL          1024
#define VB_OS_RESTART       1024
#define TEST_ITERATION_CNT  1
#define PROGRAM_WAIT_TIME   2
#define FLUSH_GRANULARITY   (1ULL << 20)

// #define USE_MPI_GATHER

//#define VB_NO_CHECK

typedef int (*syzkaller_prototype_t)(vb_syscall_info_t * scall_info, int * num_calls);

typedef enum {
    VB_INITING,
    VB_DISABLED,
    VB_ENABLED,
} vb_program_status_t;

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

typedef struct {
    vb_program_list_t * program_list;
    char * toplevel_working_dir;
    char * syscall_data_dir;
    char * json_file;

    char active_working_dir[MAX_NAME_LEN];

    char concurrency_mode;
    char execution_mode;
    int  nr_programs_per_iteration;
    int  selection_seed;

    bool generate_program_csv;
    char program_filename[MAX_NAME_LEN];
    int    program_fd;
    FILE * program_fp;
    unsigned long program_last_flush;
    unsigned long program_bytes_written;

    bool generate_syscall_csv;
    char syscall_filename[MAX_NAME_LEN];
    int    syscall_fd;
    FILE * syscall_fp;
    unsigned long syscall_last_flush;
    unsigned long syscall_bytes_written;

    vb_syscall_info_t * syscall_arr;
    MPI_Datatype syscall_dt;

    /* Global root only */
    int * global_program_indices;
    vb_syscall_info_t * global_arr;
} vb_os_info_t;


/* Just here to cause EINTR to break us out of waitpid() */
void alrm(int s) {}


static pid_t
get_ppid_of_pid(pid_t pid)
{
    char state, fname[32];
    FILE * stat_f;
    pid_t _pid, _ppid;

    snprintf(fname, 32, "/proc/%d/stat", pid);

    stat_f = fopen(fname, "r");
    if (!stat_f) {
        // vb_error("Could not open %s: %s\n", fname, strerror(errno));
        return 1;
    }

    /* Take data from /proc/[pid]/stat, see URL below for more info */
    /* http://man7.org/linux/man-pages/man5/proc.5.html */
    fscanf(stat_f, "%d %*s %c %d", &_pid, &state, &_ppid);
    fclose(stat_f);

    /* If the pid in the file doesn't match, let's bail, because 
     * I don't know what that implies
     */
    if (pid != _pid)
        return 0;

    return _ppid; 
}

static bool
pid_is_descendant(pid_t pid,
                  pid_t ancestor)
{
    if (pid <= 1)
        return false;

    if (pid == ancestor)
        return true;

    return pid_is_descendant(get_ppid_of_pid(pid), ancestor);
}

static void
kill_all_descendants(pid_t parent)
{
    struct dirent * dent;
    DIR * dir;
    pid_t pid;
    char state, fname[32];
    FILE * stat_f;

    /* Searches through all directories in /proc */
    dir = opendir("/proc/");
    while((dent = readdir(dir)) != NULL) {
        /* If numerical */
        if (dent->d_name[0] >= '0' && dent->d_name[0] <= '9') {
            pid = atoi(dent->d_name);

            /* let's not kill ourselves ... */
            if (pid == parent)
                continue;

            snprintf(fname, 32, "/proc/%d/stat", pid);
            stat_f = fopen(fname, "r");
            if (!stat_f)
                continue;

            /* Take data from /proc/[pid]/stat, see URL below for more info */
            /* http://man7.org/linux/man-pages/man5/proc.5.html */
            fscanf(stat_f, "%*d %*s %c %*d", &state);
            fclose(stat_f);

            /* Process if it's a zombie */
            if (state == 'Z') {
                if (pid_is_descendant(pid, parent)) {
                    vb_debug("Killing zombie descendant with pid %d\n", pid);
                    kill(pid, SIGKILL);
                    waitpid(pid, NULL, 0);
                }
            }
        }
    }
    closedir(dir);
}

static int
allocate_shared_mapping(vb_syscall_info_t ** syscall_arr,
                        int                  programs_per_iteration)
{
    vb_syscall_info_t * mapping = NULL;
    unsigned long bytes;

    bytes   = sizeof(vb_syscall_info_t) * MAX_SYSCALLS * programs_per_iteration;
    mapping = mmap(NULL, bytes, PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_ANONYMOUS,
            -1, 0);
    if (mapping == MAP_FAILED) {
        vb_error("Failed to allocate shared memory for system call array: %s\n",
            strerror(errno));
        return VB_GENERIC_ERROR;
    }

    *syscall_arr = mapping;
    return VB_SUCCESS;
}

static void
free_shared_mapping(vb_syscall_info_t * syscall_arr,
                    int                 nr_programs_per_iteration)
{
    unsigned long bytes = sizeof(vb_syscall_info_t) * MAX_SYSCALLS * nr_programs_per_iteration;
    munmap(syscall_arr, bytes);
}

static int
init_syscall_info_file(vb_instance_t * instance,
                       vb_os_info_t  * os_info)
{
    int status;

    if (os_info->syscall_fd)
        close(os_info->syscall_fd);

    /* instance->fmt_str hold the unique fmt_str for this run based on time of day */
#ifdef USE_MPI_GATHER
    snprintf(os_info->syscall_filename, MAX_NAME_LEN - 16, "%s/%s", 
        os_info->syscall_data_dir, instance->fmt_str);
#else
    snprintf(os_info->syscall_filename, MAX_NAME_LEN - 16, "%s/%s-rank%d", 
        os_info->syscall_data_dir, instance->fmt_str, instance->rank_info.global_id);
#endif
    strncat(os_info->syscall_filename, "-syscalls.csv", 16);

    /* we need to use open() directly to disable page cache */
    os_info->syscall_fd = open(os_info->syscall_filename, O_CREAT | O_TRUNC | O_RDWR, 0664);
    if (os_info->syscall_fd == -1) {
        vb_error_root("Could not create csv file %s\n", os_info->syscall_filename);
        return VB_GENERIC_ERROR;
    }

    os_info->syscall_fp = fdopen(os_info->syscall_fd, "w");
    if (os_info->syscall_fp == NULL) {
        vb_error_root("Could not get FD to csv file %s\n", os_info->syscall_filename);
        return VB_GENERIC_ERROR;
    }

    fprintf(os_info->syscall_fp, "rank,iteration,program_id,syscall_offset_in_program,syscall_number,ret_val,time_in,nsecs\n");
    fflush(os_info->syscall_fp);

    os_info->syscall_last_flush = 0;
    os_info->syscall_bytes_written = 0;

    return VB_SUCCESS;
}

static int
init_program_info_file(vb_instance_t * instance,
                       vb_os_info_t  * os_info)
{
    int status;

    if (os_info->program_fd)
        close(os_info->program_fd);

    /* instance->fmt_str hold the unique fmt_str for this run based on time of day */
#ifdef USE_MPI_GATHER
    snprintf(os_info->program_filename, MAX_NAME_LEN - 16, "%s/%s", 
        os_info->syscall_data_dir, instance->fmt_str);
#else
    snprintf(os_info->program_filename, MAX_NAME_LEN - 16, "%s/%s-rank%d", 
        os_info->syscall_data_dir, instance->fmt_str, instance->rank_info.global_id);
#endif
    strncat(os_info->program_filename, "-programs.csv", 16);

    /* we need to use open() directly to disable page cache */
    os_info->program_fd = open(os_info->program_filename, O_CREAT | O_TRUNC | O_RDWR, 0664);
    if (os_info->program_fd < 0) {
        vb_error_root("Could not create csv file %s\n", os_info->program_filename);
        return VB_GENERIC_ERROR;
    }

    os_info->program_fp = fdopen(os_info->program_fd, "w");
    if (os_info->program_fp == NULL) {
        vb_error_root("Could not get FD to csv file %s\n", os_info->program_filename);
        return VB_GENERIC_ERROR;
    }

    fprintf(os_info->program_fp, "rank,iteration,program_id,nsecs\n");
    fflush(os_info->program_fp);

    os_info->program_last_flush = 0;
    os_info->program_bytes_written = 0;

    return VB_SUCCESS;
}

static void
call_fn(vb_instance_t       * instance,
        syzkaller_prototype_t fn,
        int                 * exit_status,
        int                 * normal_exit,
        struct timeval      * t1,
        struct timeval      * t2,
        vb_syscall_info_t   * syscall_arr,
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
            // signal(SIGCHLD, SIG_IGN);

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
            fn(syscall_arr, num_scalls);
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

            /* Give it some time*/
            alarm(PROGRAM_WAIT_TIME);

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

            /* Kill any could be grand-children */
            //kill(-pid, SIGKILL);

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
             vb_syscall_info_t  * syscall_arr)
{
    struct timeval t1, t2;
    int exit_status, normal_exit, num_scalls;

    if (execution_mode == 'd') {
        /* Here we go ... */
        gettimeofday(&t1, NULL);
        program->fn(syscall_arr, &num_scalls);
        gettimeofday(&t2, NULL);
    } else {
        call_fn(instance, program->fn, &exit_status, &normal_exit, &t1, &t2, syscall_arr, &num_scalls);
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
                       vb_syscall_info_t  * syscall_arr)
{
    *local_status = exec_program(instance, program, execution_mode, time, syscall_arr);
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
          vb_os_info_t       * os_info,
          int                * program_indices,
          bool                 dry_run,
          unsigned long long * time_p)
{
    struct timeval t1, t2;
    int total_errors = 0, local_status, global_status;
    unsigned long long time, total_time;
    int i;

    assert(chdir(os_info->active_working_dir) == 0);

    total_time = 0;

    /* Scramble the program indices if we want random orders in each instance */
    if (os_info->concurrency_mode == 'r') {
        generate_random_permutation(os_info->nr_programs_per_iteration, program_indices, true);
    }

    /* Nothing's failed (yet ...) */
    os_info->program_list->last_failed_program = -1;

    for (i = 0; i < os_info->nr_programs_per_iteration; i++) {
        vb_program_t * program = &(os_info->program_list->program_list[program_indices[i]]);
        
        if (dry_run) {
            assert(program->status == VB_INITING);

            exec_and_check_program(instance, program, os_info->execution_mode, &time, &local_status, &global_status, os_info->syscall_arr);
            if (local_status != 0) {
                os_info->program_list->last_failed_program = program_indices[i];
            }

            if (global_status != 0) {
                goto out;
            }

            vb_debug_root("%dth (out of %d) program succeeded\n", i, os_info->nr_programs_per_iteration);
        } else {
            assert(program->status == VB_ENABLED);

            local_status = exec_program(instance, program, os_info->execution_mode, &time, &(os_info->syscall_arr[i*MAX_SYSCALLS]));
            if (local_status != 0) {
                os_info->program_list->last_failed_program = program_indices[i];
                total_errors += 1;
                break;
            }
        }

        total_time += time;
    }

    if (!dry_run) {
        vb_print_root("Verifying iteration\n");
        MPI_Allreduce(&total_errors, &global_status, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
    }

    *time_p = total_time;

out:
    /* Reap possible grandchildren that could have been created during this set
     * of programs. We do this once per every program set, to prevent PID
     * exhaustion over the course of many iterations.
     */
    kill_all_descendants(getpid());
    assert(chdir(os_info->toplevel_working_dir) == 0);

    return global_status;
}                     

#ifdef USE_MPI_GATHER
static int
gather_syscall_info(vb_instance_t     * instance,
                    vb_os_info_t      * os_info,
                    int               * program_indices,
                    unsigned long long  iteration)
{
    vb_rank_info_t * rank_info = &(instance->rank_info);
    unsigned int id            = rank_info->global_id;
    unsigned int num_instances = rank_info->num_instances;
    int program_off;
    int bytes_written;

    /* Gather syscall data for one program at a time, so as to limit the size
     * of the array we need to allocate at root
     */
    for (program_off = 0; program_off < os_info->nr_programs_per_iteration; program_off++) {
        vb_syscall_info_t * send_buf = &(os_info->syscall_arr[program_off * MAX_SYSCALLS]);

        /* First, gather the program indices */
        MPI_Gather(
            &(program_indices[program_off]),
            1,
            MPI_INT,
            os_info->global_program_indices,
            1,
            MPI_INT,
            0,
            MPI_COMM_WORLD
        );

        /* Now, gather syscall data */
        MPI_Gather(
            send_buf,
            MAX_SYSCALLS, 
            os_info->syscall_dt, 
            os_info->global_arr, 
            MAX_SYSCALLS,
            os_info->syscall_dt, 
            0, 
            MPI_COMM_WORLD
        );

        /* global_arr now holds all data from some program in each rank */
        if (id == 0) {
            unsigned int rid;
            unsigned int syscall_off;
            unsigned int syscall_nr;
            unsigned long long rank_time;

            for (rid = 0; rid < num_instances; rid++) {
                syscall_nr = 0;
                rank_time  = 0;

                for (syscall_off = 0; syscall_off < MAX_SYSCALLS; syscall_off++) {
                    vb_syscall_info_t * syscall = &(os_info->global_arr[rid * MAX_SYSCALLS + syscall_off]);
                    unsigned long long  latency = syscall->time_out - syscall->time_in;

                    if (syscall->syscall_number != NO_SYSCALL) {
                        if (os_info->generate_program_csv) {
                            rank_time += latency;
                        }

                        if (os_info->generate_syscall_csv) {
                            /* CSV format:
                             *  rank_id,iteration,program_id,syscall_off_in_program,syscall_number,ret_val,time_in,nsecs
                             */
                            bytes_written = fprintf(os_info->syscall_fp, "%d,%llu,%s,%d,%d,%li,%llu,%llu\n",
                                rid,
                                iteration,
                                os_info->program_list->program_list[os_info->global_program_indices[rid]].name,
                                syscall_nr++,
                                syscall->syscall_number,
                                syscall->ret_val,
                                syscall->time_in,
                                latency
                            );
                            fflush(os_info->syscall_fp);

                            /* to prevent us from blowing up the page cache, and potentially competing with 
                             * the workloads we're running, tell the kernel to ignore the file contents every
                             * couple MB or so that we write */
                            os_info->syscall_bytes_written += bytes_written;
                            if (os_info->syscall_bytes_written >
                                (os_info->syscall_last_flush + FLUSH_GRANULARITY))
                            {
                                assert(sync_file_range(
                                    os_info->syscall_fd,
                                    os_info->syscall_last_flush,
                                    FLUSH_GRANULARITY,
                                    SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WAIT_AFTER | SYNC_FILE_RANGE_WRITE
                                ) == 0);

                                assert(posix_fadvise(
                                    os_info->syscall_fd,
                                    os_info->syscall_last_flush,
                                    FLUSH_GRANULARITY,
                                    POSIX_FADV_DONTNEED
                                ) == 0);

                                os_info->syscall_last_flush += FLUSH_GRANULARITY;
                            }
                        }
                    }
                }

                if (os_info->generate_program_csv) {
                    /* CSV format:
                     *  rank_id,iteration,program_id,nsecs
                     */
                     bytes_written = fprintf(os_info->program_fp, "%d,%llu,%s,%llu\n",
                        rid,
                        iteration,
                        os_info->program_list->program_list[os_info->global_program_indices[rid]].name,
                        rank_time
                    );
                    fflush(os_info->program_fp);

                    /* to prevent us from blowing up the page cache, and potentially competing with 
                     * the workloads we're running, tell the kernel to ignore the file contents every
                     * couple MB or so that we write */
                    os_info->program_bytes_written += bytes_written;
                    if (os_info->program_bytes_written >
                        (os_info->program_last_flush + FLUSH_GRANULARITY))
                    {
                        assert(sync_file_range(
                            os_info->program_fd,
                            os_info->program_last_flush,
                            FLUSH_GRANULARITY,
                            SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WAIT_AFTER | SYNC_FILE_RANGE_WRITE
                        ) == 0);

                        assert(posix_fadvise(
                            os_info->program_fd,
                            os_info->program_last_flush,
                            FLUSH_GRANULARITY,
                            POSIX_FADV_DONTNEED
                        ) == 0);

                        os_info->program_last_flush += FLUSH_GRANULARITY;
                    }
                }
            }
        }
    }

    return VB_SUCCESS;
}
#else
static int
gather_syscall_info(vb_instance_t     * instance,
                    vb_os_info_t      * os_info,
                    int               * program_indices,
                    unsigned long long  iteration)
{
    vb_rank_info_t * rank_info = &(instance->rank_info);
    unsigned int id            = rank_info->global_id;
    unsigned int num_instances = rank_info->num_instances;
    int program_off;
    int bytes_written;
    unsigned int syscall_off;
    unsigned int syscall_nr;
    unsigned long long program_time;

    /* Dump all of my syscalls to the CSVs */
    for (program_off = 0; program_off < os_info->nr_programs_per_iteration; program_off++) {

        program_time = 0;
        syscall_nr = 0;

        for (syscall_off = 0; syscall_off < MAX_SYSCALLS; syscall_off++) {

            vb_syscall_info_t * syscall;
            unsigned long long latency; 

            syscall = &(os_info->syscall_arr[(program_off * MAX_SYSCALLS) + syscall_off]);
            latency = syscall->time_out - syscall->time_in;

            if (syscall->syscall_number != NO_SYSCALL) {
                if (os_info->generate_program_csv) {
                    program_time += latency;
                }

                if (os_info->generate_syscall_csv) {
                    /* CSV format:
                     *  rank_id,iteration,program_id,syscall_off_in_program,syscall_number,ret_val,time_in,nsecs
                     */
                    bytes_written = fprintf(os_info->syscall_fp, "%d,%llu,%s,%d,%d,%li,%llu,%llu\n",
                        id,
                        iteration,
                        os_info->program_list->program_list[program_indices[program_off]].name,
                        syscall_nr++,
                        syscall->syscall_number,
                        syscall->ret_val,
                        syscall->time_in,
                        latency
                    );
                    fflush(os_info->syscall_fp);

                    /* to prevent us from blowing up the page cache, and potentially competing with 
                     * the workloads we're running, tell the kernel to ignore the file contents every
                     * couple MB or so that we write */
                    os_info->syscall_bytes_written += bytes_written;
                    if (os_info->syscall_bytes_written >
                        (os_info->syscall_last_flush + FLUSH_GRANULARITY))
                    {
                        assert(sync_file_range(
                            os_info->syscall_fd,
                            os_info->syscall_last_flush,
                            FLUSH_GRANULARITY,
                            SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WAIT_AFTER | SYNC_FILE_RANGE_WRITE
                        ) == 0);

                        assert(posix_fadvise(
                            os_info->syscall_fd,
                            os_info->syscall_last_flush,
                            FLUSH_GRANULARITY,
                            POSIX_FADV_DONTNEED
                        ) == 0);

                        os_info->syscall_last_flush += FLUSH_GRANULARITY;
                    }
                }
 
                if (os_info->generate_program_csv) {
                    /* CSV format:
                     *  rank_id,iteration,program_id,nsecs
                     */
                     bytes_written = fprintf(os_info->program_fp, "%d,%llu,%s,%llu\n",
                        id,
                        iteration,
                        os_info->program_list->program_list[program_indices[program_off]].name,
                        program_time
                    );
                    fflush(os_info->program_fp);

                    /* to prevent us from blowing up the page cache, and potentially competing with 
                     * the workloads we're running, tell the kernel to ignore the file contents every
                     * couple MB or so that we write */
                    os_info->program_bytes_written += bytes_written;
                    if (os_info->program_bytes_written >
                        (os_info->program_last_flush + FLUSH_GRANULARITY))
                    {
                        assert(sync_file_range(
                            os_info->program_fd,
                            os_info->program_last_flush,
                            FLUSH_GRANULARITY,
                            SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WAIT_AFTER | SYNC_FILE_RANGE_WRITE
                        ) == 0);

                        assert(posix_fadvise(
                            os_info->program_fd,
                            os_info->program_last_flush,
                            FLUSH_GRANULARITY,
                            POSIX_FADV_DONTNEED
                        ) == 0);

                        os_info->program_last_flush += FLUSH_GRANULARITY;
                    }
                }
            }
        }
    }

    return VB_SUCCESS;
}
#endif

static int
__run_kernel(vb_instance_t     * instance,
             vb_os_info_t      * os_info,
             unsigned long long  iterations,
             int               * program_indices)
{
    unsigned long long iter, time;
    int fd, status, i;
    int dummy, failure_count;
    syzkaller_prototype_t prototype;

    status = allocate_shared_mapping(&(os_info->syscall_arr), os_info->nr_programs_per_iteration);
    if (status != VB_SUCCESS){
        vb_error("Failed to allocate syscall array\n");
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
            /* re-init the syscall array before each iteration */
            for (i = 0; i < os_info->nr_programs_per_iteration * MAX_SYSCALLS; i++) {
                os_info->syscall_arr[i].syscall_number = NO_SYSCALL;
            }

            MPI_Barrier(MPI_COMM_WORLD);

            /* execute all programs in the set */
            dummy = iteration(instance, os_info, program_indices, false, &time);

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
                            free_shared_mapping(os_info->syscall_arr, os_info->nr_programs_per_iteration);
                            return VB_GENERIC_ERROR;
                        }

                    }

                    /* Restart */
                    free_shared_mapping(os_info->syscall_arr, os_info->nr_programs_per_iteration);
                    return VB_OS_RESTART;
                }

                vb_print_root("Retrying iteration %llu\n", iter);
            }
        } while (dummy != 0);

        vb_print_root("Iteration %llu finished\n", iter);

        status = vb_gather_kernel_results_time_spent(instance, iter, time);
        if (status != 0) {
            vb_error("Could not gather kernel results\n");
            free_shared_mapping(os_info->syscall_arr, os_info->nr_programs_per_iteration);
            return status;
        }

        /* Gather the system call information, if we are generating either per program or per syscall
         * CSVs
         */
        if (os_info->generate_syscall_csv || os_info->generate_program_csv) {
            status = gather_syscall_info(instance, os_info, program_indices, iter);
            if (status != 0) {
                vb_error("Could not gather kernel syscall results\n");
                free_shared_mapping(os_info->syscall_arr, os_info->nr_programs_per_iteration);
                return status;
            }
        }
    }

    free_shared_mapping(os_info->syscall_arr, os_info->nr_programs_per_iteration);

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
                   vb_os_info_t      * os_info,
                   int              ** success_indices_p)
{
    int   i, status, local_status, global_status, successful;
    int * permutation;
    int * success_indices;
    unsigned long long time;

    /* Root broadcast selection seed */
    MPI_Bcast(
        &(os_info->selection_seed),
        1,
        MPI_UNSIGNED,
        0,
        MPI_COMM_WORLD
    );

    success_indices = malloc(sizeof(int) * os_info->nr_programs_per_iteration);
    if (!success_indices) {
        vb_error("Out of memory\n");
        return VB_GENERIC_ERROR;
    }
    
    os_info->syscall_arr = malloc(sizeof(vb_syscall_info_t) * MAX_SYSCALLS);
    if (!os_info->syscall_arr){
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
    srand(os_info->selection_seed);
    permutation = malloc(sizeof(int) * os_info->program_list->nr_programs);
    if (!permutation) {
        vb_error("Out of memory\n");
        free(success_indices);
        free(os_info->syscall_arr);
        return VB_GENERIC_ERROR;
    }
    generate_random_permutation(os_info->program_list->nr_programs, permutation, false); 

    do {

        vb_print_root("Deriving set of programs ...\n");

        for ( i = 0, successful = 0;
             (i < os_info->program_list->nr_programs) && (successful < os_info->nr_programs_per_iteration);
              i++
            ) 
        {
            vb_program_t * program = &(os_info->program_list->program_list[permutation[i]]);

            /* Program could be permanently disabled */
            if (program->status == VB_DISABLED)
                continue;

            MPI_Barrier(MPI_COMM_WORLD);

            /* (2) (always run via 'fork' when probing the corpus) */
            {
                assert(chdir(os_info->active_working_dir) == 0);
                exec_and_check_program(instance, program, 'f', &time, &local_status, &global_status, os_info->syscall_arr);
                assert(chdir(os_info->toplevel_working_dir) == 0);
            }

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
                    successful, os_info->nr_programs_per_iteration 
                );
            }
        }

        if (successful != os_info->nr_programs_per_iteration) {
            vb_error_root("Only found %d successful programs out of %d requested\n", 
                successful, os_info->nr_programs_per_iteration);

            free(permutation);
            free(success_indices);
            free(os_info->syscall_arr);
            return VB_GENERIC_ERROR;
        }

        /* Re-seed our random number generator with our local instance ID + the global seed */
        srand(os_info->selection_seed + instance->rank_info.local_id);

        /***** Test this corpus, and proceed if and only if it works *****/
        vb_print_root("Testing program corpus to make sure at least %d iterations succeed ...\n", TEST_ITERATION_CNT);
        {
            int i = 0;
            bool old_exec;

            for (i = 0; i < TEST_ITERATION_CNT; i++) {
                /* disable 'direct' execution during dry runs */
                status = iteration(instance, os_info, success_indices, true, &time);

                if (status) {
                    int st;
                    st = disable_failed_programs(instance, os_info->program_list);
                    if (st != VB_SUCCESS) {
                        vb_error("Could not disable programs\n");
                        free(permutation);
                        free(success_indices);
                        free(os_info->syscall_arr);
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
    free(os_info->syscall_arr);

    /* Enable everything in the success list */
    for (i = 0; i < os_info->nr_programs_per_iteration; i++) {
        vb_program_t * program = &(os_info->program_list->program_list[success_indices[i]]);
        program->status = VB_ENABLED;
    }
    os_info->program_list->nr_programs_enabled = os_info->nr_programs_per_iteration;

    *success_indices_p = success_indices;

    return VB_SUCCESS;
}

static void 
save_program_info(vb_instance_t * instance,
                  vb_os_info_t  * os_info,
                  int           * program_indices)
{
    /* First, add in the seed */
    {
        char seed[] = "seed";
        char * names[1];
        char * vals[1];

        names[0] = seed;
        asprintf(&(vals[0]), "%d", os_info->selection_seed);

        vb_add_misc_meta_info(instance, "operating_system_misc", "selection_seed", 1, names, vals);

        free(vals[0]);
    }
    
    /* Now, add program list */
    {
        char ** names;
        char ** vals;
        char name[] = "name";
        int i;

        names = malloc(sizeof(char *) * os_info->nr_programs_per_iteration);
        assert(names);

        vals = malloc(sizeof(char *) * os_info->nr_programs_per_iteration);
        assert(vals);

        for (i = 0; i < os_info->nr_programs_per_iteration; i++) {
            vb_program_t * program = &(os_info->program_list->program_list[program_indices[i]]);
            names[i] = name;
            vals[i]  = (char *)program->name;
        }

        vb_add_misc_meta_info(instance, "programs", "program", os_info->nr_programs_per_iteration, names, vals);

        free(names);
        free(vals);
    }
}

static int
__prepare_root_aggregation(vb_instance_t * instance,
                           vb_os_info_t  * os_info)
{
    int num_instances;
    int global_id;

    num_instances = instance->rank_info.num_instances;
    global_id     = instance->rank_info.global_id;

    /* All ranks create a new MPI datatype */
    {
        int ret, type_size;
        int count = 4;
        int array_of_blocklengths[] = {
            1,
            1,
            1,
            1
        };
        MPI_Aint array_of_displacements[] = {
            offsetof(vb_syscall_info_t, syscall_number), 
            offsetof(vb_syscall_info_t, ret_val), 
            offsetof(vb_syscall_info_t, time_in),
            offsetof(vb_syscall_info_t, time_out),
        };
        MPI_Datatype array_of_types[] = {
            MPI_SHORT,
            MPI_LONG_LONG_INT,
            MPI_UNSIGNED_LONG_LONG,
            MPI_UNSIGNED_LONG_LONG
        };

        /* Assert that the size of a pointer is size of MPI_LONG_LONG - otherwise this will fail spectacularly */
        MPI_Type_size(MPI_LONG_LONG_INT, &type_size);
        assert(type_size == sizeof(intptr_t));

        MPI_Type_create_struct(
            count,
            array_of_blocklengths,
            array_of_displacements,
            array_of_types,
            &(os_info->syscall_dt)
        );
        MPI_Type_commit(&(os_info->syscall_dt));
    }

    /* Global root prepares array */
    if (global_id == 0) {
        os_info->global_program_indices = malloc(sizeof(int) * num_instances);
        if (!os_info->global_program_indices) {
            vb_error_root("Out of memory\n");
            return VB_GENERIC_ERROR;
        }

        os_info->global_arr = malloc(sizeof(vb_syscall_info_t) * MAX_SYSCALLS * num_instances);
        if (!os_info->global_arr) {
            vb_error_root("Out of memory\n");
            free(os_info->global_program_indices);
            return VB_GENERIC_ERROR;
        }
    }

    return VB_SUCCESS;
}

static int
run_kernel(vb_instance_t    * instance,
           vb_os_info_t     * os_info,
           unsigned long long iterations)
{
    int i, status, global_seed, nr_programs;
    int * program_indices;
    cJSON * json_obj, *program_dict, * true_list;
    void * lib_handle;
    struct sigaction act, old_act;
    char saved_exec_mode;

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
    status = get_json_object(instance, os_info->json_file, &json_obj);
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
    status = build_vb_program_list(true_list, lib_handle, &(os_info->program_list));
    if (status != VB_SUCCESS) {
        vb_error("Unable to build program list\n");
        goto out_json;
    }


    /* Do this until we get a successful program run */
    do {
        /* (re-)init syscall/program files */

        /* if we're not using MPI_Gather to aggregate syscall info,
         * every instance has to do this 
         */
#ifdef USE_MPI_GATHER
        if (instance->rank_info.global_id == 0) {
#endif
            if (os_info->generate_syscall_csv) {
                status = init_syscall_info_file(instance, os_info);
                if (status != VB_SUCCESS) {
                    vb_error_root("Could not initialize syscall CSV\n");
                    goto out_csv;
                }
            }

            if (os_info->generate_program_csv) {
                status = init_program_info_file(instance, os_info);
                if (status != VB_SUCCESS) {
                    vb_error_root("Could not initialize program CSV\n");
                    goto out_csv;
                }
            }
#ifdef USE_MPI_GATHER
        }
#endif

        /* Disable any failed programs */
        status = disable_failed_programs(instance, os_info->program_list);
        if (status != VB_SUCCESS) {
            vb_error("Unable to disable failed programs\n");
            goto out_prog_list;
        }

        /* To start, set everything to INITING, unless some programs are already disabled */
        for (i = 0; i < os_info->program_list->nr_programs; i++) {
            vb_program_t * program = &(os_info->program_list->program_list[i]);

            if (program->status == VB_ENABLED)
                program->status = VB_INITING;
        }

        /* Derive set of programs to execute
         *   (first, disable 'direct' mode,
         */
        saved_exec_mode = os_info->execution_mode;
        os_info->execution_mode = 'f';
        status = derive_program_set(instance, os_info, &program_indices);
        os_info->execution_mode = saved_exec_mode;

        if (status != VB_SUCCESS) {
            vb_error_root("Unable to derive set of successful programs\n");
            goto out_prog_list;
        } 

        /* Prepare the root process for system call data aggregation */
        if (os_info->generate_syscall_csv || os_info->generate_program_csv) {
            status = __prepare_root_aggregation(instance, os_info);
            if (status != VB_SUCCESS) {
                vb_error_root("Unable to prepare for system call data aggregation\n");
                goto out_aggregate;
            }
        }
        
        /* Run the kernel */
        status = __run_kernel(instance, os_info, iterations, program_indices);

        if (instance->rank_info.global_id == 0) {
            free(os_info->global_arr);
            free(os_info->global_program_indices);
        }
            
        /* Store the program list in the XML */
        if ((status == VB_SUCCESS) && (instance->rank_info.global_id == 0)) {
            save_program_info(instance, os_info, program_indices);
        }

        free(program_indices);
    } while (status == VB_OS_RESTART);

out_aggregate:
out_prog_list:
out_csv:
    free(os_info->program_list->program_list);
    free(os_info->program_list);

out_json:
    cJSON_Delete(json_obj);

    return status;
}

/* 
 * glibc does not provide a function to remove non-empty
 * directories ... so we have to do it ourselves 
 *
 * TODO: this is currently broken
 */
static void
rm_working_dir(char * dir_name)
{
    DIR * dir;
    struct dirent * dent;
    int status;

    if (chdir(dir_name) != 0) {
        vb_error("Could not chdir to %s: %s. Current dir: %s\n", dir_name, strerror(errno),
            get_current_dir_name());
        return;
    }

    dir = opendir(dir_name);
    while ((dent = readdir(dir)) != NULL) {
        if (dent->d_type == DT_DIR) {
            if ((strcmp(dent->d_name, ".") == 0) ||
                (strcmp(dent->d_name, "..") == 0)
               )
            {
                continue;
            }

            /* recursively delete subdirectory */
            rm_working_dir(dent->d_name);
        } else {
            status = unlink(dent->d_name);

            if (status != 0) {
                vb_error("Could not remove file %s of type %d: %s\n",
                    dent->d_name, dent->d_type, strerror(errno));
            }
        }
    }

    closedir(dir);

    if (chdir("..") != 0) {
        vb_error("Could not chdir to ..: %s\n", strerror(errno));
    }

    /* now remove the directory */
    rmdir(dir_name);
}

static void
usage(void)
{
    vb_error_root("\noperating_system requires args:\n"
        "  arg 0: <path to corpus json file>\n"
        "  arg 1: <concurrency mode>: 's' OR 'r' (single or random)\n"
        "       single: each instance concurrently executes the same program order\n"
        "       random: each instance concurrently executes a random program order\n"
        "  arg 2: <execution mode>: 'f' OR 'd' (fork or direct)\n"
        "       fork: instances fork off a child to execute the programs\n"
        "       direct: instances directly execute programs, and potentially die if they crash\n"
        "  arg 3: <number of programs per iteration>\n"
        "  arg 4: <generate program csv> (0/1)\n"
        "  arg 5: <generate syscall csv> (0/1)\n"
        "  arg 6: <directory to store program/syscall data>\n"
        "  arg 7: <seed for random program selection order> (optional: default = time(NULL))\n"
        );
}

#ifdef USE_CORPUS
int 
vb_kernel_operating_system(int             argc,
                           char         ** argv,
                           vb_instance_t * instance)
{
    char * json_file, * program_id, * data_dir;
    char selection_mode, concurrency_mode, execution_mode;
    bool do_program_csv, do_syscall_csv;
    int nr_programs_per_iter, selection_seed;
    int status;
    vb_os_info_t * os_info;

    if (argc != 7 && argc != 8) {
        usage();
        return VB_BAD_ARGS;
    }

    json_file            = argv[0];
    concurrency_mode     = argv[1][0];
    execution_mode       = argv[2][0];
    nr_programs_per_iter = atoi(argv[3]);

    do_program_csv = (atoi(argv[4]) == 1) ? true : false;
    do_syscall_csv = (atoi(argv[5]) == 1) ? true : false;
    data_dir = argv[6];

    if (argc == 8) 
        selection_seed = atoi(argv[7]);
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

    os_info = malloc(sizeof(vb_os_info_t));
    if (!os_info) {
        vb_error("malloc() failed: %s\n", strerror(errno));
        return VB_GENERIC_ERROR;
    }

    /* populate os_info */
    memset(os_info, 0, sizeof(vb_os_info_t)); 

    os_info->concurrency_mode = concurrency_mode;
    os_info->execution_mode  = execution_mode;
    os_info->nr_programs_per_iteration = nr_programs_per_iter;
    os_info->selection_seed = selection_seed;

    os_info->generate_program_csv = do_program_csv;
    os_info->generate_syscall_csv = do_syscall_csv;

    os_info->toplevel_working_dir = get_current_dir_name();
    if (os_info->toplevel_working_dir == NULL) {
        vb_error("Could not determine toplevel working directory\n");
        free(os_info);
        return VB_GENERIC_ERROR;
    }

    /* Get full path of json file */
    os_info->json_file = realpath(json_file, NULL);
    if (os_info->json_file == NULL) {
        vb_error("Could not determine full path of json file %s\n", json_file);
        free(os_info);
        return VB_GENERIC_ERROR;
    }

    /* Make sure we can create files in the data dir */
    os_info->syscall_data_dir = data_dir;
    if ((do_program_csv || do_syscall_csv) && 
        (instance->rank_info.global_id == 0)) 
    {
        int tmp_fd;
        char * tmp_file;

        asprintf(&tmp_file, "%s/test.txt", os_info->syscall_data_dir);
        tmp_fd = open(tmp_file, O_CREAT | O_TRUNC | O_RDWR);

        if (tmp_fd < 0) {
            vb_error_root("Error: could not create a file in %s: %s\n", os_info->syscall_data_dir, strerror(errno));
            free(tmp_file);
            return VB_BAD_ARGS;
        }

        unlink(tmp_file);
        free(tmp_file);
        close(tmp_fd);
    }

    /* generate private working dir */
    snprintf(os_info->active_working_dir, MAX_NAME_LEN, "%s/.vb_os_%d", 
        os_info->toplevel_working_dir,
        instance->rank_info.local_id);

    /* jump into working dir to run experiment */
    assert(mkdir(os_info->active_working_dir, 0755) == 0);

    vb_print_root("\nRunning operating_system\n"
        "  Num iterations    : %llu\n"
        "  JSON file         : %s\n"
        "  Concurrency mode  : %s\n"
        "  Execution mode    : %s\n"
        "  Programs per iter : %d\n"
        "  Selection seed    : %d\n"
        "  Per-program CSV   : %s\n"
        "  Per-syscall CSV   : %s\n"
        "  Data collected in : %s\n",
        instance->options.num_iterations,
        os_info->json_file,
        (concurrency_mode == 's') ? "single"   : "random",
        (  execution_mode == 'd') ? "direct"   : "fork",
        nr_programs_per_iter,
        selection_seed,
        (do_program_csv) ? "Y" : "N",
        (do_syscall_csv) ? "Y" : "N",
        (do_program_csv || do_syscall_csv) ? data_dir : "<disabled>"
    );

    status = run_kernel(instance, os_info, instance->options.num_iterations);

    // rm_working_dir(os_info->active_working_dir);
     
    /* On failure, clear out program/syscall files */
    if (status != VB_SUCCESS) {
        if (os_info->program_fp != NULL)
            unlink(os_info->program_filename);

        if (os_info->syscall_fp != NULL)
            unlink(os_info->syscall_filename);
    }

    free(os_info->toplevel_working_dir);
    free(os_info->json_file);
    free(os_info);

    return status;
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
