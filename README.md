# Varbench

Public git repository for the Varbench performance analysis framework

## Getting Started

### Prerequisites

You will need to install ```hwloc``` and ```MPI```. Any flavor of MPI should
work, though we have only tested with OpenMPI.

### Compiling

Simply invoke

```
make
```

to build a varbench executable

### Compiling the "operating_system" kernel

To build an executable that contains an operating system stresser kernel,
invoke

```
make OPERATING_SYSTEM=default
```

This will allow you to run a kernel that does nothing but issue system
calls.

### Running

```
mpirun -np <n procs> [any other MPI options] ./varbench -k <kernel> [any other varbench options]
```

Running ./varbench with no arguments will give you a list of options and 
describe how to use the framework.

### Varbench methodology

Varbench is a performance analysis framework to characterize
performance variability on a system. 

Varbench programs consist of three logical components: (1) instances,
(2) iterations, and (3) kernels.

Instances can be thought of as "ranks" in MPI terminology. Each
instance essentially executes the same code as every other instance,
but on its own private problem state.

Iterations are self-explanatory. By default a varbench program will
execute for 100 iterations. After each instance executes an iteration,
there is a global MPI Barrier at which point timing information is
collected and stored at the root process.

Kernels constitute the singular workload executing in each instance.
Varbench currently provides a set of workloads that stress shared
resource performance.


### Data analysis

Each execution of a varbench kernel generates two files describing the results
of the experiment. The first is an XML file that gives metadata about the run,
including the options you selected, the arguments for the kernel you ran, and
other information such as the processor/node topologies. The other file is a 
CSV that gives detailed timing breakdowns for each instance and each iteration.

A set of programs is available under the vb-stats/vis directory for visualizing
experiments.


### Topology detection

Many varbench kernels are tuned to low level characteristics of the
architecture. For all kernels, each node is inspected to determine the number
of processor sockets, number of cores per socket, and the number of SMT threads
per core (e.g., `hyperthreads`). By default, these characteristics are detected
via the ```hwloc``` library. However, some architectures and operating systems
may not expose the interfaces required by hwloc to perform topology detection,
and so we provide an alternative for manually specifying these parameters via
a JSON file at the command line:

```
mpirun -np <...> ./varbench -t topo.json <...>
```
The JSON _must_ have the following format:
```
{ 
    "processor_info" : 
        {
            "num_processors"      : 4,
            "num_sockets"         : 1,
            "cores_per_socket"    : 2,
            "hw_threads_per_core" : 2
        },

    "processor_list" : [
        {
            "os_core"      : 0,
            "socket"       : 0,
            "core"         : 0,
            "hw_thread"    : 0
        },

        {
            "os_core"      : 1,
            "socket"       : 0,
            "core"         : 0,
            "hw_thread"    : 1
        },

        ... (one object for each processor)

    ]
}
```

The values for ```os_core```, ```socket```, ```core```, and ```hw_thread``` will
of course vary based on the characteristics of your architecture and operating
system.

## Authors

* **Brian Kocoloski**
