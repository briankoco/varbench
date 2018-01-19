MAKE_DIR    = $(PWD)
SRC_DIR     = $(MAKE_DIR)/src
KERNELS_DIR = $(SRC_DIR)/kernels
MODULES_DIR = $(SRC_DIR)/modules
UTILS_DIR   = $(SRC_DIR)/utils
LIBVB_DIR   = $(SRC_DIR)/libvarbench
DRIVER_DIR  = $(SRC_DIR)/driver
INCLUDE_DIR = $(MAKE_DIR)/include
LIB_DIR     = $(MAKE_DIR)/lib
CORPUS_DIR  = $(KERNELS_DIR)/corpuses/sample-corpus

KERNELS = $(LIB_DIR)/libvarbench-kernels.a
MODULES = $(LIB_DIR)/libvarbench-modules.a
UTILS   = $(LIB_DIR)/libvarbench-utils.a
LIBS    = $(KERNELS) $(MODULES) $(UTILS)
HEADERS = $(INCLUDE_DIR)/varbench.h


CC = mpicc
AR = ar

CFLAGS := -D_GNU_SOURCE -pthread -fPIC -I$(INCLUDE_DIR) -I$(UTILS_DIR) $(EXTERN_LIBS)
LDFLAGS :=
EXTERN_LIBS := -lhwloc -lm -lnuma -pthread -ldl


all: driver libvarbench


#### USER OPTIONS ####
# Toggle these if you want to run with papi perf ctr collection
#EXTERN_LIBS += -lpapi
#CFLAGS += -DUSE_PAPI
#### END USER OPTIONS



# Invoke 'make OPERATING_SYSTEM=<corpus directory>' to built an operating_system kernel with a 
# syzkaller corpus.
# To use the default corpus shipped with varbench, invoke: 'make OPERATING_SYSTEM=default'
ifdef OPERATING_SYSTEM
    ifneq ($(OPERATING_SYSTEM),default)
        PATH_TO_CORPUS_DIR=$(OPERATING_SYSTEM)
    else
        PATH_TO_CORPUS_DIR=$(CORPUS_DIR)
    endif

    EXTERN_LIBS += -L$(PATH_TO_CORPUS_DIR) -lsyzcorpus
    CFLAGS += -DUSE_CORPUS -Wl,--no-as-needed -Wl,-rpath=$(PATH_TO_CORPUS_DIR)

corpus:
	@echo "	Build corpus ..."
	@$(MAKE) -C $(PATH_TO_CORPUS_DIR)

else

PATH_TO_CORPUS_DIR=$(CORPUS_DIR)
corpus: ;

endif

LDFLAGS += $(EXTERN_LIBS)


export MAKE_DIR INCLUDE_DIR LIB_DIR HEADERS LIBS CFLAGS LDFLAGS CC AR
driver: corpus
	@$(MAKE) -C $(MODULES_DIR)
	@$(MAKE) -C $(UTILS_DIR)
	@$(MAKE) -C $(KERNELS_DIR)
	@$(MAKE) -C $(DRIVER_DIR)

libvarbench:
	@$(MAKE) -C $(MODULES_DIR)
	@$(MAKE) -C $(UTILS_DIR)
	@$(MAKE) -C $(KERNELS_DIR)
	@$(MAKE) -C $(LIBVB_DIR)

tags:
	-ctags -R
	-cscope -R -b

.PHONY: clean
clean:
	@$(MAKE) -C $(MODULES_DIR) clean
	@$(MAKE) -C $(UTILS_DIR) clean
	@$(MAKE) -C $(KERNELS_DIR) clean
	@$(MAKE) -C $(DRIVER_DIR) clean
	@$(MAKE) -C $(LIBVB_DIR) clean
	@rm -f tags
	@rm -f cscope.out

veryclean: clean
	@$(MAKE) -C $(PATH_TO_CORPUS_DIR) veryclean
