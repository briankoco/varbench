LIB  = $(LIB_DIR)/libvarbench-modules.a
SRCS = $(wildcard *.c)
OBJS = $(patsubst %.c, %.o, $(SRCS))

all: $(LIB)

$(LIB): $(OBJS)
	@echo "	mkdir -p $(LIB_DIR)"
	@mkdir -p $(LIB_DIR)
	@echo "	$(AR) cr $@ $^"
	@$(AR) cr $@ $^

$(OBJS): $(SRCS)
	@echo "	$(CC) $(CFLAGS) -c $^"
	@$(CC) $(CFLAGS) -c $^

.PHONY: clean
clean:
	@echo "	$(RM) $(LIB) $(OBJS)"
	@$(RM) $(LIB) $(OBJS)
