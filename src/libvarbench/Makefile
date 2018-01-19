PROG = $(MAKE_DIR)/lib/libvarbench.so
SRCS = $(wildcard *.c)
OBJS = $(patsubst %.c, %.o, $(SRCS))

all: $(PROG) $(HEADERS)

$(PROG): $(OBJS) $(LIBS)
	@echo "	$(CC) $^ $(CFLAGS) $(LIBS) -shared -o $@ $(LDFLAGS)"
	@$(CC) $^ $(CFLAGS) $(LIBS) -shared -o $@ $(LDFLAGS)

$(OBJS): $(SRCS)
	@echo "	$(CC) $(CFLAGS) -c -fPIC $^"
	@$(CC) $(CFLAGS) -c -fPIC $^

.PHONY: clean
clean:
	@echo "	$(RM) $(OBJS)"
	@$(RM) $(OBJS)
	@echo "	$(RM) -rf $(LIB_DIR)"
	@$(RM) -rf $(LIB_DIR)
	@echo "	$(RM) $(PROG)"
	@$(RM) $(PROG)
