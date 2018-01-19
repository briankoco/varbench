PROG = $(MAKE_DIR)/varbench
SRCS = $(wildcard *.c)
OBJS = $(patsubst %.c, %.o, $(SRCS))

all: $(PROG) $(HEADERS) $(LIBS)

$(PROG): $(OBJS) $(LIBS)
	@echo "	$(CC) $^ $(CFLAGS) $(LIBS) -o $@ $(LDFLAGS)"
	@$(CC) $^ $(CFLAGS) $(LIBS) -o $@ $(LDFLAGS)

$(OBJS): $(SRCS)
	@$(CC) $(CFLAGS) -c $^

.PHONY: clean
clean:
	@echo "	$(RM) $(OBJS)"
	@$(RM) $(OBJS)
	@echo "	$(RM) -rf $(LIB_DIR)"
	@$(RM) -rf $(LIB_DIR)
	@echo "	$(RM) $(PROG)"
	@$(RM) $(PROG)
