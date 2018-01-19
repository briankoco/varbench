SRC = libsyzcorpus.c
OBJ = libsyzcorpus.o
TARGET = libsyzcorpus.so

all: $(TARGET)

$(TARGET): $(OBJ)
	@echo "	$(CC) -shared -o $(TARGET) $(OBJ)"
	@$(CC) -shared -o $(TARGET) $(OBJ)

$(OBJ): $(SRC)
	@echo "	$(CC) -fpic -c $(SRC)"
	@$(CC) -fpic -c $(SRC)

veryclean:
	@echo "	$(RM) $(TARGET) $(OBJ)"
	@$(RM) $(TARGET) $(OBJ)
