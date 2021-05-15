.SUFFIXES: .cc .o

CC=g++

SRCDIR=src/
INC=include/
LIBS=lib/

# SRCS:=$(wildcard src/*.c)
# OBJS:=$(SRCS:.c=.o)

# main source file
TARGET_SRC:=$(SRCDIR)main.cc
TARGET_OBJ:=$(SRCDIR)main.o
STATIC_LIB:=$(LIBS)libbpt.a

# Include more files if you write another source file.
SRCS_FOR_LIB:= \
	$(SRCDIR)debug.cc \
	$(SRCDIR)file.cc \
	$(SRCDIR)page.cc \
	$(SRCDIR)table.cc \
	$(SRCDIR)buffer.cc \
	$(SRCDIR)find.cc \
	$(SRCDIR)insert.cc \
	$(SRCDIR)delete.cc \
	$(SRCDIR)find_trx.cc \
	$(SRCDIR)trx.cc \
	$(SRCDIR)lock.cc \
	$(SRCDIR)log.cc \
	$(SRCDIR)interface.cc

OBJS_FOR_LIB:=$(SRCS_FOR_LIB:.cc=.o)

CFLAGS+= -g -std=c++11 -fPIC -I $(INC)

TARGET=main

all: $(TARGET)

$(TARGET): $(TARGET_OBJ) $(STATIC_LIB) 
	$(CC) $(CFLAGS) $< -o $@ -L $(LIBS) -lbpt -lpthread
	clang++ -MJ $@.json $(CFLAGS) -o $@ -c $^

%.o: %.cc
	$(CC) $(CFLAGS) $^ -c -o $@
	clang++ -MJ $@.json $(CFLAGS) -o $@ -c $^

clean:
	rm $(TARGET) $(TARGET_OBJ) $(OBJS_FOR_LIB) $(LIBS)*

$(STATIC_LIB): $(OBJS_FOR_LIB)
	ar cr $@ $^
