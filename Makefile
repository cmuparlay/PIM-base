CC := g++
DPU_DIR := dpu
HOST_DIR := host
COMMON_LIB_DIR := include/common
DPU_LIB_DIR := include/dpu
HOST_LIB_DIR := include/host
BUILDDIR ?= build
NR_TASKLETS ?= 16
NR_DPUS ?= 2560

define conf_filename
	${BUILDDIR}/.NR_DPUS_$(1)_NR_TASKLETS_$(2).conf
endef
CONF := $(call conf_filename,${NR_DPUS},${NR_TASKLETS})

HOST_TARGET := ${BUILDDIR}/pim_base_host
DPU_TARGET := ${BUILDDIR}/pim_base_dpu

COMMON_DIR := common
COMMON_INCLUDES := $(wildcard ${COMMON_DIR}/*.hpp)
HOST_LIBS := $(wildcard ${HOST_LIB_DIR}/*.hpp)
HOST_INCLUDES := $(wildcard ${HOST_DIR}/*.hpp) 
HOST_SOURCES := $(wildcard ${HOST_DIR}/*.cpp) 
DPU_LIBS := $(wildcard ${DPU_LIB_DIR}/*.h)
DPU_INCLUDES := $(wildcard ${DPU_DIR}/*.h)
DPU_SOURCES := $(wildcard ${DPU_DIR}/*.c)

.PHONY: all clean test

__dirs := $(shell mkdir -p ${BUILDDIR})

COMMON_FLAGS := -Wall -Wno-unused-function -Wextra -g -I${COMMON_DIR} -I${COMMON_LIB_DIR}
HOST_LIB_FLAGS := -I${HOST_DIR} -isystem parlaylib/include -isystem argparse/include -Itimer_tree/include
HOST_FLAGS := ${COMMON_FLAGS} -std=c++17 -lpthread -O3 ${HOST_LIB_FLAGS} -I${HOST_LIB_DIR} `dpu-pkg-config --cflags --libs dpu` -DNR_TASKLETS=${NR_TASKLETS} -DNR_DPUS=${NR_DPUS}
DPU_FLAGS := ${COMMON_FLAGS} -I${DPU_DIR} -I${DPU_LIB_DIR} -O2 -DNR_TASKLETS=${NR_TASKLETS}

all: ${HOST_TARGET} ${DPU_TARGET}

${CONF}:
	$(RM) $(call conf_filename,*,*)
	touch ${CONF}

${HOST_TARGET}: ${HOST_SOURCES} ${HOST_LIBS} ${HOST_INCLUDES} ${COMMON_INCLUDES} ${CONF}
	$(CC) -o $@ ${HOST_SOURCES} ${HOST_FLAGS}

${DPU_TARGET}: ${DPU_SOURCES} ${DPU_LIBS} ${COMMON_INCLUDES} ${CONF}
	dpu-upmem-dpurte-clang ${DPU_FLAGS} -o $@ ${DPU_SOURCES}

clean:
	$(RM) -r $(BUILDDIR)

test_c: ${HOST_TARGET} ${DPU_TARGET}
	./${HOST_TARGET}

test: test_c

