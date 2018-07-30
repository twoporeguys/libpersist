export CC := clang
export CXX := clang++
PYTHON_VERSION := python3
PREFIX ?= /usr/local
BUILD_PYTHON ?= ON
BUILD_TYPE ?= Release

.PHONY: all clean build  install uninstall

all: build

build:
	mkdir -p build
	cd build && cmake .. \
	    -DPYTHON_VERSION=$(PYTHON_VERSION) \
	    -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) \
	    -DCMAKE_INSTALL_PREFIX=$(PREFIX) \
	    -DBUILD_PYTHON=$(BUILD_PYTHON)
	make -C build

clean:
	rm -rf *~ build

install:
	make -C build install
	@if [ "`uname -s`" = "Linux" ]; then ldconfig || true; fi

uninstall:
	make -C build uninstall
