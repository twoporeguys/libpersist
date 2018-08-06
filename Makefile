export CC := clang
export CXX := clang++
PYTHON_VERSION := python3
PREFIX ?= /usr/local
BUILD_PYTHON ?= ON
BUILD_TYPE ?= Release
OS := $(shell uname -s)

.PHONY: bootstrap bootstrap_librpc bootstrap_$(OS) all clean build install
.PHONY: uninstall

all: build

bootstrap: bootstrap_$(OS) bootstrap_librpc

bootstrap_librpc:
	mkdir -p build
	git -C build clone https://github.com/twoporeguys/librpc.git
	make -C build/librpc bootstrap build BUILD_TYPE=Debug
	make -C build/librpc install

bootstrap_Linux:
	apt-get -y install \
	    cmake clang git libglib2.0-dev libsqlite3-dev python3-dev \
	    libblocksruntime-dev

bootstrap_Darwin:
	port install cmake pkgconfig glib2 sqlite3
	port select --set python3 python36

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
