.PHONY: all
all: helm-all

ROOT_DIR := ../..
TOOLS_DIR := $(ROOT_DIR)/tools
export TOOLBIN := $(TOOLS_DIR)/bin
export ABSTOOLBIN := $(shell pwd)/$(TOOLS_DIR)/bin

DOCKER_NAME := afm
DOCKER_TAGNAME := latest

include $(ROOT_DIR)/.mk/helm.mk
include $(ROOT_DIR)/.mk/docker.mk
