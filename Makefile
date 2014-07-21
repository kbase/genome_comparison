KB_TOP ?= /kb/dev_container
KB_RUNTIME ?= /kb/runtime
DEPLOY_RUNTIME ?= $(KB_RUNTIME)
TARGET ?= /kb/deployment
CURR_DIR = $(shell pwd)
SERVICE_NAME = $(shell basename $(CURR_DIR))
SERVICE_DIR = $(TARGET)/services/$(SERVICE_NAME)
LIB_JARS_DIR = $(KB_TOP)/modules/jars/lib/jars
TARGET_PORT = 7123
THREADPOOL_SIZE = 8

default: compile

deploy-all: deploy

deploy: deploy-client deploy-service deploy-scripts deploy-docs

test: test-client test-service test-scripts

test-client:
	@echo "No tests for client"

test-service:
	./run_tests.sh $(LIB_JARS_DIR)

test-scripts:
	@echo "No tests for scripts"

compile: src
	./make_war.sh $(SERVICE_DIR) $(LIB_JARS_DIR)

deploy-client:
	@echo "No deployment for client"

deploy-service:
	@echo "Service folder: $(SERVICE_DIR)"
	mkdir -p $(SERVICE_DIR)
	cp -f ./dist/service.war $(SERVICE_DIR)
	cp -f ./glassfish_start_service.sh $(SERVICE_DIR)
	cp -f ./glassfish_stop_service.sh $(SERVICE_DIR)
	echo "./glassfish_start_service.sh $(SERVICE_DIR)/service.war $(TARGET_PORT) $(THREADPOOL_SIZE)" > $(SERVICE_DIR)/start_service
	chmod +x $(SERVICE_DIR)/start_service
	echo "./glassfish_stop_service.sh $(TARGET_PORT)" > $(SERVICE_DIR)/stop_service
	chmod +x $(SERVICE_DIR)/stop_service
	./create_config.sh $(SERVICE_DIR) $(THREADPOOL_SIZE)

deploy-scripts:
	@echo "No deployment for scripts"

deploy-docs:
	./prepare_docs.sh
	rsync -a ./docs $(SERVICE_DIR)

clean:
	rm -rf ./classes
	rm -rf ./docs
