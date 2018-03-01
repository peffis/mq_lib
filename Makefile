PROJECT = mq
PROJECT_DESCRIPTION = Rabbit Message queue conveniance lib
PROJECT_VERSION = 0.0.1

DEPS = amqp_client
dep_amqp_client = git https://github.com/jbrisbin/amqp_client master
include erlang.mk
