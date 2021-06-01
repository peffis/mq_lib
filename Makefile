PROJECT = mq
PROJECT_DESCRIPTION = Nats conveniance layer
PROJECT_VERSION = 0.0.1

DEPS = teacup_nats lager
dep_teacup_nats = git https://github.com/yuce/teacup_nats.git master
dep_lager = git https://github.com/erlang-lager/lager.git master
include erlang.mk
