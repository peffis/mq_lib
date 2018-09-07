-module(mq_app).
-behaviour(application).

-export([start/2]).
-export([stop/1]).

-compile([{parse_transform, lager_transform}]).

start(_Type, _Args) ->
    mq_sup:start_link().

stop(_State) ->
    ok.
