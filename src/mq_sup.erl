-module(mq_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Procs = [
	     {mq, {mq, start_link, [mq]}, 
	      permanent, 10000, worker, [mq]}
	    ],
    {ok, {{one_for_one, 1, 5}, Procs}}.
