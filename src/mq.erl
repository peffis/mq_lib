-module(mq).
-behaviour(gen_server).

%% API.
-export([start_link/1]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).
-export([get_env/1, add_route/1, remove_route/1, cast/2, call/3, register_handler/1]).
-export([test_client/0, test_server/0]).


-record(state, {
	  connection = undefined,
	  channel = undefined,
	  queue = undefined,
	  sub_tag = undefined,
	  handler = undefined,
	  rpc_queue = undefined,
	  rpc_calls = dict:new()
	 }).

-include("deps/amqp_client/include/amqp_client.hrl").
-ifdef(flycheck).
-include("deps/rabbit_common/include/rabbit_framing.hrl").
-endif.

-define(THE_EXCHANGE, <<"the_exchange">>).

exchange() ->
    exchange(get_env(exchange)).

exchange(undefined) ->
    ?THE_EXCHANGE.

%% API.
test_client() ->
    call(<<"hello">>, <<"rpc_call">>, <<"secret">>).

test_server() ->
    mq:add_route(<<"rpc_call">>),
    mq:register_handler(fun(_Payload, _RKey) ->
				     <<"okidokay">>
			     end).
    

start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [], []).

get_env(EnvAtom) ->
    case os:getenv(string:to_upper(atom_to_list(EnvAtom))) of 
        false ->
            application:get_env(mq, EnvAtom);
        Value -> 
            {ok, Value}
    end.

add_route(RoutingKey) when is_list(RoutingKey) ->
    add_route(list_to_binary(RoutingKey));
add_route(RoutingKey) ->
    gen_server:cast(?MODULE, {add_route, RoutingKey}).

remove_route(RoutingKey) when is_list(RoutingKey) ->
    remove_route(list_to_binary(RoutingKey));
remove_route(RoutingKey) ->
    gen_server:cast(?MODULE, {remove_route, RoutingKey}).

cast(Msg, RoutingKey) when is_list(Msg) ->
    cast(list_to_binary(Msg), RoutingKey);
cast(Msg, RoutingKey) when is_list(RoutingKey) ->
    cast(Msg, list_to_binary(RoutingKey));
cast(Msg, RoutingKey) when is_binary(Msg) ->
    gen_server:cast(?MODULE, {cast, Msg, RoutingKey}).

call(Msg, RoutingKey, Id) when is_list(Msg) ->
    call(list_to_binary(Msg), RoutingKey, Id);
call(Msg, RoutingKey, Id) when is_list(RoutingKey) ->
    call(Msg, list_to_binary(RoutingKey), Id);
call(Msg, RoutingKey, Id) when is_list(Id) ->
    call(Msg, RoutingKey, list_to_binary(Id));
call(Msg, RoutingKey, Id) when is_binary(Msg) ->
    gen_server:cast(?MODULE, {call, Msg, RoutingKey, Id, self()}),
    receive 
	{Id, Payload} ->
	    Payload

    after 10000 -> 
	    io:format("call (~p, ~p, ~p) timedout ~n", [Msg, RoutingKey, Id])
    end.
    
register_handler(Fun) ->
    gen_server:cast(?MODULE, {register_handler, Fun}).

    

%% gen_server.

init(_Args) ->
    {ok, ServerHost} = get_env(mq_server_host),
    ServerPort = case get_env(mq_server_port) of 
		     {ok, SP} when is_list(SP) ->
			 list_to_integer(SP);

		     {ok, SP} when is_integer(SP) ->
			 SP
		 end,

    {ok, Connection} = amqp_connection:start(#amqp_params_network{host = ServerHost, 
								  port = ServerPort}),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    Declare = #'exchange.declare'{exchange = exchange(), type = <<"topic">>},
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, Declare),
    #'queue.declare_ok'{queue = Queue} = amqp_channel:call(Channel, #'queue.declare'{}),
    
    Sub = #'basic.consume'{queue = Queue},
    #'basic.consume_ok'{consumer_tag = Tag} = amqp_channel:call(Channel, Sub),

    Binding = #'queue.bind'{queue       = Queue,
			    exchange    = exchange(),
			    routing_key = Queue},
    #'queue.bind_ok'{} = amqp_channel:call(Channel, Binding),


    {ok, #state{connection = Connection, 
		channel = Channel, 
		queue = Queue, 
		sub_tag = Tag,
		rpc_queue = Queue
	       }}.


handle_call(_Request, _From, State) ->
    {reply, ignored, State}.


handle_cast({add_route, RoutingKey}, State) ->
    Binding = #'queue.bind'{queue       = State#state.queue,
			    exchange    = exchange(),
			    routing_key = RoutingKey},
    #'queue.bind_ok'{} = amqp_channel:call(State#state.channel, Binding),
    {noreply, State};

handle_cast({remove_route, RoutingKey}, State) ->
    Binding = #'queue.unbind'{queue       = State#state.queue,
			      exchange    = exchange(),
			      routing_key = RoutingKey},
    #'queue.unbind_ok'{} = amqp_channel:call(State#state.channel, Binding),
    {noreply, State};

handle_cast({cast, Msg, RoutingKey}, State) ->
    Payload = Msg,
    Publish = #'basic.publish'{exchange = exchange(), routing_key = RoutingKey},
    amqp_channel:cast(State#state.channel, Publish, #amqp_msg{payload = Payload}),
    {noreply, State};

handle_cast({call, Msg, RoutingKey, CorrelationId, RPid}, State) ->
    Payload = Msg,
    Props = #'P_basic'{correlation_id = CorrelationId,
		       reply_to = State#state.rpc_queue},
    Publish = #'basic.publish'{exchange = exchange(), 
			       routing_key = RoutingKey
			      },
    amqp_channel:cast(State#state.channel, Publish, #amqp_msg{props = Props, payload = Payload}),
    {noreply, State#state{rpc_calls = dict:store(CorrelationId, RPid, State#state.rpc_calls)}};

handle_cast({register_handler, Fun}, State) ->
    {noreply, State#state{handler = Fun}};

handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};

handle_info(#'basic.cancel_ok'{}, State) ->
    {noreply, State};

handle_info({#'basic.deliver'{delivery_tag = Tag, routing_key = RKey}, Content}, State) ->
    #amqp_msg{payload = Payload, 
	      props = #'P_basic'{correlation_id = CorrelationId,
				 reply_to = Q}} = Content,    

    case State#state.handler of

	undefined -> nop;

	Fun ->

	    Response = Fun(Payload, RKey),

	    case Response of 
		noreply -> nop;

		_ ->
		    
		    Props = #'P_basic'{correlation_id = CorrelationId},
		    Publish = #'basic.publish'{exchange = exchange(),
					       routing_key = Q,
					       mandatory = true},

		    amqp_channel:cast(State#state.channel, Publish, #amqp_msg{props = Props,
									      payload = Response})
	    end
    end,

    amqp_channel:cast(State#state.channel, #'basic.ack'{delivery_tag = Tag}),

    NextState = case dict:find(CorrelationId, State#state.rpc_calls) of
		    error -> State;
		    {ok, Pid} -> Pid ! {CorrelationId, Payload}, 
				 State#state{rpc_calls = dict:erase(CorrelationId, 
								    State#state.rpc_calls)}
		end,		 


    
    {noreply, NextState};

handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
