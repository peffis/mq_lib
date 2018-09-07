-module(mq).
-behaviour(gen_server).

%% API.
-export([start_link/1]).

-compile([{parse_transform, lager_transform}]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-export([add_route/1, add_route/2, remove_route/1, cast/2, register_handler/1]).


-record(state, {
	  connection = undefined,
	  handler = undefined
	 }).




%% API.
start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [], []).

add_route(RoutingKey) ->
    add_route(RoutingKey, #{}).

add_route(RoutingKey, Opts) when is_list(RoutingKey) ->
    add_route(list_to_binary(RoutingKey), Opts);
add_route(RoutingKey, Opts) ->
    gen_server:cast(?MODULE, {add_route, RoutingKey, Opts}).

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

register_handler(Fun) ->
    gen_server:cast(?MODULE, {register_handler, Fun}).



%% gen_server.

init(_Args) ->
    ServerHost = config:get(mq_server_host),
    ServerPort = config:get(mq_server_port),

    {ok, Connection} = nats:connect(list_to_binary(ServerHost), ServerPort, #{verbose => true}),
    lager:info("connected to NATS bus at ~s:~p", [ServerHost, ServerPort]),
    {ok, #state{connection = Connection}}.



handle_call(_Request, _From, State) ->
    {reply, ignored, State}.


handle_cast({add_route, Subject, Opts}, #state{connection = Conn} = State) ->
    ok = nats:sub(Conn, Subject, Opts),
    {noreply, State};

handle_cast({remove_route, Subject}, #state{connection = Conn} = State) ->
    ok = nats:unsub(Conn, Subject),
    {noreply, State};

handle_cast({cast, Msg, Subject}, #state{connection = Conn} = State) ->
    ok = nats:pub(Conn, Subject, #{payload => Msg}),
    {noreply, State};

handle_cast({register_handler, Fun}, State) ->
    {noreply, State#state{handler = Fun}};

handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info({_Conn, {msg, _Subject, _ReplyTo, _Payload}}, #state{handler = undefined} = State) ->
    {noreply, State};

handle_info({Conn, {msg, Subject, _ReplyTo, Payload}}, #state{handler = Handler, connection = Conn} = State) ->
    Handler(Payload, Subject),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
