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

-export([subscribe/2, subscribe/3, unsubscribe/1, publish/2, publish/3]).


-record(state, {
	  connection = undefined,
	  handlers = #{}
	 }).

%% API.
start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [], []).

subscribe(Subject, Handler) ->
    subscribe(Subject, Handler, #{}).

subscribe(Subject, Handler, Opts) when is_list(Subject) ->
    subscribe(list_to_binary(Subject), Handler, Opts);
subscribe(Subject, Handler, Opts) when is_binary(Subject) ->
    gen_server:cast(?MODULE, {subscribe, Subject, Handler, Opts}).


unsubscribe(Subject) when is_list(Subject) ->
    unsubscribe(list_to_binary(Subject));
unsubscribe(Subject) when is_binary(Subject) ->
    gen_server:cast(?MODULE, {unsubscribe, Subject}).



publish(Msg, Subject) when is_list(Msg) ->
    publish(list_to_binary(Msg), Subject);
publish(Msg, Subject) when is_list(Subject) ->
    publish(Msg, list_to_binary(Subject));
publish(Msg, Subject) when is_binary(Msg) ->
    gen_server:cast(?MODULE, {publish, Msg, Subject}).

publish(Msg, Subject, ReplyTo) when is_list(Msg) ->
    publish(list_to_binary(Msg), Subject, ReplyTo);
publish(Msg, Subject, ReplyTo) when is_list(Subject) ->
    publish(Msg, list_to_binary(Subject), ReplyTo);
publish(Msg, Subject, ReplyTo) when is_list(ReplyTo) ->
    publish(Msg, Subject, list_to_binary(ReplyTo));
publish(Msg, Subject, ReplyTo) when is_binary(Msg), is_binary(Subject), is_binary(ReplyTo) ->
    gen_server:cast(?MODULE, {publish, Msg, Subject, ReplyTo}).



%% gen_server.

init(_Args) ->
    ServerHost = config:get(mq_server_host),
    ServerPort = config:get(mq_server_port),

    {ok, Connection} = nats:connect(list_to_binary(ServerHost), ServerPort, #{verbose => true}),
    lager:info("connected to NATS bus at ~s:~p", [ServerHost, ServerPort]),
    {ok, #state{connection = Connection}}.



handle_call(_Request, _From, State) ->
    {reply, ignored, State}.


handle_cast({subscribe, Subject, Handler, Opts}, #state{connection = Conn, handlers = Handlers} = State) ->
    ok = nats:sub(Conn, Subject, Opts),
    {noreply, State#state{handlers = Handlers#{Subject => Handler}}};

handle_cast({unsubscribe, Subject}, #state{connection = Conn, handlers = Handlers} = State) ->
    ok = nats:unsub(Conn, Subject),
    {noreply, State#state{handlers = maps:remove(Subject, Handlers)}};

handle_cast({publish, Msg, Subject}, #state{connection = Conn} = State) ->
    ok = nats:pub(Conn, Subject, #{payload => Msg}),
    {noreply, State};

handle_cast({publish, Msg, Subject, ReplyTo}, #state{connection = Conn} = State) ->
    ok = nats:pub(Conn, Subject, #{payload => Msg, reply_to => ReplyTo}),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info({Conn, {msg, Subject, ReplyTo, Payload}}, #state{handlers = Handlers, connection = Conn} = State) ->
    case maps:get(Subject, Handlers, undefined) of
        undefined ->
            ok;

        Handler -> Handler(Payload, ReplyTo)
    end,
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
