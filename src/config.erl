-module(config).

-export([get/1]).

%% reads a configuration value Key. Values are placed in sys.config as atoms
%% but can be overridden by setting environment variables in the os. The environment
%% variable name is given by the atom name converted to a string/list and then uppercased
%% example: my_atom_key would translate to the environment variable MY_ATOM_KEY
get(Key) when is_atom(Key) ->
    {ok, Default} = application:get_env(mq, Key),
    EnvName = string:to_upper(atom_to_list(Key)),
    case os:getenv(EnvName) of
        false ->
            Default; %% assume it has the right type in config

        OSVal ->
            convert(OSVal, Default)
    end.

convert(Val, Default) when is_list(Val), is_integer(Default) ->
    list_to_integer(Val);
convert(Val, Default) when is_list(Val), is_binary(Default) ->
    list_to_binary(Val);
convert(Val, Default) when is_list(Val), is_boolean(Default) ->
    list_to_atom(Val);
convert(Val, Default) when is_list(Val), is_list(Default) ->
    Val.
