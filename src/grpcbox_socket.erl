-module(grpcbox_socket).

-behaviour(gen_server).

-export([start_link/3]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         code_change/3,
         terminate/2]).

%% public api

start_link(Pool, ListenOpts, AcceptorOpts) ->
    gen_server:start_link(?MODULE, [Pool, ListenOpts, AcceptorOpts], []).

%% gen_server api

init([Pool, ListenOpts, PoolOpts]) ->
    {Port, SockOpts} = port_and_socket_opts_from_listen_opts(ListenOpts),
    AcceptorPoolSize = maps:get(size, PoolOpts, 10),
    %% Trapping exit so can close socket in terminate/2
    _ = process_flag(trap_exit, true),
    case gen_tcp:listen(Port, SockOpts) of
        {ok, Socket} ->
            %% acceptor could close the socket if there is a problem
            MRef = monitor(port, Socket),
            grpcbox_pool:accept_socket(Pool, Socket, AcceptorPoolSize),
            {ok, {Socket, MRef}};
        {error, Reason} ->
            {stop, Reason}
    end.

port_and_socket_opts_from_listen_opts(ListenOpts) ->
    ExplicitSocketOpts = maps:get(socket_options, ListenOpts, #{}),
    {Port, SocketOptsBySockFamily, DefaultSocketOpts} = port_and_socket_opts_helper(
        maps:get(ifaddr, ExplicitSocketOpts, undefined),
        maps:get(ip, ListenOpts, undefined),
        maps:get(ip, ExplicitSocketOpts, undefined),
        maps:get(port, ListenOpts, undefined)),
    MergedSockOpts = case ExplicitSocketOpts of
        [] ->
            DefaultSocketOpts;
        M when is_map(M) ->
            %% merge explicit options (preferred) with default options
            maps:to_list(maps:merge_with(
                fun (_K, V1, _V2) -> V1 end,
                M,
                maps:from_list(DefaultSocketOpts)
            ))
    end,
    {Port, SocketOptsBySockFamily ++ MergedSockOpts}.

port_and_socket_opts_helper({local, _UnixPath} = IfAddr, IpFromListenOpts,
                            IpFromSocketOpts, PortFromListenOpts) ->
    Ip = first_defined([IpFromListenOpts, IpFromSocketOpts], undefined),
    Port = first_defined([PortFromListenOpts], 0),
    %% Ip must not be set anywhere, and port must be zero
    %% (or unset, which defaults to 0 here)
    case Port of
        0 ->
            case Ip of
                undefined ->
                    SocketOptsBySockFamily = [{active, false},
                                              {mode, binary},
                                              {packet, raw}],
                    DefaultSocketOpts = [{backlog, 32768}],
                    {Port, SocketOptsBySockFamily, DefaultSocketOpts};
                _ ->
                    error({ip_forbidden_for_unix_socket, {ip, Ip}, {ifaddr, IfAddr}})
            end;
        _ ->
            error({nonzero_port_forbidden_for_unix_socket, {port, Port}, {ifaddr, IfAddr}})
    end;
port_and_socket_opts_helper(IfAddr, IpFromListenOpts,
                            IpFromSocketOpts, PortFromListenOpts) ->
    Port = first_defined([PortFromListenOpts], 8080),
    IPAddress = first_defined([IpFromListenOpts, IpFromSocketOpts, IfAddr], {0, 0, 0, 0}),
    SocketOptsBySockFamily = [{active, false},
                              {mode, binary},
                              {packet, raw},
                              {ip, IPAddress}],
    DefaultSocketOpts = [{reuseaddr, true},
                         {nodelay, true},
                         {backlog, 32768},
                         {keepalive, true}],
    {Port, SocketOptsBySockFamily, DefaultSocketOpts}.

first_defined([], Default) ->
    Default;
first_defined([Elem | Rest], Default) ->
    if Elem == undefined -> first_defined(Rest, Default);
       true -> Elem
    end.

handle_call(Req, _, State) ->
    {stop, {bad_call, Req}, State}.

handle_cast(Req, State) ->
    {stop, {bad_cast, Req}, State}.

handle_info({'DOWN', MRef, port, Socket, Reason}, {Socket, MRef} = State) ->
    {stop, Reason, State};
handle_info(_, State) ->
    {noreply, State}.

code_change(_, State, _) ->
    {ok, State}.

terminate(_, {Socket, MRef}) ->
    %% Socket may already be down but need to ensure it is closed to avoid
    %% eaddrinuse error on restart
    case demonitor(MRef, [flush, info]) of
        true  -> gen_tcp:close(Socket);
        false -> ok
    end.
