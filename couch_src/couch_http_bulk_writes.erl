% Copyright 2011,  Filipe David Manana  <fdmanana@apache.org>
% Web site:  http://github.com/fdmanana/basho_bench_couch
%
% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_http_bulk_writes).

-export([new/1, run/4]).

-include("basho_bench.hrl").
-include("couch.hrl").

-define(TIMEOUT, 30000).

-import(couch_utils, [
    get_value/1, get_value/2,
    json_encode/1, json_decode/1,
    to_list/1, to_binary/1
]).

-record(state, {
    db_url,
    server,
    json_docs,
    socket_opts
}).



new(_Id) ->
    application:start(inets),
    Server = basho_bench_config:get(couch_server),
    DbName = basho_bench_config:get(couch_test_db),
    DbUrl = Server ++ "/" ++ DbName,
    DocFile = basho_bench_config:get(couch_doc_template),
    case DocFile of
    [$/ | _] ->
        {ok, Json0} = file:read_file(DocFile);
    _ ->
        {ok, Json0} = file:read_file("../../" ++ DocFile)
    end,
    EJson = try
        json_decode(Json0)
    catch _:_ ->
        exit("Invalid JSON in doc template file `" ++ DocFile ++ "`")
    end,
    % eliminate white spaces
    BulkBody = {[
        {docs, lists:duplicate(basho_bench_config:get(couch_batch_size), EJson)}
    ]},
    State = #state{
        server = Server,
        db_url = DbUrl,
        json_docs = iolist_to_binary(json_encode(BulkBody)),
        socket_opts = basho_bench_config:get(socket_options, [])
    },
    {ok, State}.


run(put, _KeyGen, _ValueGen, State) ->
    #state{db_url = DbUrl, json_docs = Docs, socket_opts = SocketOpts} = State,
    case httpc:request(
        post,
        {DbUrl ++ "/_bulk_docs", [], "application/json", Docs},
        [{timeout, ?TIMEOUT}, {connect_timeout, ?TIMEOUT}],
        [{sync, true}, {socket_opts, SocketOpts}]) of
    {ok, {{_, 201, _}, _Headers, Body}} ->
        lists:foldl(
            fun({Result}, Acc) ->
                case get_value(<<"ok">>, Result) of
                true ->
                    Acc;
                _ ->
                    Id = get_value(<<"id">>, Result),
                    case get_value(<<"error">>, Result) of
                    undefined ->
                        Acc;
                    Error ->
                        Msg = iolist_to_binary(
                            ["Error saving bulk saving doc `", Id, "`, error: ", Error]),
                        ?ERROR("~s~n", [Msg]),
                        {error, Msg}
                    end
                end
            end,
            {ok, State}, json_decode(Body));
    {ok, {{_, _Code, _}, _Headers, Body}} ->
        {error, "Error on _bulk_docs request, response is: " ++ Body};
    {error, Error} ->
        {error, "Error on _bulk_docs request: " ++ to_list(Error)}
    end.
