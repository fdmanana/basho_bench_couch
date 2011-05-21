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

-module(couch_http_writes).

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
    server,
    db_url,
    json_doc,
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
    Json = json_encode(EJson),
    State = #state{
        server = Server,
        db_url = DbUrl,
        json_doc = iolist_to_binary(Json),
        socket_opts = basho_bench_config:get(socket_options, [])
    },
    {ok, State}.


run(put, _KeyGen, _ValueGen, State) ->
    #state{db_url = DbUrl, json_doc = Doc, socket_opts = SocketOpts} = State,
    case httpc:request(
        post,
        {DbUrl, [], "application/json", Doc},
        [{timeout, ?TIMEOUT}, {connect_timeout, ?TIMEOUT}],
        [{sync, true}, {socket_opts, SocketOpts}]) of
    {ok, {{_, 201, _}, _Headers, Body}} ->
        {Props} = json_decode(Body),
        case get_value(<<"ok">>, Props) of
        true ->
            {ok, State};
        _ ->
            {error, "Error creating doc, response is: " ++ Body}
        end;
    {ok, {{_, _Code, _}, _Headers, Body}} ->
        {error, "Error creating doc, response is: " ++ Body};
    {error, Error} ->
        {error, "Error creating doc: " ++ to_list(Error)}
    end.
