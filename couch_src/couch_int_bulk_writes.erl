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

-module(couch_int_bulk_writes).

-export([new/1, run/4]).

-include("basho_bench.hrl").
-include("couch.hrl").

-import(couch_utils, [
    get_value/1, get_value/2,
    json_encode/1, json_decode/1,
    to_list/1, to_binary/1
]).

-record(state, {
    db_name,
    docs,
    uuid = couch_uuids:new(),
    counter = 1
}).



new(_Id) ->
    DbName = basho_bench_config:get(couch_test_db),
    DocFile = basho_bench_config:get(couch_doc_template),
    case DocFile of
    [$/ | _] ->
        {ok, Json} = file:read_file(DocFile);
    _ ->
        {ok, Json} = file:read_file("../../" ++ DocFile)
    end,
    Doc = try
        json_decode(Json)
    catch _:_ ->
        exit("Invalid JSON in doc template file `" ++ DocFile ++ "`")
    end,
    State = #state{
        db_name = ?l2b(DbName),
        docs = lists:duplicate(basho_bench_config:get(couch_batch_size), Doc)
    },
    {ok, State}.


run(put, _KeyGen, _ValueGen, #state{db_name = DbName, docs = Docs0} = State) ->
    {ok, Db} = couch_db:open_int(DbName, []),
    {NewCounter, Docs} = lists:foldr(
        fun({Doc0}, {C, Acc}) ->
            Id = iolist_to_binary([State#state.uuid, "-", integer_to_list(C)]),
            Doc = couch_doc:from_json_obj(
                {lists:keystore(<<"_id">>, 1, Doc0, {<<"_id">>, Id})}),
            {C + 1, [Doc | Acc]}
        end,
        {State#state.counter, []}, Docs0),
    try
        case couch_db:update_docs(Db, Docs, []) of
        {ok, Results} ->
            ErrorCount = lists:foldl(
                fun({ok, _}, Acc) ->
                    Acc;
                (_, Acc) ->
                    Acc + 1
                end,
                0, Results),
            case ErrorCount > 0 of
            true ->
                Msg = io_lib:format("~p doc update errors", [ErrorCount]),
                ?ERROR("~s~n", [Msg]),
                {error, Msg, State#state{counter = NewCounter}};
            false ->
                {ok, State#state{counter = NewCounter}}
            end;
        Error ->
            ?ERROR("Error saving saving docs: ~s~n", [to_list(Error)])
        end
    catch
    Tag:Err ->
        Msg1 = io_lib:format("Error saving saving docs: ~s", [to_list({Tag, Err})]),
        ?ERROR("~s~n", [Msg1]),
        {error, Msg1, State#state{counter = NewCounter}}
    after
        couch_db:close(Db)
    end.
