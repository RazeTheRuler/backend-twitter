-module(twitterminer_riak).

-export([start_stream/2, twitter_save_pipeline/5, get_riak_hostport/1


	, initiate_bucket_purge/0, print/1, gather_tweets/0, counter_loop/4, init/1, count_tweets/1, tryPurge/0, gather_concurrent/0, delete_all_in_bucket/3, concurrent_purge/3]).  

-record(hostport, {host, port}). 
-record(account_keys, {api_key, api_secret,
                       access_token, access_token_secret}).



% This file contains example code that connects to Twitter and saves tweets to Riak.
% It would benefit from refactoring it together with twitterminer_source.erl.

keyfind(Key, L) ->
  {Key, V} = lists:keyfind(Key, 1, L), 	
  V.

%% @doc Get Twitter account keys from a configuration file.
get_riak_hostport(Name) ->
  {ok, Nodes} = application:get_env(twitterminer, riak_nodes),
  {Name, Keys} = lists:keyfind(Name, 1, Nodes),
  #hostport{host=keyfind(host, Keys),
            port=keyfind(port, Keys)}.

start_stream({Category, Account}, Track) ->
  URL = "https://stream.twitter.com/1.1/statuses/filter.json",

  % We get our keys from the twitterminer.config configuration file.
  Keys = twitterminer_source:get_account_keys(Account),

  RHP = get_riak_hostport(riak1),
  {ok, Pid} = riakc_pb_socket:start_link(RHP#hostport.host, RHP#hostport.port),

  % Run our pipeline
  P = twitterminer_pipeline:build_link(twitter_save_pipeline(Pid, URL, Keys, Category, Track)),   %Track

  % If the pipeline does not terminate after 60 s, this process will
  % force it.
  T = spawn_link(fun () ->
        receive
          cancel -> ok
        after 30000 -> % Sleep fo 60 s
            twitterminer_pipeline:terminate(P),io:format("Stream done for Pid: ~p and Category: ~p~n", [Pid, Category])
        
        end
    end),

  Res = twitterminer_pipeline:join(P),
  T ! cancel,
  Res.

%%Concurrency for the streaming part. 

gather_concurrent() ->
            spawn(twitterminer_riak, start_stream, [{index, account1}, {track, "svpol, nyval, extraval, omval, migpol"}]), 
            spawn(twitterminer_riak, start_stream, [{sosse, account2}, {track, "socialdemokraterna, löfven"}]),
            spawn(twitterminer_riak, start_stream, [{moderaterna, account6}, {track, "reinfeldt, batra, moderaterna, nymoderaterna"}]),
            spawn(twitterminer_riak, start_stream, [{sd, account1}, {track, "sverigedemokraterna, jimmi åkesson, invandring"}]),
            spawn(twitterminer_riak, start_stream, [{miljopartiet, account2}, {track, "miljö, fridolin, miljöpartiet, romsom"}]),
            spawn(twitterminer_riak, start_stream, [{kristdemokraterna, account6}, {track, "kristdemokraterna, hägglund, kyrkan"}]),
            spawn(twitterminer_riak, start_stream, [{vanstern, account5}, {track, "vänstern, kommunism, vänsterpartiet, ungvänster, vansterpartiet"}]),
            spawn(twitterminer_riak, start_stream, [{folkpartiet, account5}, {track, "folkpartiet, jan björklund, skolan"}]),
            spawn(twitterminer_riak, start_stream, [{feminism, account4}, {track, "schyman, feminist, feministiskt initiativ"}]),
            spawn(twitterminer_riak, start_stream, [{centerpartiet, account4}, {track, "centerpartiet, annie lööf"}]).



gather_tweets() ->  
Trackers = [{{index, account1}, {track, "svpol, nyval, extraval, omval, migpol"}}, {{sosse, account2}, {track, "socialdemokraterna, löfven"}}, 
{{moderaterna, account6}, {track, "reinfeldt, batra, moderaterna, nymoderaterna"}}, {{sd, account1}, {track, "sverigedemokraterna, jimmi åkesson, invandring"}},
 {{miljopartiet, account2}, {track, "miljö, fridolin, miljöpartiet, romsom"}}, {{kristdemokraterna, account6}, {track, "kristdemokraterna, hägglund, kyrkan"}},
  {{vanstern, account5}, {track, "vänstern, kommunism, vänsterpartiet, ungvänster, vansterpartiet"}}, {{folkpartiet, account5}, {track, "folkpartiet, jan björklund, skolan"}},
  {{feminism, account4}, {track, "schyman, feminist, feministiskt initiativ"}}, {{centerpartiet, account4}, {track, "centerpartiet, annie lööf"}}],
[start_stream({Category, Account}, {Track, Words}) || {{Category, Account}, {Track, Words}} <- Trackers], timer:sleep(300000), initiate_bucket_purge(). 


%% @doc Create a pipeline that connects to twitter and
%% saves tweets to Riak. We save all messages that have ids,
%% which might include delete notifications etc.
twitter_save_pipeline(Pid, URL, Keys, Category, Track) ->


  Prod = twitterminer_source:twitter_producer(URL, Keys, Track),

  % Pipelines are constructed 'backwards' - consumer is first, producer is last.
  [
    twitterminer_pipeline:consumer(    
      fun(Msg, N) -> save_tweet({Category, Pid}, Msg), N+1 end, 0, Category),
    twitterminer_pipeline:map(
      fun twitterminer_source:decorate_with_id/1),
    twitterminer_source:split_transformer(),
    Prod].

% We save only objects that have ids. 
save_tweet({Category, Pid}, {parsed_tweet, _L, Body, {id, Id}}) ->
Self= self(), 
  Obj = riakc_obj:new(binary(Category), list_to_binary(integer_to_list(Id)), Body), io:format("Tweet saved with ID: ~p Category = ~p, Pid = ~p~n", [Id, Category, Self]),
  riakc_pb_socket:put(Pid, Obj, [{w, 0}]);
save_tweet(_, _) -> io:format("save_tweet did not match: ~n", []).



  

binary(Category) ->
list_to_binary(atom_to_list(Category)).


check_id(C, Id) ->
    {C, Id}.

sort_list( Category_list, Id_list)->

    Zip = lists:zip(Category_list, Id_list),
    Res = lists:map(fun({C, Ids}) -> [check_id( C, Id) || Id <- Ids] end, Zip),
    lists:flatten(Res).


             %%Example of using the sorting part concurrency.

          tryPurge() -> spawn(twitterminer_riak, concurrent_purge, [account4, index, 20]),
            spawn(twitterminer_riak, concurrent_purge, [account1, sosse, 20]), 
            spawn(twitterminer_riak, concurrent_purge, [account2, moderaterna, 20]),
            spawn(twitterminer_riak, concurrent_purge, [account6, sd, 20]),
            spawn(twitterminer_riak, concurrent_purge, [account1, miljopartiet, 20]),
            spawn(twitterminer_riak, concurrent_purge, [account2, kristdemokraterna, 20]),
            spawn(twitterminer_riak, concurrent_purge, [account6, vanstern, 20]),
            spawn(twitterminer_riak, concurrent_purge, [account5, folkpartiet, 20]),
            spawn(twitterminer_riak, concurrent_purge, [account5, feminism, 20]),
            spawn(twitterminer_riak, concurrent_purge, [account4, centerpartiet, 20]).




concurrent_purge(Account, Bucket, Constraint) ->

Spawn = spawn(twitterminer_riak, init, [Bucket]), 

RHP = get_riak_hostport(riak1),
  {ok, Pid} = riakc_pb_socket:start_link(RHP#hostport.host, RHP#hostport.port),

Key_list = riakc_pb_socket:list_keys(Pid, binary(Bucket)), Filtered = element(2, Key_list),


[update_tweet(Pid, Constraint, Spawn, {{Account, Bucket}, list_to_integer(binary_to_list(X))}) || X <- Filtered], Spawn ! complete . 

 %, timer:sleep(5000), gather_tweets().

            


initiate_bucket_purge() ->

Spawn = spawn(twitterminer_riak, init, [allthebuckets]), 

RHP = get_riak_hostport(riak1),
  {ok, Pid} = riakc_pb_socket:start_link(RHP#hostport.host, RHP#hostport.port),

  Constraint = 5,

Category_buckets = [{index, account1}, {sosse, account2}, {moderaterna, account6}, {sd, account1}, {miljopartiet, account2}, {kristdemokraterna, account6}, {vanstern, account5},
{folkpartiet, account5}, {feminism, account4}, {centerpartiet, account4}],

Get_categories = [element(2, X) || X <- Category_buckets],

Binary_List = [binary(X) || X <- Get_categories], 

Key_list = [riakc_pb_socket:list_keys(Pid, X) || X <- Binary_List], Filtered = [element(2, X) || X <- Key_list],




 Tuples = sort_list( Category_buckets, Filtered),

 [update_tweet(Pid, Constraint, Spawn, {{Account, C},list_to_integer(binary_to_list(T)) }) || {{Account, C}, T} <- Tuples], Spawn ! complete, timer:sleep(20000), gather_tweets().



print(Key) ->

 URL = "https://api.twitter.com/1.1/statuses/show.json", 	

  Keys = twitterminer_source:get_account_keys(account6),


  Consumer = {Keys#account_keys.api_key, Keys#account_keys.api_secret, hmac_sha1},
  AccessToken = Keys#account_keys.access_token,
  AccessTokenSecret = Keys#account_keys.access_token_secret,


  SignedParams = oauth:sign("GET", URL, [{id, Key}], Consumer, AccessToken, AccessTokenSecret), io:format("This is it: ~p", [oauth:uri(URL, SignedParams)]).






 update_tweet(Pid, Constraint, Spawn, {{Account, Category}, Bucket_key}) ->  


  URL = "https://api.twitter.com/1.1/statuses/show.json", 	

  Keys = twitterminer_source:get_account_keys(Account),


  Consumer = {Keys#account_keys.api_key, Keys#account_keys.api_secret, hmac_sha1},
  AccessToken = Keys#account_keys.access_token,
  AccessTokenSecret = Keys#account_keys.access_token_secret,


  SignedParams = oauth:sign("GET", URL, [{id, Bucket_key}], Consumer, AccessToken, AccessTokenSecret),



{_, _, _, Result} = ibrowse:send_req(oauth:uri(URL, SignedParams), [], get, []), 

Decorated = twitterminer_source:decorate_with_id(Result),
case Decorated of {parsed_tweet, [{_, [{L}]}], _Tweet_body, no_id} -> 


case lists:keyfind(<<"code">>, 1, L) of
  {_, 88} -> lists:foreach(fun(X) -> lists:foreach(fun(Y) -> io:format("Limit reached for Category ~p! ~p minutes and ~p seconds remaining ..~n",[Category, X, Y]), timer:sleep(1000) end, lists:reverse(lists:seq(0, 59))) end, lists:reverse(lists:seq(0, 14)));
  {_, _Er} -> io:format("Error: ~p~n", [L]), riakc_pb_socket:delete(Pid, binary(Category), list_to_binary(integer_to_list(Bucket_key))), Spawn ! {errors, L}  end;
 
  _ -> sort_to_tweet(Pid, Constraint, Spawn, Category, Decorated) end. 


sort_to_tweet(Pid, Constraint, Spawn, Category, {parsed_tweet, L, Tweet_body, {id, Id}}) -> 
  		case lists:keyfind(<<"retweet_count">>, 1, L) of
   		 {_, Count} -> 
					case Count > Constraint of
      				true -> save_popular(Pid, Category, {parsed_tweet, L, Tweet_body, {id, Id}}), delete_tweet(Pid, Category, {parsed_tweet, L, Tweet_body, {id, Id}}) , io:format("Saved: ~p into ~p~n", [Id, Category]), Spawn ! {saved, L};
      				false -> delete_tweet(Pid, Category, {parsed_tweet, L, Tweet_body, {id, Id}}), io:format("Deleted ~p from ~p~n", [Id, Category]), Spawn ! {deleted, L} end;
   		 false -> io:format("No retweet_count found..~n", []) end;

sort_to_tweet(_,_ , _, _, Stuff) -> io:format("Unknown input: ~p~n", [Stuff]).



counter_loop(Saved, Deleted, Errors, Category) ->                 %Puts every tweet in a list
receive {saved, Body} -> counter_loop(Saved ++ [Body], Deleted, Errors, Category);
        {deleted, Body}->  counter_loop(Saved, Deleted ++ [Body], Errors, Category);
        {errors, Body} ->   counter_loop(Saved, Deleted, Errors ++ [Body], Category);
        complete ->  calculate(Saved, Deleted, Errors, Category) end.

init(Category) -> counter_loop([], [], [], Category).



count_tweets([]) -> 0;              % Reduces the tweets to a value
count_tweets([_|T]) -> 1 + count_tweets(T).

calculate(Saved, Deleted, Errors, Category) -> 

SaveFix = count_tweets(Saved), DeleteFix = count_tweets(Deleted), ErrorFix = count_tweets(Errors), Merge = SaveFix + DeleteFix + ErrorFix, 

case Merge > 0 of 

  true -> 


SaveResult = SaveFix / Merge * 100, DeleteResult = DeleteFix / Merge * 100, ErrorResult = ErrorFix / Merge * 100, 


io:format("Tweets sorted: ~p in bucket <<~p>>. Results are as following:~n Popular tweets: ~p percent.~nUnpopular tweets: ~p percent.~nRemove/error tweets: ~p percent.~n", [Merge, Category, SaveResult, DeleteResult, ErrorResult]);

false -> io:format("No tweets sorted in bucket <<~p>>.~n", [Category]) end.



updateCategory(Category) -> list_to_atom(atom_to_list(Category) ++ "popular"). 



delete_tweet(Pid, Category, {parsed_tweet, _L, _Tweet_body, {id, Id}}) ->                             
riakc_pb_socket:delete(Pid, binary(Category), list_to_binary(integer_to_list(Id)));
	delete_tweet(_, _, _) -> io:format("delete_tweet did not match: ~n", []).

save_popular(Pid, Category, {parsed_tweet, _L, Tweet_body, {id, Id}}) ->
  Obj = riakc_obj:new(binary(updateCategory(Category)), list_to_binary(integer_to_list(Id)), Tweet_body),
  riakc_pb_socket:put(Pid, Obj, [{w, 0}]);
save_popular(_, _, _) -> io:format("save_popular did not match: ~n", []).



delete_all_in_bucket(Pid, Category, Id) -> 
riakc_pb_socket:delete(Pid, binary(Category), Id).

