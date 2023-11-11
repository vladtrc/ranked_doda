from parser import parse_file
from spark_common import spark, view

lines = open("../../data/input.txt").readlines()
parse_file(lines)

print('Всего игр: ' + str(spark.sql("select * from match").count()))

print('Топ игроков по статам:')

view("winrate", """
select 
    cast(user_result.is_radiant = match.radiant_won as int) as is_winner, 
    player_id, 
    match.match_id, 
    match.duration_sec, 
    match.radiant_won, 
    user_result.is_radiant, 
    match.finished_at
from user_result
left join match on match.match_id = user_result.match_id 
order by match.finished_at asc
""")

view("main_pos_played_times", """
select 
    player_id, 
    pos, 
    count(*) as played_times
from user_result
group by player_id, pos
""")

view("main_pos_max_played_times", """
select 
    player_id, 
    max(played_times) as max_played_times 
from main_pos_played_times
group by player_id
""")

view("main_pos", """
select main_pos_played_times.player_id, min(pos) as pos
from main_pos_played_times
inner join main_pos_max_played_times on 
     main_pos_max_played_times.player_id = main_pos_played_times.player_id 
     and main_pos_max_played_times.max_played_times = main_pos_played_times.played_times
group by main_pos_played_times.player_id
""")

view("winrate", """
select 
    player_id,
    cast(sum(is_winner)/count(match_id) * 100 as int) as winrate, 
    count(match_id) as total_matches 
from winrate
    group by player_id
""")

view("res_net_worth", "select player_id, cast(avg(net_worth)/1000 as int) as avg_net_worth from user_result group by player_id")
view("res_kills", "select player_id, cast(avg(kill) as int) as avg_kills from user_result group by player_id")
view("res_assists", "select player_id, cast(avg(assist) as int) as avg_assists from user_result group by player_id")
view("res_deaths", "select player_id, cast(avg(death) as int) as avg_death from user_result group by player_id")

view("res", """
select 
    user.name,
    main_pos.pos,
    concat(avg_net_worth, 'k') as avg_gold,
    concat(avg_kills, '/', avg_death, '/', avg_assists) as avg_kda,
    concat(winrate.winrate, '%') as winrate,
    winrate.total_matches as matches
from user 
    join res_net_worth on res_net_worth.player_id = user.user_id
    join res_kills on res_kills.player_id = user.user_id
    join res_assists on res_assists.player_id = user.user_id
    join res_deaths on res_deaths.player_id = user.user_id
    join winrate on winrate.player_id = user.user_id
    join main_pos on main_pos.player_id = user.user_id
order by winrate.winrate desc
""")

spark.sql("select * from res").show(100)
