from parser import parse_file
from spark_common import spark, view

lines = open("../../data/input.txt").readlines()
parse_file(lines)

view("player_impact",
     """
select 
     *
from user_result  
join user on player_id = user.user_id
     """)

view("team_net_worth", """
select 
     match_id, 
     is_radiant, 
     sum(net_worth) as team_net_worth 
from user_result
group by match_id, is_radiant
""")

view("player_impact", """
select 
    kill,
    death,
    assist,
    player_impact.player_id as player_id, 
    player_impact.net_worth as net_worth, 
    team_net_worth.is_radiant as is_radiant, 
    team_net_worth.match_id as match_id, 
    player_impact.net_worth/team_net_worth.team_net_worth as net_worth_percentage,
    net_worth_percentage * (kill*2 + assist + pos) / coalesce(NULLIF(power(death, 2),0), 1) as impact
from player_impact 
     join team_net_worth on team_net_worth.match_id = player_impact.match_id and team_net_worth.is_radiant = player_impact.is_radiant
     join match on team_net_worth.match_id = match.match_id
""")

view("team_impact", """
select 
     player_impact.match_id, 
     is_radiant, 
     sum(impact) as team_impact 
from player_impact
group by match_id, is_radiant
""")

view("player_impact", """
select 
     *, 
    cast(player_impact.is_radiant = match.radiant_won as int) as is_winner,
    player_impact.impact/team_impact.team_impact as impact_percentage
from player_impact 
     join team_impact on team_impact.match_id = player_impact.match_id and team_impact.is_radiant = player_impact.is_radiant
     join match on player_impact.match_id = match.match_id
order by player_impact.match_id
""")

view("player_rating", """
select 
     match.match_id as match_id,
     is_radiant,
     pos,
     player_id,
     (case when 1 = row_number() over (partition by player_id order by finished_at) then 1000 end) as rating
from user_result
join match on user_result.match_id = match.match_id
order by finished_at asc
""")

view("pr_skew", """
select 
     *,
     -- radiant_kills/coalesce(NULLIF(dire_kills,0), 1) as radiant_nw,
     radiant_kills/coalesce(NULLIF(dire_kills,0), 1) as radiant_kd,
     pow(radiant_kd, (1.5*60*60) / duration_sec) as pr_skew
from match
""")

view("team_rating_contribution", """
select 
     match_id, 
     is_radiant,
     sum(rating*(6-pos)) as team_rating_contribution
from player_rating
group by match_id, is_radiant
""")

view("th_skew", """
select 
     match_id,
     sum(team_rating_contribution *  (1 - cast(is_radiant as int))) as   dire_rating_contribution,
     sum(team_rating_contribution *       cast(is_radiant as int)) as radiant_rating_contribution,
     radiant_rating_contribution/dire_rating_contribution as th_skew
from 
     team_rating_contribution
group by match_id
""")

view("abs", """
select 
*,
0.5 * ((th_skew.th_skew + pr_skew.pr_skew) + abs(th_skew.th_skew - pr_skew.pr_skew)) as max_skew,
0.5 * ((th_skew.th_skew + pr_skew.pr_skew) - abs(th_skew.th_skew - pr_skew.pr_skew)) as min_skew,
max_skew / min_skew as disbalance
from player_rating
     join team_rating_contribution
          on team_rating_contribution.match_id = player_rating.match_id 
          and team_rating_contribution.is_radiant = player_rating.is_radiant
    join match on player_rating.match_id = match.match_id 
    join th_skew on th_skew.match_id = match.match_id
    join pr_skew on pr_skew.match_id = match.match_id
order by match.finished_at, player_rating.is_radiant
""")

spark.sql("select * from abs").show(100)

exit(0)
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
    cast(sum(is_winner) / count(match_id) * 100 as int) as winrate, 
    count(match_id) as total_matches 
from winrate
    group by player_id
""")

view("res_net_worth", """
select 
     player_id, 
     cast(avg(net_worth)/1000 as int) as avg_net_worth 
from user_result 
group by player_id
""")
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

# spark.sql("select * from res").show(100)

# spark.sql("select * from user_result").repartition(1).write.csv('user_result.csv')
# spark.sql("select * from user").repartition(1).write.csv('user.csv')
# spark.sql("select * from match").repartition(1).write.csv('match.csv')
