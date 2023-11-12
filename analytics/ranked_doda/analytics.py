from collections import defaultdict

from pyspark import Row
from pyspark.sql.types import StructField, FloatType, StructType

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
    net_worth_percentage * (kill + assist + pos) / coalesce(NULLIF(power(death, 0.5),0), 1) as impact
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
    player_impact.player_id as player_id, 
    player_impact.match_id, 
    cast(player_impact.is_radiant = match.radiant_won as int) as is_winner,
    player_impact.impact/team_impact.team_impact as impact_percentage
from player_impact 
     join team_impact on team_impact.match_id = player_impact.match_id and team_impact.is_radiant = player_impact.is_radiant
     join match on player_impact.match_id = match.match_id
order by player_impact.match_id
""")

view("pr_skew", """
select 
     *,
     -- radiant_kills/coalesce(NULLIF(dire_kills,0), 1) as radiant_nw,
     radiant_kills/coalesce(NULLIF(dire_kills,0), 1) as radiant_kd,
     pow(radiant_kd, (1.5*60*60) / duration_sec) as pr_skew
from match
""")

view("th_skew", """
select 
    match.finished_at,
    player_impact.impact_percentage,
     cast(user_result.is_radiant = match.radiant_won as int) as is_winner, 
     pr_skew.pr_skew,
     user_result.player_id,
     user_result.is_radiant,
     match.match_id,
     array_agg((user_result.player_id, pos)) over (partition by match.match_id, is_radiant) as teammates,
     array_agg((user_result.player_id, pos)) over (partition by match.match_id) as match_players,
     array_except(match_players, teammates) as enemys
from user_result
     join match on user_result.match_id = match.match_id
     join pr_skew on pr_skew.match_id = user_result.match_id
     join player_impact on player_impact.player_id = user_result.player_id and player_impact.match_id = user_result.match_id
""")

player_ratings = defaultdict(lambda: 1000)


def th_calculator(r):
    player_rating_by_id = lambda row: player_ratings[row.player_id] * (6 - row.pos)
    mates_sum = sum(map(player_rating_by_id, r.teammates))
    enemys_sum = sum(map(player_rating_by_id, r.enemys))
    th_skew = mates_sum / enemys_sum if r.is_radiant else enemys_sum / mates_sum
    disbalance = max(th_skew, r.pr_skew) / min(th_skew, r.pr_skew)
    delta = 50 * disbalance * (r.is_winner + r.impact_percentage - 1)
    rating = player_ratings[r.player_id] + delta
    player_ratings[r.player_id] = rating
    temp_r = r.asDict()
    temp_r['player_rating'] = rating
    return Row(**temp_r)


th_skew_df = spark.sql("select * from th_skew")
th_skew_list = th_skew_df.collect()
schema = StructType(th_skew_df.schema.fields + [StructField("player_rating", FloatType(), True)])
spark.createDataFrame(map(th_calculator, th_skew_list), schema=schema).createOrReplaceTempView("rating_history")

view("rating", """
select 
player_id,
max(finished_at) as max_finished_at
from rating_history
group by player_id
""")
view("rating", """
select
    cast(rating_history.player_rating as int),
    rating.player_id
from rating
    left join rating_history on rating.max_finished_at = rating_history.finished_at
where finished_at = max_finished_at and rating.player_id = rating_history.player_id
""")

# spark.sql("select * from rating").show(100)
# exit(0)
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
    winrate.total_matches as matches,
    rating.player_rating
from user 
    join res_net_worth on res_net_worth.player_id = user.user_id
    join res_kills on res_kills.player_id = user.user_id
    join res_assists on res_assists.player_id = user.user_id
    join res_deaths on res_deaths.player_id = user.user_id
    join winrate on winrate.player_id = user.user_id
    join main_pos on main_pos.player_id = user.user_id
    join rating on user.user_id = rating.player_id
order by rating.player_rating desc
""")

spark.sql("select * from res").show(100)

# spark.sql("select * from user_result").repartition(1).write.csv('user_result.csv')
# spark.sql("select * from user").repartition(1).write.csv('user.csv')
# spark.sql("select * from match").repartition(1).write.csv('match.csv')
