import itertools
from collections import defaultdict
from pprint import pprint
from transpose import transpose
from pyspark import Row
from pyspark.sql.types import StructField, FloatType, StructType

from parser import parse_file
from spark_common import spark, view

lines = open("../data/input.txt").readlines()
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
    pos,
    player_impact.player_id as player_id, 
    player_impact.net_worth as net_worth, 
    team_net_worth.is_radiant as is_radiant, 
    team_net_worth.match_id as match_id, 
    player_impact.net_worth/team_net_worth.team_net_worth as net_worth_percentage,
    (kill + assist/2 + pos) / coalesce(NULLIF(power(death, 0.2),0), 1) as impact,
     power(death, 1.2) / (coalesce(NULLIF((kill + assist/2 + pos),0), 1) * net_worth_percentage) as negative_impact
from player_impact 
     join team_net_worth on team_net_worth.match_id = player_impact.match_id and team_net_worth.is_radiant = player_impact.is_radiant
     join match on team_net_worth.match_id = match.match_id
""")

view("player_impact_res", "select * from player_impact")
     
view("player_impact", """
select 
    player_impact.player_id as player_id, 
    player_impact.match_id, 
    cast(player_impact.is_radiant = match.radiant_won as int) as is_winner,
    sum(impact) over (partition by match.match_id, is_radiant order by finished_at) as team_impact,
    player_impact.impact/team_impact as impact_percentage,
    sum(negative_impact) over (partition by match.match_id, is_radiant order by finished_at) as team_negative_impact,
    player_impact.negative_impact/team_negative_impact as negative_impact_percentage
from player_impact 
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
    user.name as username,
    match.finished_at,
    player_impact.negative_impact_percentage,
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
     join user on user_result.player_id = user.user_id
     join match on user_result.match_id = match.match_id
     join pr_skew on pr_skew.match_id = user_result.match_id
     join player_impact on player_impact.player_id = user_result.player_id and player_impact.match_id = user_result.match_id
order by finished_at asc
""")

player_id_by_username = {}
player_username_by_id = {}
player_ratings = defaultdict(lambda: 500)
player_ratings_by_match = defaultdict(dict)


def player_impact(rating, pos):
    return rating * (-0.25 * (pos - 1) + 2)


def th_calculator(r):
    player_id_by_username[r.username] = r.player_id
    player_username_by_id[r.player_id] = r.username
    player_rating_by_id = lambda row: player_impact(player_ratings[row.player_id], row.pos)
    if r.match_id in player_ratings_by_match:
        radiant_sum = player_ratings_by_match[r.match_id]['radiant_sum']
        dire_sum = player_ratings_by_match[r.match_id]['dire_sum']
    else:
        radiant_team, dire_team = r.teammates, r.enemys
        if not r.is_radiant:
            radiant_team, dire_team = dire_team, radiant_team
        radiant_sum = sum(map(player_rating_by_id, radiant_team))
        dire_sum = sum(map(player_rating_by_id, dire_team))
        player_ratings_by_match[r.match_id]['radiant_sum'] = radiant_sum
        player_ratings_by_match[r.match_id]['dire_sum'] = dire_sum
    th_skew = radiant_sum / dire_sum
    disbalance = (max(th_skew, r.pr_skew) / min(th_skew, r.pr_skew)) ** 0.2
    percentage = r.impact_percentage if r.is_winner else r.negative_impact_percentage
    pool = min(50 * disbalance, 100.0)
    sign = 1 if r.is_winner else -1
    delta = pool * sign * percentage
    rating = player_ratings[r.player_id] + delta
    player_ratings[r.player_id] = rating
    temp_r = r.asDict()
    temp_r['player_rating'] = rating
    temp_r['player_rating_delta'] = delta
    temp_r['th_skew'] = th_skew
    temp_r['pool'] = pool
    return Row(**temp_r)


th_skew_df = spark.sql("select * from th_skew")
th_skew_list = th_skew_df.collect()
schema = StructType(th_skew_df.schema.fields + [
    StructField("player_rating", FloatType(), True),
    StructField("player_rating_delta", FloatType(), True),
    StructField("th_skew", FloatType(), True),
    StructField("pool", FloatType(), True),
])
spark.createDataFrame(map(th_calculator, th_skew_list), schema=schema).createOrReplaceTempView("rating_history")

view("rating_history_human_readable", """
select 
    finished_at as match,
    case when user_result.is_radiant then 'RAD' else 'DIR' end as team,
    concat(cast(th_skew * 50 as int), '%') as th_skew,
    concat(cast(pr_skew * 50 as int), '%') as pr_skew,
    cast(pool as int) as pool,
    case when is_winner=1 then 'W' else 'L' end as r,
    username,
    pos,
    net_worth,
    concat(kill, '/', death, '/', assist) as kda,
    concat(cast(negative_impact_percentage * 100 as int), '%') as ruined,
    concat(cast(impact_percentage * 100 as int), '%') as carried,
    concat(cast(player_rating - player_rating_delta as int),' -> ' , cast(player_rating as int)) as mmr,
    concat(case when player_rating_delta > 0 then '+' else '' end, cast(player_rating_delta as int)) as delta
from rating_history
    join user_result on user_result.player_id = rating_history.player_id and rating_history.match_id = user_result.match_id
order by finished_at desc, is_winner
""")



view("rating", """
select 
player_id,
max(finished_at) as max_finished_at
from rating_history
group by player_id
""")
view("rating", """
select
    max(cast(rating_history.player_rating as int)) as player_rating,
    rating.player_id
from rating
    left join rating_history on rating.max_finished_at = rating_history.finished_at and rating.player_id = rating_history.player_id
where finished_at = max_finished_at and rating.player_id = rating_history.player_id
group by rating.player_id
""")


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

view("players_leaderboard", """
select 
    row_number() over (partition by 1 order by rating.player_rating desc) AS N,
    rating.player_rating as rating,
    -- user.user_id,
    user.name,
    main_pos.pos,
    concat(avg_net_worth, 'k') as gold,
    concat(avg_kills, '/', avg_death, '/', avg_assists) as kda,
    concat(winrate.winrate, '%/', winrate.total_matches) as wins
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

view("leaderboard_raw_player_streaks", """
with prep as (
    select cast(user_result.is_radiant = match.radiant_won as int) as is_winner, user_result.player_id, user_result.match_id, match.finished_at
    from user_result
    join match on user_result.match_id = match.match_id
),
lagged as (select lag(is_winner, 1) over (partition by player_id order by finished_at asc) as is_winner_prev, * from prep),
streak_changed as (select case when is_winner_prev <> is_winner then 1 else 0 end as streak_changed, * from lagged),
streak_id as (select sum(streak_changed) over (partition by player_id order by finished_at asc) as streak_id, * from streak_changed),
streak_length as (select row_number() over (partition by player_id, streak_id order by finished_at asc) as streak_length, * from streak_id),
streak_rank as (select rank() over (partition by player_id, streak_id order by streak_length desc) as streak_rank, * from streak_length)
select * from streak_rank where streak_rank = 1
""")

view("leaderboard_raw_player_impact", """
    select 
        player_impact_res.pos, 
        player_impact_res.kill, 
        player_impact_res.assist, 
        player_impact_res.death, 
        player_impact_res.net_worth as networth, 
        int(impact.negative_impact_percentage * 100) as ruined, 
        int(impact.impact_percentage * 100) as carried, 
        date_format(match.finished_at, 'yyyy-MM-dd') as finished_at,
        case when impact.is_winner = 1 then 'W' else 'L' end as result,
        losestreak.streak_length as losestreak,
        winstreak.streak_length as winstreak,
        int(player_rating_delta) as rating_diff,
        user.name as name
    from player_impact_res
        join user on user.user_id = player_impact_res.player_id
        join user_result on user_result.match_id = player_impact_res.match_id
        join match on player_impact_res.match_id = match.match_id
        join player_impact impact on impact.match_id = player_impact_res.match_id and player_impact_res.player_id = impact.player_id
        left join leaderboard_raw_player_streaks losestreak on losestreak.match_id = player_impact_res.match_id and player_impact_res.player_id = losestreak.player_id and losestreak.is_winner = 0
        left join leaderboard_raw_player_streaks winstreak on winstreak.match_id = player_impact_res.match_id and player_impact_res.player_id = winstreak.player_id and winstreak.is_winner = 1
        left join rating_history on rating_history.match_id = player_impact_res.match_id and player_impact_res.player_id = rating_history.player_id
    where year(match.finished_at) = year(now())
""")

view("leaderboard", """
    select distinct pos from leaderboard_raw_player_impact
""")

for parameter in ['kill', 'assist', 'death', 'networth', 'ruined', 'carried', 'winstreak', 'losestreak', 'rating_diff']:
    view("leaderboard_pos_agg", f"""
        select 
            pos,
            max({parameter}) as max_{parameter},
            min({parameter}) as min_{parameter}
        from leaderboard_raw_player_impact  
        group by pos
    """)
    spark.sql(f"""
    with res as (
        select
            board.*,
            concat(raw_max_{parameter}.name, ' | ', max_{parameter}, ' | ',  raw_max_{parameter}.result, ' | ',  raw_max_{parameter}.finished_at) as max_{parameter},
            concat(raw_min_{parameter}.name, ' | ', min_{parameter}, ' | ',  raw_min_{parameter}.result, ' | ',  raw_min_{parameter}.finished_at) as min_{parameter},
            row_number() over (partition by agg.pos order by raw_max_{parameter}.finished_at desc, raw_min_{parameter}.finished_at desc) AS N
        from leaderboard board
        join leaderboard_pos_agg agg on agg.pos = board.pos
        join leaderboard_raw_player_impact raw_max_{parameter} on raw_max_{parameter}.{parameter} = agg.max_{parameter} and raw_max_{parameter}.pos = board.pos
        join leaderboard_raw_player_impact raw_min_{parameter} on raw_min_{parameter}.{parameter} = agg.min_{parameter} and raw_min_{parameter}.pos = board.pos
    )
    select * from res where N=1
    """).drop('N').createOrReplaceTempView("leaderboard")



def show_leaderboard(leaderboard_df):
    leaderboard_df = spark.createDataFrame([leaderboard_df.schema.names], leaderboard_df.schema.names).union(leaderboard_df)
    spark.createDataFrame(leaderboard_df.toPandas().set_index('pos').T).show(100, 100)

spark.sql("select * from rating_history_human_readable").show(100)
print('Топ игроков по статам:')
spark.sql("select * from players_leaderboard").show(100, 100)
ld_df = spark.sql("select * from leaderboard")
print('Особо отличившиеся')
show_leaderboard(ld_df.select('pos', 'max_rating_diff', 'max_winstreak', 'max_kill', 'max_assist', 'min_death', 'max_networth', 'max_carried', 'min_ruined'))
print('Не особо отличившиеся...')
show_leaderboard(ld_df.select('pos', 'min_rating_diff', 'max_losestreak', 'min_kill', 'min_assist', 'max_death', 'min_networth', 'min_carried', 'max_ruined'))

