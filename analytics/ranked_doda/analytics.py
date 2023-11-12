import itertools
from collections import defaultdict
from pprint import pprint

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
    (kill + assist/2 + pos) / coalesce(NULLIF(power(death, 0.2),0), 1) as impact,
     power(death, 1.2) / (coalesce(NULLIF((kill + assist/2 + pos),0), 1) * net_worth_percentage) as negative_impact
from player_impact 
     join team_net_worth on team_net_worth.match_id = player_impact.match_id and team_net_worth.is_radiant = player_impact.is_radiant
     join match on team_net_worth.match_id = match.match_id
""")

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
    disbalance = (max(th_skew, r.pr_skew) / min(th_skew, r.pr_skew)) ** 0.3
    percentage = r.impact_percentage if r.is_winner else r.negative_impact_percentage
    pool = 50 * disbalance
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

spark.sql("select * from rating_history_human_readable").show(100000)

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
    row_number() over (partition by 1 order by rating.player_rating desc) AS N,
    rating.player_rating as rating,
    -- user.user_id,
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
    join rating on user.user_id = rating.player_id
order by rating.player_rating desc
""")

spark.sql("select * from res").show(100)


def calc_fair_game(usernames: list[str]) -> dict[list[str]]:
    results = {}
    if len(usernames) != 10:
        raise RuntimeError('need 10 players')
    players = set(player_id_by_username.get(name) or -1 for name in usernames)
    for combination in itertools.combinations(players, 5):
        team_1 = set(combination)
        team_2 = players - team_1
        team_1_sum = sum(
            [player_impact(rating, pos) for pos, rating in enumerate(sorted(player_ratings[p] for p in team_1), 1)])
        team_2_sum = sum(
            [player_impact(rating, pos) for pos, rating in enumerate(sorted(player_ratings[p] for p in team_2), 1)])
        how_imperfect = abs(1 - team_1_sum / team_2_sum)
        results[how_imperfect] = {
            'team 1': [player_username_by_id[p] for p in team_1],
            'team 2': [player_username_by_id[p] for p in team_2],
        }
    print('MOST FAIR:')
    for i in range(5):
        min_key = min(results)
        min_value = results.pop(min_key)
        print(min_key)
        pprint(min_value)
    print('MOST UNFAIR:')
    for i in range(5):
        min_key = max(results)
        min_value = results.pop(min_key)
        print(min_key)
        pprint(min_value)


fair_game = calc_fair_game([
    'pizza',
    'fishscale',
    'katokan',
    'poopy',
    'dextron',
    'starlight',
    'imba',
    'palec',
    '',
    '',
])
# spark.sql("select * from user_result").repartition(1).write.csv('user_result.csv')
# spark.sql("select * from user").repartition(1).write.csv('user.csv')
# spark.sql("select * from match").repartition(1).write.csv('match.csv')
