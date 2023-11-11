from parser import parse_file
from spark_common import spark, view

lines = open("../../data/input.txt").readlines()
parse_file(lines)

print('Всего игр: ' + str(spark.sql("select * from match").count()))

print('Топ игроков по статам:')

view("res_net_worth", "select player_id, round(avg(net_worth), 1) as avg_net_worth from user_result group by player_id")
view("res_kills", "select player_id, round(avg(kill), 1) as avg_kills from user_result group by player_id")
view("res_assists", "select player_id, round(avg(assist), 1) as avg_assists from user_result group by player_id")
view("res_deaths", "select player_id, round(avg(death), 1) as avg_death from user_result group by player_id")
view("res", """
select user.name,avg_net_worth,avg_kills,avg_assists,avg_death from user 
left join res_net_worth on res_net_worth.player_id = user.user_id
left join res_kills on res_kills.player_id = user.user_id
left join res_assists on res_assists.player_id = user.user_id
left join res_deaths on res_deaths.player_id = user.user_id
order by avg_net_worth desc
""")

spark.sql("select * from res").show(100)
