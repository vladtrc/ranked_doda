from analytics import *

def calc_fair_game(usernames: list[str], premade_teams: list[str]) -> dict[list[str]]:
    team_size = len(usernames) // 2
    for user in usernames:
        if user not in player_id_by_username:
            print(f'unknown user {user}')
    results = {}
    if len(usernames) % 2 != 0:
        raise RuntimeError('need even number of players')
    players = set(player_id_by_username.get(name) or -1 for name in usernames)
    for combination in itertools.combinations(players, team_size):
        team_1 = set(combination)
        team_2 = players - team_1
        allowed = True
        for team in premade_teams:
            player_team = lambda p: 1 if player_id_by_username[p] in team_1 else 2
            player_teams = {p:player_team(p) for p in team}
            if len(set(player_teams.values())) != 1:
                allowed = False
        if not allowed:
            continue
        team_1_sum = sum(
            [player_impact(rating, pos) for pos, rating in enumerate(sorted([player_ratings[p] for p in team_1], reverse=True), 1)])
        team_2_sum = sum(
            [player_impact(rating, pos) for pos, rating in enumerate(sorted([player_ratings[p] for p in team_2], reverse=True), 1)])
        how_imperfect = abs(team_1_sum - team_2_sum)
        results[how_imperfect] = {
            'RADIANT': [player_username_by_id[p] for p in team_1],
            'DIRE': [player_username_by_id[p] for p in team_2],
        }
    print('MOST FAIR:')
    def print_result(result):
        pprint({t:sorted([(int(player_ratings[player_id_by_username[p]]), p) for p in players], reverse=True) for t,players in result.items()})
    for i in range(2):
        min_key = min(results)
        min_value = results.pop(min_key)
        print(min_key)
        print_result(min_value)
    print('MOST UNFAIR:')
    for i in range(2):
        min_key = max(results)
        min_value = results.pop(min_key)
        print(min_key)
        print_result(min_value)

premade_teams = [
   ['toyota', 'doyota' ,'svetlana'],
   
   ['imba', 'katokan', 'poopy']
]

# premade_teams = []

# fair_game = calc_fair_game([
#     'pizza',
#     'cashier',
#     'fishscale',
#     'imba',
#     'katokan',
#     'angry_duck',
#     'palec',
#     'kebab',
#     'toyota',
#     'ginecolog',
# ], premade_teams)
# spark.sql("select * from user_result").repartition(1).write.csv('user_result.csv')
# spark.sql("select * from user").repartition(1).write.csv('user.csv')
# spark.sql("select * from match").repartition(1).write.csv('match.csv')
