from parser import split_by_14, parse_14
from repository.in_memory_repository import InMemoryMatchRepository, InMemoryUserRepository, \
    InMemoryUserResultRepository
from repository.repository import MatchRepository, UserRepository, UserResultRepository
from spark_common import spark

lines = open("../../data/input.txt").readlines()
split_lines = split_by_14(lines)
matches = map(parse_14, split_lines)
matches = [m for m in matches if m]

match_repo: MatchRepository = InMemoryMatchRepository()
user_repo: UserRepository = InMemoryUserRepository()
user_result_repo: UserResultRepository = InMemoryUserResultRepository()

for match in matches:
    match_id = match_repo.get_or_create_match_by_date(
        match.finished_at,
        {
            'duration_sec': match.duration_sec,
            'radiant_kills': int(match.score_radiant),
            'dire_kills': int(match.score_dire),
            'radiant_won': match.radiant_won,
        }
    )
    for player in match.dire_players + match.radiant_players:
        player_id = user_repo.get_or_create_user_by_name(
            player.name
        )
        user_result_repo.create_user_result({
            'player_id': player_id,
            'match_id': match_id,
            'net_worth': player.net_worth,
            'kill': player.kills,
            'death': player.deaths,
            'assist': player.assists,
            'is_radiant': player in match.radiant_players,
            'pos': player.pos,
        })
    print(match)

spark.sql("select * from match").show()
