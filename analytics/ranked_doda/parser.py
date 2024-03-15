import datetime
import logging
from dataclasses import dataclass
from typing import Iterator, Optional
from itertools import takewhile
from repository.in_memory_repository import table_schemas
from spark_common import spark


@dataclass
class ParsedPlayer:
    name: str
    pos: int
    net_worth: int
    kills: int
    deaths: int
    assists: int


@dataclass
class ParsedMatch:
    duration_sec: int
    finished_at: datetime.datetime
    score_radiant: str
    score_dire: str
    radiant_won: bool
    radiant_players: list[ParsedPlayer]
    dire_players: list[ParsedPlayer]


def split_by_14(inp: list[str]) -> list[list[str]]:
    inp_copy = [line.strip() for line in inp]
    results = []
    while inp_copy:
        spaces  = list(takewhile(lambda line: not line, inp_copy))
        inp_copy = inp_copy[len(spaces):]
        partial_res = list(takewhile(lambda line: line, inp_copy))
        if not partial_res:
            break
        if len(partial_res) != 14:
            raise RuntimeError('wrong input, not 14 : ' + '\n'.join(partial_res))
        results += [partial_res]
        inp_copy = inp_copy[len(partial_res):]
    return results


def parse_player(player_line: str) -> ParsedPlayer:
    s = player_line.split()
    kills, deaths, assists = s[-1].split('/')
    networth = s[-2]
    pos = s[-3]
    name = ' '.join(s[0: (-3)]).lower()
    return ParsedPlayer(
        name=name,
        pos=int(pos),
        net_worth=int(networth),
        kills=int(kills),
        deaths=int(deaths),
        assists=int(assists)
    )


def parse_14(ft: list[str]) -> Optional[ParsedMatch]:
    try:
        return unsafe_parse_14(ft)
    except Exception as e:
        logging.error('! ERROR ! ' + 'unable to parse 14 lines :\n' + '\n'.join(ft) + '\n\n' + str(e))
        return None


def unsafe_parse_14(ft: list[str]) -> ParsedMatch:
    finished_at = ft[0]
    length, score, who_won = ft[1].split()
    duration_sec = int(length.split(':')[0]) * 60 + int(length.split(':')[1])
    score_rad, score_dire = score.split('-')
    radiant_won = who_won.lower() == 'radiant'
    radiant_start_id = 2
    dire_start_id = 8
    if 'dire' in ft[2].lower():
        radiant_start_id, dire_start_id = dire_start_id, radiant_start_id
    dire_players = list(map(parse_player, ft[(dire_start_id + 1): (dire_start_id + 6)]))
    radiant_players = list(map(parse_player, ft[(radiant_start_id + 1): (radiant_start_id + 6)]))
    return ParsedMatch(
        duration_sec=duration_sec,
        finished_at=datetime.datetime.strptime(finished_at, '%Y-%m-%d %H:%M'),
        score_radiant=score_rad,
        score_dire=score_dire,
        radiant_won=radiant_won,
        radiant_players=radiant_players,
        dire_players=dire_players,
    )


def parse_file_to_matches(lines: list[str]) -> list[ParsedMatch]:
    split_lines = list(split_by_14(lines))
    parsed_matches = map(parse_14, split_lines)
    return [m for m in parsed_matches if m]


def parse_file(lines: list[str]):
    parsed_matches = parse_file_to_matches(lines)
    matches = []
    users = []
    user_results = []

    users_map = {}
    heroes_map = {}

    for match_id, match in enumerate(parsed_matches, 1):
        matches += [
            {
                'duration_sec': match.duration_sec,
                'radiant_kills': int(match.score_radiant),
                'dire_kills': int(match.score_dire),
                'radiant_won': match.radiant_won,
                'finished_at': match.finished_at,
                'match_id': match_id,
            }
        ]
        for player in match.dire_players + match.radiant_players:
            player_id = users_map.get(player.name, len(users_map) + 1)
            users_map[player.name] = player_id
            user_results += [
                {
                    'player_id': player_id,
                    'match_id': match_id,
                    'net_worth': player.net_worth,
                    'kill': player.kills,
                    'death': player.deaths,
                    'assist': player.assists,
                    'is_radiant': player in match.radiant_players,
                    'pos': player.pos,
                    'name': player.name,
                }
            ]

    users = [{'name': k, 'user_id': v} for k, v in users_map.items()]

    # print(matches)
    # print(users)

    user_schema = table_schemas['user']
    user_result_schema = table_schemas['user_result']
    match_schema = table_schemas['match']

    spark.createDataFrame(users, user_schema).createOrReplaceTempView("user")
    spark.createDataFrame(matches, match_schema).createOrReplaceTempView("match")
    spark.createDataFrame(user_results, user_result_schema).createOrReplaceTempView("user_result")
