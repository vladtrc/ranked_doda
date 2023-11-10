import datetime
import logging
from dataclasses import dataclass
from time import strptime
from typing import Iterator, Optional


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


def split_by_14(inp: list[str]) -> Iterator[list[str]]:
    partial_res = []
    for e in inp:
        e = e.strip()
        if e:
            partial_res += [e]
            continue
        if not partial_res:
            continue
        if len(partial_res) != 14:
            raise RuntimeError('wrong input, not 14 : ' + '\n'.join(partial_res))
        yield partial_res
        partial_res = []


def parse_player(player_line: str) -> ParsedPlayer:
    s = player_line.split()
    kills, deaths, assists = s[-1].split('/')
    networth = s[-2]
    pos = s[-3]
    name = ' '.join(s[0: (-3)])
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
