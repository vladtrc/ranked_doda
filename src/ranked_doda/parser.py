from dataclasses import dataclass
from typing import Iterator


@dataclass
class ParsedPlayer:
    name: str
    pos: str
    net_worth: str
    kills: str
    deaths: str
    assists: str


@dataclass
class ParsedMatch:
    length: str
    finished_at: str
    score_rad: str
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
    name = ' '.join(player_line[0 - (-4)])
    return ParsedPlayer(
        name=name,
        pos=pos,
        net_worth=networth,
        kills=kills,
        deaths=deaths,
        assists=assists
    )


def parse_14(ft: list[str]) -> ParsedMatch:
    try:
        return unsafe_parse_14(ft)
    except Exception as e:
        raise RuntimeError('unable to parse 14 lines :\n' + '\n'.join(ft) + '\n\n' + str(e))


def unsafe_parse_14(ft: list[str]) -> ParsedMatch:
    finished_at = ft[0]
    length, score, who_won = ft[1].split()
    score_rad, score_dire = score.split('-')
    radiant_won = who_won.lower() == 'radiant'
    radiant_start_id = 2
    dire_start_id = 8
    if 'dire' in ft[2].lower():
        radiant_start_id, dire_start_id = dire_start_id, radiant_start_id
    dire_players = list(map(parse_player, ft[(dire_start_id + 1): (dire_start_id + 6)]))
    radiant_players = list(map(parse_player, ft[(radiant_start_id + 1): (radiant_start_id + 6)]))
    return ParsedMatch(
        length=length,
        finished_at=finished_at,
        score_rad=score_rad,
        score_dire=score_dire,
        radiant_won=radiant_won,
        radiant_players=radiant_players,
        dire_players=dire_players,
    )


lines = open("../../data/input.txt").readlines()
split_lines = split_by_14(lines)
matches = map(parse_14, split_lines)

for m in matches:
    print(m)
