import json
import random
from typing import Dict


def get_row() -> Dict:
    row = {
        'int': {'ts': 1641672504 + random.randint(0, 100_000)},
        'str': {
            'event': random.choice(['init_app', 'exception', 'edit'])}
    }
    if random.randint(0, 1):
        row['str']['message'] = random.choice(['ok', 'yay', 'hi'])
    if random.randint(0, 1):
        row['int']['count'] = random.randint(0, 1000)
    return row


def gen_fixtures(num_rows: int = 100, file_name: str = 'log.json') -> None:
    with open(file_name, 'w') as cur_f:
        for _ in range(num_rows):
            cur_f.write(f"{json.dumps(get_row(), separators=(',', ':'))}\n")


if __name__ == "__main__":
    gen_fixtures()
