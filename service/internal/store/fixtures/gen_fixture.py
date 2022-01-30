import json
import random
import sys
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


def gen_fixtures(num_rows: int, file_name: str) -> None:
    with open(file_name, 'w') as cur_f:
        for _ in range(num_rows):
            cur_f.write(f"{json.dumps(get_row(), separators=(',', ':'))}\n")


if __name__ == "__main__":
    num_rows = int(sys.argv[1]) if len(sys.argv) >= 2 else 100
    file_name = sys.argv[2] if len(sys.argv) >= 3 else 'log.json'
    gen_fixtures(num_rows, file_name)
