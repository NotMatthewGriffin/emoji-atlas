from itertools import starmap
from datetime import datetime

div = {"type": "divider"}


def home_view(extra_blocks=[]):
    return {
        "type": "home",
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": ":star: Emoji Atlas :star:",
                    "emoji": True,
                },
            },
            div,
        ]
        + extra_blocks,
    }


def emoji_to_line(word, num, entry):
    return f"{num}. :{entry[1]}: {word}: {entry[0]}"


def emoji_help_line(num, entry):
    return f"{num}. :{entry[1]}: type with `:{entry[1]}:`"


def emoji_added(num, entry):
    return (
        f"{num}. :{entry[0]}: {'first used' if entry[2] else 'added'}: "
        f"{datetime.fromtimestamp(entry[1])}"
    )


def mrkdwn_section(text):
    return {"type": "section", "text": mrkdwn(text)}


def mrkdwn(text):
    return {"type": "mrkdwn", "text": text}


def top_n(entries, to_line):
    fields = list(map(mrkdwn, starmap(to_line, enumerate(entries, 1))))
    return {"type": "section", "fields": fields if fields else [mrkdwn("Nothing yet!")]}
