from functools import partial
from itertools import chain
from os import environ
from re import match
import sqlite3
import logging
import signal
import json

from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from db import Database, emoji_user_ts_from_event
from views import (
    top_n,
    home_view,
    mrkdwn_section,
    div,
    emoji_to_line,
    emoji_added,
    emoji_help_line,
)

logging.basicConfig(level=logging.INFO)

database = Database(environ["db_file"])
app = App(token=environ["bot_token"])


def signal_handler(sig, frame):
    logging.info("Closing app")
    exit()


def add_analysis_to_message(message_id, analysis, model_name):
    model_id = database.get_model_by_name(model_name) or database.insert_model(
        model_name
    )
    analysis_id = database.get_analysis(message_id, model_id)
    if not analysis_id:
        message = database.get_message_text(message_id)
        result = json.dumps(analysis(message))
        database.insert_analysis(message_id, model_id, result)


def add_message_to_reaction(client, logger, reaction_id, reaction_item):
    # only care about messages
    if reaction_item["type"] != "message":
        logger.info("Reaction to none message")
        return
    channel, message_ts = reaction_item["channel"], reaction_item["ts"]
    try:
        result = client.conversations_history(
            channel=channel, inclusive=True, oldest=message_ts, limit=1
        )
    except Exception as e:
        logger.info(f"Couldn't retrieve message for reaction {e}")
        return
    message = result["messages"][0]
    text, user, ts = message["text"], message["user"], message["ts"]
    user_id = database.get_user_with_id(user) or database.insert_user_with_id(user)
    message_id = database.get_message(user_id, text, ts) or database.insert_message(
        user_id, channel, text, ts
    )
    database.update_reaction_with_message(reaction_id, message_id)

    add_analysis_to_message(
        message_id, SentimentIntensityAnalyzer().polarity_scores, "vader"
    )


def reaction_event(remove_flag, logger, ack, body, client):
    ack()
    emoji, user, ts = emoji_user_ts_from_event(body)
    logger.info(f"reaction: {emoji} by: {user}!")

    user_id = database.get_user_with_id(user) or database.insert_user_with_id(user)
    emoji_id = database.get_emoji_with_name(emoji) or database.insert_emoji_with_name(
        emoji, ts
    )
    reaction_id = database.insert_reaction(user_id, emoji_id, ts, remove_flag)
    add_message_to_reaction(client, logger, reaction_id, body["event"]["item"])


app.event("reaction_added")(partial(reaction_event, 0))
app.event("reaction_removed")(partial(reaction_event, 1))


def emoji_remove(names):
    ids = database.get_emoji_ids_by_names(names)
    flat_ids = list(chain(*ids))
    dels = database.delete_emoji_ids(flat_ids)


@app.event("emoji_changed")
def emoji_changed(logger, ack, event):
    sub_type = event["subtype"]
    if sub_type == "remove":
        emoji_remove(event["names"])
    elif sub_type == "rename":
        database.rename_emoji_with_name(event["old_name"], event["new_name"])
    elif sub_type == "add":
        database.insert_emoji_with_name(event["name"], event["event_ts"])
    else:
        raise NotImplementedError(f"Unhandled subtype {event}")


@app.shortcut("emote")
def emote(client, logger, ack, shortcut):
    ack()
    logger.info("Recieved emote request")
    react_to, channel, ts = (
        shortcut["message"]["text"],
        shortcut["channel"]["id"],
        shortcut["message"]["ts"],
    )
    logger.info(f"Message for reaction: {react_to}")
    sentiment = SentimentIntensityAnalyzer().polarity_scores(react_to)
    comp = sentiment["compound"]
    emoji_per_view = 4
    if comp >= 0.05:
        emojis = database.top_n_positive_emojis(emoji_per_view, 0)
        name = "positive"
    elif comp <= -0.05:
        emojis = database.top_n_negative_emojis(emoji_per_view, 0)
        name = "negative"
    else:
        emojis = database.top_n_neutral_emojis(emoji_per_view, 0)
        name = "neutral"
    client.views_open(
        trigger_id=shortcut["trigger_id"],
        view={
            "type": "modal",
            "title": {"type": "plain_text", "text": "Emoji help"},
            "close": {"type": "plain_text", "text": "Close"},
            "blocks": [
                mrkdwn_section(
                    f"That message seems to express {name} sentiment,"
                    " one of these emojis would work well as a reaction!"
                ),
                top_n(emojis, emoji_help_line),
            ],
        },
    )


@app.command("/top-emojis")
def show_user_top_emoji(ack, logger, command, client):
    ack()
    query_user = match(r"^<@(.*?)[|>]", command["text"].strip())
    query_channel = match(r"<@.*?> <#(.*?)[|>]", command["text"].strip())
    channel = query_channel.groups()[0] if query_channel else None
    channel_text = f" in <#{channel}>" if channel else ""
    emoji_uses = partial(emoji_to_line, "Uses")
    blocks = (
        [
            mrkdwn_section(f"Top Emojis for <@{query_user.groups()[0]}>{channel_text}"),
            top_n(
                database.top_n_emojis_by_user(10, query_user.groups()[0], 0, channel),
                emoji_uses,
            ),
        ]
        if query_user
        else [mrkdwn_section("No user found in command")]
    )
    client.views_open(
        trigger_id=command["trigger_id"],
        view={
            "type": "modal",
            "title": {"type": "plain_text", "text": "Top Emojis"},
            "close": {"type": "plain_text", "text": "Close"},
            "blocks": blocks,
        },
    )


@app.event("app_home_opened")
def home_tab(client, event, logger):
    logger.info("Home page visited")
    emoji_uses = partial(emoji_to_line, "Uses")
    top_10_emojis = top_n(database.top_n_emojis(10, 0), emoji_uses)
    top_10_remove = top_n(
        database.top_n_emojis(10, 1), partial(emoji_to_line, "Removals")
    )
    top_10_recent = top_n(database.top_n_recent(10), emoji_added)
    top_10_positive = top_n(database.top_n_positive_emojis(10, 0), emoji_uses)
    top_10_negative = top_n(database.top_n_negative_emojis(10, 0), emoji_uses)
    top_10_neutral = top_n(database.top_n_neutral_emojis(10, 0), emoji_uses)
    try:
        client.views_publish(
            user_id=event["user"],
            view=home_view(
                extra_blocks=(
                    [
                        mrkdwn_section("Most used Emoji"),
                        top_10_emojis,
                        div,
                        mrkdwn_section("Most removed Emoji"),
                        top_10_remove,
                        div,
                        mrkdwn_section("Most recently added or first used"),
                        top_10_recent,
                        div,
                        mrkdwn_section("Top reactions for positive messages"),
                        top_10_positive,
                        div,
                        mrkdwn_section("Top reactions for negative messages"),
                        top_10_negative,
                        div,
                        mrkdwn_section("Top reactions for neutral messages"),
                        top_10_neutral,
                    ]
                )
            ),
        )
    except Exception as e:
        logger.error(f"Error publishing home tab: {e}")


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    SocketModeHandler(
        app,
        environ["app_token"],
    ).start()
