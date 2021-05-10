from multiprocessing import Process, Manager
from functools import partial
from time import sleep
import sqlite3
import logging
import signal


def start_db(con):
    create_slack_user = (
        "CREATE TABLE IF NOT EXISTS slack_user( "
        "id INTEGER PRIMARY KEY, "
        "slack_user_id TEXT UNIQUE ON CONFLICT FAIL);"
    )
    create_emoji = (
        "CREATE TABLE IF NOT EXISTS emoji( "
        "id INTEGER PRIMARY KEY, "
        "name TEXT UNIQUE ON CONFLICT FAIL, "
        "first_used_created REAL);"
    )
    create_message = (
        "CREATE TABLE IF NOT EXISTS message( "
        "id INTEGER PRIMARY KEY, "
        "user_id INTEGER NOT NULL, "
        "channel TEXT, "
        "m_text TEXT, "
        "timestamp REAL, "
        "FOREIGN KEY (user_id) REFERENCES slack_user(id));"
    )
    create_reaction = (
        "CREATE TABLE IF NOT EXISTS reaction( "
        "id INTEGER PRIMARY KEY, "
        "user_id INTEGER, "
        "message_id INTEGER, "
        "emoji_id INTEGER, "
        "timestamp REAL, "
        "remove INTEGER, "
        "FOREIGN KEY (user_id) REFERENCES slack_user(id), "
        "FOREIGN KEY (message_id) REFERENCES message(id), "
        "FOREIGN KEY (emoji_id) REFERENCES emoji (id));"
    )
    create_model = (
        "CREATE TABLE IF NOT EXISTS model( "
        "id INTEGER PRIMARY KEY, "
        "name TEXT UNIQUE ON CONFLICT FAIL, "
        "description TEXT);"
    )
    create_analysis = (
        "CREATE TABLE IF NOT EXISTS analysis( "
        "id INTEGER PRIMARY KEY, "
        "message_id INTEGER NOT NULL, "
        "model_id INTEGER NOT NULL, "
        "result JSON);"
    )
    con.execute(create_slack_user)
    con.execute(create_emoji)
    con.execute(create_message)
    con.execute(create_reaction)
    con.execute(create_model)
    con.execute(create_analysis)


def get_user_with_id(con, user_id):
    res = con.execute(
        "SELECT id FROM slack_user WHERE slack_user_id = ?", (user_id,)
    ).fetchall()
    return res[0][0] if res else False


def get_emoji_with_name(con, emoji):
    res = con.execute("SELECT id FROM emoji WHERE name = ?", (emoji,)).fetchall()
    return res[0][0] if res else False


def get_message(con, user_id, text, ts):
    res = con.execute(
        "SELECT id FROM message WHERE user_id = ? AND m_text = ? AND timestamp = ?",
        (user_id, text, ts),
    ).fetchall()
    return res[0][0] if res else False


def get_message_text(con, message_id):
    res = con.execute(
        "SELECT m_text FROM message WHERE id = ?", (message_id,)
    ).fetchall()
    return res[0][0] if res else False


def get_model_by_name(con, model_name):
    res = con.execute("SELECT id FROM model WHERE name = ?", (model_name,)).fetchall()
    return res[0][0] if res else False


def get_analysis(con, message_id, model_id):
    res = con.execute(
        "SELECT id FROM analysis WHERE message_id = ? AND model_id = ?",
        (message_id, model_id),
    ).fetchall()
    return res[0][0] if res else False


def get_emoji_ids_by_names(con, emojis):
    qs = ", ".join("?" for _ in emojis)
    return con.execute(f"SELECT id from emoji WHERE name in ({qs})", emojis).fetchall()


def delete_emoji_ids(con, ids):
    qs = ", ".join("?" for _ in ids)
    first = con.execute(
        f"DELETE FROM reaction WHERE emoji_id IN ({qs})", ids
    ).fetchall()
    second = con.execute(f"DELETE FROM emoji WHERE id in ({qs})", ids).fetchall()
    con.commit()
    return first + second


def insert_user_with_id(con, user_id):
    cur = con.cursor()
    cur.execute(
        "INSERT INTO slack_user (slack_user_id) values (?)", (user_id,)
    ).fetchall()
    res = cur.lastrowid
    cur.close()
    con.commit()
    return res


def insert_emoji_with_name(con, emoji_name, ts):
    cur = con.cursor()
    cur.execute(
        "INSERT INTO emoji (name, first_used_created) values (?, ?)", (emoji_name, ts)
    ).fetchall()
    res = cur.lastrowid
    cur.close()
    con.commit()
    return res


def insert_message(con, user_id, channel, text, ts):
    cur = con.cursor()
    cur.execute(
        "INSERT INTO message (user_id, channel, m_text, timestamp) VALUES (?, ?, ?, ?);",
        (user_id, channel, text, ts),
    ).fetchall()
    res = cur.lastrowid
    cur.close()
    con.commit()
    return res


def insert_model(con, model_name):
    cur = con.cursor()
    cur.execute("INSERT INTO model (name) VALUES (?)", (model_name,)).fetchall()
    res = cur.lastrowid
    cur.close()
    con.commit()
    return res


def insert_analysis(con, message_id, model_id, result):
    cur = con.cursor()
    cur.execute(
        "INSERT INTO analysis (message_id, model_id, result) VALUES (?, ?, ?)",
        (message_id, model_id, result),
    ).fetchall()
    res = cur.lastrowid
    cur.close()
    con.commit()
    return res


def rename_emoji_with_name(con, emoji_name, new_name):
    res = con.execute(
        "UPDATE emoji SET name = ? WHERE name = ?", (new_name, emoji_name)
    ).fetchall()
    con.commit()
    return res


def insert_reaction(con, user_id, emoji_id, ts, remove):
    cur = con.cursor()
    cur.execute(
        "INSERT INTO reaction (user_id, emoji_id, timestamp, remove) values (?, ?, ?, ?)",
        (user_id, emoji_id, ts, remove),
    ).fetchall()
    res = cur.lastrowid
    cur.close()
    con.commit()
    return res


def update_reaction_with_message(con, reaction_id, message_id):
    res = con.execute(
        "UPDATE reaction SET message_id = ? WHERE id = ?;", (message_id, reaction_id)
    ).fetchall()
    con.commit()
    return res


def top_n_emojis(con, n, remove):
    return con.execute(
        "SELECT count(name) as uses, name FROM emoji "
        "INNER JOIN reaction ON emoji.id = reaction.emoji_id "
        "WHERE remove = ? "
        "GROUP BY name "
        "ORDER BY uses DESC "
        "LIMIT ?",
        (remove, n),
    ).fetchall()


def top_n_recent(con, n):
    return con.execute(
        "SELECT name, first_used_created, MAX(timestamp = first_used_created) as used "
        "FROM emoji "
        "LEFT JOIN reaction ON reaction.emoji_id = emoji.id "
        "GROUP BY name, first_used_created "
        "ORDER BY first_used_created DESC "
        "LIMIT ?",
        (n,),
    ).fetchall()


def top_n_positive_emojis(con, n, remove):
    return con.execute(
        "SELECT count(emoji.name) as uses, emoji.name "
        "FROM analysis "
        "INNER JOIN message ON analysis.message_id = message.id "
        "INNER JOIN reaction ON reaction.message_id = message.id "
        "INNER JOIN emoji ON reaction.emoji_id = emoji.id "
        "WHERE reaction.remove = ? "
        "AND json_extract(\"result\", '$.compound') >= 0.05 "
        "GROUP BY emoji.name "
        "ORDER BY uses DESC "
        "LIMIT ?",
        (remove, n),
    ).fetchall()


def top_n_neutral_emojis(con, n, remove):
    return con.execute(
        "SELECT count(emoji.name) as uses, emoji.name "
        "FROM analysis "
        "INNER JOIN message ON analysis.message_id = message.id "
        "INNER JOIN reaction ON reaction.message_id = message.id "
        "INNER JOIN emoji ON reaction.emoji_id = emoji.id "
        "WHERE reaction.remove = ? "
        "AND json_extract(\"result\", '$.compound') < 0.05 "
        "AND json_extract(\"result\", '$.compound') > -0.05 "
        "GROUP BY emoji.name "
        "ORDER BY uses DESC "
        "LIMIT ?",
        (remove, n),
    ).fetchall()


def top_n_negative_emojis(con, n, remove):
    return con.execute(
        "SELECT count(emoji.name) as uses, emoji.name "
        "FROM analysis "
        "INNER JOIN message ON analysis.message_id = message.id "
        "INNER JOIN reaction ON reaction.message_id = message.id "
        "INNER JOIN emoji ON reaction.emoji_id = emoji.id "
        "WHERE reaction.remove = ? "
        "AND json_extract(\"result\", '$.compound') <= -0.05 "
        "GROUP BY emoji.name "
        "ORDER BY uses DESC "
        "LIMIT ?",
        (remove, n),
    ).fetchall()


def top_n_emojis_by_user(con, n, user, remove, channel=None):
    query, params = (
        (
            "SELECT count(emoji.name) as uses, emoji.name "
            "FROM emoji "
            "INNER JOIN reaction ON reaction.emoji_id = emoji.id "
            "INNER JOIN slack_user ON slack_user.id = reaction.user_id "
            "INNER JOIN message ON message.id = reaction.message_id "
            "WHERE slack_user.slack_user_id = ? "
            "AND reaction.remove = ? "
            "AND message.channel = ? "
            "GROUP BY emoji.name "
            "ORDER BY uses DESC "
            "LIMIT ?",
            (user, remove, channel, n),
        )
        if channel
        else (
            "SELECT count(emoji.name) as uses, emoji.name "
            "FROM emoji "
            "INNER JOIN reaction ON reaction.emoji_id = emoji.id "
            "INNER JOIN slack_user ON slack_user.id = reaction.user_id "
            "WHERE slack_user.slack_user_id = ? "
            "AND reaction.remove = ? "
            "GROUP BY emoji.name "
            "ORDER BY uses DESC "
            "LIMIT ?",
            (user, remove, n),
        )
    )
    return con.execute(query, params).fetchall()


def close(con):
    con.close()


options = {
    "insert_reaction": insert_reaction,
    "insert_message": insert_message,
    "insert_emoji_with_name": insert_emoji_with_name,
    "insert_user_with_id": insert_user_with_id,
    "get_emoji_with_name": get_emoji_with_name,
    "get_user_with_id": get_user_with_id,
    "get_message": get_message,
    "get_message_text": get_message_text,
    "get_emoji_ids_by_names": get_emoji_ids_by_names,
    "get_model_by_name": get_model_by_name,
    "get_analysis": get_analysis,
    "delete_emoji_ids": delete_emoji_ids,
    "top_n_emojis": top_n_emojis,
    "top_n_recent": top_n_recent,
    "top_n_positive_emojis": top_n_positive_emojis,
    "top_n_negative_emojis": top_n_negative_emojis,
    "top_n_neutral_emojis": top_n_neutral_emojis,
    "top_n_emojis_by_user": top_n_emojis_by_user,
    "rename_emoji_with_name": rename_emoji_with_name,
    "update_reaction_with_message": update_reaction_with_message,
    "insert_model": insert_model,
    "insert_analysis": insert_analysis,
    "close": close,
}


def _run_remote_db(name, q, rq):
    con = sqlite3.connect(name)
    start_db(con)

    def signal_handler(sig, frame):
        con.close()
        logging.info("remote db closing down")
        exit()

    signal.signal(signal.SIGINT, signal_handler)
    while True:
        command = q.get()
        res = options[command[0]](con, *command[1:])
        rq.put(res)
        if command[0] == "close":
            break


class Database:
    def __init__(self, name):
        self.name = name
        self.inq = Manager().Queue()
        self.rq = Manager().Queue()
        self.proc = Process(target=_run_remote_db, args=(self.name, self.inq, self.rq))
        self.proc.start()

    def _remote_call(self, name, *args):
        logging.debug(f"calling {name}")
        self.inq.put([name] + list(args))
        return self.rq.get()

    def __getattr__(self, attr):
        if attr in options:
            return partial(self._remote_call, attr)
        raise AttributeError(attr)


def emoji_user_ts_from_event(body):
    event = body["event"]
    return event["reaction"], event["user"], event["event_ts"]
