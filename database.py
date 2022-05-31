import sqlite3


DATABASE = "citilink.db"


def connect():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn


def create_tables():
    db = connect()
    cur = db.cursor()

    cur.execute(
        """
CREATE TABLE IF NOT EXISTS category (
    id                 CHAR(64) PRIMARY KEY,
    name               TEXT     NOT NULL,
    url                TEXT     NOT NULL,
    is_traversed       INTEGER  NOT NULL DEFAULT 0,
    is_leaf            INTEGER  DEFAULT NULL,
    parent_category_id CHAR(64) DEFAULT NULL,

    FOREIGN KEY (parent_category_id) REFERENCES category(id)
);
        """
    )

    cur.execute(
        """
CREATE TABLE IF NOT EXISTS item (
    category_id  CHAR(64) NOT NULL,
    id           CHAR(64) PRIMARY KEY,
    name         TEXT,
    url          TEXT     NOT NULL,
    is_traversed INTEGER  NOT NULL,
    price        INTEGER,

    FOREIGN KEY (category_id) REFERENCES category(id)
);
        """
    )

    cur.execute(
        """
CREATE TABLE IF NOT EXISTS item_property (
    item_id CHAR(64) NOT NULL,
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    name    TEXT NOT NULL,
    value   TEXT NOT NULL,

    FOREIGN KEY (item_id) REFERENCES item(id)
);
        """
    )

    cur.execute(
        """
CREATE TABLE IF NOT EXISTS item_image (
    item_id CHAR(64),
    url     TEXT PRIMARY KEY,
    path    TEXT,
    number  INTEGER,

    FOREIGN KEY (item_id) REFERENCES item(id)
);
        """
    )

    db.commit()
    cur.close()
    db.close()
