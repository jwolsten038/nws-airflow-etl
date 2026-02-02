import os
import pandas as pd
from sqlalchemy import text, create_engine
from sqlalchemy.engine import URL
from sqlalchemy.dialects.postgresql import JSON, JSONB
import json

def get_engine():
    url = URL.create(
        "postgresql+psycopg2",
        username=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD"),
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        database=os.getenv("PGDATABASE", "fraud_analysis"),
    )
    return create_engine(url)

def reset_tables(engine):
    ddl = """
    DROP TABLE IF EXISTS public.test_json;
    DROP TABLE IF EXISTS public.test_jsonb;

    CREATE TABLE public.test_json (
        id serial primary key,
        payload json NOT NULL
    );

    CREATE TABLE public.test_jsonb (
        id serial primary key,
        payload jsonb NOT NULL
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))

def inspect(engine, table):
    q = f"""
    SELECT id,
           pg_typeof(payload) AS payload_type,
           left(payload::text, 200) AS payload_text,
           payload->>'foo' AS foo
    FROM public.{table}
    ORDER BY id;
    """
    with engine.begin() as conn:
        return conn.execute(text(q)).fetchall()

def main():
    engine = get_engine()
    reset_tables(engine)

    payload_dict = {"foo": "bar", "nested": {"a": 1, "b": [1, 2]}, "list": [1, 2, 3]}
    payload_json_str = json.dumps(payload_dict)

    # dict column (this is what breaks when dtype is omitted)
    df_dict = pd.DataFrame([{"payload": payload_dict}])

    # string column (works even if dtype omitted, but may store as text depending on table type/casts)
    df_str = pd.DataFrame([{"payload": payload_json_str}])

    # ---- dict tests (dtype provided) ----
    df_dict.to_sql("test_jsonb", engine, if_exists="append", index=False, dtype={"payload": JSONB})
    df_dict.to_sql("test_jsonb", engine, if_exists="append", index=False, dtype={"payload": JSON})
    df_dict.to_sql("test_json", engine, if_exists="append", index=False, dtype={"payload": JSON})

    # ---- omission tests (must use string, otherwise psycopg2 can't adapt dict) ----
    df_str.to_sql("test_json", engine, if_exists="append", index=False)     # dtype omitted
    df_str.to_sql("test_jsonb", engine, if_exists="append", index=False)    # dtype omitted

    print("\n== test_json ==")
    for r in inspect(engine, "test_json"):
        print(r)

    print("\n== test_jsonb ==")
    for r in inspect(engine, "test_jsonb"):
        print(r)

if __name__ == "__main__":
    main()