from contextlib import contextmanager
from functools import wraps

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

import hvac


def get_secrets(path, parse_data=True):
    vault_client = hvac.Client()
    data = vault_client.secrets.kv.v2.read_secret_version(path=path)
    if parse_data:
        data = data["data"]["data"]
    return data


engine = create_engine(get_secrets(path="etl_meta")["conn_string"])


def get_session():
    session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine, expire_on_commit=False))
    return session()


@contextmanager
def create_session():
    sess = get_session()
    try:
        yield sess
        sess.commit()
    except Exception as e:
        print(e)
        sess.rollback()
        raise e
    finally:
        sess.close()


def provide_session(func):
    """
    Function decorator that provides a session if it isn't provided.
    If you want to reuse a session or run the function as part of a
    database transaction, you pass it to the function, if not this wrapper
    will create one and close it for you.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        arg_session = "session"

        func_params = func.__code__.co_varnames
        session_in_args = arg_session in func_params and func_params.index(arg_session) < len(args)
        session_in_kwargs = arg_session in kwargs

        if session_in_kwargs or session_in_args:
            return func(*args, **kwargs)
        else:
            with create_session() as sess:
                kwargs[arg_session] = sess
                return func(*args, **kwargs)

    return wrapper
