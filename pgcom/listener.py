# -*- coding: utf-8 -*-
__all__ = ['Listener']

import logging
from select import select
from typing import (
    Any,
    Callable,
    Optional,
)

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from . import Commuter
from .commuter import fix_schema

logger = logging.getLogger('pgcom')


class Listener(Commuter):
    """Listener on the notification channel.

    This class implements an asynchronous interaction with database
    offered by PostgreSQL commands `LISTEN` and `NOTIFY`.

    Notifications are received after trigger is fired, the `poll()`
    method can be used to check for the new notifications without
    wasting resources.

    Methods `create_notify_function()` and `create_trigger()` present
    basic query constructors, which can be used to define a new trigger
    and a new function associated with this trigger. Some custom
    definitions can be done using `execute()` method.
    """

    def __init__(
            self,
            host: str,
            port: str,
            user: str,
            password: str,
            db_name: str,
            **kwargs: str
    ) -> None:
        super().__init__(host, port, user, password, db_name, **kwargs)

    def poll(
            self,
            channel: str,
            on_notify: Optional[Callable[[str], None]] = None,
            on_timeout: Optional[Callable] = None,
            on_close: Optional[Callable] = None,
            on_error: Optional[Callable[[Exception], None]] = None,
            timeout: int = 5
    ) -> None:
        """Listen to the channel and activate callbacks on the notification.

        This function sleeps until awakened when there is some data
        to read on the connection.

        Args:
            channel:
                Name of the notification channel.
            on_notify:
                Callback to be executed when the notification has arrived.
            on_timeout:
                Callback to be executed by timeout.
            on_close:
                Callback to be executed when connection is closed.
            on_error:
                Callback to be executed if error occurs.
            timeout:
                Timeout in seconds.
        """

        with self.connector.make_connection() as conn:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            with conn.cursor() as cur:
                cur.execute(f'LISTEN {channel}')

                try:
                    while 1:
                        if select([conn], [], [], timeout) == ([], [], []):
                            self._callback(on_timeout)
                        else:
                            conn.poll()

                            while conn.notifies:
                                notify = conn.notifies.pop(0)
                                self._callback(on_notify, notify.payload)
                except (Exception, KeyboardInterrupt, SystemExit) as e:
                    cur.execute(f'UNLISTEN {channel}')

                    if (isinstance(e, KeyboardInterrupt) or
                        isinstance(e, SystemExit)):  # noqa: E129
                        self._callback(on_close)
                    else:
                        self._callback(on_error, e)

        self.connector.close_connection()

    def create_notify_function(
            self,
            func_name: str,
            channel: str,
            schema: str = 'public'
    ) -> None:
        """Create a function called by trigger.

        This function generates a notification, which is sending
        to the specified channel when trigger is fired.

        Args:
            func_name:
                Name of the function.
            channel:
                Name of the the channel the notification is sending to.
            schema:
                If not specified, then the public schema is used.
        """

        self.execute(f"""
            CREATE OR REPLACE FUNCTION {schema}.{func_name}()
                RETURNS trigger
            LANGUAGE plpgsql
                AS $function$
            BEGIN
                PERFORM pg_notify('{channel}', row_to_json(NEW)::text);
                RETURN NEW;
            END;
            $function$
            """)

    @fix_schema
    def create_trigger(
            self,
            table_name: str,
            func_name: str,
            schema: str = 'public',
            trigger_name: Optional[str] = None,
            when: str = 'AFTER',
            event: str = 'INSERT',
            for_each: str = 'STATEMENT'
    ) -> None:
        """Create trigger.

        Creates a new trigger associated with the table `table_name` and
        executed the specified function `func_name` when certain
        events occur.

        Args:
            table_name:
                The name of the table the trigger is for.
            func_name:
                A user-supplied function, which is executed when the
                trigger fires.
            schema:
                If not specified, then the public schema is used.
            trigger_name:
                The name to give to the new trigger. If not specified, then
                the automatically created name will be assigned.
            when:
                One of `BEFORE`, `AFTER`, `INSTEAD OF`.
                Determines when function is called.
            event:
                One of `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`.
                Use `OR` for event combinations, e.g. `INSERT OR UPDATE`.
            for_each:
                One of `ROW`, `STATEMENT`. This specifies whether the
                trigger should be fired once for every row affected by the
                event, or just once per SQL statement.
        """

        if trigger_name is None:
            trigger_name = str.lower(
                table_name + '_' + event.replace(' ', '_'))

        self.execute(f"""
            DROP TRIGGER IF EXISTS {trigger_name}
            ON {schema}.{table_name}
            """)

        cmd = f"""
        CREATE TRIGGER {trigger_name} {when} {event}
            ON {schema}.{table_name}
            FOR EACH {for_each}
            EXECUTE FUNCTION {schema}.{func_name}()
        """

        self.execute(cmd)

    @staticmethod
    def _callback(callback: Optional[Callable] = None, *args: Any) -> None:
        if callback:
            try:
                callback(*args)
            except Exception as e:
                logger.error(f'error from callback {callback}: {e}')
