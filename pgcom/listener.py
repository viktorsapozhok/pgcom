# -*- coding: utf-8 -*-
__all__ = ['Listener']

import json
import logging
from select import select
from typing import (
    Any,
    Callable,
    Dict,
    Optional,
)

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from . import Commuter

logger = logging.getLogger('pgcom')


class Listener(Commuter):
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
            on_notify: Optional[Callable[[Dict[str, Any]], None]] = None,
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
                cur.execute(f'LISTEN {channel};')

                try:
                    while 1:
                        if select([conn], [], [], timeout) == ([], [], []):
                            self._callback(on_timeout)
                        else:
                            conn.poll()

                            while conn.notifies:
                                notify = conn.notifies.pop(0)
                                self._callback(
                                    on_notify,
                                    json.loads(notify.payload))
                except (Exception, KeyboardInterrupt, SystemExit) as e:
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
            schema: Optional[str] = None
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

        schema, _ = self.get_schema(schema=schema)

        self.execute(f"""
            CREATE OR REPLACE FUNCTION {schema}.{func_name}()
                RETURNS trigger
            LANGUAGE plpgsql
                AS $function$
            BEGIN
                PERFORM pg_notify('{channel}', row_to_json(NEW)::text);
                RETURN new;
            END;
            $function$
            """)

    def create_trigger(
            self,
            func_name: str,
            table_name: str,
            when: str = 'AFTER',
            event: str = 'INSERT',
            for_each: str = 'STATEMENT',
            trigger_name: Optional[str] = None,
            schema: Optional[str] = None
    ) -> None:
        """Create trigger.

        Creates a new trigger associated with the table `table_name` and
        executed the specified function `func_name` when certain
        events occur.

        Args:
            func_name:
                A user-supplied function, which is executed when the
                trigger fires.
            table_name:
                The name of the table the trigger is for.
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
            trigger_name:
                The name to give to the new trigger. If not specified, then
                the automatically created name will be assigned.
            schema:
                If not specified, then the public schema is used.
        """

        schema, _ = self.get_schema(schema=schema)

        if trigger_name is None:
            trigger_name = str.lower(
                table_name + '_' + event.replace(' ', '_'))

        self.execute(f"""
            DROP TRIGGER IF EXISTS {trigger_name}
            ON {schema}.{table_name}
            """)

        self.execute(f"""
            CREATE TRIGGER {trigger_name} {when} {event}
            ON {schema}.{table_name}
            FOR EACH {for_each}
            EXECUTE FUNCTION {schema}.{func_name}()
            """)

    @staticmethod
    def _callback(callback: Optional[Callable] = None, *args: Any) -> None:
        if callback:
            try:
                callback(*args)
            except Exception as e:
                logger.error(f'error from callback {callback}: {e}')
