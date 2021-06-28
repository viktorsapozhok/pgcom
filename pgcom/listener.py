__all__ = ["Listener"]

import logging
from select import select
from typing import (
    Any,
    Callable,
    Optional,
)

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from .base import BaseCommuter
from .connector import Connector

logger = logging.getLogger("pgcom")


class Listener(BaseCommuter):
    """Listener on the notification channel.

    This class implements an asynchronous interaction with database
    offered by PostgreSQL commands LISTEN and NOTIFY.

    Notifications are received after trigger is fired, the
    :func:`~pgcom.listener.Listener.poll` method can be used
    to check for the new notifications without wasting resources.

    Methods :func:`~pgcom.listener.Listener.create_notify_function` and
    :func:`~pgcom.listener.Listener.create_trigger` present
    basic query constructors, which can be used to define a new trigger
    and a new function associated with this trigger. Some custom
    definitions can be done using :func:`~pgcom.commuter.Commuter.execute`
    method.

    Args:
        pool_size:
            The maximum amount of connections the pool will support.
        pre_ping:
            If True, the pool will emit a "ping" on the connection to
            test if the connection is alive. If not, the connection will
            be reconnected.
        max_reconnects:
            The maximum amount of reconnects, defaults to 3.
    """

    def __init__(
        self,
        pool_size: int = 20,
        pre_ping: bool = False,
        max_reconnects: int = 3,
        **kwargs: str,
    ) -> None:
        super().__init__(Connector(pool_size, pre_ping, max_reconnects, **kwargs))

    def poll(
        self,
        channel: str,
        on_notify: Optional[Callable[[str], None]] = None,
        on_timeout: Optional[Callable] = None,
        on_close: Optional[Callable] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
        timeout: int = 5,
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

        with self.connector.open_connection() as conn:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            with conn.cursor() as cur:
                cur.execute(f"LISTEN {channel}")

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
                    cur.execute(f"UNLISTEN {channel}")

                    if isinstance(e, KeyboardInterrupt) or isinstance(
                        e, SystemExit
                    ):  # noqa: E129
                        self._callback(on_close)
                    else:
                        self._callback(on_error, e)

    def create_notify_function(self, func_name: str, channel: str) -> None:
        """Create a function called by trigger.

        This function generates a notification, which is sending
        to the specified channel when trigger is fired.

        Args:
            func_name:
                Name of the function.
            channel:
                Name of the the channel the notification is sending to.
        """

        _schema, _func_name = self._get_schema(func_name)

        self.execute(
            f"""
            CREATE OR REPLACE FUNCTION {_schema}.{_func_name}()
                RETURNS trigger
            LANGUAGE plpgsql
                AS $function$
            BEGIN
                PERFORM pg_notify('{channel}', row_to_json(NEW)::text);
                RETURN NEW;
            END;
            $function$
            """
        )

    def create_trigger(
        self,
        table_name: str,
        func_name: str,
        trigger_name: Optional[str] = None,
        when: str = "AFTER",
        event: str = "INSERT",
        for_each: str = "STATEMENT",
    ) -> None:
        """Create trigger.

        Creates a new trigger associated with the table and
        executed the specified function when certain events occur.

        Args:
            table_name:
                The name of the table the trigger is for.
            func_name:
                A user-supplied function, which is executed when the
                trigger fires.
            trigger_name:
                The name to give to the new trigger. If not specified, then
                the automatically created name will be assigned.
            when:
                One of "BEFORE", "AFTER", "INSTEAD OF".
                Determines when function is called.
            event:
                One of "INSERT", "UPDATE", "DELETE", "TRUNCATE".
                Use "OR" for event combinations, e.g. "INSERT OR UPDATE".
            for_each:
                One of "ROW", "STATEMENT". This specifies whether the
                trigger should be fired once for every row affected by the
                event, or just once per SQL statement.
        """

        _schema, _table_name = self._get_schema(table_name)

        if trigger_name is None:
            trigger_name = str.lower(_table_name + "_" + event.replace(" ", "_"))

        self.execute(
            f"""
            DROP TRIGGER IF EXISTS {trigger_name}
            ON {_schema}.{_table_name}
        """
        )

        cmd = f"""
        CREATE TRIGGER {trigger_name} {when} {event}
            ON {_schema}.{_table_name}
            FOR EACH {for_each}
            EXECUTE FUNCTION {_schema}.{func_name}()
        """

        self.execute(cmd)

    @staticmethod
    def _callback(callback: Optional[Callable] = None, *args: Any) -> None:
        if callback:
            try:
                callback(*args)
            except Exception as e:
                logger.error(f"error from callback {callback}: {e}")
