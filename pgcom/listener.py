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
        """Listen to the channel and activate callback on the notification.
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

    def create_notification(
            self,
            notification_name: str,
            channel: str,
            schema: Optional[str] = None
    ) -> None:
        """Create notify function called on trigger.
        """

        schema, _ = self.get_schema(schema=schema)

        cmd = f"""
        CREATE OR REPLACE FUNCTION {schema}.{notification_name}()
            RETURNS trigger
            LANGUAGE plpgsql
        AS $function$
        BEGIN
            PERFORM pg_notify('{channel}', row_to_json(NEW)::text);
            RETURN new;
        END;
        $function$
        """

        self.execute(cmd)

    def create_insert_trigger(
            self,
            notification_name: str,
            table_name: str,
            schema: Optional[str] = None
    ) -> None:
        """Create trigger for each table update.
        """

        schema, _ = self.get_schema(schema=schema)
        trigger_name = table_name + '_insert'

        cmd = f"""
        DROP TRIGGER IF EXISTS {trigger_name}
        ON {schema}.{table_name}
        """

        self.execute(cmd)

        cmd = f"""
        CREATE TRIGGER {trigger_name}
        AFTER INSERT OR UPDATE
        ON {schema}.{table_name}
        FOR EACH ROW EXECUTE FUNCTION {schema}.{notification_name}()
        """

        self.execute(cmd)

    @staticmethod
    def _callback(callback: Optional[Callable] = None, *args: Any) -> None:
        if callback:
            try:
                callback(*args)
            except Exception as e:
                logger.error(f'error from callback {callback}: {e}')
