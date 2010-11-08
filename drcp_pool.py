import cx_Oracle
import sqlalchemy as sa

class DRCPPool(sa.pool.Pool):
    """
        Use Oracle Database Resident Connection Pooling instead of
        SQLAlchemy's connection pooling.

        In contrast to the other SQLAlchemy connection pools, the DRCPPool
        requires database connection information as a parameter to the
        pool. The URL given to the engine will be ignored. This means
        that you have to create the DRCPPool explicitly and use the
        "pool" parameter to pass the DRCPPool to the engine.

        For this to actually use DRCP, you will have to start the
        connection pool in your db and connect with (SERVER = POOLED)
        in your tnsnames.ora, or :pooled at the end of your easy connect
        string.
    """
    def __init__(self, db_user, db_pass, db_conn,
            min, max, increment, connectiontype=None, threaded=False,
            cclass=None, **kwargs):
        sa.pool.Pool.__init__(self, self.__creator, echo=None, **kwargs)
        self._cclass = cclass
        self.__pool = cx_Oracle.SessionPool(
            db_user, db_pass, db_conn, min, max, increment,
            threaded=threaded, connectiontype=connectiontype)
        self.recreate_args = (db_user, db_pass, db_conn,
                min, max, increment, connectiontype, threaded, cclass)
        self.recreate_kwargs = kwargs

    def __creator(self):
        if self._cclass is None:
            return self.__pool.acquire()
        else:
            return self.__pool.acquire(cclass=self._cclass)

    def do_return_conn(self, conn):
        if conn.connection is not None:
            self.__pool.release(conn.connection)

    def status(self):
        return 'DRCPPool for database %s; open connections: %d/%d'\
                 % (self.__pool.dsn, self.__pool.opened, self.__pool.max)

    def do_return_invalid(self, conn):
        pass

    def do_get(self):
        return self.create_connection()

    def dispose(self):
        pass

    def recreate(self):
        return DRCPPool(*self.recreate_args, **self.recreate_kwargs)
