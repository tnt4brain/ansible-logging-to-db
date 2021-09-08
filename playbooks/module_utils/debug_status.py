import os
import pg8000

HAS_PG8K = True


def connect_to_db(module, conn_params, autocommit=False, fail_on_conn=True):
    """Connect to a PostgreSQL database.

    Return psycopg2 connection object.

    Args:
        module (AnsibleModule) -- object of ansible.module_utils.basic.AnsibleModule class
        conn_params (dict) -- dictionary with connection parameters

    Kwargs:
        autocommit (bool) -- commit automatically (default False)
        fail_on_conn (bool) -- fail if connection failed or just warn and return None (default True)
    """

    db_connection = None
    try:
        del conn_params['sslmode']
        db_connection = pg8000.connect(**conn_params)
        if autocommit:
            db_connection.set_session(autocommit=True)

        # Switch role, if specified:
        if conn_params.get('session_role'):
            cursor = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            try:
                cursor.execute('SET ROLE %s' % module.params['session_role'])
            except Exception as e:
                module.fail_json(msg="Could not switch role: %s" % to_native(e))
            finally:
                cursor.close()

    except TypeError as e:
        if 'sslrootcert' in e.args[0]:
            module.fail_json(msg='Postgresql server must be at least '
                                 'version 8.4 to support sslrootcert')

        if fail_on_conn:
            module.fail_json(msg="unable to connect to database: %s" % to_native(e))
        else:
            module.warn("PostgreSQL server is unavailable: %s" % to_native(e))
            db_connection = None

    except Exception as e:
        if fail_on_conn:
            module.fail_json(msg="unable to connect to database: %s" % to_native(e))
        else:
            module.warn("PostgreSQL server is unavailable: %s" % to_native(e))
            db_connection = None

    return db_connection


os.setgid(26)
os.setuid(26)
# /var/run/postgresql/.s.PGSQL.5432
conn = connect_to_db(None, {'user': 'postgres', 'unix_sock': "/tmp/.s.PGSQL.5432", 'sslmode': None})
cursor = conn.cursor()
cursor.execute("SELECT rolname FROM pg_roles;")
print(cursor.statusmessage)
conn.close()

