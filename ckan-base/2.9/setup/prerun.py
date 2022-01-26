import os
import sys
import subprocess
import psycopg2
try:
    from urllib.request import urlopen
    from urllib.error import URLError
except ImportError:
    from urllib2 import urlopen
    from urllib2 import URLError

import time
import re

ckan_ini = os.environ.get("CKAN_INI", "/srv/app/ckan.ini")

RETRY = 5


def update_plugins():

    plugins = os.environ.get("CKAN__PLUGINS", "")
    print(("[prerun] Setting the following plugins in {}:".format(ckan_ini)))
    print(plugins)
    cmd = ["ckan", "config-tool", ckan_ini, "ckan.plugins = {}".format(plugins)]
    subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    print("[prerun] Plugins set.")


def check_main_db_connection(retry=None):

    conn_str = os.environ.get("CKAN_SQLALCHEMY_URL")
    if not conn_str:
        print("[prerun] CKAN_SQLALCHEMY_URL not defined, not checking db")
    return check_db_connection(conn_str, retry)


def check_datastore_db_connection(retry=None):

    conn_str = os.environ.get("CKAN_DATASTORE_WRITE_URL")
    if not conn_str:
        print("[prerun] CKAN_DATASTORE_WRITE_URL not defined, not checking db")
    return check_db_connection(conn_str, retry)


def check_db_connection(conn_str, retry=None):

    if retry is None:
        retry = RETRY
    elif retry == 0:
        print("[prerun] Giving up after 5 tries...")
        sys.exit(1)

    try:
        connection = psycopg2.connect(conn_str)

    except psycopg2.Error as e:
        print(str(e))
        print("[prerun] Unable to connect to the database, waiting...")
        time.sleep(10)
        check_db_connection(conn_str, retry=retry - 1)
    else:
        connection.close()


def check_solr_connection(retry=None):

    if retry is None:
        retry = RETRY
    elif retry == 0:
        print("[prerun] Giving up after 5 tries...")
        sys.exit(1)

    url = os.environ.get("CKAN_SOLR_URL", "")
    search_url = "{url}/select/?q=*&wt=json".format(url=url)

    try:
        connection = urlopen(search_url)
    except URLError as e:
        print(str(e))
        print("[prerun] Unable to connect to solr, waiting...")
        time.sleep(10)
        check_solr_connection(retry=retry - 1)
    else:
        eval(connection.read())


def init_db():

    db_command = ["ckan", "-c", ckan_ini, "db", "init"]
    print("[prerun] Initializing or upgrading db - start")
    try:
        subprocess.check_output(db_command, stderr=subprocess.STDOUT)
        print("[prerun] Initializing or upgrading db - end")
    except subprocess.CalledProcessError as e:
        if "OperationalError" in e.output:
            print(e.output)
            print("[prerun] Database not ready, waiting a bit before exit...")
            time.sleep(5)
            sys.exit(1)
        else:
            print(e.output)
            raise e


def init_datastore_db():

    conn_str = os.environ.get("CKAN_DATASTORE_WRITE_URL")
    if not conn_str:
        print("[prerun] Skipping datastore initialization")
        return

    datastore_perms_command = ["ckan", "-c", ckan_ini, "datastore", "set-permissions"]

    connection = psycopg2.connect(conn_str)
    cursor = connection.cursor()

    print("[prerun] Initializing datastore db - start")
    try:
        datastore_perms = subprocess.Popen(
            datastore_perms_command, stdout=subprocess.PIPE
        )

        perms_sql = datastore_perms.stdout.read()
        # Remove internal pg command as psycopg2 does not like it
        perms_sql = re.sub(b'\\\\connect "(.*)"', b"", perms_sql)
        cursor.execute(perms_sql)
        for notice in connection.notices:
            print(notice)

        connection.commit()

        print("[prerun] Initializing datastore db - end")
        print(datastore_perms.stdout.read())
    except psycopg2.Error as e:
        print("[prerun] Could not initialize datastore")
        print(str(e))

    except subprocess.CalledProcessError as e:
        if "OperationalError" in e.output:
            print(e.output)
            print("[prerun] Database not ready, waiting a bit before exit...")
            time.sleep(5)
            sys.exit(1)
        else:
            print(e.output)
            raise e
    finally:
        cursor.close()
        connection.close()


def create_sysadmin():

    name = os.environ.get("CKAN_SYSADMIN_NAME")
    password = os.environ.get("CKAN_SYSADMIN_PASSWORD")
    email = os.environ.get("CKAN_SYSADMIN_EMAIL")

    if name and password and email:

        # Check if user exists
        command = ["ckan", "-c", ckan_ini, "user", "show", name]

        out = subprocess.check_output(command)
        if b"User:None" not in re.sub(b"\s", b"", out):
            print("[prerun] Sysadmin user exists, skipping creation")
            return

        # Create user
        command = [
            "ckan",
            "-c",
            ckan_ini,
            "user",
            "add",
            name,
            "password=" + password,
            "email=" + email,
        ]

        subprocess.call(command)
        print("[prerun] Created user {0}".format(name))

        # Make it sysadmin
        command = ["ckan", "-c", ckan_ini, "sysadmin", "add", name]

        subprocess.call(command)
        print("[prerun] Made user {0} a sysadmin".format(name))

def init_harvester_db_tables():
    plugins = os.environ.get("CKAN__PLUGINS", "")
    if "ckan_harvester" in plugins.strip().split(" "):
        print("[prerun] Harvester DB Tables init - Started")
        command = ["ckan", "-c", ckan_ini, "harvester", "initdb"]
        try:
            subp = subprocess.Popen(
                command, stdout=subprocess.PIPE
            )
            print("[prerun] Harvester DB Tables init - Ended")
            print(subp.stdout.read())
        except subprocess.CalledProcessError as e:
            print(e.output)
            print("[prerun] Harvester DB Tables init - Failed")

def init_spatial_db_tables():
    plugins = os.environ.get("CKAN__PLUGINS", "")
    p = plugins.strip().split(" ")
    if "spatial_metadata" in p and "spatial_query" in p:
        print("[prerun] Spatial DB Tables init - Started")
        command = ["ckan", "-c", ckan_ini, "spatial", "initdb"]
        try:
            subp = subprocess.Popen(
                command, stdout=subprocess.PIPE
            )
            print(subp.stdout.read())
            conn_str = os.environ.get("CKAN_SQLALCHEMY_URL")
            ckan_database = conn_str.split('postgresql://')[1].split(':')[0]

            connection = psycopg2.connect(conn_str)
            cursor = connection.cursor()

            statement_sql = f"""
                ALTER VIEW geometry_columns OWNER TO {ckan_database};
                ALTER TABLE spatial_ref_sys OWNER TO {ckan_database};
            """

            cursor.execute(statement_sql)
            for notice in connection.notices:
                print(notice)
            
            connection.commit()
            cursor = connection.cursor()
            print("[prerun] Spatial DB Tables init - Ended")
            
        except subprocess.CalledProcessError as e:
            print(e.output)
            print("[prerun] Spatial DB Tables init - Failed")
        except psycopg2.Error as e:
            print(e)
            print("[prerun] Spatial DB Tables init - Failed")
        finally:
            cursor.close()
            connection.close()

def init_taxonomy_db_tables():
    plugins = os.environ.get("CKAN__PLUGINS", "")
    if "taxonomy" in plugins.strip().split(" "):
        print("[prerun] Taxonomy DB Tables init - Started")
        command = ["ckan", "-c", ckan_ini, "taxonomy", "init"]
        try:
            subp = subprocess.Popen(
                command, stdout=subprocess.PIPE
            )
            print("[prerun] Taxonomy DB Tables init - Ended")
            print(subp.stdout.read())
        except subprocess.CalledProcessError as e:
            print(e.output)
            print("[prerun] Taxonomy DB Tables init - Failed")

if __name__ == "__main__":

    maintenance = os.environ.get("MAINTENANCE_MODE", "").lower() == "true"

    if maintenance:
        print("[prerun] Maintenance mode, skipping setup...")
    else:
        check_main_db_connection()
        init_db()
        update_plugins()
        check_datastore_db_connection()
        init_datastore_db()
        create_sysadmin()
        init_harvester_db_tables()
        init_spatial_db_tables()
        init_taxonomy_db_tables()
        check_solr_connection()
