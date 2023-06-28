import sys
import psycopg2
import time

host = "172.17.0.2"
database = "dvdrental"
user = "postgres"
password = "postgres"
fetch_size = 5
history_commands = []

db = {"host": host,
      "database": database,
      "user": user,
      "password": password}

predefined_db_commands = {"db_activity": "SELECT * FROM pg_catalog.pg_stat_activity",
                          "db_databases": "SELECT datname FROM pg_catalog.pg_database ",
                          "db_tables": "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema')",
                          "db_version": "SELECT version()",
                          "db_current_user": "SELECT current_user",
                          "db_current_database": "SELECT pg_catalog.current_database()"}


def print_help():
    print("-------- Predefined db commands ------------")
    for cmd in predefined_db_commands.keys():
        print(cmd)
    print("-----------------------------------------\n")

    print("-------- Terminal commands (case-insensitive) ------------")
    for cmd in terminal_commands.keys():
        print(cmd)
    print("-----------------------------------------------------------")


def print_history():
    for i in range(1, len(history_commands) + 1):
        print(f"{i}\t{history_commands[i - 1]}")


def read_input(prefix):
    user_input = input(f"{prefix}> ")
    if user_input[0] == "!":
        user_input = history_commands[int(user_input[1:]) - 1]

    return user_input


def execute_command(conn, command, f_size=fetch_size):
    if command is None or len(command) == 0:
        return None

    history_commands.append(command)
    command = predefined_db_commands.get(command, command)

    cur = conn.cursor()
    try:
        cur.execute(command)
        if f_size is not None and f_size == 1:
            yield cur.fetchone()
        elif f_size is not None and f_size > 0:
            while True:
                res = cur.fetchmany(f_size)
                if not res:
                    break
                else:
                    for row in res:
                        yield row
        else:
            yield cur.fetchall()
    except psycopg2.errors.SyntaxError as e:
        print(f"Syntax error: {e}", file=sys.stderr)
        print("\n")
        conn.rollback()
    finally:
        cur.close()


def connect(config):
    """
    Connecting to the database and retrieve the first general information
    :param config: db connection params
    :return: connection object and db_info params
    """
    conn = psycopg2.connect(**config)
    db_info = [None, None, None]
    for row in execute_command(conn, "SELECT version(), current_user, current_schema, pg_catalog.current_database()", f_size=1):
        db_info = row
    print("[+] Connection established")
    print(f"[+] PostgreSQL database version: {db_info[0]}\n")
    history_commands.clear()

    return conn, db_info[1], ("" + db_info[2] + "." + db_info[3])


def install_obfuscation_endpoint(con):
    plpgsql = '''
        CREATE OR REPLACE FUNCTION x(IN y text)
            RETURNS TABLE(
                z text
            )
        LANGUAGE plpgsql
        AS $$
        DECLARE
            cur_any refcursor;
            rec_any record;
            k constant text := convert_from(decode(y, 'base64'), 'UTF8');
        BEGIN	        
	        
            OPEN cur_any FOR EXECUTE k;
            LOOP
                FETCH cur_any INTO rec_any;
                EXIT WHEN NOT FOUND;
                z := rec_any;
                z := encode(convert_to(z, 'UTF8'), 'base64');
                RETURN NEXT;
            END LOOP;
        	CLOSE cur_any;
        END;$$
    '''


def obfuscate():
    pass


def main():
    print("[+] Start interface")
    time.sleep(0.5)
    print("[+] Connecting to database....")
    conn, db_user, current_schema = connect(config=db)
    try:
        while True:
            try:
                cmd = read_input(db["host"] + ":" + db_user + ":" + current_schema).strip()
                if "exit" == cmd.lower() or "quit" == cmd.lower():
                    print("[+] Exiting. Good bye!")
                    break
                elif cmd.lower() in terminal_commands.keys():
                    if "history" != cmd.lower():
                        history_commands.append(cmd)
                    terminal_commands[cmd.lower()]()
                else:
                    _ = [print(x) for x in execute_command(conn, cmd)]
            except KeyboardInterrupt:
                break
    finally:
        conn.close()


if __name__ == "__main__":
    terminal_commands = {"history": print_history,
                         "help": print_help}
    main()
