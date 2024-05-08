import sys
import string
import random
import argparse
from datetime import datetime
from urllib.parse import quote_plus
from sqlalchemy.dialects.postgresql import INET
from sqlalchemy.exc import OperationalError
from sqlalchemy import (
    create_engine, MetaData, PrimaryKeyConstraint, text, inspect, and_, Table, Column, Boolean, Text, String, DateTime,
    Integer, BigInteger, Enum, ARRAY, Date, Time, LargeBinary, DOUBLE_PRECISION
)

CHUNK_SIZE = 1000

data_types_mapping = {
        'TINYINT': Boolean,
        'LONGTEXT': Text,
        'DATETIME': DateTime,
        'DOUBLE': DOUBLE_PRECISION,
        'MEDIUMINT': Integer,
        'INT UNSIGNED': Integer,
        'BIGINT UNSIGNED': BigInteger,
        'ENUM': Enum,
        'SET': ARRAY,
        'TINYINT UNSIGNED': Integer,
        'DATE': Date,
        'TIME': Time,
        'TIMESTAMP': DateTime,
        'DEFAULT CURRENT_TIMESTAMP': DateTime,
        'BINARY': LargeBinary,
        'VARBINARY': LargeBinary,
        'MEDIUMBLOB': LargeBinary,
        'LONGBLOB': LargeBinary
    }

clean_tables = {
    'mysql': (),
    'postgresql': ()
}

self_ref_tables = ()

non_csv_tables = ()


def generate_random_string(length=5):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for _ in range(length))


def check_column_existence(engine, table, column_name):
    inspector = inspect(engine)
    columns = inspector.get_columns(table)
    for column in columns:
        if column['name'] == column_name:
            return True
    return False


def check_sequence_existence(connection, table_name):
    query = text(f"select exists (select 1 from pg_catalog.pg_sequences where sequencename = '{table_name}_id_seq');")
    return connection.execute(query).scalar()


def check_table_existence(connection, table_name):
    query = text(f"select exists (select 1 from information_schema.tables where table_name = '{table_name}');")
    return connection.execute(query).scalar()


def repair_ids(connection, postgres_table):
    if 'id' in postgres_table.columns:
        if postgres_table.primary_key is None or not len(postgres_table.primary_key.columns.keys()):
            query = f"alter table {postgres_table.name} add constraint {postgres_table.name}_pkey primary key (id);"
            connection.execute(text(query))


def repair_autoincrement(connection, postgres_table):
    if postgres_table.autoincrement_column is None:
        if postgres_table.primary_key is None or not len(postgres_table.primary_key.columns.keys()):
            id_col = 'id'
        else:
            id_col = list(postgres_table.primary_key.columns.keys())[0]
        seq_name = f"{postgres_table.name}_id_seq"
        queries = [
            f"create sequence if not exists {seq_name};",
            f"alter table {postgres_table.name} "
            f"alter column {id_col} set default nextval('{seq_name}');",
            f"update {postgres_table.name} set "
            f"{id_col} = nextval('{seq_name}') where {id_col} is null;"
        ]
        for query in queries:
            connection.execute(text(query))


def repair_sequences(connection, postgres_table, echo=False):
    id = postgres_table.autoincrement_column.name
    if echo:
        sys.stdout.write(f'\n({postgres_table.name}) - id: {id}')
    if id:
        max_id = connection.execute(text(f'select max({id}) from "{postgres_table.name}";')).scalar()
        if max_id is not None:
            connection.execute(text(f'alter sequence "{postgres_table.name}_id_seq" restart with {max_id + 1};'))
    else:
        raise RuntimeError()


def repair_datatypes(connection, mysql_table, postgres_table):
    for mysql_column in mysql_table.columns:
        if str(mysql_column.type).startswith('VARCHAR'):
            for column in postgres_table.columns:
                if column.name == mysql_column.name:
                    length = mysql_column.type.length
                    if length != column.type.length:
                        print(mysql_column.type.length, column.type.length)
                        connection.execute(text(f'alter table {postgres_table.name} alter column '
                                                f'"{column.name}" type varchar({length})'))
                    break


def repair_all(postgres_engine, postgres_metadata, echo=False):
    postgres_tables = postgres_metadata.tables.values()
    for postgres_table in postgres_tables:
        with postgres_engine.begin() as connection:
            repair_ids(connection, postgres_table)
    for postgres_table in postgres_tables:
        with postgres_engine.begin() as connection:
            repair_autoincrement(connection, postgres_table)
    for postgres_table in postgres_tables:
        with postgres_engine.begin() as connection:
            repair_sequences(connection, postgres_table, echo=echo)


def update_content_data(mysql_engine, postgres_engine, mysql_table, postgres_table):
    with mysql_engine.begin() as mysql_connection:
        result_proxy = mysql_connection.execute(mysql_table.select())
        total_rows = mysql_connection.execute(text(f'select count(*) from {mysql_table}')).scalar()  # noqa
        j = 0

        for row in result_proxy:
            with postgres_engine.begin() as connection:
                query = postgres_table.select().where(and_(
                    postgres_table.c.app_label == row.app_label, postgres_table.c.model == row.model))
                existing_row = connection.execute(query).fetchone()
                if not existing_row:
                    insert_data = {'app_label': row.app_label, 'model': row.model}
                    connection.execute(postgres_table.insert().values(**insert_data))
                    connection.commit()

            j += 1
            completion_percentage = (j / total_rows) * 100
            txt = f"Row {j}/{total_rows} {completion_percentage:.2f}% complete"
            txt = '\r' + txt + 5 * '\t' if j > 1 else '\n' + txt
            sys.stdout.write(txt)


def add_table_data(connection, mysql_engine, postgres_engine, mysql_table, postgres_table, use_csv, sep='\n'):
    with mysql_engine.begin() as mysql_connection:
        if not use_csv or (mysql_table.name in non_csv_tables):
            result_proxy = mysql_connection.execute(mysql_table.select())
            total_rows = mysql_connection.execute(text(f'select count(*) from {mysql_table}')).scalar() # noqa
            chunk_size = CHUNK_SIZE
            j, k = 0, 0
            while True:
                chunk = result_proxy.fetchmany(chunk_size)
                if not chunk:
                    break
                data_to_insert = []
                for row in chunk:
                    new_row = {}
                    for key, value in row._asdict().items(): # noqa
                        new_row[key.lower()] = value
                    data_to_insert.append(new_row)

                if data_to_insert:
                    connection.execute(postgres_table.insert().values(data_to_insert))
                    connection.commit()

                j += len(chunk)
                completion_percentage = (j / total_rows) * 100
                txt = f"Table {postgres_table.name}: row {j}/{total_rows} {completion_percentage:.2f}% complete"
                txt = '\r' + txt + 5 * '\t' if k > 0 else sep + txt
                sys.stdout.write(txt)
                k += 1

        if (
            check_column_existence(postgres_engine, postgres_table.name, 'id')
            and check_sequence_existence(connection, postgres_table.name)
            and check_table_existence(connection, postgres_table.name)
        ):
            try:
                repair_sequences(connection, postgres_table)
            except: # noqa
                pass


def create_new_table(connection, mysql_table, table_name, postgres_metadata):
    columns = []
    for col in mysql_table.columns:  # noqa
        col_type = data_types_mapping[str(col.type)] if str(col.type) in data_types_mapping else col.type
        if col.name == 'ip':
            col_type = INET
        elif isinstance(col_type, String):
            col_type = String(255)

        if col_type == Enum:
            enum_name = f'{table_name}_{col.name}_enum'.lower()
            enum_values = col.type.enums
            enum_column = Enum(*enum_values, name=enum_name)
            columns.append(Column(col.name.lower(), enum_column, nullable=col.nullable))
        else:
            columns.append(Column(col.name.lower(), col_type, nullable=col.nullable))

    postgres_table = Table(table_name, postgres_metadata, *columns)
    primary_key_columns = [postgres_table.columns[col.name.lower()] for col in mysql_table.primary_key.columns]
    postgres_table.append_constraint(PrimaryKeyConstraint(*primary_key_columns))
    postgres_table.create(connection)

    for mysql_foreign_key in mysql_table.foreign_keys:
        foreign_key_name = mysql_foreign_key.constraint.name[:63].lower()
        foreign_key_columns = [
            postgres_table.columns[col.name.lower()] for col in mysql_foreign_key.constraint.columns]

        referred_table_name = mysql_foreign_key.constraint.referred_table.name[:63].lower()
        try:
            referred_table = postgres_metadata.tables[referred_table_name]
        except KeyError:
            referred_table = create_new_table(
                connection, mysql_foreign_key.constraint.referred_table, referred_table_name, postgres_metadata)

        referred_columns = [
            referred_table.columns[item.column.name.lower()] for col in mysql_foreign_key.constraint.columns for item in
            col.foreign_keys
        ]

        ondelete = mysql_foreign_key.constraint.ondelete
        onupdate = mysql_foreign_key.constraint.onupdate

        cols = ', '.join([str(item.name) for item in foreign_key_columns])
        ref_cols = ', '.join([str(item.name) for item in referred_columns])

        query = f'alter table {table_name} ' \
                f'add constraint {foreign_key_name} ' \
                f'foreign key ({cols}) ' \
                f'references {referred_table_name} ({ref_cols})'
        query += f' on delete {ondelete}' if ondelete else ''
        query += f' on update {onupdate}' if onupdate else ''
        connection.execute(text(query))

    return postgres_table


def migrate_data(
    mysql_user,
    mysql_password,
    mysql_host,
    mysql_name,
    postgres_user,
    postgres_password,
    postgres_host,
    postgres_name,
    con=False,
    use_csv=False,
    repair=False,
    info=False,
    echo=True
):
    use_csv = False  # TODO: later
    postgres_engine = create_engine(
        f'postgresql://{postgres_user}:{quote_plus(postgres_password)}@{postgres_host}/{postgres_name}')
    postgres_metadata = MetaData()
    postgres_metadata.reflect(bind=postgres_engine, views=True)

    start_time = datetime.now()

    if repair:
        repair_all(postgres_engine, postgres_metadata, echo=echo)
        try:
            mysql_engine = create_engine(f'mysql://{mysql_user}:{quote_plus(mysql_password)}@{mysql_host}/{mysql_name}')
            mysql_metadata = MetaData()
            mysql_metadata.reflect(bind=mysql_engine, views=True)
            mysql_tables = mysql_metadata.tables.values()
            for mysql_table in mysql_tables:
                postgres_table = postgres_metadata.tables.get(mysql_table.name[:63].lower())
                if postgres_table is None:
                    continue
                with postgres_engine.begin() as connection:
                    repair_datatypes(connection, mysql_table, postgres_table)
        except OperationalError:
            pass

        if echo:
            txt = f"\nRepair finished, time spent: {datetime.now() - start_time}"
            sys.stdout.write(txt)

    elif info:
        mysql_engine = create_engine(f'mysql://{mysql_user}:{quote_plus(mysql_password)}@{mysql_host}/{mysql_name}')
        mysql_metadata = MetaData()
        mysql_metadata.reflect(bind=mysql_engine, views=True)
        mysql_tables = mysql_metadata.tables.values()

        mismatched_tables_count = 0
        for mysql_table in mysql_tables:
            table_name = mysql_table.name[:63].lower()
            postgres_table = postgres_metadata.tables.get(table_name)
            if postgres_table is None:
                mismatched_tables_count += 1
                sys.stdout.write(f'\nTable {table_name} not found in postgres database')
        sys.stdout.write(f'\nTotal mismatched tables: {mismatched_tables_count},'
                         f' time spent: {datetime.now() - start_time}')

    else:
        mysql_engine = create_engine(f'mysql://{mysql_user}:{quote_plus(mysql_password)}@{mysql_host}/{mysql_name}')
        mysql_metadata = MetaData()
        mysql_metadata.reflect(bind=mysql_engine, views=True)
        mysql_tables = mysql_metadata.tables.values()

        total_tables = len(mysql_tables)
        migrated_tables = []

        if not con:
            with postgres_engine.begin() as connection:
                for table_name in clean_tables['postgresql']:
                    table = postgres_metadata.tables.get(table_name)
                    if table is not None:
                        connection.execute(table.delete())

        while True:
            tables_update_count = 0
            for mysql_table in mysql_tables:
                txt_prefix = '\n'
                table_name = mysql_table.name[:63].lower()
                postgres_table = postgres_metadata.tables.get(table_name)
                if postgres_table is None:
                    with postgres_engine.begin() as connection:
                        postgres_table = create_new_table(connection, mysql_table, table_name, postgres_metadata)

                if postgres_table.name in migrated_tables:
                    continue

                if mysql_table.name not in self_ref_tables:
                    is_con = False
                    for key in mysql_table.foreign_keys:
                        target_table_name = key.column.table.name.lower()
                        if target_table_name not in migrated_tables and target_table_name != table_name:
                            is_con = True
                            break
                    if is_con:
                        continue

                with postgres_engine.begin() as connection:
                    repair_datatypes(connection, mysql_table, postgres_table)

                with postgres_engine.begin() as connection:
                    if mysql_table.name in clean_tables['mysql']:
                        pass
                    elif not connection.execute(postgres_table.select().limit(1)).fetchone():
                        add_table_data(connection, mysql_engine, postgres_engine, mysql_table, postgres_table, use_csv)
                        txt_prefix = '\r'

                migrated_tables.append(postgres_table.name)
                tables_update_count += 1

                count = len(migrated_tables)
                completion_percentage = (count / total_tables) * 100
                txt = f"Table {count}/{total_tables} ({table_name}): {completion_percentage:.2f}% complete"
                txt = txt_prefix + txt + 5 * '\t' if count > 1 else '\n' + txt
                sys.stdout.write(txt)

            if not tables_update_count:
                if len(migrated_tables) < len(mysql_tables):
                    raise RuntimeError('Not all tables migrated successfully')
                break

        repair_all(postgres_engine, postgres_metadata)
        migrate_data(
            mysql_user,
            mysql_password,
            mysql_host,
            mysql_name,
            postgres_user,
            postgres_password,
            postgres_host,
            postgres_name,
            repair=True,
            echo=False
        )
        sys.stdout.write(f'\nMigration finished! Time spent: {datetime.now() - start_time}')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Migrate data from MySQL to PostgreSQL')
    parser.add_argument('mysql_user', type=str, help='MySQL username')
    parser.add_argument('mysql_password', type=str, help='MySQL password')
    parser.add_argument('mysql_host', type=str, help='MySQL host')
    parser.add_argument('mysql_name', type=str, help='MySQL database name')
    parser.add_argument('postgres_user', type=str, help='PostgreSQL username')
    parser.add_argument('postgres_password', type=str, help='PostgreSQL password')
    parser.add_argument('postgres_host', type=str, help='PostgreSQL host')
    parser.add_argument('postgres_name', type=str, help='PostgreSQL database name')
    parser.add_argument('-c', '--con', action='store_true', help='Set to continue previous migration')
    parser.add_argument('-u', '--use_csv', action='store_true', help='Set to use CSV for migration')
    parser.add_argument('-r', '--repair', action='store_true', help='Repair sequences')
    parser.add_argument('-i', '--info', action='store_true', help='Database comparison info')

    args = parser.parse_args()

    migrate_data(args.mysql_user, args.mysql_password, args.mysql_host, args.mysql_name,
                 args.postgres_user, args.postgres_password, args.postgres_host, args.postgres_name,
                 con=args.con, use_csv=args.use_csv, repair=args.repair, info=args.info)
