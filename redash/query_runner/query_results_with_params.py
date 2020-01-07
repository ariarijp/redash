import json
import logging
import re
import sqlite3
import uuid
from typing import Optional

import pystache
import sqlparse

from redash.models import Query, User
from redash.query_runner import TYPE_STRING, guess_type, register
from redash.query_runner.query_results import Results, _load_query, create_table
from redash.utils import json_dumps

logger = logging.getLogger(__name__)


class ChildQueryExecutionError(Exception):
    pass


def extract_child_queries(query: str) -> list:
    pattern = re.compile(r"^query_(\d+)(?:\('({.+})'\))?", re.IGNORECASE)
    stmt = sqlparse.parse(query)[0]

    function_tokens = collect_tokens(stmt, [])

    queries = []
    for token in function_tokens:
        m = pattern.match(token.value)
        if not m:
            continue

        queries.append({
            'query_id': int(m.group(1)),
            'params': {} if m.group(2) is None else json.loads(m.group(2)),
            'table': 'tmp_{}'.format(str(uuid.uuid4()).replace('-', '_')),
            'token': token.tokens[0].value if isinstance(token.tokens[0], sqlparse.sql.Function) else token.value,
        })

    return queries


def collect_tokens(token: sqlparse.sql.Token, tokens: list) -> list:
    if isinstance(token, sqlparse.sql.Function):
        tokens.append(token)
    elif isinstance(token, sqlparse.sql.Identifier) and token.value.startswith('query_'):
        tokens.append(token)
    elif token.is_group:
        for t in token.tokens:
            tokens = collect_tokens(t, tokens)

    return tokens


def create_tables_from_child_queries(user: User, connection: sqlite3.Connection, query: str, child_queries: list) -> str:
    for i, child_query in enumerate(child_queries):
        _child_query = _load_query(user, child_query['query_id'])

        params = child_query.get('params', {})
        if not params:
            params = get_default_params(_child_query)

        _rendered_child_query = pystache.render(_child_query.query_text, params)
        logger.debug('ResultsWithParams child query[{}]: {}'.format(i, _rendered_child_query))
        results, error = _child_query.data_source.query_runner.run_query(_rendered_child_query, user)

        if error:
            raise ChildQueryExecutionError('Failed loading results for query id {}.'.format(i, _child_query.id))

        results = json.loads(results)
        table_name = child_query['table']
        create_table(connection, table_name, results)
        query = query.replace(child_query['token'], table_name, 1)

    return query


def get_default_params(query: Query) -> dict:
    return {p['name']: p['value'] for p in query.options.get('parameters', {})}


class ResultsWithParams(Results):
    @classmethod
    def name(cls):
        return 'Query Results with parameters(PoC)'

    def run_query(self, query: Query, user: User) -> (Optional[str], Optional[str]):
        child_queries = extract_child_queries(query)

        connection = None
        cursor = None

        try:
            connection = sqlite3.connect(':memory:')
            query = create_tables_from_child_queries(user, connection, query, child_queries)
            logger.debug('ResultsWithParams running query: {}'.format(query))

            cursor = connection.cursor()
            cursor.execute(query)

            if cursor.description is None:
                return None, 'Query completed but it returned no data.'

            columns = self.fetch_columns([(d[0], None) for d in cursor.description])

            rows = []
            column_names = [c['name'] for c in columns]

            for i, row in enumerate(cursor):
                if i == 0:
                    for j, col in enumerate(row):
                        guess = guess_type(col)

                        if columns[j]['type'] is None:
                            columns[j]['type'] = guess
                        elif columns[j]['type'] != guess:
                            columns[j]['type'] = TYPE_STRING

                rows.append(dict(zip(column_names, row)))

            return json_dumps({'columns': columns, 'rows': rows}), None
        except KeyboardInterrupt:
            if connection:
                connection.interrupt()
            return None, 'Query cancelled by user.'
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()


register(ResultsWithParams)
