from enum import Enum, auto

class QueryType(Enum):
    """
    Enumerate all possible query types
    """
    # data queries
    SELECT = auto()
    UPDATE = auto()
    DELETE = auto() # Bonus (in case mau ngerjain nanti)
    INSERT_INTO = auto() # Bonus
    CREATE_TABLE = auto() # Bonus
    DROP_TABLE = auto() # Bonus
    
    # transaction queries
    BEGIN_TRANSACTION = auto()
    COMMIT = auto()
    ABORT = auto()
    ROLLBACK = auto()
    
    # for error
    UNKNOWN = auto()

DATA_QUERIES = {
    QueryType.SELECT,
    QueryType.UPDATE,
    QueryType.DELETE,
    QueryType.INSERT_INTO,
    QueryType.CREATE_TABLE,
    QueryType.DROP_TABLE,
}

TRANSACTION_QUERIES = {
    QueryType.BEGIN_TRANSACTION,
    QueryType.COMMIT,
    QueryType.ABORT,
    QueryType.ROLLBACK
}

def get_query_type(query: str) -> QueryType:
    """
    Parses the start of a raw query string to determine its type
    """
    if not query:
        return QueryType.UNKNOWN

    q = query.strip().upper()

    
    if q.startswith("INSERT INTO"):
        return QueryType.INSERT_INTO
    elif q.startswith("CREATE TABLE"):
        return QueryType.CREATE_TABLE
    elif q.startswith("DROP TABLE"):
        return QueryType.DROP_TABLE
    elif q.startswith("BEGIN TRANSACTION"):
        return QueryType.BEGIN_TRANSACTION
    elif q.startswith("SELECT"):
        return QueryType.SELECT
    elif q.startswith("UPDATE"):
        return QueryType.UPDATE
    elif q.startswith("DELETE"):
        return QueryType.DELETE
    elif q.startswith("COMMIT"):
        return QueryType.COMMIT
    elif q.startswith("ABORT"):
        return QueryType.ABORT
    elif q.startswith("ROLLBACK"):
        return QueryType.ROLLBACK
    
    else:
        return QueryType.UNKNOWN
