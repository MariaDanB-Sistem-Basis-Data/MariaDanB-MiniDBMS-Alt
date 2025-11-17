from datetime import datetime
import uuid

from query_processor.QueryExecutor import QueryExecutor
from query_processor.model.ExecutionResult import ExecutionResult
from query_processor.model.Rows import Rows
from query_processor.helper.query_utils import *

class QueryProcessor:
    def __init__(self):
        self.query_executor = QueryExecutor()

    def execute_query(self, query : str) -> ExecutionResult:

        transaction_id = uuid.uuid4().int

        query_type = get_query_type(query)

        try:
            # NOTE: harusnya yang handle keywords kaya FROM, JOIN, WHERE itu dari optimizer karena kan itu keyword bagian dari select atau query lain. soalnya disini cuma ngeidentifikasi tipe query dari keyword awalnya doang (CMIIW)

            # NOTE: feel free to adjust, kode ini dibikin dalam kondisi mengantuk kurang tidur

            result_data = Rows.from_list([])
            if query_type == QueryType.SELECT:
                result_data = self.query_executor.execute_select(query)

            elif query_type == QueryType.UPDATE:
                result_data = self.query_executor.execute_update(query)

            elif query_type == QueryType.DELETE:
                result_data = self.query_executor.execute_delete(query)

            elif query_type == QueryType.INSERT_INTO:
                result_data = self.query_executor.execute_insert(query)

            elif query_type == QueryType.CREATE_TABLE:
                result_data = self.query_executor.execute_create_table(query)

            elif query_type == QueryType.DROP_TABLE:
                result_data = self.query_executor.execute_drop_table(query)

            elif query_type == QueryType.BEGIN_TRANSACTION:
                result_data =  self.query_executor.execute_begin_transaction(query)
            
            elif query_type == QueryType.COMMIT:
                result_data = self.query_executor.execute_commit(query)

            elif query_type == QueryType.ABORT:
                result_data = self.query_executor.execute_abort(query)

            elif query_type == QueryType.ROLLBACK:
                result_data = self.query_executor.execute_rollback(query)

            elif query_type in DATA_QUERIES or query_type in TRANSACTION_QUERIES:
                return ExecutionResult(transaction_id=transaction_id, timestamp=datetime.now(), message=f"Cek helper/query_utils.py, harusnya ini query type dari bonus yg belum consider dikerjain (query_type: {query_type})", data=0, query=query)

            else: # query_type == QueryType.UNKNOWN
                return ExecutionResult(transaction_id=transaction_id, timestamp=datetime.now(), message="Error: unknown query syntax", data=-1, query=query)

            # if result_data != error maybe

            return ExecutionResult(transaction_id=transaction_id, timestamp=datetime.now(), message="Success", data=result_data, query=query)

        except Exception as e:
            print(f"Error processing query: {e}")
            return ExecutionResult(transaction_id=transaction_id, timestamp=datetime.now(), message="Error occured when processing query", data=-1, query=query)




