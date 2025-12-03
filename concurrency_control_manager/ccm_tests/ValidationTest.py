from ConcurrencyControlManager import ConcurrencyControlManager
from ccm_methods.Validation import Validation
from ccm_helper.Row import Row
from ccm_model.Enums import Action

ccm = ConcurrencyControlManager()
ccm.set_method(Validation())

A = Row(table_name="A", pk_value=1, data={"x": 10}, version=[0])
B = Row(table_name="B", pk_value=1, data={"x": 10}, version=[0])

t1 = ccm.begin_transaction()
ccm.validate_object(A, t1, Action.READ)
ccm.validate_object(B, t1, Action.WRITE)

t2 = ccm.begin_transaction()
ccm.validate_object(A, t2, Action.WRITE)
ccm.end_transaction(t2)  

ccm.end_transaction(t1)
