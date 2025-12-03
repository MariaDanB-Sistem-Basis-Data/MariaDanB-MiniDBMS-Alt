from __future__ import annotations
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class NormalizedCondition:
    column: str
    operator: str  # '=', '!=', '>', '<', '>=', '<='
    value: str
    
    @classmethod
    def from_optimizer_node(cls, node: Any) -> NormalizedCondition | None:
        # Oke, ini apaan? ini buat nge-parse condition dari query optimizer
        # kurleb ini struktur expected ConditionNode :
        #- node.attr: dict with 'column' key or string
        #- node.op: comparison operator string
        #- node.value: comparison value
        try:
            if isinstance(node.attr, dict):
                col_name = node.attr.get("column")
            else:
                col_name = str(node.attr)
            
            if not col_name:
                return None
            
            operator = str(node.op)
            value = str(node.value)
            
            return cls(column=col_name, operator=operator, value=value)
        except (AttributeError, KeyError, TypeError):
            return None
    
    @classmethod
    def from_string(cls, condition_str: str) -> NormalizedCondition | None:
        operators = [">=", "<=", "!=", "=", ">", "<"]
        
        for op in operators:
            if op in condition_str:
                parts = condition_str.split(op, 1)
                if len(parts) == 2:
                    col = parts[0].strip()
                    val = parts[1].strip().strip("'\"")
                    return cls(column=col, operator=op, value=val)
        
        return None
    
    @classmethod
    def normalize(cls, condition: Any) -> NormalizedCondition | None:
        if isinstance(condition, str):
            return cls.from_string(condition)
        
        result = cls.from_optimizer_node(condition)
        if result:
            return result
        
        return None
