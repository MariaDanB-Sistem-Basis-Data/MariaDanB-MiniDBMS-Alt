# Failure Recovery Manager


## How to Run

> [!NOTE]
> add query processor submodule
 ```bash
   git submodule add https://github.com/MariaDanB-Sistem-Basis-Data/Query-Processor.git Query-Processor
   ```

- unit test (from frm root folder) 
```bash
  python frm_test/unittest.py
```

> [!NOTE]
> if error import, check the import path (may not match)
 ```bash
   from qp_model.ExecutionResult import ...
   ```

## Dev Notes

**Code convention**
> [!IMPORTANT]
> https://www.oracle.com/java/technologies/javase/codeconventions-namingconventions.html

- for private method, add _ (underscore) before the definitions. e.g. _save(a, b)

<br/>

> [!NOTE]
> Tentative (dev will based on concurrency control manager - TBA)
