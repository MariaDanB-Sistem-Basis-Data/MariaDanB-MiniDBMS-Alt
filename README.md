# Failure Recovery Manager
The Failure Recovery Manager component is responsible for handling recovery when a failure occurs in the implemented mDBMS. This component provides logging and checkpointing mechanisms to support recovery.

### How to Run Unit Test
1. Clone this repository.
```bash
git clone https://github.com/MariaDanB-Sistem-Basis-Data/Failure-Recovery-Manager.git
```
2. Open the project folder in terminal.
```bash
cd Failure-Recovery-Manager
```
3. Pull the Query Processor component submodule.
```bash
git submodule add https://github.com/MariaDanB-Sistem-Basis-Data/Query-Processor.git Query-Processor
```
> [!NOTE]
> The Query Processor component is required due to an import in `FailureRecovery.py`. 
4. Run the Failure Recovery Manager unit test.
```bash
python frm_test/unittest.py
```