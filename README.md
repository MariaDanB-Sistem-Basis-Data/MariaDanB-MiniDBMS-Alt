# Failure Recovery Manager

## Dev Notes

**uv setup**
1. Install `uv`:

- Windows (PowerShell):
  ```powershell
  irm https://astral.sh/uv/install.ps1 | iex
  ```
- Linux/macOS (bash):
  ```bash
  curl -LsSf https://astral.sh/uv/install.sh | sh
  ```

2. `uv` setup:

```sh
uv venv --python 3.12 && uv pip sync pyproject.toml
```

<br/>

**Code convention**
> [!IMPORTANT]
> https://www.oracle.com/java/technologies/javase/codeconventions-namingconventions.html

- for private method, add _ (underscore) before the definitions. e.g. _save(a, b)

<br/>

**Modules Synchronization**

1. To update modules, do:
   ```sh
   uv run Sync.py
   ```

> [!NOTE]
> Tentative (dev will based on concurrency control manager - TBA)