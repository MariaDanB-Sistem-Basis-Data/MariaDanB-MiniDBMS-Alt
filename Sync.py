import configparser
import subprocess
import sys
from pathlib import Path
from typing import Optional


class SubmoduleInfo:

    def __init__(self, name: str, path: str, url: str):
        self.name = name
        self.path = path
        self.url = url

    def __str__(self) -> str:
        return f"SubmoduleInfo(name={self.name}, path={self.path}, url={self.url})"


class GitSubmoduleManager:

    GITMODULES_FILENAME = ".gitmodules"
    SUBMODULE_SECTION_PREFIX = "submodule "

    def __init__(self, repositoryPath: Optional[Path] = None):
        if repositoryPath is None:
            self.repositoryPath = Path(__file__).resolve().parent
        else:
            self.repositoryPath = Path(repositoryPath).resolve()

        self.parentPath = self.repositoryPath.parent
        self.gitmodulesPath = self.repositoryPath / self.GITMODULES_FILENAME

    def validateRepository(self) -> bool:
        return self.gitmodulesPath.exists()

    def parseGitmodules(self) -> list[SubmoduleInfo]:
        if not self.validateRepository():
            raise FileNotFoundError(f"No {self.GITMODULES_FILENAME} found in {self.repositoryPath}")

        parser = configparser.ConfigParser()
        parser.read(self.gitmodulesPath)

        submodules = []
        for section in parser.sections():
            if not section.startswith(self.SUBMODULE_SECTION_PREFIX):
                continue

            name = section[len(self.SUBMODULE_SECTION_PREFIX):].strip('"')
            path = parser[section].get("path")
            url = parser[section].get("url")

            if path and url:
                submodules.append(SubmoduleInfo(name, path, url))

        return submodules

    def isSubmodulePresent(self, submodule: SubmoduleInfo) -> bool:
        targetPath = self.parentPath / submodule.path
        return targetPath.exists()

    def cloneSubmodule(self, submodule: SubmoduleInfo) -> bool:
        targetPath = self.parentPath / submodule.path
        targetPath.parent.mkdir(parents=True, exist_ok=True)

        try:
            print(f"Cloning {submodule.url} -> {submodule.path}")
            subprocess.check_call(["git", "clone", submodule.url, str(targetPath)])
            return True
        except subprocess.CalledProcessError as e:
            print(f"Error cloning {submodule.name}: {e}", file=sys.stderr)
            return False

    def pullSubmodule(self, submodule: SubmoduleInfo) -> bool:
        targetPath = self.parentPath / submodule.path

        try:
            print(f"Pulling updates for {submodule.path}")
            subprocess.check_call(["git", "-C", str(targetPath), "pull", "origin"])
            return True
        except subprocess.CalledProcessError as e:
            print(f"Error pulling {submodule.name}: {e}", file=sys.stderr)
            return False

    def updateSubmodule(self, submodule: SubmoduleInfo) -> bool:
        targetPath = self.parentPath / submodule.path

        try:
            print(f"Fetching updates for {submodule.path}")
            subprocess.check_call(["git", "-C", str(targetPath), "fetch", "origin"])

            print(f"Resetting to origin/HEAD for {submodule.path}")
            subprocess.check_call(["git", "-C", str(targetPath), "reset", "--hard", "origin/HEAD"])
            return True
        except subprocess.CalledProcessError as e:
            print(f"Error updating {submodule.name}: {e}", file=sys.stderr)
            return False

    def synchronizeSubmodules(self, shouldPull: bool = False) -> tuple[int, int]:
        try:
            submodules = self.parseGitmodules()
        except FileNotFoundError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)

        clonedCount = 0
        updatedCount = 0

        for submodule in submodules:
            if self.isSubmodulePresent(submodule):
                if shouldPull:
                    if self.pullSubmodule(submodule):
                        updatedCount += 1
                else:
                    print(f"Skipping {submodule.path}: already present")
            else:
                if self.cloneSubmodule(submodule):
                    clonedCount += 1

        return clonedCount, updatedCount


class SyncApplication:

    @staticmethod
    def parseArguments() -> bool:
        shouldPull = False
        if len(sys.argv) > 1:
            if sys.argv[1] in ["--pull", "-p"]:
                shouldPull = True
        return shouldPull

    @staticmethod
    def main() -> None:
        manager = GitSubmoduleManager()

        if not manager.validateRepository():
            print(f"Error: No {GitSubmoduleManager.GITMODULES_FILENAME} found", file=sys.stderr)
            sys.exit(1)

        shouldPull = SyncApplication.parseArguments()

        if shouldPull:
            print("Starting submodule synchronization with pull...")
        else:
            print("Starting submodule synchronization...")

        clonedCount, updatedCount = manager.synchronizeSubmodules(shouldPull)

        if clonedCount > 0:
            print(f"\nSuccessfully cloned {clonedCount} submodule(s)")
        if updatedCount > 0:
            print(f"Successfully updated {updatedCount} submodule(s)")
        if clonedCount == 0 and updatedCount == 0:
            print("\nAll submodules are up to date")


if __name__ == "__main__":
    SyncApplication.main()
