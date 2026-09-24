import sys

from sjp import commands  # noqa: F401  imported for the side effect of registering
from sjp.cli import main

if __name__ == "__main__":
    sys.exit(main())
