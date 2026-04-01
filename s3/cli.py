try:
    from .query import main
except ImportError:
    from query import main


if __name__ == "__main__":
    raise SystemExit(main())
