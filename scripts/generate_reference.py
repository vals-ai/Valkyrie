if __package__:
    from .reference_docs import main
else:
    from reference_docs import main

if __name__ == "__main__":
    raise SystemExit(main())
