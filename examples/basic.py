from memory_foam import get_entries


if __name__ == "__main__":
    s = set()
    uri = "s3://ldb-public/remote/data-lakes/ISIA_500/Croissant"
    for file in get_entries(uri, {"anon": True}):
        s.add(file.path)
        # do stuff

    print(f"Total unique files: {len(s)}")
