import random


try:
    import os

    from matplotlib import pyplot as plt

    os.environ["HF_HUB_ENABLE_HF_TRANSFER"] = "1"

    from huggingface_hub import hf_hub_download
    import open_clip

    from io import BytesIO
    from memory_foam import FilePointer, iter_files, iter_pointers
    from PIL import Image
    from tqdm.auto import tqdm
    import duckdb
except ImportError:
    print(
        "There are missing dependencies, install the memory-foam package with the [examples] optional extras."
    )
    import sys

    sys.exit(1)

current_directory = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(current_directory, "embeddings.duckdb")


def setup_db():
    with duckdb.connect(db_path) as conn:
        conn.install_extension("vss")
        conn.load_extension("vss")
        conn.sql("DROP TABLE IF EXISTS img_embeddings")
        conn.sql(
            "CREATE TABLE img_embeddings ("
            "source VARCHAR,"
            "path VARCHAR,"
            "size BIGINT,"
            "version VARCHAR,"
            "last_modified TIMESTAMPTZ,"
            "embeddings FLOAT[512]"
            ")"
        )


def setup_embeddings_model():
    for file in ["open_clip_config.json", "open_clip_model.safetensors"]:
        hf_hub_download("timm/vit_medium_patch16_clip_224.tinyclip_yfcc15m", file)

    model, preprocess = open_clip.create_model_from_pretrained(
        "hf-hub:timm/vit_medium_patch16_clip_224.tinyclip_yfcc15m"
    )
    return model, preprocess


def _write_buffer(buffer: list[str]):
    if buffer:
        with duckdb.connect(db_path) as conn:
            conn.sql(
                f"INSERT INTO img_embeddings BY POSITION VALUES {','.join(buffer)};"
            )


def _process_buffer(buffer: list[str], pointer: FilePointer, emb: list) -> list[str]:
    buffer.append(
        f"('{pointer.source}', '{pointer.path}', {pointer.size}, '{pointer.version}', '{pointer.last_modified.isoformat()}', {emb}::FLOAT[512])"
    )

    if len(buffer) % 50 == 0:
        _write_buffer(buffer)
        return []

    return buffer


def _open_image(contents: bytes):
    return Image.open(BytesIO(contents))


def similarity_search() -> tuple[list[FilePointer], dict[str, tuple[int, float]]]:
    print("*** Demonstrate vector search against a loaded image ***")
    offset = random.randint(0, 199)
    conn = duckdb.connect(db_path)
    conn.load_extension("vss")
    conn.execute("SET hnsw_enable_experimental_persistence = true;")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx ON img_embeddings USING HNSW (embeddings);"
    )
    SELECT = f"(select embeddings from img_embeddings limit 1 offset {offset})"
    DISTANCE = f"array_distance(embeddings, {SELECT})"
    conn.sql(
        f"""
        SELECT
            path,
            version,
            {DISTANCE} as distance
        FROM img_embeddings
        ORDER BY {DISTANCE}
        LIMIT 3;
        """
    ).show()

    pointers = []
    details = {}
    for i, record in enumerate(
        conn.execute(
            f"""
            SELECT
                source,
                path,
                size,
                version,
                last_modified,
                {DISTANCE} as distance
            FROM img_embeddings
            ORDER BY {DISTANCE}
            LIMIT 3;
            """
        )
        .fetch_df()
        .to_dict(orient="records")
    ):
        pointer = FilePointer.from_dict(record)
        pointers.append(pointer)
        details[pointer.path] = (i, record["distance"])

    conn.close()
    return pointers, details


def setup_plot():
    f, axarr = plt.subplots(1, 3)
    f.suptitle("DuckDB Vector Search")
    return axarr


setup_db()
model, preprocess = setup_embeddings_model()

buffer: list[str] = []
uri = "gs://datachain-demo/dogs-and-cats/"

with tqdm(desc="Processing embeddings", unit=" files") as pbar:
    for pointer, contents in iter_files(
        uri, glob="*.jpg", client_config={"anon": True}
    ):
        img = preprocess(_open_image(contents)).unsqueeze(0)
        emb = model.encode_image(img).tolist()[0]
        buffer = _process_buffer(buffer, pointer, emb)
        pbar.update(1)

    _write_buffer(buffer)


pointers, details = similarity_search()
source = pointers[0].source
axarr = setup_plot()
titles = ["Original", "Nearest", "Next Nearest"]

with tqdm(desc="Plotting images", unit=" files", total=3) as pbar:
    for pointer, contents in iter_pointers(
        source, pointers=pointers, client_config={"anon": True}
    ):
        order, distance = details[pointer.path]
        axarr[order].imshow(_open_image(contents))
        axarr[order].set_title(
            f"{titles[order]}\ndistance: {round(distance, 3)}\n{pointer.path}",
            fontsize=8,
        )
        axarr[order].axis("off")
        pbar.update()

plt.show()
