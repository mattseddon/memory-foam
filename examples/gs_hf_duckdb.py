import os


os.environ["HF_HUB_ENABLE_HF_TRANSFER"] = "1"

from huggingface_hub import hf_hub_download
import open_clip

from io import BytesIO
from memory_foam import FilePointer, iter_files
from PIL import Image
from tqdm.auto import tqdm
import duckdb

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


def _write_buffer(buffer: list):
    if buffer:
        with duckdb.connect(db_path) as conn:
            conn.sql(
                f"INSERT INTO img_embeddings BY POSITION VALUES {','.join(buffer)};"
            )


def _update_buffer(buffer: list, pointer: FilePointer, emb: list) -> list:
    buffer.append(
        f"('{pointer.source}', '{pointer.path}', {pointer.size}, '{pointer.version}', '{pointer.last_modified.isoformat()}', {emb}::FLOAT[512])"
    )

    if len(buffer) % 50 == 0:
        _write_buffer(buffer)
        return []

    return buffer


def load_img_embeddings(uri, glob, model, preprocess):
    buffer = []
    with tqdm(desc=f"Processing {uri}", unit=" files") as pbar:
        for pointer, contents in iter_files(uri, glob, client_config={"anon": True}):
            img = preprocess(Image.open(BytesIO(contents))).unsqueeze(0)
            emb = model.encode_image(img).tolist()[0]
            buffer = _update_buffer(buffer, pointer, emb)
            pbar.update(1)

        _write_buffer(buffer)


def show_example_similarity_search():
    conn = duckdb.connect(db_path)
    conn.load_extension("vss")
    conn.execute("SET hnsw_enable_experimental_persistence = true;")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx ON img_embeddings USING HNSW (embeddings);"
    )
    conn.sql(
        """
        SELECT
            source,
            path,
            version,
            array_distance(embeddings, (select embeddings from img_embeddings limit 1)) as distance
        FROM img_embeddings
        ORDER BY array_distance(embeddings, (select embeddings from img_embeddings limit 1))
        LIMIT 3;
        """
    ).show()
    conn.close()


setup_db()
model, preprocess = setup_embeddings_model()
load_img_embeddings("gs://datachain-demo/dogs-and-cats/", "*.jpg", model, preprocess)
show_example_similarity_search()
