import os


os.environ["YOLO_VERBOSE"] = "false"

from io import BytesIO
from memory_foam import iter_files, FilePointer
from PIL import Image
from ultralytics import YOLO
from tqdm.auto import tqdm
import dlt


def transform_yolo_results(pointer: FilePointer, results):
    box = []
    for r in results:
        for s in r.summary():
            box.append(
                {
                    "confidence": s["confidence"],
                    "class": s["class"],
                    "name": s.get("name", ""),
                    "box": s.get("box"),
                }
            )
    yield pointer.to_dict_with({"box": box})


@dlt.resource(table_name="yolo_data")
def yolo_data():
    with tqdm(desc=f"Processing {uri}", unit=" files") as pbar:
        for pointer, contents in iter_files(uri, {"anon": True}):
            results = yolo(Image.open(BytesIO(contents)))
            yield from transform_yolo_results(pointer, results)
            pbar.update(1)


yolo = YOLO("yolo11n.pt", verbose=False)
uri = "s3://ldb-public/remote/data-lakes/ISIA_500/Croissant"

pipeline = dlt.pipeline(
    pipeline_name="yolo_data",
    destination="duckdb",
)

load_info = pipeline.run(yolo_data())
print(load_info)
dataset = pipeline.dataset()
print(dataset.yolo_data.df())
print(dataset.yolo_data__box.df())
