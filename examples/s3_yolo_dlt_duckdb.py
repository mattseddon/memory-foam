try:
    import os

    os.environ["YOLO_VERBOSE"] = "false"

    from io import BytesIO
    from memory_foam import iter_files, FilePointer
    from PIL import Image
    from ultralytics import YOLO
    from tqdm.auto import tqdm
    import dlt

except ImportError:
    print(
        "There are missing dependencies, install the memory-foam package with the [examples] optional extras."
    )
    import sys

    sys.exit(1)


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
    return pointer.to_dict_with({"box": box})


@dlt.resource(table_name="yolo_data")
def yolo_data():
    with tqdm(desc=f"Processing {uri}", unit=" files") as pbar:
        for pointer, contents in iter_files(uri, client_config={"anon": True}):
            results = yolo(Image.open(BytesIO(contents)))
            yield transform_yolo_results(pointer, results)
            pbar.update(1)


yolo = YOLO("yolo11n.pt", verbose=False)
uri = "s3://argoverse/datasets/av2/sensor/test/fee0f78c-cf00-35c5-975b-72724f53fd64/sensors/cameras/ring_front_center/"

pipeline = dlt.pipeline(
    pipeline_name="yolo_data",
    destination="duckdb",
)

load_info = pipeline.run(yolo_data())
print(load_info)
dataset = pipeline.dataset()
print(dataset.yolo_data.df())
print(dataset.yolo_data__box.df())
