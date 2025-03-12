import os

os.environ["YOLO_VERBOSE"] = "false"

from io import BytesIO
from memory_foam import get_entries
from PIL import Image
from ultralytics import YOLO
from tqdm.auto import tqdm


if __name__ == "__main__":
    yolo = YOLO("yolo11n.pt", verbose=False)
    uri = "s3://ldb-public/remote/data-lakes/ISIA_500/Croissant"

    with tqdm(desc=f"Processing {uri}", unit=" files") as pbar:
        for pointer, contents in get_entries(uri, {"anon": True}):
            results = yolo(Image.open(BytesIO(contents)))
            # combined data
            # save to database
            pbar.update(1)
