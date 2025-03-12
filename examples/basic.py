from io import BytesIO
from memory_foam import get_entries
from PIL import Image
from ultralytics import YOLO


if __name__ == "__main__":
    yolo = YOLO("yolo11n.pt", verbose=False)
    uri = "s3://ldb-public/remote/data-lakes/ISIA_500/Croissant"
    for pointer, contents in get_entries(uri, {"anon": True}):
        results = yolo(Image.open(BytesIO(contents)))
        # combined data
        # save to database
