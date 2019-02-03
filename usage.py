from cnvrg_dataset import CnvrgDataset
import time
cd = CnvrgDataset(dataset="hazilimmmm", path="/Users/omershacham/data/yoyo", threads=20)
t = cd.download_dataset()


while t.is_alive():
    print("Progress: {0}%".format(round(cd.progress() * 100, 2)))
    time.sleep(5)

