# cnvrg Dataset
a class which helps to download datasets using python.

to use this library you just need to import it and run

```
from cnvrg_dataset import CnvrgDataset

cd = CnvrgDataset(dataset='my ds')
t = cd.download_dataset() # returns threading.Thread

### you can also check the progress by
import time
while t.is_alive():
    print("Progress: {0}%".format(round(cd.progress() * 100, 2)))
    time.sleep(5)
```