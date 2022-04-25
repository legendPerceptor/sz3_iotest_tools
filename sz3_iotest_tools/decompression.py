#!/usr/bin/env python
from pprint import pprint
from pathlib import Path
from libpressio import PressioCompressor
import numpy as np

compressed_dataset_path = Path.home() / "Desktop/CLDHGH.sz"
compressed = np.fromfile(compressed_dataset_path, dtype=np.uint8)
compressor = PressioCompressor.from_config({
    "compressor_id": "sz",
    "early_config": {
            "pressio:metric": "composite",
            "composite:plugins": ["time", "size", "error_stat"]
        },
    "compressor_config": {
        "sz:error_bound_mode_str": "abs",
        "sz:abs_err_bound": 1e-6,
        "sz:metric": "size"
        }
    })

decompressed_data = np.zeros((1800, 3600), dtype=np.float32)
decompressed = compressor.decode(compressed, decompressed_data)

dataset_path = Path.home() / "Desktop/CLDHGH_1_1800_3600.dat"
uncompressed_data = np.fromfile(dataset_path, dtype=np.float32)
uncompressed_data = uncompressed_data.reshape((1800, 3600))

print("decompressed shape:", decompressed.shape)
print("uncompressed shape:", uncompressed_data.shape)

print(decompressed[:10])
print(uncompressed_data[:10])

diff_array = decompressed - uncompressed_data
print(diff_array[:10])
print("max diff: ", np.amax(diff_array))
print("min diff: ", np.amin(diff_array))


pprint(compressor.get_metrics())