#!/usr/bin/env python
from bz2 import compress
from pprint import pprint
from pathlib import Path
from libpressio import PressioCompressor
import numpy as np
from pydantic import BaseSettings as _BaseSettings
from typing import Type, TypeVar, Union, Optional, List
import pydantic
import yaml
import json
import argparse
import os
import subprocess
_T = TypeVar("_T")


PathLike = Union[str, Path]
class BaseSettings(_BaseSettings):
    def dump_yaml(self, cfg_path: PathLike) -> None:
        with open(cfg_path, mode="w") as fp:
            yaml.dump(json.loads(self.json()), fp, indent=4, sort_keys=False)

    @classmethod
    def from_yaml(cls: Type[_T], filename: PathLike) -> _T:
        with open(filename) as fp:
            raw_data = yaml.safe_load(fp)
        return cls(**raw_data)  # type: ignore[call-arg]

    @classmethod
    def from_bytes(cls: Type[_T], raw_bytes: bytes) -> _T:
        raw_data = yaml.safe_load(raw_bytes)
        return cls(**raw_data)  # type: ignore[call-arg]

class CompressionConfig(BaseSettings):
    data_path: Optional[Path] = "/home/data_path"
    metrics_path: Optional[Path] = "/home/metrics"
    work_dir: Optional[Path] = "/home/ac.yuanjian/sz_workdir"
    compressor_id: Optional[str] = "sz"
    file_extension: Optional[str] = ".f32"
    dimension: Optional[List[int]] = None
    abs_error_bound: Optional[float] = 1e-3

def main(cfg: CompressionConfig, file :Optional[str]):
    dataset_path = cfg.data_path
    if file is None:
        files = os.listdir(dataset_path)
        data_files = list(filter(lambda x: x.endswith(cfg.file_extension), files))
    else:
        data_files = [file]
    compressed_folder = cfg.work_dir / f"compressed_{cfg.compressor_id}"
    if not os.path.exists(compressed_folder):
        os.makedirs(compressed_folder)
    for file in data_files:
        uncompressed_data = np.fromfile(file, dtype=np.float32)
        uncompressed_data = uncompressed_data.reshape(cfg.dimension)
        # decompressed_data = uncompressed_data.copy()

        # load and configure the compressor
        compressor = PressioCompressor.from_config({
            "compressor_id": "sz",
            "early_config": {
                    "pressio:metric": "composite",
                    "composite:plugins": ["time", "size", "error_stat"]
                },
            "compressor_config": {
                "sz:error_bound_mode_str": "abs",
                "sz:abs_err_bound": cfg.abs_error_bound,
                }
            })

        # print out some metadata
        print(compressor.codec_id)
        pprint(compressor.get_config())
        pprint(compressor.get_compile_config())


        # preform compression and decompression
        filename = Path(file).name
        compressed = compressor.encode(uncompressed_data)
        # decompressed = compressor.decode(compressed, decompressed_data)
        compressed_path = str(compressed_folder / f"{filename}.sz")
        print("compressed path: ", compressed_path)
        with open(compressed_path, 'w') as f:
            compressed.tofile(f)
        metrics_path = str(cfg.metrics_path / f"{filename}.json")
        print("metrics path:", metrics_path)
        with open(metrics_path, 'w') as f:
            json.dump(compressor.get_metrics(), f)
        # print out some metrics collected during compression
        pprint(compressor.get_metrics())

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c", "--config", help="YAML config file", type=str, required=True
    )
    parser.add_argument(
         "-f", "--file", help="File to be compressed", type=str, required=False
    )
    args = parser.parse_args()
    cfg = CompressionConfig.from_yaml(args.config)
    # if args.file is None:
    #     print("file is none")
    # else:
    #     print("file is not none")
    # print(cfg.dimension)
    main(cfg, args.file)