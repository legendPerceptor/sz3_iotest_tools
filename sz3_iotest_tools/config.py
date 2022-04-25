import json
import yaml
import argparse
from pathlib import Path
from typing import Type, TypeVar, Union, Optional, List
from pydantic import BaseSettings as _BaseSettings

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

class GlobusAgentConfig(BaseSettings):
    source_endpoint: Optional[str] = "61f9954c-a4fa-11ea-8f07-0a21f750d19b" # Bebop endpoint
    destination_endpoint: Optional[str] = "08925f04-569f-11e7-bef8-22000b9a448b" # Theta endpoint
    client_id : Optional[str] = "1fb9c8a9-1aff-4d46-9f37-e3b0d44194f2" # My Globus App ID
    source_folder : Optional[Path] = Path("/lcrc/project/ECP-EZ/public/compression/aggregated-science-data/NYX/512x512x512/SDRBENCH-EXASKY-NYX-512x512x512/")
    destination_folder: Optional[Path] = Path("~/")
    transfer_result_path: Optional[Path] = Path("./result.json")
    is_directory: bool = True # Transferring a whole directory
    do_compression: bool = False # Whether to do compression before tranferring files
    do_decompression: bool = False # Whether to decompress the file after transferring files
    compression_config_path: Optional[Path] = Path("./sz_config.yaml")
    host_source: Optional[str] = "ac.yuanjian@bebop.lcrc.anl.gov"
    host_destination: Optional[str] = "yuanjian@cori.nersc.gov"

class CompressionConfig(BaseSettings):
    data_path: Optional[Path] = "/home/data_path"
    work_dir: Optional[Path] = "/home/szworkdir"
    metrics_path: Optional[Path] = "/home/metrics"
    compressor_id: Optional[str] = "sz"
    file_extension: Optional[str] = ".f32"
    dimension: Optional[List[int]] = None
    abs_error_bound: Optional[float] = 1e-3
    


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c", "--config", help="YAML config file", type=str, required=True
    )
    args = parser.parse_args()
    return args