import re
import pathlib
from typing import Any


def is_cloud_url(path: str) -> bool:
    return re.match("^s3:\/\/|^az:\/\/|^gs:\/\/", path) is not None


def get_pathlib(path: str) -> Any:
    if is_cloud_url(path):
        import cloudpathlib
        return cloudpathlib.CloudPath(path)
    return pathlib.Path(path)
