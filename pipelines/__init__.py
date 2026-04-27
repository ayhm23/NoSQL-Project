"""
pipelines/__init__.py
"""
from pipelines.base_pipeline  import BasePipeline
from pipelines.mongo_pipeline import MongoPipeline

__all__ = ["BasePipeline", "MongoPipeline"]
