from abc import ABC, abstractmethod


class ClusterRepository(ABC):
    @abstractmethod
    def health(self) -> dict:
        ...
